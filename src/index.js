const fs = require('fs');
const path = require('path');
const uuid = require('uuid/v4');
const crypto = require('crypto');
const uuidParse = require('uuid-parse');
const level = require('level-packager')(require('rocksdb'));

const { fork } = require('child_process');

const JSONPatch = require('fast-json-patch');

const Batcher = require('./batcher');
const Tracker = require('./tracker');

const dbPath = './db-';
const branchIdentifier = (process.argv[1] === __filename) ? 'branch' : 'master';

const debug = require('debug')(`rL:${branchIdentifier}`);

const db = level(dbPath + branchIdentifier, {
  keyEncoding: 'binary',
  valueEncoding: 'json'
});

function formatKey(key) {
  return key.toString('hex').replace(/(.{8})(.{2}).{28}(.{2})(.{8})(.*)/, '$1 $2..$3 $4 $5');
}

function dbgPad(obj, length) {
  return JSON.stringify(obj, null, 2).split('\n').join(`\n${' '.repeat(length)}| `);
}

function isArrayDifferent(oldCmdSet, newCmdSet) {
  return oldCmdSet.length !== newCmdSet.length || oldCmdSet.some((v, i) => v !== newCmdSet[i]);
}

function setContainsAny(elems, set) {
  return elems.some(e => set.has(e));
}

function serializeCommandHandlers(handlers) {
  return Object.keys(handlers).reduce((a, c) => {
    a[c] = handlers[c].toString();
    return a;
  }, {});
}

function deserializeCommandHandlers(handlers) {
  return Object.keys(handlers).reduce((a, c) => {
    // eslint-disable-next-line no-new-func
    a[c] = Function(`return ${handlers[c]}`)();
    return a;
  }, {});
}

const PRINT_DATABASE = false;
const DESTROY_DATABASE = true;

let functionIndex = 0;

function printDB(database, prefix) {
  return new Promise((resolve, reject) => {
    database.createReadStream()
      .on('data', (data) => {
        if (PRINT_DATABASE) {
          console.log(`[${prefix}]`, formatKey(data.key), JSON.stringify(data.value));
        }
      })
      .on('error', reject)
      .on('end', resolve);
  });
}

// clean up database before exiting
process.on('beforeExit', async () => {
  await printDB(db, dbPath + branchIdentifier);

  await db.close();

  if (DESTROY_DATABASE) {
    await new Promise(resolve => level.destroy(dbPath + branchIdentifier, resolve));
  }
  console.log('[done] database destroyed.');
});

const dbBatcher = new Batcher(100, ops => db.batch(ops));

// holds a byte representation for all aggregate keys
const aggregateKeyCache = new Map();
function getAggregateKey(id) {
  if (!aggregateKeyCache.has(id)) {
    aggregateKeyCache.set(id, crypto.createHash('md5').update(id).digest());
  }

  return aggregateKeyCache.get(id);
}

const aggregates = {};
function createAggregate(id, initialState, commandHandlers) {
  if (aggregates.hasOwnProperty(id)) {
    throw new Error(`Aggregate already exists "${id}"`);
  }

  const aggregate = {
    state: initialState,

    clock: 0,
    query: () => initialState
  };

  const observer = JSONPatch.observe(initialState);
  const tracker = new Tracker(initialState, true);

  aggregate.handleCommand = (command, ...args) => {
    let res;

    if (!commandHandlers.hasOwnProperty(command)) {
      res = {
        clock: aggregate.clock,
        aggregate: id,
        params: args,
        command,
        event: 'ERROR',
        update: [],
        dependencies: [],
        state: { reason: 'No such command' }
      };
    } else {
      const event = commandHandlers[command](tracker.proxy, ...args);
      res = {
        clock: aggregate.clock,
        aggregate: id,
        params: args,
        command,
        event,
        update: JSONPatch.generate(observer),
        dependencies: tracker.generate(),
        state: JSON.parse(JSON.stringify(initialState))
      };
    }

    aggregate.clock += 1;
    return res;
  };

  aggregates[id] = aggregate;
}

const functionStore = {
  retroactive: {},
  retrospective: {},
  update: {},
  view: {}
};
function createFunction(type, functionName, constructor) {
  functionStore[type][functionName] = [constructor];

  // command/event sourcing
  const key = Buffer.alloc(28);
  key.writeUInt32BE(functionIndex, 20);
  dbBatcher.push({
    key,
    type: 'put',
    value: {
      type,
      functionName,
      function: constructor.toString()
    }
  }, false);

  const actionListener = {
    then: (stepFunction) => {
      const stepKey = Buffer.from(key);
      stepKey.writeUInt32BE(functionStore[type][functionName].length, 24);

      dbBatcher.push({
        key: stepKey,
        type: 'put',
        value: { function: stepFunction.toString() }
      }, false);

      functionStore[type][functionName].push(stepFunction);
      return actionListener;
    }
  };

  functionIndex += 1;
  return actionListener;
}

function createRetrospectiveFunction(functionName, constructor) {
  createFunction('retrospective', functionName, constructor);
}

function invokeRetrospectiveFunction(funcName, clock, ...args) {
  const restoredAggregates = {};
  const queriedAggregates = new Map();

  const query = {
    clock,
    params: args,
    queryAggregate: aggregateId => new Promise((resolve, reject) => {
      if (!queriedAggregates.has(aggregateId)) {
        queriedAggregates.set(aggregateId, []);
        restoredAggregates[aggregateId] = {};
      }

      queriedAggregates.get(aggregateId).push({ resolve, reject });
    })
  };

  const promise = functionStore.retrospective[funcName][0](query);

  const firstKey = Buffer.alloc(28);
  firstKey.fill(255, 20);
  const targetKey = Buffer.alloc(28, 255);
  targetKey.writeUInt32BE(clock, 0);

  return new Promise((resolve, reject) => {
    dbBatcher.flush();
    db.createReadStream({ gt: firstKey, lte: targetKey })
      .on('data', (data) => {
        if (data.value.type !== 'command') {
          return;
        } else if (!queriedAggregates.has(data.value.aggregate)) {
          if (data.value.aggregate === ''
            && data.value.command === 'createAggregate'
            && queriedAggregates.has(data.value.params[0])) {
            // eslint-disable-next-line prefer-destructuring
            restoredAggregates[data.value.params[0]] = data.value.params[1];
          }

          return;
        }

        JSONPatch.applyPatch(restoredAggregates[data.value.aggregate], data.value.update);
      })
      .on('end', () => {
        queriedAggregates.forEach((v, k) => {
          if (restoredAggregates.hasOwnProperty(k)) {
            v.forEach(o => o.resolve(restoredAggregates[k]));
          } else {
            v.forEach(o => o.reject());
          }
        });
        resolve(promise);
      })
      .on('error', reject);
  });
}

function createRetroactiveFunction(functionName, constructor) {
  return createFunction('retroactive', functionName, constructor);
}

function invokeRetroactiveFunctions(functions, cb) {
  dbBatcher.flush();

  const configuration = {
    master: branchIdentifier,
    external: cb.toString(),
    forceReExecution: false,
    functions
  };

  // fork child process for branch
  fork(__filename, [JSON.stringify(configuration)]);
}

function createViewFunction(functionName, constructor) {
  createFunction('view', functionName, constructor);
}

function invokeViewFunction(funcName, ...args) {
  const query = {
    params: args,
    queryAggregate: aggregateId => new Promise((resolve, reject) => {
      if (!aggregates.hasOwnProperty(aggregateId)) {
        return reject(new Error('No such aggregate'));
      }

      return resolve(aggregates[aggregateId].query());
    })
  };
  return functionStore.view[funcName][0](query);
}

function executeActionStep(step, context) {
  debug('=> executeActionStep', step, context.actionName);

  const {
    action,
    actionId,
    actionName,
    trackers,
    commands,
    externals
  } = context;

  // reset list of invoked externals and commands
  externals.length = 0;
  commands.length = 0;

  // invoke next step
  functionStore.update[actionName][step](action);

  // reset result array for next step
  action.result = [];

  // persist action in action log
  const actionKey = Buffer.alloc(28);
  uuidParse.parse(actionId, actionKey, 4);
  actionKey.writeUInt32BE(step, 20);
  const actionEntry = {
    actionId,
    type: 'action',
    commands: JSON.parse(JSON.stringify(commands.slice().map((command) => {
      if (command[0] === '') {
        return [
          ...command.slice(0, 4),
          serializeCommandHandlers(command[4])
        ];
      }

      return command;
    }))),
    externals: JSON.parse(JSON.stringify(externals.slice()))
  };

  if (step === 0) {
    actionEntry.actionName = actionName;
    actionEntry.parameters = action.params;
  } else {
    actionEntry.dependencies = trackers.map(tracker => tracker.generate());
  }

  dbBatcher.push({ type: 'put', key: actionKey, value: actionEntry });
}

function executeCommand([aggregateId, command, ...args], commandIndex) {
  return new Promise(resolve => setImmediate(() => {
    let res;

    if (aggregateId === '') {
      const [id, initialState, commandHandler] = args;
      try {
        createAggregate(String(id), initialState, commandHandler);

        res = {
          clock: 0,
          aggregate: aggregateId,
          params: [
            id,
            JSON.parse(JSON.stringify(initialState)),
            serializeCommandHandlers(commandHandler)
          ],
          command,
          event: 'AGGREGATE_CREATED',
          update: [],
          dependencies: [],
          state: JSON.parse(JSON.stringify(initialState))
        };
      } catch (err) {
        res = {
          clock: 0,
          aggregate: aggregateId,
          params: args,
          command,
          event: 'ERROR',
          update: [],
          dependencies: [],
          state: { reason: 'Aggregate already exists' }
        };
      }
    } else if (aggregates.hasOwnProperty(aggregateId)) {
      res = aggregates[aggregateId].handleCommand(command, ...args);
    } else {
      res = {
        clock: 0,
        aggregate: aggregateId,
        params: args,
        command,
        event: 'ERROR',
        update: [],
        dependencies: [],
        state: { reason: 'No such aggregate' }
      };
    }

    // update write set
    res.update.forEach(update => this.aggregateWrites.add(aggregateId + update.path));

    // command/event sourcing
    const eventKey = Buffer.alloc(28);
    getAggregateKey(aggregateId).copy(eventKey, 4);
    eventKey.writeUInt32BE(res.clock, 20);
    res.type = 'command';
    res.actionId = this.actionId;
    res.actionStep = this.currentStep;
    res.commandIndex = commandIndex;
    dbBatcher.push({ type: 'put', key: eventKey, value: res });

    const result = { event: res.event, state: res.state };

    // command was executed
    debug(`[${aggregateId}] executed '${command}' -> ${JSON.stringify(result)}`);

    // pass result to next step
    resolve(result);
  }));
}

function createUpdateFunction(functionName, constructor) {
  return createFunction('update', functionName, constructor);
}

function executeUpdateFunction(actionName, cb, ...args) {
  return new Promise(async (resolve) => {
    const actionId = uuid();

    const context = {
      currentStep: 0,
      action: {
        result: [],
        events: [],
        params: args,

        external: (...params) => {
          context.externals.push(params);

          debug(`[${actionName}:${actionId}] invoking external: ${params}`);
          cb(...params);
        }
      },
      actionId,
      actionName,
      trackers: [],
      commands: [],
      externals: [],
      writeSet: new Set(),
      aggregateWrites: new Set()
    };

    const aggregateProxys = {};
    context.action.getAggregate = (aggregateId) => {
      if (!aggregateProxys.hasOwnProperty(aggregateId)) {
        aggregateProxys[aggregateId] = Proxy.revocable({}, {
          get: (t, cmd) => (...params) => context.commands.push([aggregateId, cmd, ...params])
        });
      }

      return aggregateProxys[aggregateId].proxy;
    };

    context.action.createAggregate = (id, initialState, commandHandler) => {
      context.action.getAggregate('').createAggregate(id, initialState, commandHandler);
    };

    for (; ; context.currentStep += 1) {
      // execute action step
      executeActionStep(context.currentStep, context);

      // check if the step resulted in new commands or if we reached the end of the function
      if (context.commands.length === 0) {
        debug('[function finished]', actionId);

        // revoke all aggregate proxies
        Object.keys(aggregateProxys).forEach((id) => {
          aggregateProxys[id].revoke();
          delete aggregateProxys[id];
        });

        return resolve(context);
      }

      // execute commands
      // eslint-disable-next-line no-await-in-loop
      context.action.result = await Promise.all(context.commands.map(executeCommand, context));

      // if all invoked commands were executed, continue with next step
      if (context.action.result.length === context.commands.length) {
        // wrap results in a dependency tracker
        const tracker = new Tracker(context.action.result);
        context.action.result = tracker.proxy;
        context.trackers.push(tracker);

        // keep history of results
        context.action.events.push(context.action.result);
      } else {
        console.log(context.action.result);
        console.log(context.commands);
        console.log('WHAT THE FUCK?!?!??!');
        process.exit(1);
      }
    }
  });
}

function invokeUpdateFunction(actionName, ...params) {
  return new Promise((resolve) => {
    executeUpdateFunction(actionName, (...res) => {
      if (res.length > 1) {
        resolve(res);
      } else {
        resolve(res[0]);
      }
    }, ...params);
  });
}

const retroLambda = {
  createViewFunction,
  invokeViewFunction,

  createUpdateFunction,
  invokeUpdateFunction,

  createRetrospectiveFunction,
  invokeRetrospectiveFunction,

  createRetroactiveFunction,
  invokeRetroactiveFunctions,
  invokeRetroactiveFunction: (func, clock, cb, ...params) =>
    invokeRetroactiveFunctions([[func, clock, ...params]], cb),

  getMetrics: async () => {
    await dbBatcher.flush();
    return fs.readdirSync(dbPath + branchIdentifier)
      .filter(file => file.endsWith('.sst'))
      .map(file => fs.statSync(path.join(dbPath + branchIdentifier, file)).size)
      .reduce((i, c) => i + c, 0);
  }
};

if (process.argv[1] === __filename) {
  const metrics = {
    functions: {
      skipped: 0,
      reExecuted: 0,
      finished: 0
    },
    commands: {
      applied: 0,
      reExecuted: 0
    },
    workerClock: 0,
    timers: []
  };

  // this process handles a branch
  const configuration = JSON.parse(process.argv[2]);

  const masterDb = level(dbPath + configuration.master, {
    readOnly: true,
    keyEncoding: 'binary',
    valueEncoding: 'json'
  });

  // eslint-disable-next-line no-new-func
  const external = Function(`return ${configuration.external}`)();

  debug(configuration);

  const restorePoints = configuration.functions.sort((a, b) => b[1] - a[1]);
  debug('restorePoints', restorePoints);

  // initialize empty write set to catch modified entities in the new timeline
  const writeSet = new Set();
  const contextMap = new Map();

  const readLogSection = (firstKey, lastKey) => {
    const section = [];
    return new Promise((resolve, reject) => {
      masterDb.createReadStream({ gte: firstKey, lte: lastKey, limit: 1000 })
        .on('data', data => section.push(data))
        .on('end', () => resolve(section))
        .on('error', reject);
    });
  };

  // used to restore function steps
  let steps = [];

  const nullKey = Buffer.alloc(20);
  let lastKey = Buffer.alloc(28);

  (async () => {
    /* eslint-disable no-await-in-loop */
    for (;;) {
      // find maximum possible target clock
      let targetClock = -1;
      let restorePoint = null;
      let refilling = true;
      const targetKey = Buffer.alloc(28, 255);

      if (restorePoints.length > 0) {
        restorePoint = restorePoints.pop();

        // eslint-disable-next-line prefer-destructuring
        targetClock = restorePoint[1];
        targetKey.writeUInt32BE(targetClock, 0);
      }

      debug('########################');
      debug('# replaying until %s #', String(targetClock).padEnd(4));
      debug('########################');

      // process previous actions/commands until the target state was restored
      const start = process.hrtime();
      const logSection = await readLogSection(lastKey, targetKey);

      while (logSection.length > 0) {
        const data = logSection.shift();
        metrics.workerClock = data.key.readUInt32BE(0);

        if (refilling && logSection.length < 500) {
          const lastEntry = logSection.pop();
          const buffer = await readLogSection(lastEntry.key, targetKey);
          logSection.push(...buffer);

          if (buffer.length === 1) {
            refilling = false;
          }
        }

        debug('# replaying:', formatKey(data.key));
        // debug(dbgPad(data.value, 2));

        if (nullKey.compare(data.key, 0, 20) === 0) {
          // this is a function definition, which we have to add to the function store

          if (data.key.readUInt32BE(24) === 0) {
            steps = [];

            if (data.value.type === 'retroactive') {
              data.value.type = 'update';
              data.value.functionName = `retroactive-${data.value.functionName}`;
            }

            functionStore[data.value.type][data.value.functionName] = steps;
          }

          // eslint-disable-next-line no-new-func
          steps.push(Function(`return ${data.value.function}`)());

          // copy entry to the event log of the branch
          dbBatcher.push({ type: 'put', key: data.key, value: data.value }, false);
        } else if (data.value.type === 'action') {
          const action = data.value;
          const step = data.key.readUInt32BE(20);

          if (!contextMap.has(action.actionId)) {
            const context = {
              action: {
                result: [],
                events: [],
                params: action.parameters,

                external: (...params) => {
                  // save list of invoked externals
                  contextMap.get(action.actionId).externals.push(params);
                },

                createAggregate: (id, initialState, commandHandler) => {
                  context.action.getAggregate('')
                    .createAggregate(id, initialState, commandHandler);
                },

                getAggregate: aggregateId => new Proxy({}, {
                  get: (target, command) => (...args) => {
                    console.log('ddbg->cmd', command);
                    // save invoked commands
                    contextMap.get(action.actionId).commands.push([
                      aggregateId,
                      command,
                      ...args
                    ]);
                  }
                })
              },
              actionId: action.actionId,
              actionName: action.actionName,
              currentStep: 0,
              trackers: [],
              commands: [],
              externals: [],
              externalsMap: [],
              originalResult: [],
              originalEvents: [],
              originalExternals: [],
              reExecuted: false,
              writeSet: new Set(),
              aggregateWrites: writeSet,
              commandHistory: []
            };
            contextMap.set(action.actionId, context);
          }
          const context = contextMap.get(action.actionId);
          context.currentStep = step;
          context.commandHistory.push([]);

          const eventDependencies = [];
          const aggregateDependencies = [];
          if (step > 0) {
            action.dependencies.forEach((stepDependencies, i) => {
              stepDependencies.forEach((dependency) => {
                dependency = dependency.split('/');

                dependency[0] = i;
                if (dependency[2] === 'event') {
                  eventDependencies.push(`${action.actionId}/${dependency.join('/')}`);
                  return;
                }

                const j = parseInt(dependency[1], 10);
                aggregateDependencies.push([context.commandHistory[i][j][0], ...dependency.slice(3)].join('/'));
              });
            });
          }

          // check whether action has to be re-executed or whether it can be skipped
          if (configuration.forceReExecution || (step > 0 && (
            setContainsAny(eventDependencies, context.writeSet)
            || setContainsAny(aggregateDependencies, writeSet)
          ))) {
            // action has to be re-executed
            context.reExecuted = true;
            console.log('CANNOT IGNORE ACTION!!! -> REEXECUTE');

            // copy result to events array in context
            if (step > 0) {
              // wrap results in a dependency tracker
              const tracker = new Tracker(context.action.result);
              context.action.result = tracker.proxy;
              context.trackers.push(tracker);

              context.currentStep = step;
              context.action.events.push(context.action.result);
              context.originalEvents.push(context.originalResult);
              context.originalResult = [];

              // keep list of external calls for every step
              context.externalsMap[step - 1] = context.externals.slice();
              if (!action.fake) {
                context.originalExternals.push(action.externals);
              }
            }

            // re-execute the step
            console.log('-> executing', context.actionName);
            await executeActionStep(step, context);
            console.log('~~~ original externals', JSON.stringify(action.externals));
            console.log('~~~   branch externals', JSON.stringify(context.externals));
            console.log('~~~');
            console.log('~~~  original commands', JSON.stringify(action.commands));
            console.log('~~~    branch commands', JSON.stringify(context.commands));

            // call external cb if the re-execution is finished
            if (context.commands.length === 0) {
              external(context.actionName, context.actionId, context.reExecuted, {
                externals: [...context.externalsMap, context.externals],
                events: context.action.events
              }, {
                externals: context.originalExternals,
                events: context.originalEvents
              });

              metrics.functions.finished += 1;

              // clean up state that is not longer needed
              contextMap.delete(action.actionId);
            }

            if (context.commands.length > action.commands.length) {
              // add fake entries to the read event log, to execute an additional step
              console.log('OLD CHAIN NOT LONG ENOUGH');

              const actionKey = Buffer.from(data.key);
              actionKey.writeUInt32BE(step + 1, 20);
              // fake action should depend on all fake events to ensure re-execution
              const dependencies = Array(step).fill(null).map(() => []);
              dependencies.push(context.commands.map((c, i) => `/${i}/event`));

              console.log('INJECTING FAKE ACTION EVENT');
              logSection.unshift({
                key: actionKey,
                value: {
                  type: 'action',
                  fake: true,
                  actionId: action.actionId,
                  commands: [],
                  externals: [],
                  dependencies
                }
              });

              context.commands.forEach((command, index) => {
                console.log('INJECTING FAKE EVENT');
                logSection.unshift({
                  key: data.key,
                  value: {
                    actionId: action.actionId,
                    fake: true,
                    type: 'command',
                    // clock: 3,
                    aggregate: '',
                    command: '',
                    params: [],
                    event: '',
                    update: [],
                    dependencies: [],
                    actionStep: step,
                    commandIndex: index
                  }
                });
              });
            }

            metrics.functions.reExecuted += 1;
          } else {
            // first step, can always be skipped but we have to reconstruct some action state
            if (action.dependencies !== undefined) {
              // keep list of external calls for every step
              context.externalsMap[step - 1] = context.externals.slice();

              // copy result to events array in context
              context.action.events.push(context.action.result);
              context.originalEvents.push(context.originalResult);
              context.originalResult = [];
              context.action.result = [];
            }

            // action can be skipped, however we have to reconstruct some state
            context.commands = JSON.parse(JSON.stringify(action.commands)).map((command) => {
              if (command[0] === '') {
                return [
                  ...command.slice(0, 4),
                  deserializeCommandHandlers(command[4])
                ];
              }

              return command;
            });
            context.externals = action.externals;
            context.originalExternals.push(action.externals.slice());

            // check this action is finished and we can collect some garbage
            if (action.dependencies !== undefined && action.commands.length === 0) {
              // this was the last step, so we can remove the action from the context map

              // call external after execution with context and without history
              external(context.actionName, context.actionId, context.reExecuted, {
                externals: [...context.externalsMap, context.externals],
                events: context.action.events
              }, {
                externals: context.originalExternals,
                events: context.originalEvents
              });

              console.log('finished skipped action');
              contextMap.delete(action.actionId);

              metrics.functions.finished += 1;
            }

            // copy entry to the event log of the branch
            dbBatcher.push({ type: 'put', key: data.key, value: data.value });

            metrics.functions.skipped += 1;
          }
        } else if (data.value.type === 'command') {
          const cmd = data.value;
          const context = contextMap.get(cmd.actionId);

          // check whether command has to be re-executed or whether it can be replayed
          const oldCmd = [cmd.aggregate, cmd.command, ...cmd.params];
          const newCmd = context.commands[cmd.commandIndex];
          context.commandHistory[context.currentStep][cmd.commandIndex] = newCmd;

          // store the original result for external decision making
          if (cmd.fake === undefined && cmd.aggregate !== '') {
            context.originalResult[cmd.commandIndex] = {
              event: cmd.event,
              state: cmd.state
            };
          }

          // restore the command handler map, if this is a create command
          if (cmd.aggregate === '') {
            cmd.params[2] = Object.keys(cmd.params[2]).reduce((h, k) => {
              // eslint-disable-next-line no-new-func
              h[k] = Function(`return ${cmd.params[2][k]}`)();
              return h;
            }, {});
          }

          // re-execute command if any of the following conditions is true
          if (configuration.forceReExecution
              // the command created an aggregate
              || cmd.aggregate === ''
              // in the new timeline a different command should be executed
              || isArrayDifferent(oldCmd, newCmd)
              // the command read any value that is included in the write set
              || setContainsAny(cmd.dependencies.map(dep => cmd.aggregate + dep), writeSet)
          ) {
            // re-execute command
            console.log('CANNOT REPLAY EVENT!!! -> REEXECUTE');

            console.log('-> handle command', ...newCmd);
            const res = await executeCommand.call(context, newCmd, cmd.commandIndex);
            console.log('~~~ original event', JSON.stringify([cmd.event, cmd.state]));
            console.log('~~~   branch event', JSON.stringify([res.event, res.state]));

            // if the new command resulted in a different command,
            // then add the changed event to the write set
            if (res.event !== cmd.event) {
              context.writeSet.add(`${cmd.actionId}/${cmd.actionStep}/${cmd.commandIndex}/event`);
            }

            // add writes of the original command to the write set as well,
            // as they were not written in this alternative timeline and have thus changed
            cmd.update.forEach((update) => {
              context.writeSet.add(`${cmd.actionId}/${cmd.actionStep}/${cmd.commandIndex}/state${update.path}`);
              writeSet.add(cmd.aggregate + update.path);
            });

            // pass result to next step
            context.action.result[cmd.commandIndex] = res;

            metrics.commands.reExecuted += 1;
          } else {
            // do not apply error events
            if (cmd.event !== 'ERROR') {
              // apply event
              aggregates[cmd.aggregate].state = JSONPatch.applyPatch(
                aggregates[cmd.aggregate].state,
                cmd.update
              ).newDocument;

              // cache result in case the next step has to be re-executed
              context.action.result[cmd.commandIndex] = {
                event: cmd.event,
                state: JSON.parse(JSON.stringify(aggregates[cmd.aggregate].state))
              };
            } else {
              // cache result in case the next step has to be re-executed
              context.action.result[cmd.commandIndex] = {
                event: cmd.event,
                state: cmd.state
              };
            }

            // copy entry to the event log of the branch
            dbBatcher.push({ type: 'put', key: data.key, value: data.value });
            metrics.commands.applied += 1;
          }
        }
      }
      metrics.timers.push(process.hrtime(start));

      lastKey = targetKey;
      // check if we are finished, or if we have to execute another retroactive action
      if (targetClock < 0) {
        break;
      }
      console.log('all commands until', targetClock, 'replayed!');

      // invoke retroactive action
      const functionName = `retroactive-${restorePoint[0]}`;
      const context = await executeUpdateFunction(
        functionName,
        () => {},
        ...restorePoint.slice(2)
      );

      // call external after execution with context and without history
      external(functionName, context.actionId, true, {
        externals: context.externals,
        events: context.action.events
      }, null);

      // update write set
      context.aggregateWrites.forEach(e => writeSet.add(e));
    }
    /* eslint-enable no-await-in-loop */
  })().then(() => {
    console.log('done replaying');
    metrics.time = metrics.timers.reduce((a, time) => a + (time[0] * 1e6) + (time[1] / 1e3), 0);
    console.error([
      metrics.workerClock,
      metrics.time,
      metrics.functions.skipped,
      metrics.functions.reExecuted,
      metrics.functions.finished,
      metrics.commands.applied,
      metrics.commands.reExecuted
    ].join(','));
  });
} else {
  // this process handles the master branch
  module.exports = retroLambda;
}
