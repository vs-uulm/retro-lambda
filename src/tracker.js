module.exports = class Tracker {
  constructor(obj, mutable = false) {
    const dependencies = new Set();
    const proxies = new Map();

    const immutable = (target, property) => {
      throw new ReferenceError(`Property "${property}" is not writable.`);
    };

    const proxify = (o, path = '') => {
      if (!proxies.has(o)) {
        const handler = {
          get: (target, property) => {
            if (property in target) {
              if (typeof property === 'symbol') {
                return Reflect.get(target, property);
              }

              if (typeof target[property] === 'object') {
                return proxify(target[property], `${path}/${property}`);
              }

              // console.log('read:', path + '/' + property, typeof target[property]);
              dependencies.add(`${path}/${property}`);

              return target[property];
            }

            return undefined;
          }
        };

        if (mutable === false) {
          handler.set = immutable;
          handler.deleteProperty = immutable;
          handler.defineProperty = immutable;
        }

        proxies.set(o, Proxy.revocable(o, handler));
      }

      return proxies.get(o).proxy;
    };

    this.proxy = proxify(obj);

    this.generate = () => {
      const deps = Array.from(dependencies);
      dependencies.clear();
      return deps;
    };

    this.delete = () => {
      proxies.forEach(proxy => proxy.revoke());
    };
  }
};
