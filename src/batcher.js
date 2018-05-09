class Batcher {
  constructor(batchSize, callback, timeout = 1000) {
    this.batchSize = batchSize;
    this.callback = callback;
    this.timeout = timeout;
    this.queue = [];

    this.workerClock = 0;
  }

  push(element, overwriteClock = true) {
    if (overwriteClock) {
      element.key.writeUInt32BE(this.workerClock, 0);
      this.workerClock += 1;
    }

    this.queue.push(element);

    if (this.queue.length >= this.batchSize) {
      this.flush();
    } else if (!this.timer) {
      this.timer = setTimeout(() => {
        delete this.timer;
        this.callback(this.queue.splice(0));
      }, this.timeout);
    }
  }

  flush() {
    if (this.timer) {
      clearTimeout(this.timer);
      delete this.timer;
    }
    return this.callback(this.queue.splice(0));
  }
}

module.exports = Batcher;
