var low = require('lowdb')
var FileSync = require('lowdb/adapters/FileSync')
var uuid = require('uuid');

function JsonStore(opts) {
  opts = opts || {};
  var self = this;
  self._tableName = opts.tableName || 'tasks';
}

JsonStore.prototype.connect = function (cb) {
  var self = this;

  const adapter = new FileSync('db.json');
  self._db = low(adapter);
  self._db.defaults({ [self._tableName]: [], count: 0 }).write();
  self._db._.mixin({
    upsert: function(collection, obj, key) {
      key = key || 'id';
      for (var i = 0; i < collection.length; i++) {
        var el = collection[i];
        if(el[key] === obj[key]){
          collection[i] = obj;
          return collection;
        }
      };
      collection.push(obj);
    }
  });
  self._db.read()
  return cb(null, self._db.get(self._tableName).filter({ lock: '' }).size().value());
};

JsonStore.prototype.getTask = function (id, cb) {
  console.log('get id', id)
  var self = this;
  var row = self._db.get(self._tableName).find({ id, lock: '' }).value();
  if(row) {
    return cb(null, row.task);
  }
  return cb();
};


JsonStore.prototype.deleteTask = function (id, cb) {
  var self = this;
  self._db.get(self._tableName).remove({ id }).write();
  cb();
};


JsonStore.prototype.putTask = function (id, task, priority, cb) {
  console.log('put id', id)
  var self = this;
  var added = self._db.get('count').value() + 1;
  var row = self._db.get(self._tableName).find({ id }).value();
  // if (row) {
  //   self._db.get(self._tableName).find({ id }).assign({ task, priority, lock: '', added }).write();
  // } else {
    self._db.get(self._tableName).upsert({ id, task, priority, lock: '', added }).write();
  // }

  // Increment count
  self._db.update('count', n => n + 1).write();

  cb();
};


JsonStore.prototype.getLock = function (lockId, cb) {
  var self = this;

  var rows = self._db.get(self._tableName).filter({ lock: (lockId || '') }).value();
  var tasks = {};

  rows.forEach((row) => {
    tasks[row.id] = row.task;
  })

  cb(null, tasks);
};


JsonStore.prototype.getRunningTasks = function (cb) {
  var self = this;

  var rows = self._db.get(self._tableName).filter(function (r) { return r.lock !== '' }).value();
  var tasks = {};
  rows.forEach(function (row) {
    if (!row.lock) return;
    tasks[row.lock] = tasks[row.lock] || [];
    tasks[row.lock][row.id] = row.task;
  })
  cb(null, tasks);
};

JsonStore.prototype.releaseLock = function (lockId, cb) {
  var self = this;
  self._db.get(self._tableName).remove({ lock: lockId }).write();
  cb()
};


JsonStore.prototype.takeFirstN = function (num, cb) {
  var self = this;
  var lockId = uuid.v4();

  var ids = self._db.get(self._tableName).filter({ lock: '' }).orderBy(['priority', 'added'], ['desc', 'asc']).take(num).value().map((r) => r.id);
  ids.forEach(id => {
    self._db.get(self._tableName).find({ id }).assign({ lock: lockId }).write();
  });

  console.log('takeFirstN ids', ids)

  cb(null, ids.length > 0 ? lockId : '');
}

JsonStore.prototype.takeLastN = function (num, cb) {
  var self = this;
  var lockId = uuid.v4();

  var ids = self._db.get(self._tableName).filter({ lock: '' }).orderBy(['priority', 'added'], ['desc', 'desc']).take(num).value().map((r) => r.id);
  ids.forEach(id => {
    self._db.get(self._tableName).find({ id }).assign({ lock: lockId }).write();
  });

  console.log('takeLastN ids', ids)

  cb(null, ids.length > 0 ? lockId : '');
}

module.exports = JsonStore;
