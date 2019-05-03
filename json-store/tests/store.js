var test = require('better-queue-store-test');
var JsonStore = require('../jsonStore');

var store = new JsonStore()

test.basic('No options', {
  create: function (cb) {
    cb(null, store);
  },
  destroy: function (cb) { cb() } // Pass
});
