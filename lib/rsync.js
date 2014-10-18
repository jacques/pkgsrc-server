'use strict';
var spawn = require('child_process').spawn;
var EventEmitter = require('events').EventEmitter;
var util = require('util');


var log = require('bunyan').createLogger({
  name: 'pkgsrc-repository',
  stream: process.stdout,
  level: 'info'
});

function Rsync(src, dst) {
  this.src = src;
  this.dst = dst;
}

util.inherits(Rsync, EventEmitter);

Rsync.prototype.run = function(done) {
  var me = this;

  if (!done) {
    done = noop;
  }

  if (!me.src || !me.dst) {
    return done(null);
  }

  var rsync = spawn('rsync', ['-irz', '--size-only', me.src, me.dst]);

  rsync.on('close', function (code) {
    if (code !== 0) {
      log.error({ "exitcode": code }, 'rsync exited with non-zero return value');
    }

    done(null);
  });
};

function noop() {};

module.exports = {
  "Rsync": Rsync
};
