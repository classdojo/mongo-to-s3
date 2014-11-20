// var Readable = require("stream").Readable;
// var Tail     = require("always-tail");

// module.exports = function(file) {
//   var tail = new Tail(file);
//   tail.on('line', tail.emit.bind(tail,"data"));
//   return new Readable().wrap(tail);
// };

var ChildProcess = require("child_process");
var Readable     = require("stream").Readable;
var inherits     = require("util").inherits;
var fs           = require("fs");
var async        = require("async");

module.exports = function(file) {
  var childProcess = ChildProcess.spawn("tail", ["-f", file]);
  var tail = new Tail(file);
  tail.init();
  return tail;
};

function Tail(file) {
  this.__file = file;
}

Tail.prototype.init = function() {
  this.childProcess = ChildProcess.spawn("tail", ["-f", this.__file]);
  this.stream = new Readable().wrap(this.childProcess.stdout);
};

/* Define EOF as two successful file stats*/
Tail.prototype.waitForEof = function(cb) {
  var me = this;
  var atEOF = false;
  var previousStat;
  async.until(function() {
    return atEOF;
  }, function(cb) {
    fs.stat(me.__file, function(err, stat) {
      if(err) {
        return cb(err);
      }
      if(!previousStat) {
        previousStat = stat;
      } else {
        if(previousStat.size === stat.size) {
          atEOF = true;
        }
        previousStat = stat;
      }
      setTimeout(cb, 5);
    });
  }, cb);
};