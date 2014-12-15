var ChildProcess = require("child_process");
var Readable     = require("stream").Readable;
var inherits     = require("util").inherits;
var fs           = require("fs");
var async        = require("async");
var FsTail       = require("fs-tail");

module.exports = function(file) {
  var tail = new Tail(file);
  tail.init();
  return tail;
};

function Tail(file) {
  this.__file = file;
}

Tail.prototype.init = function() {
  this.stream = FsTail(this.__file);
};

Tail.prototype.waitForEof = function(cb) {
  this.stream.once("EOF", cb);
};
