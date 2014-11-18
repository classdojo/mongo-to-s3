// var Readable = require("stream").Readable;
// var Tail     = require("always-tail");

// module.exports = function(file) {
//   var tail = new Tail(file);
//   tail.on('line', tail.emit.bind(tail,"data"));
//   return new Readable().wrap(tail);
// };

var ChildProcess = require("child_process");
var Readable     = require("stream").Readable;

module.exports = function(file) {
  var childProcess = ChildProcess.spawn("tail", ["-f", file]);
  return new Readable().wrap(childProcess.stdout);
};
