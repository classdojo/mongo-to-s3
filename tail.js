var Readable = require("stream").Readable;
var Tail     = require("always-tail");

module.exports = function(file) {
  var tail = new Tail(file);
  tail.on('line', tail.emit.bind(tail, "data"));
  return new Readable().wrap(tail);
}
