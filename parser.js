var Transform   = require("stream").Transform;
var _           = require("lodash");
var JSONStream  = require("JSONStream");
var Duplex      = require("stream").Duplex;
var Readable    = require("stream").Readable;

module.exports = function(opts) {
  var parser = JSONStream.parse();
  var stream = new Duplex();
  stream._write = function(chunk, enc, next) {
    console.log("--", chunk.toString());
    parser.write(chunk);
    next();
  };
  var jsonLines = [];
  parser.on("data", function(val) {
    jsonLines.push(val);
  });
  stream._read = function(size) {
    var me;
    if(jsonLines.length) {
      this.push(jsonLines.shift());
    } else {
      //let's listen for data event
      me = this;
      parser.once("data", function(){
        me.push(jsonLines.shift());
      });
    }
  };
  stream._readableState.objectMode = true;
  return stream;
};
