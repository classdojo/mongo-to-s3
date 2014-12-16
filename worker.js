var fs = require("fs");
var Tail        = require("./tail");
var JSONParse   = require("./jsonparser");
var async       = require("async");
var ChildProcess = require("child_process");
var Readable = require("stream").Readable;

var initialized = false;
process.on("message", function(pipelineFiles) {
  if(!initialized) {
    var from, through, to, parser, tail;
    through = require(pipelineFiles.through);
    to = fs.createWriteStream(pipelineFiles.to, {flags: "a"});
    var err = new Error("File might not exist yet");
    tail = Tail(pipelineFiles.from);
    parser = JSONParse();
    tail.stream
      .pipe(parser)
      .pipe(through)
      .pipe(to);
    initialized = true;
  }
});
