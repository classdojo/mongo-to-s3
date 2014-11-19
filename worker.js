var fs = require("fs");
var Tail        = require("./tail");
var JSONStream  = require("JSONStream");
var async       = require("async");
var ChildProcess = require("child_process")
var Readable = require("stream").Readable;

var initialized = false;
process.on("message", function(pipelineFiles) {
  if(!initialized) {
    var from, through, to;
    through = require(pipelineFiles.through);
    to = fs.createWriteStream(pipelineFiles.to, {flags: "a"});
    var err = new Error("File might not exist yet");
    from = ChildProcess.spawn("tail", ["-f", pipelineFiles.from]);
    var fromStream = new Readable().wrap(from.stdout);
    var parser = JSONStream.parse();
    fromStream
      .pipe(parser)
      .pipe(through)
      .pipe(to);
    initialized = true;
  }
});
