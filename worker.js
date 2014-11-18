var fs = require("fs");
var initialized = false;
var Tail        = require("./tail");

process.on("message", function(pipelineFiles) {
  if(!initialized) {
    var from, through, to;
    from = Tail(pipelineFiles.from);
    through = require(pipelineFiles.through);
    to = fs.createWriteStream(pipelineFiles.to, {flags: "a"});
    from
      .pipe(through)
      .pipe(to);
    initialized = true;
  }
});
