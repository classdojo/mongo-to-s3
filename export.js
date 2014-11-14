var childProcess = require("child_process");
var async        = require("async");
var chain        = require("slide").chain;
var fs           = require("fs");
var Readable     = require("stream").Readable;
var uuid         = require("node-uuid");


function Exports(config) {
  this.__config = config;
  this.__id     = uuid.v1();
};

/* 
 * takes {exportOptions: "", workingDirectory: ""}
*/
Exports.createExportJob = function(config, cb) {

  var exportJob = new this(config);
  exportJob.init(function(err) {
    if(err) {
      return cb(err);
    }
    cb(null, exportJob);
  });
};


Exports.prototype.init = function(cb) {
  var me = this;
  //do we have a working directory? If not use in memory
  chain([
    this.__config.workingDirectory && [this.__createTempFile.bind(this)],
    [this.__spawnProcess.bind(this)]
  ], cb);
};


Exports.prototype.__spawnProcess = function(cb) {
  var spawn;
  var spawnOpts = {
    stdio: ['ignore', null, 2]
  };
  if(this.__fd) {
    spawnOpts.stdio = ['ignore', this.__fd, 2];
  }
  this.__spawn = childProcess.spawn("mongoexport", this.__config.exportOptions.split(" "), spawnOpts);
  this.pause();
  if(this.__fd) {
    stream = fs.createReadStream(this.__tempFile);
  } else {
    stream = new Readable().wrap(this.__spawn.stdout);
  }
  this.__stream = stream;
  //TODO: figure out proper error interface with file readstream and process stdout wrap
  cb();
};

Exports.prototype.__createTempFile = function(cb) {
  this.__workingFile = this.workingDirectory + "/mongoexport_worker_" + this.__id;
  fs.open(this.__workingFile, "w+", cb);
};

Exports.prototype.pause = function() {
  this.__spawn.kill("SIGSTOP");
  //remove all stream handlers on process
  this.__spawn.removeAllListeners("data");
  this.__spawn.removeAllListeners("error");
  this.__stream = null;
};

Exports.prototype.resume = function() {
  this.__spawn.kill("SIGCONT");
  this.__stream = new Readable().wrap(this.__spawn.stdout);
  return this.__stream;
};

module.exports = Exports;
