var childProcess = require("child_process");
var async        = require("async");
var chain        = require("slide").chain;
var fs           = require("fs");
var Readable     = require("stream").Readable;
var uuid         = require("node-uuid");
var exportsDebug = require("debug")("exports");
var inherits     = require("util").inherits;
var EventEmitter = require("events").EventEmitter;
var Transform    = require("stream").Transform;
var Tail         = require("./tail");
var _            = require("lodash");
var __           = require("highland");


/* Simple collection operations on MongoExport*/
inherits(MongoExports, Readable);
function MongoExports(mongoExports) {
  Readable.call(this);
  this.__mongoExports = mongoExports;
  for(var i in this.__mongoExports) {
    this.__mongoExports[i].on("close", this._closeListener.bind(this));
  };
  this.streams = __(this.__mongoExports.map(function(m) { return m.stream; }))
                    .merge();
  var t = new Transform();
  t._transform = function(a,b,c) {
    c();
  };
  // this.streams.pipe(t);
}

MongoExports.prototype._closeListener = function(mongoExport) {
  exportsDebug("MongoExport finished with code " + mongoExport.exitCode);
  var mongoexport;
  for(var i in this.__mongoExports) {
    mongoexport = this.__mongoExports[i];
    if(mongoexport.status !== "closed") {
      return;
    }
  }
  this.emit("end");
};

MongoExports.prototype.resume = function() {
  //resume each export and merge
  var exportStreams, combinedStreams;
  exportStreams = this.__mongoExports.map(function(exports) {
    return exports.resume();
  })
  return this;
};

MongoExports.prototype.pause = function() {
  this.__mongoExports.forEach(function(mongoExport) {
    mongoExport.pause();
  });
};

MongoExports.create = function(configs, cb) {
  async.map(configs, MongoExport.create.bind(MongoExport), function(err, mongoExportArr) {
    if(err) {
      return cb(err);
    }
    cb(null, new MongoExports(mongoExportArr));
  });
};

inherits(MongoExport, EventEmitter);
function MongoExport(config) {
  this.__config  = config;
  this.__id      = uuid.v1();
  this.status    = "uninitialized";
};

/* 
 * takes {exportOptions: "", workingDirectory: ""}
*/
MongoExport.create = function(config, cb) {

  var exportJob = new this(config);
  exportJob.init(function(err) {
    if(err) {
      return cb(err);
    }
    cb(null, exportJob);
  });
};


MongoExport.prototype.init = function(cb) {
  var me = this;
  this._createWorkingFile(function(err, workingFilePath) {
    if(err) {
      return cb(err);
    }
    me.stream = Tail(workingFilePath);
    me._spawnMongoExport();
    me.pause();
    cb();
  });
};

MongoExport.prototype._createWorkingFile = function(cb) {
  var me = this;
  this.__workingFile = this.__config.workingDirectory + "/mongoexport-" + this.__id;
  fs.open(this.__workingFile, "w+", function(err, fd) {
    if(err) {
      return cb(err);
    }
    me.__fd = fd;
    cb(null, me.__workingFile)
  });
};



MongoExport.prototype._spawnMongoExport = function() {
  var me = this;
  var options = this.__config.exportOptions + " -o " + this.__workingFile;
  this.__spawn = childProcess.spawn("mongoexport", options.split(" "));
  this.__spawn.on("close", function(exitCode) {
    me.exitCode = exitCode;
    me.status = "closed";
    me.emit("close", me);
  });
  return this.__spawn;
};

MongoExport.prototype.pause = function() {
  this.__spawn.kill("SIGSTOP");
  this.status = "paused";
};

MongoExport.prototype.resume = function() {
  this.__spawn.kill("SIGCONT");
  this.status = "running";
};


exports.MongoExports = MongoExports;
exports.MongoExport = MongoExport;
