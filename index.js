var S3              = require("aws-sdk").S3;
var Readable        = require("stream").Readable;
var childProcess    = require("child_process");
var Writable        = require("stream").Writable;
var _               = require("lodash");
var fs              = require("fs");
var inherits        = require("util").inherits;
var S3Multipart     = require("./s3-multipart");
var S3MultipartNew  = require("./s3-multipart-new");
var MongoExports    = require("./export").MongoExports;
var async           = require("async");
var __              = require("highland");
var uuid            = require("node-uuid");
var Tail            = require("./tail");
var EventEmitter    = require("events").EventEmitter;
var JSONParse       = require("./jsonparser");

//debuggers
var mongoDebug    = require("debug")("mongo");
var s3Debug       = require("debug")("s3");
var systemDebug   = require("debug")("system");
var workerDebug   = require("debug")("worker");

inherits(MongoToS3Upload, EventEmitter);
function MongoToS3Upload(s3Client, workingFile) {
  Readable.call(this);
  this.__s3Client  = s3Client;
  this.__workingFile = workingFile;
  this.__id = uuid.v1();
}

/*
 * @param options
 *   chunkUploadSize: Number of bytes to upload at a time. Minimum size is 5MB (DEFAULT = 5MB)
 *   s3: Any options accepted by the aws.S3#createMultipartUpload. Reference below:
 *
 *
 *         {
 *           Bucket: 'STRING_VALUE',
 *           Key: 'STRING_VALUE',
 *           ACL: 'private | public-read | public-read-write | authenticated-read | bucket-owner-read | bucket-owner-full-control',
 *           CacheControl: 'STRING_VALUE',
 *           ContentDisposition: 'STRING_VALUE',
 *           ContentEncoding: 'STRING_VALUE',
 *           ContentLanguage: 'STRING_VALUE',
 *           ContentType: 'STRING_VALUE',
 *           Expires: new Date || 'Wed Dec 31 1969 16:00:00 GMT-0800 (PST)' || 123456789,
 *           GrantFullControl: 'STRING_VALUE',
 *           GrantRead: 'STRING_VALUE',
 *           GrantReadACP: 'STRING_VALUE',
 *           GrantWriteACP: 'STRING_VALUE',
 *           Metadata: {
 *             someKey: 'STRING_VALUE',
 *             //another key
 *           },
 *           SSECustomerAlgorithm: 'STRING_VALUE',
 *           SSECustomerKey: 'STRING_VALUE',
 *           SSECustomerKeyMD5: 'STRING_VALUE',
 *           ServerSideEncryption: 'AES256',
 *           StorageClass: 'STANDARD | REDUCED_REDUNDANCY',
 *           WebsiteRedirectLocation: 'STRING_VALUE'
 *         };
 *   workingDirectory: ""
 *
*/
MongoToS3Upload.prototype.createS3Sink = function(options, cb) {
  var opts = {
    chunkUploadSize: options.chunkUploadSize,
    multipartCreationParams: options.s3,
    workingDirectory: options.workingDirectory
  };
  S3MultipartNew.create(this.__s3Client, opts, cb);
};

MongoToS3Upload.prototype.fromMongo = function(options, cb) {
  var me = this;
  if(!_.isArray(options)) {
    options = [options];
  }
  this.__collectionOptions = options;
  if(!cb) {
    this._prepareForWorkerMode();
    this._createMongoExports(options, function(err, mongoExports) {
      //create join workers
      if(err) {
        return me.emit("error", err);
      }
      me._createWorkerProcesses();
    });
    return this;
  } else {
    me._createMongoExports(options, cb);
  }
};

//returns a stream that represents
MongoToS3Upload.prototype.throughPipeline  = function(filePath) {
  //let's fork a worker processor for every copy of mongoExport
  this.__pipelineFilePath = filePath;
  return this.__joinTail.stream.pipe(JSONParse());
};

MongoToS3Upload.prototype._createWorkerProcesses = function() {
  //create a worker process per
  workerDebug("Creating worker...", this.__mongoExports.exports.length);
  this.__workers = [];
  for(var i = 0; i < this.__mongoExports.exports.length; i++) {
    this.__workers.push(childProcess.fork(__dirname + "/worker.js"));
    this.__workers[i].send({
      from: this.__mongoExports.exports[i].workingFile,
      through: this.__pipelineFilePath,
      to: this.__joinFile
    });
  }
};

/*
 Helper method for worker mode that proxies this
 call to an initialized MongoExports.

 Waits for initialization.
*/
MongoToS3Upload.prototype.resume = function() {
  var me = this;
  if(this.__mongoExports) {
    this.__mongoExports.resume();
  } else {
    this._waitForMongoExportsInit(function() {
      me.__mongoExports.resume();
    });
  }
};

MongoToS3Upload.prototype._waitForMongoExportsInit = function(cb) {
  var me = this;
  async.until(function(){
    return me.__mongoExports;
  }, setTimeout.bind(null, function(cb) {cb();}, 100),
  cb);
};

MongoToS3Upload.prototype._prepareForWorkerMode = function() {
  this.__workerMode = true;
  workerDebug("Preparing for worker mode");
  //Let's pop off the working directory from the first mongoexport job
  var workingDirectory = this.__collectionOptions[0].workingDirectory;
  this.__joinFile = workingDirectory + "/mongo-to-s3-join-" + this.__id;
  this.__joinfd = fs.openSync(this.__joinFile, "w+");
  this.__joinTail = Tail(this.__joinFile);
  return this;
};

MongoToS3Upload.prototype._createMongoExports = function(options, cb) {
  var me = this;
  MongoExports.create(options, function(err, mongoExports) {
    if(err) {
      return cb(err);
    }
    me.__mongoExports = mongoExports;
    cb(null, mongoExports);
  });
};

//helpers
var waitUntil = function(condition, cb) {
  systemDebug("Waiting until...");
  var i = setInterval(function() {
    if(condition()) {
      systemDebug("Continuing...");
      clearInterval(i);
      cb();
    }
  }, 10);
};

var ensureNoOutputInString = function(mongoExportOptions) {
  return mongoExportOptions;
};

module.exports = MongoToS3Upload;


