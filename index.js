var S3            = require("aws-sdk").S3;
var Readable      = require("stream").Readable;
var childProcess  = require("child_process");
var Writable      = require("stream").Writable;
var _             = require("lodash");
var fs            = require("fs");
var inherits      = require("util").inherits;
var S3Multipart   = require("./s3-multipart");
var EventEmitter  = require("events").EventEmitter;


//debuggers
var mongoDebug    = require("debug")("mongo");
var s3Debug       = require("debug")("s3");
var systemDebug   = require("debug")("system");


inherits(MongoToS3Upload, EventEmitter);
function MongoToS3Upload(s3Client, workingFile) {
  this.__s3Client  = s3Client;
  this.__workingFile = workingFile;
};


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
 *
 *
*/
MongoToS3Upload.prototype.createS3Sink = function(options, cb) {
  S3Multipart.create(this.__s3Client, options.chunkUploadSize, options.s3, cb);
};

MongoToS3Upload.prototype.fromMongo = function(options) {
  this.__collectionOptions = options;
  var mongoExport = this._spawnMongoExport();
  var streamableMongoExport = new Readable().wrap(mongoExport.stdout);


  //does stdout emit end?
  mongoExport.on("close", function(code) {
    //
    if(!code) {
      //success
      streamableMongoExport.emit("end");
    } else {
      //failure
      streamableMongoExport.emit("error", new Error("mongoexport failed"));
    }
  });

  /*
   *  TODO: attach an error listener to mongoexport stdout that transforms output
   *        to an emitted error.
   */

  return streamableMongoExport;
};

MongoToS3Upload.prototype._spawnMongoExport = function(cb) {
  //construct the mongoexport string!
  var mongoExportOptions = this.__collectionOptions.exportOptions;
  /*
   * We need to remove the -o command if one is specified and then define our
   * own in the mongoExportCmd string.
  */
  mongoExportCmd = ensureNoOutputInString(mongoExportOptions);
  mongoDebug("Spawning mongoexport command: " + mongoExportOptions);
  return childProcess.spawn("mongoexport", mongoExportOptions.split(" "));
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


