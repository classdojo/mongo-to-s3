/*
 * Simple implementation of a write stream
 * to aws using multipart upload.
*/
var Writable       = require("stream").Writable;
var inherits       = require("util").inherits;
var chain          = require("slide").chain;
var _              = require("lodash");
var EventEmitter   = require("events").EventEmitter;
var fs             = require("fs");
var uuid           = require("node-uuid");

var uploaderDebug  = require("debug")("uploader");
var uploadDebug    = require("debug")("upload");
var multiDebug     = require("debug")("multi");

var MINIMUM_CHUNK_UPLOAD_SIZE = 5242880;
var PARALLEL_UPLOADS = 5;

// function MultipartWriteS3(s3Client) {
//   this.__s3Client = s3Client;
//   this.__uploads = [];
// }


/*
 *  @param options
 *          options.chunkUploadSize
 *          options.workingDirectory
 *
*/
inherits(MultipartWriteS3Upload, Writable);
function MultipartWriteS3Upload(s3Client, options) {
  Writable.call(this);
  this.__s3Client = s3Client;
  this.__partNumber = 1;
  this.__chunks = [];
  this.__uploadedParts = [];
  this.__queuedUploadSize = 0;
  this.__uploadsInProgress = 0;
  this.waitingUploads = [];
  this.__uploader = new Uploader(this);
  this.__chunkUploadSize = _.isNaN(options.chunkUploadSize) || options.chunkUploadSize < MINIMUM_CHUNK_UPLOAD_SIZE ?
                                MINIMUM_CHUNK_UPLOAD_SIZE : options.chunkUploadSize;

}


/*
 *
 * @param s3Client - fully configured aws-sdk instance
 * @param options
 *          options.chunkUploadSize
 *          options.multipartCreationParams -     {
 *               Bucket: 'STRING_VALUE',
 *               Key: 'STRING_VALUE',
 *               ACL: 'private | public-read | public-read-write | authenticated-read | bucket-owner-read | bucket-owner-full-control',
 *               CacheControl: 'STRING_VALUE',
 *               ContentDisposition: 'STRING_VALUE',
 *               ContentEncoding: 'STRING_VALUE',
 *               ContentLanguage: 'STRING_VALUE',
 *               ContentType: 'STRING_VALUE',
 *               Expires: new Date || 'Wed Dec 31 1969 16:00:00 GMT-0800 (PST)' || 123456789,
 *               GrantFullControl: 'STRING_VALUE',
 *               GrantRead: 'STRING_VALUE',
 *               GrantReadACP: 'STRING_VALUE',
 *               GrantWriteACP: 'STRING_VALUE',
 *               Metadata: {
 *                 someKey: 'STRING_VALUE',
 *                 //another key
 *               },
 *               SSECustomerAlgorithm: 'STRING_VALUE',
 *               SSECustomerKey: 'STRING_VALUE',
 *               SSECustomerKeyMD5: 'STRING_VALUE',
 *               ServerSideEncryption: 'AES256',
 *               StorageClass: 'STANDARD | REDUCED_REDUNDANCY',
 *               WebsiteRedirectLocation: 'STRING_VALUE'
 *             };
 *          options.workingDirectory
 *
 *
 *
*/
MultipartWriteS3Upload.create = function(s3Client, options, cb) {
  var chunkUploadSize = options.chunkUploadSize || MINIMUM_CHUNK_UPLOAD_SIZE;
  var myS3Upload = new this(s3Client, chunkUploadSize);
  /* */
  s3Client.createMultipartUpload(options.multipartCreationParams, function(err, s3MultipartUploadConfig) {
    if(err) {
      return cb(err);
    }
    myS3Upload.s3MultipartUploadConfig = s3MultipartUploadConfig;
    MultipartWriteS3Upload._addFinishHandler(myS3Upload, options.workingDirectory);
    myS3Upload.__uploader.start();
    cb(null, myS3Upload);
  });
};

MultipartWriteS3Upload._addFinishHandler = function(multipartWriteS3Upload) {
  /* adds finish logic */
  multipartWriteS3Upload.on("finish", function() {
    multipartWriteS3Upload.finishUpload(function(err) {
      if(err) {
        return multipartWriteS3Upload.emit("error", err);
      }
      multipartWriteS3Upload.emit("done");
    });
  });
};


/* Initial naive implementation of writing each chunk serially to s3.
 * Only accepts Buffer|String right now.
*/

MultipartWriteS3Upload.prototype._write = function(chunk, enc, cb) {
  var me = this;
  this.__chunks.push(chunk);
  this.__queuedUploadSize = this.__queuedUploadSize + _.size(chunk);
  if(this.__queuedUploadSize > this.__chunkUploadSize) {
    this._queueChunksForUpload();
  }
  cb();
};


MultipartWriteS3Upload.prototype._queueChunksForUpload = function(cb) {
  var partNumber = this.__partNumber++;
  /* 
     Unpack data from the partial function so the uploader can handle
     error scenario logging.
  */
  this.waitingUploads.push({
    chunks: this.__chunks,
    chunkSize: this.__queuedUploadSize,
    partNumber: partNumber,
    uploadFn: this._uploadChunks.bind(this, partNumber, this.__chunks)
  });

  //reset working sets
  this.__chunks = [];
  this.__queuedUploadSize = 0;
  if(cb) {
    cb();
  }
};

/* Set optional upload params that will override the defaults */
MultipartWriteS3Upload.prototype.setUploadParams = function(uploadParams) {
  this.__uploadParams = uploadParams;
};

MultipartWriteS3Upload.prototype.finishUpload = function(cb) {
  chain([
    !_.isEmpty(this.__chunks) && [this._queueChunksForUpload.bind(this)],
    [this._completeMultipartUpload.bind(this)]
  ], cb);
};

MultipartWriteS3Upload.prototype._completeMultipartUpload = function(cb) {
  var me = this;
  var completeConfig = {
    UploadId         : this.s3MultipartUploadConfig.UploadId,
    Bucket           : this.s3MultipartUploadConfig.Bucket,
    Key              : this.s3MultipartUploadConfig.Key,
    MultipartUpload  : {
      Parts: this.__uploadedParts
    }
  };
  multiDebug("Waiting for uploader to finish");
  this.__uploader.once("empty", function() {
    multiDebug("Completing upload transfer");
    me.__uploader.stop();
    me.__s3Client.completeMultipartUpload(completeConfig, cb);
  });
};

/*
 * TODO: Add retries.
*/
MultipartWriteS3Upload.prototype._uploadChunks = function(partNumber, chunks, cb) {
  var body;
  var me = this;
  var uploadId = this.s3MultipartUploadConfig.UploadId;
  if(_.isString(chunks[0])) {
    body = chunks.join("");
  } else {
    //chunks are buffer objects
    body = Buffer.concat(chunks);
  }
  var uploadPayload = _.merge(
    this.__uploadParams || {},
    {
      UploadId    : uploadId,
      Body        : body,
      PartNumber  : partNumber,
      Bucket      : this.s3MultipartUploadConfig.Bucket, 
      Key         : this.s3MultipartUploadConfig.Key
    }
  );
  this.__s3Client.uploadPart(uploadPayload, function(err, uploadResponse) {
    if(err) {
      multiDebug("Error uploading chunks " + err);
      return cb(err);
    }
    me.__uploadedParts.push({
      ETag: uploadResponse.ETag,
      PartNumber: partNumber
    });
    multiDebug("Uploaded chunk " + partNumber);
    cb(null, uploadResponse);
  });
};

inherits(Uploader, EventEmitter);
function Uploader(s3Multipart, workingDirectory) {
  this.__waitingUploads     = s3Multipart.waitingUploads;
  this.__workingDirectory   = workingDirectory;
  this.__id                 = uuid.v1();
  this.__journalFile        = workingDirectory + "/upload-job-" + this.__id + ".json";
  this.__journal            = {
    uploadConfig: s3Multipart.s3MultipartUploadConfig,
    segments: {
      success: [],
      failed: []
    }
  };
  this.__outstandingUploads = [];
  this.__failedUploads      = [];
}

Uploader.prototype.start = function() {
  uploaderDebug("Starting");
  this.__i = setInterval(this.serviceUploads.bind(this), 1000);
};

Uploader.prototype.stop = function() {
  uploaderDebug("Stopping");
  clearInterval(this.__i);
};

Uploader.prototype.serviceUploads = function() {
  var upload;
  //cleanup outstandingUploads array
  this._cleanupAndLogFinishedJobs();
  while(this.__outstandingUploads.length < PARALLEL_UPLOADS && !_.isEmpty(this.__waitingUploads)) {
    uploaderDebug("Initiating upload");
    upload = new UploadJob(this.__waitingUploads.shift());
    upload.start();
    this.__outstandingUploads.push(upload);
  }
  if(_.isEmpty(this.__waitingUploads) && _.isEmpty(this.__outstandingUploads)) {
    uploaderDebug("Empty");
    this.emit("empty");
  }
};

Uploader.prototype._cleanupAndLogFinishedJobs = function() {
  var upload, i, compacted;
  for(i in this.__outstandingUploads) {
    upload = this.__outstandingUploads[i];
    if(/failed|success/.test(upload.status)) {
      this.__journal.segments[upload.status].push(upload.serialize());
      this.__outstandingUploads[i] = null;
    }
  }
  compacted = _.compact(this.__outstandingUploads);
  if(this.__outstandingUploads !== compacted.length) {
    this.commitJournal();
  }
  this.__outstandingUploads = compacted;
};

/* Only allow at most one outstanding commit to fs */
Uploader.prototype.commitJournal = function() {
  var me = this;
  if(!this.__outstandingJournal) {
    this.__outstandingJournal = true;
    fs.writeFile(this.__journalFile, JSON.stringify(this.__journal), function(err) {
      if(err) {
        //file error
        return me.emit("journalError", err);
      }
      me.__outstandingJournal = false;
    });
  }
};

function UploadJob(config) {
  this.config = config;
  this.status = "waiting";
}

UploadJob.prototype.start = function() {
  var me = this;
  this.status = "inProgress";
  uploadDebug("Start");
  this.config.uploadFn(function(err, uploadResponse) {
    if(err) {
      me.status = "failed";
      me.error = err;
      //flush success data
    } else {
      me.status = "success";
      me.uploadResponse = uploadResponse;
    }
    uploadDebug("Result " + me.status);
  });
};

UploadJob.prototype.serialize = function() {
  var encodedData = [],
  serialized = {
    partNumber: this.config.partNumber,
  },
  chunk;
  if(this.status === "failed") {
    //encode data
    for(var i = 0; i < this.config.chunks.length; i++) {
      chunk = this.config.chunks[i];
      if(_.isString(chunk)) {
        //String. let's transform string into char val array
        encodedData = encodedData.concat(_.map(chunk, function(e) {
          return e.charCodeAt(0);
        }));
      } else {
        //Buffer
        encodedData = encodedData.concat(chunk.toJSON());
      }
    }
    serialized.error = this.error.message;
    serialized.data  = encodedData;
    return serialized;
  } else if(this.status === "success") {
    serialized.ETag = this.uploadResponse.ETag;
    return serialized;
  } else {
    serialized.status = this.status;
    return serialized;
  }
};

module.exports = MultipartWriteS3Upload;

//export for testing
module.exports.__Uploader = Uploader;
module.exports.__UploadJob = UploadJob;
