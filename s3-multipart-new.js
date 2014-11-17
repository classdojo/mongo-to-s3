/*
 * Simple implementation of a write stream
 * to aws using multipart upload.
*/
var Writable       = require("stream").Writable;
var inherits       = require("util").inherits;
var chain          = require("slide").chain;
var _              = require("lodash");
var EventEmitter   = require("events").EventEmitter;


var uploaderDebug  = require("debug")("uploader");
var uploadDebug    = require("debug")("upload");
var multiDebug     = require("debug")("multi");

var MINIMUM_CHUNK_UPLOAD_SIZE = 5242880
var PARALLEL_UPLOADS = 5;

// function MultipartWriteS3(s3Client) {
//   this.__s3Client = s3Client;
//   this.__uploads = [];
// }

/*
 *  
    @param multipartCreationParams -
    {
      Bucket: 'STRING_VALUE',
      Key: 'STRING_VALUE',
      ACL: 'private | public-read | public-read-write | authenticated-read | bucket-owner-read | bucket-owner-full-control',
      CacheControl: 'STRING_VALUE',
      ContentDisposition: 'STRING_VALUE',
      ContentEncoding: 'STRING_VALUE',
      ContentLanguage: 'STRING_VALUE',
      ContentType: 'STRING_VALUE',
      Expires: new Date || 'Wed Dec 31 1969 16:00:00 GMT-0800 (PST)' || 123456789,
      GrantFullControl: 'STRING_VALUE',
      GrantRead: 'STRING_VALUE',
      GrantReadACP: 'STRING_VALUE',
      GrantWriteACP: 'STRING_VALUE',
      Metadata: {
        someKey: 'STRING_VALUE',
        //another key
      },
      SSECustomerAlgorithm: 'STRING_VALUE',
      SSECustomerKey: 'STRING_VALUE',
      SSECustomerKeyMD5: 'STRING_VALUE',
      ServerSideEncryption: 'AES256',
      StorageClass: 'STANDARD | REDUCED_REDUNDANCY',
      WebsiteRedirectLocation: 'STRING_VALUE'
    };
 * 
*/
// MultipartWriteS3.prototype.createUpload = function(chunkUploadSize, multipartCreationParams, cb) {

//   MultipartWriteS3Upload.create(this.__s3Client, chunkUploadSize, multipartCreationParams, cb);

// };

inherits(MultipartWriteS3Upload, Writable);
function MultipartWriteS3Upload(s3Client, chunkUploadSize) {
  Writable.call(this);
  this.__s3Client = s3Client;
  this.__uploadCounter = 1;
  this.__chunks = [];
  this.__uploadedParts = [];
  this.__queuedUploadSize = 0;
  this.__uploadsInProgress = 0;
  this.waitingUploads = [];
  this.__uploader = new Uploader(this.waitingUploads);
  this.__chunkUploadSize = chunkUploadSize < MINIMUM_CHUNK_UPLOAD_SIZE ?
                                MINIMUM_CHUNK_UPLOAD_SIZE : chunkUploadSize;

}

MultipartWriteS3Upload.create = function(s3Client, chunkUploadSize, multipartCreationParams, cb) {
  chunkUploadSize = chunkUploadSize || MINIMUM_CHUNK_UPLOAD_SIZE;
  var myS3Upload = new this(s3Client, chunkUploadSize);
  /* */
  s3Client.createMultipartUpload(multipartCreationParams, function(err, s3MultipartUploadConfig) {
    if(err) {
      return cb(err);
    }
    myS3Upload.__s3MultipartUploadConfig = s3MultipartUploadConfig;
    MultipartWriteS3Upload._addFinishHandler(myS3Upload);

    //find better place for uploader start
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
}


/* Initial naive implementation of writing each chunk serially to s3.
 * Only accepts Buffer|String right now.
*/

MultipartWriteS3Upload.prototype._write = function(chunk, enc, cb) {
  var me = this;
  if(this._shouldUploadChunks(chunk)) {
    this._queueChunksForUpload();
  }
  cb();
};


MultipartWriteS3Upload.prototype._queueChunksForUpload = function(cb) {
  var chunks = this.__chunks;
  this.__chunks = [];
  this.waitingUploads.push(this._uploadChunks.bind(this, this.__uploadCounter++, chunks));
  cb && cb();
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
    UploadId         : this.__s3MultipartUploadConfig.UploadId,
    Bucket           : this.__s3MultipartUploadConfig.Bucket,
    Key              : this.__s3MultipartUploadConfig.Key,
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
  var uploadId = this.__s3MultipartUploadConfig.UploadId;
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
      Bucket      : this.__s3MultipartUploadConfig.Bucket, 
      Key         : this.__s3MultipartUploadConfig.Key
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


MultipartWriteS3Upload.prototype._shouldUploadChunks = function(chunk) {
  this.__chunks.push(chunk);
  if(!chunk.length) {
    return true;
  }

  this.__queuedUploadSize = this.__queuedUploadSize + _.size(chunk);
  if(this.__queuedUploadSize > this.__chunkUploadSize) {
    this.__queuedUploadSize = 0;
    return true;
  }
  return false;
};

inherits(Uploader, EventEmitter);
function Uploader(waitingUploads) {
  this.__waitingUploads = waitingUploads;
  this.__outstandingUploads = [];
  this.__failedUploads = [];
};

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
  this._cleanupOutstandingJobs();
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

Uploader.prototype._cleanupOutstandingJobs = function() {
  var upload, i;
  for(i in this.__outstandingUploads) {
    upload = this.__outstandingUploads[i];
    if(/failed|success/.test(upload.status)) {
      if(upload.status === "failed") {
        this.__failedUploads.push(upload);
      }
      this.__outstandingUploads[i] = null;
    }
  }
  this.__outstandingUploads = _.compact(this.__outstandingUploads);
};

function UploadJob(partial) {
  this.__partial = partial;
  this.status = "waiting";
}

UploadJob.prototype.start = function() {
  var me = this;
  this.status = "inProgress";
  uploadDebug("Start");
  this.__partial(function(err, result) {
    if(err) {
      me.status = "failed";
      me.error = err;
    } else {
      me.status = "success";
      me.result = result;
    }
    uploadDebug("Result " + me.status);
  });
};

UploadJob.prototype.serialize = function() {
  if(this.status == "failed") {}
}

module.exports = MultipartWriteS3Upload;
