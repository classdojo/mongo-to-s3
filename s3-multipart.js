/*
 * Simple implementation of a write stream
 * to aws using multipart upload.
*/
var multiDebug = require("debug")("multi");
var Writable   = require("stream").Writable;
var inherits   = require("util").inherits;
var chain      = require("slide").chain;
var _          = require("lodash");


var MINIMUM_CHUNK_UPLOAD_SIZE = 5242880;


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
  if(this._shouldUploadChunks(chunk)) {
    var chunks = this.__chunks;
    this.__chunks = [];
    this._uploadChunks(this.__uploadCounter++, chunks, cb);
  } else {
    cb();
  }
};


/* Set optional upload params that will override the defaults */
MultipartWriteS3Upload.prototype.setUploadParams = function(uploadParams) {
  this.__uploadParams = uploadParams;
};

MultipartWriteS3Upload.prototype.finishUpload = function(cb) {
  multiDebug("Finishing upload");

  chain([
    !_.isEmpty(this.__chunks) && [this._uploadChunks.bind(this), this.__uploadCounter, this.__chunks], //upload last chunk
    [this._completeMultipartUpload.bind(this)]
  ], cb);
};

MultipartWriteS3Upload.prototype._completeMultipartUpload = function(cb) {
  //finish uploading remaining chunks
  var completeConfig = {
    UploadId         : this.__s3MultipartUploadConfig.UploadId,
    Bucket           : this.__s3MultipartUploadConfig.Bucket,
    Key              : this.__s3MultipartUploadConfig.Key,
    MultipartUpload  : {
      Parts: this.__uploadedParts
    }
  };
  multiDebug("_completeMultipartUpload");
  this.__s3Client.completeMultipartUpload(completeConfig, cb);
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

  this.__queuedUploadSize = this.__queuedUploadSize + _.size(chunk);
  if(this.__queuedUploadSize > this.__chunkUploadSize) {
    this.__queuedUploadSize = 0;
    return true;
  }
  return false;
};

module.exports = MultipartWriteS3Upload;
