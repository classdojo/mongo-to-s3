var expect = require("expect.js");
var sinon  = require("sinon");
var rewire = require("rewire");
var s3Multipart = rewire("../s3-multipart.js");
var MINIMUM_CHUNK_UPLOAD_SIZE = 25;
s3Multipart.__set__("MINIMUM_CHUNK_UPLOAD_SIZE", MINIMUM_CHUNK_UPLOAD_SIZE);





describe("s3-multipart", function(){
  var mockS3Client;
  var largeString;
  var s3;

  // creating a nice large string
  (function(){
    var str = "";
    for (var i = 0; i < (MINIMUM_CHUNK_UPLOAD_SIZE + 1) / 2; i++){
      str+="+";
    }
    largeString = str;
  })();

  beforeEach(function(){
    mockS3Client = sinon.stub({
      createMultipartUpload: function(){},
      completeMultipartUpload: function(){},
      uploadPart: function(){}
    });
    s3 = new s3Multipart(mockS3Client, 1);
    s3.__s3MultipartUploadConfig = {};
  });


  describe("#_shouldUploadChunks", function(){
    it("should update __queuedUploadSize, push to chunks, and return true if large enough, and false otherwise", function(){
      var shouldUpload;

      shouldUpload = s3._shouldUploadChunks(largeString);
      expect(shouldUpload).to.be(false);
      expect(s3.__queuedUploadSize).to.be(largeString.length);
      expect(s3.__chunks.length).to.be(1);

      shouldUpload = s3._shouldUploadChunks(largeString);
      expect(shouldUpload).to.be(true);
      expect(s3.__queuedUploadSize).to.be(0);
      expect(s3.__chunks.length).to.be(2);
    });
  });

  describe("#_write", function(){

    beforeEach(function(){
      sinon.spy(s3, "_uploadChunks");
    });

    it("only calls _uploadChunks when __chunks is large enough", function(){
      var cb = sinon.spy();
      s3._write(largeString, "utf8", cb);
      expect(cb.callCount).to.be(1);
      expect(s3._uploadChunks.callCount).to.be(0);

      s3._write("a" + largeString, "utf8", function(){});
      expect(s3._uploadChunks.callCount).to.be(1);
      expect(s3._uploadChunks.args[0][1][0]).to.be(largeString);
      expect(s3._uploadChunks.args[0][1][1]).to.be("a" + largeString);
    });

  });

  describe("#_uploadChunks", function(){
    it("passes a good body to uploadPart", function(){
      s3._write(largeString, "uf8", function(){});
      s3._write("foo", "uf8", function(){});
      s3._write(largeString, "uf8", function(){});

      expect(mockS3Client.uploadPart.callCount).to.be(1);
      var payload = mockS3Client.uploadPart.args[0][0];
      expect(payload.Body).to.be(largeString + "foo" + largeString);
      expect(payload.PartNumber).to.be(1);
      ["UploadId", "Bucket", "Key"].forEach(function(prop){
        expect(payload.hasOwnProperty(prop));
      });
    });

    it("updates PartNumber appropriately", function(){
      var i, payload, toUpload;
      toUpload = largeString + largeString;
      for (i = 0; i < 5; i++){
        s3._write(toUpload, "utf8", function(){});
      }
      expect(mockS3Client.uploadPart.callCount).to.be(5);
      for (i = 0; i < 5; i++){
        payload = mockS3Client.uploadPart.args[i][0];
        expect(payload.PartNumber).to.be(i + 1);
      }
      expect(s3.__uploadCounter).to.be(6);
    });

  });

  describe("#finishUpload", function(){
    it("uploads remaining chunks if this.__chunks not empty", function(){
      s3._write(largeString + largeString, "uf8", function(){});
      s3._write("foo", "utf8", function(){});
      s3._write(largeString, "utf8", function(){});

      var cb = sinon.stub();
      s3.finishUpload(cb);
      expect(mockS3Client.uploadPart.callCount).to.be(2);
      var payload = mockS3Client.uploadPart.args[1][0];
      expect(payload.Body).to.be("foo" + largeString);
    });
  });

});

