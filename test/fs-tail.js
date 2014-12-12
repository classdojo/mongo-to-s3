var expect   = require("expect.js");
var FsTail   = require("../fs-tail");
var fs       = require("fs");
var sinon    = require("sinon");
var Writable = require("stream").Writable;


describe("fs-tail", function() {
  var testFile = __dirname + "/test.txt";

  describe("integration test", function() {

    before(function(done) {
      fs.unlink(testFile, function() {
        fs.open(testFile, "w+", done);
      });
    });

    // after(function(done) {
    // });

    describe("initial static file", function() {
      var fileData = new Buffer("full\nfile\ncontents\n");
      var data, collector, fullData, tail;
      before(function(done) {
        fs.writeFile(testFile, fileData, function(err) {
          if(err) return done(err);
          tail = FsTail(testFile, {EOFAfter: 10});
          done();
        });
      });

      after(function(done) {
        tail.cleanUp(done);
      });

      before(function() {
        data = [];
        collector = new Writable();
        collector._write = function(obj, enc, cb) {
          data.push(obj);
          cb();
        };
      });

      it("reads the full file then emits EOF", function(done) {
        tail.once("EOF", function() {
          fullData = Buffer.concat(data);
          expect(fullData.toString()).to.be(fileData.toString());
          done();
        });
        tail.pipe(collector);
      });

      describe("that is subsequently appended to", function() {
        var moreFileData = new Buffer("with\nmore\nafter\ntime\n");
        it("properly outputs the diff", function(done) {
          var expectedFileData = [fileData, moreFileData];

          tail.once("EOF", function() {
            fullData = Buffer.concat(data);
            expect(fullData.toString()).to.be(Buffer.concat(expectedFileData).toString());
            done();
          });
          fs.appendFile(testFile, moreFileData, function(err) {
            expect(!!err).to.be(false);
          });
        });

        describe("that is appended to again", function() {
          var someMoreFileData = new Buffer("zzzz\nzzzzz\n");
          it("properly outputs the diff", function(done) {
            var expectedFileData = [fileData, moreFileData, someMoreFileData];

            tail.once("EOF", function() {
              fullData = Buffer.concat(data);
              expect(fullData.toString()).to.be(Buffer.concat(expectedFileData).toString());
              done();
            });
            fs.appendFile(testFile, someMoreFileData, function(err) {
              expect(!!err).to.be(false);
            });
          });
        });
      });

    });
  });

});
