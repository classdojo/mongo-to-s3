## mongo-to-s3 (WIP)

```javascript
var MongoToS3 = require("mongo-to-s3");
var through = require("through");

var s3 = new AWS.S3({
  accessKeyId: "myAccessKey",
  secretAccessKey: "mySecretAccessKey",
  region: "us-east-1"
});

mongoToS3 = new MongoToS3(s3);

mongoToS3.createS3Sink({s3: {
  Bucket: "myBucket",
  Key: "myKey",
  ACL: "public-read"
}}, function(err, myS3Sink) {


  mongoToS3.fromMongo([{
    //anything accepted by 'mongoexport'
    exportOptions: "-h localhost:27017 -d database -c collection",
    workingDirectory: "/tmp" //some writable path on your machine
  }])
  .pipe(through(function(chunk, enc, cb) {
    //some processing step
    console.log("Processing:", chunk);
    this.push(chunk);
    cb();
  }))
  .pipe(myS3Sink);
});


/*
 *  If you want to process data from multiple mongoexport commands
 *  just pass in more configuration objects into `fromMongo`
*/
  mongoToS3.fromMongo([
    {
      exportOptions: "-h localhost:27017 -d database -c collection1",
      workingDirectory: "/tmp"
    },
    {
      exportOptions: "-h localhost:27017 -d database -c collection2",
      workingDirectory: "/tmp"
    }
  ])
  .pipe(through(function(chunk, enc, cb) {
    //both collection 1 and collection 2 are joined here.
    this.push(chunk);
    cb();
  }))
  .pipe(myS3Sink);


/*
 * Sometimes you might want to process mongoexport results in
 * separate processes to increase throughput.
*/
  mongoToS3.fromMongo([
    {
      exportOptions: "-h localhost:27017 -d database -c collection1",
      workingDirectory: "/tmp"
    },
    {
      exportOptions: "-h localhost:27017 -d database -c collection2",
      workingDirectory: "/tmp"
    }
  ])
  .thoughPipeline(__dirname + "/somePipeline.js")
  .pipe(myS3Sink);
/*
 * `throughPipeline` takes the pathname of a file that exports a Duplex (or Transform) stream.
 * Each mongoexports stream gets its own processing pipe that runs in an external
 * process. The results are then aggregated into the main process. In the above example both
 * collection1 and collection2 are uploaded to s3 after being processed by the stream exported
 * by /somePipeline.js
*/
```
