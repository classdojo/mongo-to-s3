```javascript
var MongoToS3 = require("mongo-to-s3");

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
}}, function(err, awardrecordsSink) {

  mongoToS3.fromMongo({
    exportOptions: "-h localhost:27017 -d database -c collection"
  }).pipe(awardrecordsSink);
});

```
