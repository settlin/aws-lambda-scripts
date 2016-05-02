// dependencies
var async = require('async');
var AWS = require('aws-sdk');
var util = require('util');
var path = require('path');

// constants
var VERSIONS = [{width: 1080, height: 1080, dstSuffix: "-1080"}, {width: 200, height: 200, dstSuffix: "-200"}, {width: 100, height: 100, dstSuffix: "-100"}];

// get reference to S3 client
var s3 = new AWS.S3();

exports.handler = function(event, context, callback) {
	// Read options from the event.
	console.log("Reading options from event:\n", util.inspect(event, {depth: 5}));
	var srcBucket = event.Records[0].s3.bucket.name; // eg. images-uploads
	// Object key may have spaces or unicode non-ASCII characters.
	var srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
	var dstBucket = srcBucket.replace("-uploads", ""); // eg. images

	// Sanity check: validate that source and destination are different buckets.
	if (srcBucket == dstBucket) {
		callback("Source and destination buckets are the same. Src: " + srcBucket + " & Dest: " + dstBucket);
		return;
	}

	async.waterfall([
		function deleteFiles(next) {
			files = VERSIONS.map(function(it) {
				return { Key: path.dirname(srcKey) + it.dstSuffix + "/" + path.basename(srcKey) };
			});
			console.log(files);
			s3.deleteObjects({
				Bucket: dstBucket,
				Delete: {
					Objects: files
				}
			}, next);
		}
	], function (err) {
		if (err) console.error('Unable to delete versions' + err);
		else console.log('Successfully deleted versions ');

		callback(null, "message");
	}
);
};
