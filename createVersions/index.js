// dependencies
var async = require('async');
var AWS = require('aws-sdk');
var gm = require('gm').subClass({ imageMagick: true }); // Enable ImageMagick integration.
var util = require('util');
var path = require('path');
var fs = require('fs');

// constants
var VERSIONS = process.env.VERSIONS ? JSON.parse(process.env.VERSIONS) : [
	// {width: 1080, height: 1080, dstSuffix: '-normal-1080'},
	// {width: 1080, height: 1080, dstSuffix: '-watermarked-1080', watermark: {
	// 	logo: {
	// 		path: 'logo/pure.png', // in the srcBucket
	// 		width: 150,
	// 		height: 90
	// 	},
	// 	text: 'Settlin',
	// }},
	// {width: 200, height: 200, dstSuffix: '-thumbnail-200'},
	// {width: 100, height: 100, dstSuffix: '-thumbnail-100'}
];

// get reference to S3 client
var s3 = new AWS.S3({
	region: 'ap-south-1',
});

exports.handler = function(event, context, callback) {
	if (!VERSIONS.length) {
		callback('Please provide proper array of VERSIONS as an environment variable');
		return;
	}

	// Read options from the event.
	// console.log('Reading options from event:\n', util.inspect(event, {depth: 5}));
	var srcBucket = event.Records[0].s3.bucket.name; // eg. images-store
	// Object key may have spaces or unicode non-ASCII characters.
	var srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
	var dstBucket = srcBucket.replace('-store', ''); // eg. images

	// Sanity check: validate that source and destination are different buckets.
	if (srcBucket == dstBucket) {
		callback('Source and destination buckets are the same. Src: ' + srcBucket + ' & Dest: ' + dstBucket);
		return;
	}

	// Infer the image type.
	var typeMatch = srcKey.match(/\.([^.]*)$/);
	if (!typeMatch) {
		callback('Could not determine the image type.');
		return;
	}
	var imageType = typeMatch[1].toLowerCase();
	if (!~['jpg', 'jpeg', 'png'].indexOf(imageType)) {
		callback(`Unsupported image type: ${imageType}`);
		return;
	}

	// Download the image from S3, transform, and upload to a different S3 bucket.
	async.waterfall([
		function download(next) {
			// Download the image from S3 into a buffer.
			s3.getObject({
				Bucket: srcBucket,
				Key: srcKey
			},
			next);
		},
		function transform(response, next) {
			gm(response.Body).size(function(err, size) {
				var self = this;

				var createVersion = function(versions, ind, buffers) {
					if (ind === versions.length) {
						next(null, response.ContentType, buffers);
						return;
					}

					// Infer the scaling factor to avoid stretching the image unnaturally.
					const scalingFactor = Math.min(versions[ind].width / size.width, versions[ind].height / size.height);
					const width	= scalingFactor * size.width;
					const height = scalingFactor * size.height;

					// Waterfall resize and watermarks as per the version definitions
					const watermark = versions[ind].watermark || {};
					async.waterfall([
						function resize(last) {
							self.resize(width, height).toBuffer(imageType, function(err, buffer) {
								if (err) last(err);
								else last(null, buffer);
							});
						},
						function watermarkLogo(buffer, last) {
							const imgToDraw = watermark.logo;
							if (imgToDraw) {
								s3.getObject({
									Bucket: dstBucket, // or wherever you keep the logo
									Key: imgToDraw.path
								}, function(err, res) {
									if (err) {
										console.error('Failed to fetch logo');
										last(err);
									}
									else {
										fs.writeFile('/tmp/logo.png', res.Body, function(err){
											if (err) last(err);
											else {
												let imageTxt= 'image Over ';
												imageTxt += parseInt(width - imgToDraw.width - 50, 10) + ","
												imageTxt += parseInt(height - imgToDraw.height - 50, 10) + " ";
												imageTxt += imgToDraw.width + "," + imgToDraw.height;
												imageTxt += " '/tmp/logo.png'";
												gm(buffer)
												.draw([imageTxt])
												.toBuffer(imageType, function(err, buffer) {
													if (err) last(err);
													else last(null, buffer);
												});
											}
										});
									}									
								});
							}
							else last(null, buffer);
						},
						function watermarkText(buffer, last) {
							if (watermark.text) {
								gm(buffer)
								.fill('rgba(0,0,0,0.05)')
								.fontSize(200)
								.gravity("Center")
								.draw(["rotate -45 text 0,0 '" + watermark.text + "'"]).toBuffer(imageType, function(err, buffer) {
									if (err) last(err);
									else last(null, buffer);
								});
							}
							else last(null, buffer);
						}
					], function(err, buffer) {
							// any error means failed upload - do not create any version
							if (err) next(err);
							else {
								buffers.push(buffer);
								createVersion(versions, ind + 1, buffers);
							}
						}
					);
				}

				// Transform the image buffer in memory.
				var buffers = [];
				createVersion(VERSIONS, 0, buffers);
			});
		},
		function upload(contentType, buffers, next) {
			// Stream the transformed image to a different S3 bucket.
			var putFile = function(versions, ind) {
				if (ind === versions.length - 1) cb = next;
				else cb = function() { putFile(versions, ind + 1); };

				s3.putObject({
					Bucket: dstBucket,
					Key: path.dirname(srcKey) + versions[ind].dstSuffix + '/' + path.basename(srcKey),
					Body: buffers[ind],
					ContentType: contentType
				}, cb);
			};

			putFile(VERSIONS, 0);
		}
	], function (err) {
		if (err) {
			console.error(
				'Unable to resize ' + srcBucket + '/' + srcKey +
				' and upload to ' + dstBucket + '/' + srcKey +
				' due to an error: ' + err
			);
		} else {
			console.log(
				'Successfully resized ' + srcBucket + '/' + srcKey +
				' and uploaded to ' + dstBucket + '/' + srcKey
			);
		}

		callback(null, 'message');
	});
};
