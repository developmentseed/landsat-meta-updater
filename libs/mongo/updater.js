'use strict';

var _ = require('lodash');
var LineByLineReader = require('line-by-line');
var moment = require('moment');
var csv = require('csv');
var async = require('async');
var Landsat = require('./model.js');

var skipFields = ['dateUpdated', 'sceneStopTime', 'sceneStartTime', 'acquisitionDate'];

var landsatMetaObject = function (header, record) {
  var output = {};
  var value;

  // Build the landsat object
  for (var j = 0; j < header.length; j++) {
    // convert numbers to float
    if (header[j] === 'sceneStartTime' || header[j] === 'sceneStopTime') {
      value = moment(record[j], 'YYYY:DDD:HH:mm:SSSS').format();
    } else {
      value = parseFloat(record[j]);
      if (_.isNaN(value) || skipFields.indexOf(header[j]) !== -1) {
        value = record[j];
      }
    }
    output[header[j]] = value;
  }

  // Create bounding box geojson
  output.boundingBox = {
    type: 'Polygon',
    coordinates: [[
      [output.upperRightCornerLongitude, output.upperRightCornerLatitude],
      [output.upperLeftCornerLongitude, output.upperLeftCornerLatitude],
      [output.lowerLeftCornerLongitude, output.lowerLeftCornerLatitude],
      [output.lowerRightCornerLongitude, output.lowerRightCornerLatitude],
      [output.upperRightCornerLongitude, output.upperRightCornerLatitude]
    ]]
  };

  return output;
};

// Process records in bulk
var processBulk = function (header, data, bulk, bulkSize, cb) {
  var skipped = false;
  var added = true;

  // Create object and add it to bulk
  // only if bulk size is less than or equal to bulk size
  if (bulk.length <= bulkSize) {
    added = true;
    var meta = landsatMetaObject(header, data);
    bulk.push(meta);
  }

  // if bulk object length equal bulksize insert it to mongoDB
  if (bulk.length >= bulkSize) {
    // Insert to DB
    Landsat.collection.insert(bulk, {ordered: false}, function () {
      // Ignore errs. Because every duplicate record throws and error
      cb(null, added, skipped, []);
    });
  } else {
    cb(null, added, skipped, bulk);
  }
};

// Process a single record and add it to MongoDB
var processSingle = function (header, data, cb) {
  var skipped = false;
  var added = false;

  async.waterfall([
    function (callback) {
      Landsat.findOne({sceneID: data[0]}, callback);
    },
    function (meta, callback) {
      // if no record is found add the new row, otherwise skip
      if (meta === null) {
        meta = landsatMetaObject(header, data);

        var record = new Landsat(meta);
        record.save(function (err, record) {
          if (err) {
            return callback(err);
          }
          added = true;
          callback(null);
        });
      } else {
        skipped = true;
        callback(null);
      }
    }
  ],
  function (err) {
    cb(err, added, skipped);
  });
};

module.exports.toMongoDb = function (filename, bulkSize, cb) {
  var header = null;
  var total = 0;
  var added = 0;
  var skipped = 0;
  var bulk = [];
  var lastLine;

  // Read the file line by line
  var rstream = new LineByLineReader(filename);
  rstream.on('line', function (line) {
    // pause until processing is done
    rstream.pause();

    // Keep last line
    async.waterfall([
      // Parse the CSV
      function (callback) {
        csv.parse(line, callback);
      },

      // Transform it and send to DB
      function (data, callback) {
        csv.transform(data, function (data) {
          // First line is the header
          if (!header) {
            header = data;
            callback(null);

          // The rest is data
          } else {
            lastLine = line;
            total++;

            // Do bulk upload if bulksize is provided
            if (bulkSize) {
              // console.log(data[0]);
              processBulk(header, data, bulk, bulkSize, callback);
            } else {
              processSingle(header, data, callback);
            }
          }
        });
      }

    // Final step
    ], function (err, a, s, bulkReturn) {
      if (err) {
        return cb(err);
      }

      if (a) added++;
      if (s) skipped++;

      if (bulkReturn) {
        bulk = bulkReturn;
      }

      process.stdout.write('Log: processed: ' + total + ' added: ' + added + ' skipped: ' + skipped + '\r');
      rstream.resume();
    });
  });

  // Fire when the file read is done
  rstream.on('end', function () {
    // Process the remaining items in bulk if any
    if (bulk.length > 0) {
      processBulk(header, lastLine, bulk, bulk.length - 1, function () {
        cb(null, '\nProcess is complete!');
      });
    } else {
      return cb(null, '\nProcess is complete!');
    }
  });

  // catch errors
  rstream.on('error', function (err) {
    return cb(err);
  });
};
