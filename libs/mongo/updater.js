'use strict';

var _ = require('lodash');
var LineByLineReader = require('line-by-line');
var format = require('date-format');
var csv = require('csv');
var async = require('async');
var Landsat = require('./model.js');

var skipFields = ['dateUpdated', 'sceneStopTime', 'sceneStartTime', 'acquisitionDate'];

var dateConverter = function (value) {
  var arr = value.split(':');

  var blank_date = new Date(arr[0], 0);
  blank_date.setDate(arr[1]);
  blank_date.setHours(arr[2]);
  blank_date.setMinutes(arr[3]);
  blank_date.setMilliseconds(arr[4]);
  var date = new Date(blank_date);

  return format('yyyy-MM-ddThh:mm:ss.SSS', date);
};

var landsatMetaObject = function (header, record) {
  var output = {};
  // output.extras = {};
  var value;

  // Get model fields
  // var modelHeaders = _.pull(_.keys(Landsat.schema.paths), '_id', '__v');

  // Build the landsat object
  for (var j = 0; j < header.length; j++) {
    if (header[j] === 'sceneStartTime' || header[j] === 'sceneStopTime') {
      output[header[j]] = dateConverter(record[j]);
    } else {
      output[header[j]] = record[j];
    }

    // convert numbers to float
    if (header[j] === 'sceneStartTime' || header[j] === 'sceneStopTime') {
      value = dateConverter(record[j]);
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

  var meta = landsatMetaObject(header, data);

  // if bulk object length is less than bulksize add to object
  // otherwise insert to mongoDB
  if (bulk.length < bulkSize) {
    added = true;
    bulk.push(meta);
    cb(null, added, skipped, bulk);
  } else {
    // Insert to DB
    Landsat.collection.insert(bulk, {ordered: false}, function (err) {
      // Ignore errs. Because every duplicate record throws and error
      cb(null, added, skipped, []);
    });
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

  // Read the file line by line
  var rstream = new LineByLineReader(filename);
  rstream.on('line', function (line) {
    // pause until processing is done
    rstream.pause();
    total++;

    async.waterfall([
      function (callback) {
        csv.parse(line, callback);
      },
      function (data, callback) {
        csv.transform(data, function (data) {
          if (!header) {
            header = data;
            callback(null);
          } else {
            // Do bulk upload if bulksize is provided
            if (bulkSize) {
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
    return cb(null, '\nProcess is complete!');
  });

  // catch errors
  rstream.on('error', function (err) {
    return cb(err);
  });
};
