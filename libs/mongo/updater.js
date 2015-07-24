'use strict';

var _ = require('lodash');
var LineByLineReader = require('line-by-line');
var Promise = require('bluebird');
var Landsat = require('./model.js');
var format = require('date-format');
var csv = require('csv');

var skipFields = ['dateUpdated', 'sceneStopTime', 'sceneStartTime', 'acquisitionDate'];
var total = 0;
var added = 0;
var skipped = 0;

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

module.exports.toMongoDb = function (filename, collelction, cb) {
  var header;
  var i = 0;

  return new Promise(function (resolve, reject) {
    // Read the file line by line
    var rstream = new LineByLineReader(filename);
    rstream.on('line', function (line) {
      // pause until processing is done
      rstream.pause();

      csv.parse(line, function (err, data) {
        if (err) {
          return reject(err);
        }

        csv.transform(data, function (data) {
          // get the header
          if (i === 0) {
            header = data;
            i++;
            rstream.resume();
          } else {
            total++;
            // Check if record exist, if not add it
            Landsat.findOne({sceneID: data[0]}, function (err, meta) {
              if (err) {
                return reject(err);
              }

              // if no record is found add the new row, otherwise skip
              if (meta === null) {
                var meta = landsatMetaObject(header, data);

                var record = new Landsat(meta);
                record.save(function (err, record) {
                  if (err) {
                    return reject(err);
                  }
                  added++;
                  rstream.resume();
                });
              } else {
                skipped++;
                rstream.resume();
              }
            });
          }
          process.stdout.write('Log: processed: ' + total + ' added: ' + added + ' skipped: ' + skipped + '\r');
        });
      });
    });

    rstream.on('end', function () {
      resolve('\nProcess is complete!');
    });

    rstream.on('error', function (err) {
      reject(err);
    });

  }).nodeify(cb);
};
