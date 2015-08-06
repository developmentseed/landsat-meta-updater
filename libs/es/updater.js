'use strict';

var _ = require('lodash');
var csv = require('csv');
var moment = require('moment');
var LineByLineReader = require('line-by-line');
var async = require('async');
var client = require('../connections.js').elasticsearch;

// Global variables
var skipFields = ['dateUpdated', 'sceneStopTime', 'sceneStartTime', 'acquisitionDate'];

var landsatMetaObject = function (header, record) {
  var output = {};
  var value;

  for (var j = 0; j < header.length; j++) {
    // convert numbers to float
    if (header[j] === 'sceneStartTime' || header[j] === 'sceneStopTime') {
      value = moment(record[j], 'YYYY:DDD:HH:mm:SSSS').format();
    } else if (header[j] === 'acquisitionDate' || header[j] === 'dateUpdated') {
      value = moment(record[j], 'YYYY-MM-DD').format();
    } else if (header[j] === 'row' || header[j] === 'path') {
      value = parseInt(record[j], 10);
    } else {
      value = parseFloat(record[j]);
      if (_.isNaN(value) || skipFields.indexOf(header[j]) !== -1) {
        value = record[j];
      }
    }

    output[header[j]] = value;
  }

  // Create bounding box
  output.boundingBox = {
    type: 'polygon',
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

var addMapping = module.exports.addMapping = function (indexName, typeName, callback) {
  var mapping = {};

  mapping[typeName] = {
    properties: {
      sceneID: {'type': 'string', 'index': 'not_analyzed'},
      sensor: {'type': 'string'},
      receivingStation: {'type': 'string'},
      dayOrNight: {'type': 'string'},
      row: {'type': 'integer'},
      path: {'type': 'integer'},
      sunAzimuth: {'type': 'float'},
      sunElevation: {'type': 'float'},
      cloudCoverFull: {'type': 'float'},
      sceneStartTime: {'type': 'date'},
      sceneStopTime: {'type': 'date'},
      acquisitionDate: {'type': 'date'},
      boundingBox: {'type': 'geo_shape', 'precision': '1mi'}
    }
  };

  // Put the mapping
  client.indices.putMapping({
    index: indexName,
    type: typeName,
    body: mapping
  }, function (err) {
    console.log('Added Mapping');
    callback(err);
  });
};

var createIndex = module.exports.createIndex = function (indexName, typeName, cb) {
  async.waterfall([
    // Create the index
    function (callback) {
      client.indices.create({index: indexName}, callback);
    },

    // Add mapping
    function (a, b, callback) {
      addMapping(indexName, typeName, callback);
    }
  ], function (err) {
    cb(err);
  });
};

var indexExist = function (indexName, typeName, cb) {
  async.waterfall([
    // Check if index exists
    function (callback) {
      client.indices.exists({index: indexName}, callback);
    },

    // Create it if it doesn't exist
    function (resp, status, callback) {
      if (resp) {
        callback();
      } else {
        console.log(indexName + ' does not exist');
        createIndex(indexName, typeName, callback);
      }
    }
  ], function (err) {
    cb(err);
  });
};

var processBulk = module.exports.processBulk = function (header, data, bulk, bulkSize, esIndex, esType, cb) {
  // Create object and add it to bulk
  var record = landsatMetaObject(header, data);
  bulk.push({create: {_index: esIndex, _type: esType, _id: record.sceneID}});
  bulk.push(record);

  if (bulk.length >= bulkSize * 2) {
    client.bulk({
      body: bulk
    }, function (err) {
      if (err) {
        console.log(err);
      }
      cb(null, true, false, []);
    });
  } else {
    cb(null, true, false, bulk);
  }
};

var processSingle = module.exports.processSingle = function (header, data, esIndex, esType, cb) {
  var record = landsatMetaObject(header, data);
  var skipped = false;
  var added = false;

  client.index({
    index: esIndex,
    type: esType,
    id: record.sceneID,
    body: record,
    opType: 'create'
  }, function (err) {
    if (err) {
      if (err.status === '409') {
        skipped = true;
      } else {
        console.log(err);
      }
    } else {
      added = true;
    }
    cb(err, added, skipped);
  });
};

var processCsv = function (filename, esIndex, esType, bulkSize, callback) {
  var rstream = new LineByLineReader(filename);
  var counter = 0;
  var skipped = 0;
  var added = 0;
  var header = false;
  var lastLine;
  var bulk = [];

  var processLine = function (data, cb) {
    async.waterfall([
      // Check if the record is already added to ES
      function (callback) {
        client.exists({ index: esIndex, type: esType, id: data[0]}, callback);
      },

      // If it exists skip
      function (exists, status, callback) {
        if (exists) {
          callback(true);
        } else {
          callback();
        }
      },

      // Process record
      function (callback) {
        if (bulkSize) {
          processBulk(header, data, bulk, bulkSize, esIndex, esType, callback);
        } else {
          processSingle(header, data, esIndex, esType, callback);
        }
      }

    // final step
    ], function (err, a, s, bulkReturn) {
      if (err !== true && err) {
        cb(err);
      } else {
        if (a) added++;
        if (s) skipped++;
        if (bulkReturn) bulk = bulkReturn;

        process.stdout.write('Log: processed: ' + counter + ' added: ' + added + ' skipped: ' + skipped + '\r');
        cb();
      }
    });
  };

  // Read file line by line
  rstream.on('line', function (line) {
    // Pause reading the line until the process is done
    rstream.pause();

    async.waterfall([
      // Parse the CSV
      function (callback) {
        csv.parse(line, callback);
      },

      // check if header is set
      function (data, callback) {
        // Read the header if not already read
        csv.transform(data, function (data) {
          if (!header) {
            header = data;

            // end the water fall
            callback(true);
          } else {
            counter++;
            lastLine = data;
            callback(null, data);
          }
        });
      },

      // process the line
      function (data, callback) {
        processLine(data, callback);
      }

    // Final step
    ], function (err) {
      if (err !== true && err) {
        callback(err);
      } else {
        rstream.resume();
      }
    });
  });

  // When finished
  rstream.on('end', function () {
    if (bulk.length > 0) {
      processBulk(header, lastLine, bulk, bulkSize, esIndex, esType, function () {
        callback(null, '\nProcess is complete!');
      });
    } else {
      callback(null, '\nProcess is complete!');
    }
  });

  // If there are errors
  rstream.on('error', function (err) {
    callback(err);
  });
};

module.exports.toElasticSearch = function (filename, esIndex, esType, bulkSize, cb) {
  indexExist(esIndex, esType, function (err) {
    if (err) {
      return cb(err);
    }
    processCsv(filename, esIndex, esType, bulkSize, cb);
  });
};
