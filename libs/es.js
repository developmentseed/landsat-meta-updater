'use strict';

var _ = require('lodash');
var csv = require('csv');
var Promise = require('bluebird');
var LineByLineReader = require('line-by-line');
var client = require('./connections.js').elasticsearch;

// Global variables
var skipFields = ['dateUpdated', 'sceneStopTime', 'sceneStartTime', 'acquisitionDate'];
var total = 0;
var added = 0;
var skipped = 0;
var header;

var addMapping = function (indexName, typeName) {
  var mapping = {
    '8': {
      properties: {
        sceneID: {'type': 'string', 'index': 'not_analyzed'},
        row: {'type': 'integer'},
        path: {'type': 'integer'},
        cloudCover: {'type': 'float'},
        cloudCoverFull: {'type': 'float'},
        upperLeftCornerLatitude: {'type': 'double'},
        upperLeftCornerLongitude: {'type': 'double'},
        lowerLeftCornerLatitude: {'type': 'double'},
        lowerLeftCornerLongitude: {'type': 'double'},
        sceneCenterLatitude: {'type': 'double'},
        sceneCenterLongitude: {'type': 'double'},
        lowerRightCornerLatitude: {'type': 'double'},
        lowerRightCornerLongitude: {'type': 'double'},
        upperRightCornerLatitude: {'type': 'double'},
        upperRightCornerLongitude: {'type': 'double'},
        acquisitionDate: {'type': 'date', format: 'date'},
        boundingBox: {'type': 'geo_shape', 'precision': '1mi'}
      }
    }
  };

  return client.indices.putMapping({
    index: indexName,
    type: typeName,
    body: mapping
  }).then(function () {
    console.log('Added Mapping');
    return true;
  }).catch(function (err) {
    throw err;
  });
};

var createIndex = function (indexName, typeName) {
  return client.indices.create({index: indexName}).then(function () {
    return addMapping(indexName, typeName);
  }).catch(function (err) {
    throw err;
  });
};

var indexExist = function (indexName, typeName) {
  return client.indices.exists({index: indexName}).then(function (resp) {
    if (resp) {
      return false;
    } else {
      console.log(indexName + ' does not exist');
      return createIndex(indexName, typeName);
    }
  }).catch(function (err) {
    throw err;
  });
};

var landsatMetaObject = function (header, record) {
  var output = {};

  for (var j = 0; j < header.length; j++) {
    // convert numbers to float
    var value = parseFloat(record[j]);
    if (_.isNaN(value) || skipFields.indexOf(header[j]) !== -1) {
      value = record[j];
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

var processBulk = module.exports.processBulk = function (bulk) {
  client.bulk({
    body: bulk
  }, function (err) {
    if (err) {
      // this is an error
    }
  });
};

var processSingle = module.exports.processSingle = function (record, esIndex, esType, cb) {
  client.index({
    index: esIndex,
    type: esType,
    id: record.sceneID,
    body: record,
    opType: 'create'
  }, function (err) {
    if (err) {
      if (err.status === '409') {
        skipped++;
      } else {
        console.log(err);
      }
    } else {
      added++;
    }
    cb(err);
  });
};

module.exports.toElasticSearch = function (filename, esIndex, esType, bulkSize, callback) {
  return indexExist(esIndex, esType).then(function (state) {
    if (state) {
      console.log(esIndex, 'index created!');
    } else {
      console.log(esIndex, 'index already exists!');
    }
    return;
  }).then(function () {
    return new Promise(function (resolve, reject) {
      var rstream = new LineByLineReader(filename);

      var i = 0;
      var bulkCounter = 0;
      var bulk = [];

      rstream.on('line', function (line) {
        // pause until processing is done

        rstream.pause();
        process.stdout.write('Log: processed: ' + total + ' added: ' + added + ' skipped: ' + skipped + '\r');
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
              client.exists({
                index: esIndex,
                type: esType,
                id: data[0]
              }, function (error, exists) {

                if (exists === true) {
                  skipped++;
                  rstream.resume();
                } else {
                  var meta = landsatMetaObject(header, data);

                  if (bulkSize) {
                    bulk.push({create: {_index: esIndex, _type: esType, _id: meta.sceneID}});
                    bulk.push(meta);

                    if (bulkCounter < bulkSize) {
                      bulkCounter++;
                      rstream.resume();
                    } else {
                      processBulk(bulk);
                      bulkCounter = 0;
                      bulk = [];
                      added = added + bulkSize;
                      rstream.resume();
                    }
                  } else {
                    processSingle(meta, esIndex, esType, function (err) {
                      if (err) {
                        console.log(err);
                      }
                      rstream.resume();
                    });
                  }
                }
              });

            }
          });
        });
      });

      rstream.on('end', function () {
        resolve('\nProcess is complete!');
      });

      rstream.on('error', function (err) {
        reject(err);
      });
    });
  }).catch(function (err) {
    throw err;
  }).nodeify(callback);
};
