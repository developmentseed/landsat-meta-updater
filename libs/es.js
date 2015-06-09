'use strict';

var _ = require('lodash');
var fs = require('fs');
var csv = require('csv');
var Promise = require("bluebird");
var client = require('./connections.js').elasticsearch;

// Global variables
var skipFields = ['dateUpdated', 'sceneStopTime', 'sceneStartTime', 'acquisitionDate'];
var groupId = 0;
var group = [];
var bulk = [];
var bulkSize = 2000;
var total = 0;
var added = 0;
var header;


var addMapping = function (indexName, typeName) {

  var mapping = {
    '8': {
      properties: {
        sceneID                     : {'type' : 'string', 'index' : 'not_analyzed'},
        row                         : {'type' : 'integer'},
        path                        : {'type' : 'integer'},
        cloudCover                  : {'type' : 'float'},
        cloudCoverFull              : {'type' : 'float'},
        upperLeftCornerLatitude     : {'type' : 'double'},
        upperLeftCornerLongitude    : {'type' : 'double'},
        lowerLeftCornerLatitude     : {'type' : 'double'},
        lowerLeftCornerLongitude    : {'type' : 'double'},
        sceneCenterLatitude         : {'type' : 'double'},
        sceneCenterLongitude        : {'type' : 'double'},
        lowerRightCornerLatitude    : {'type' : 'double'},
        lowerRightCornerLongitude   : {'type' : 'double'},
        upperRightCornerLatitude    : {'type' : 'double'},
        upperRightCornerLongitude   : {'type' : 'double'},
        acquisitionDate             : {'type' : 'date', format: 'date'},
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

var processBulk = module.exports.processBulk = function (id) {
  client.bulk({
    body: group[id]
  }, function (err) {
    if (err) {
      console.log(err);
    }

    added = added + group[id].length / 2;
    group[id] = [];
    // id++;
    if (id <= groupId) {
      process.stdout.write('Log: processed: ' + total + ' added: ' + added + '\r');
      // processBulk(id);
    }
  });
};

var processSingle = module.exports.processSingle = function (id) {
  client.index({
    index: ES_INDEX,
    type: ES_TYPE,
    id: bulk[id].sceneID,
    body: bulk[id],
    timeout: 1000
  }, function (err) {
    if (err) {
      console.log(err);
    }

    bulk[id] = null;
    id++;
    added += 1;
    processSingle(id);
  });
};

var addToBulk = module.exports.addToBulk = function (counter, record) {
  // Read the header
  if (counter === 0) {
    header = record;
  } else {
    var output = {};

    for (var j = 0; j < header.length; j++) {
      // convert numbers to float
      var value = parseFloat(record[j]);
      if (_.isNaN(value) || skipFields.indexOf(header[j]) !== -1) {
        value = record[j];
      }
      output[header[j]] = value;
    }

    if (bulk.length < bulkSize) {
      bulk.push({index: {_index: ES_INDEX, _type: ES_TYPE, _id: record[0]}});
      bulk.push(output);
      total++;
    } else {
      group[groupId] = bulk;
      bulk = [];
      processBulk(groupId);
      groupId++;
    }
  }

  return counter + 1;
};

module.exports.toElasticSearch = function (filename, esIndex, esType, callback) {

  return indexExist(esIndex, esType).then(function (state) {
    if (state) {
      console.log(ES_INDEX, 'index created!');
    } else {
      console.log(ES_INDEX, 'index already exists!');
    }
    return;
  }).done(function () {

    return new Promise(function (resolve, reject) {
      var rstream = fs.createReadStream(filename);
      var i = 0;

      rstream.pipe(csv.parse())
        .pipe(csv.transform(function (record) {
          i = addToBulk(i, record);
          process.stdout.write('Log: processed: ' + total + ' added: ' + added + '\r');
        }))
        .on('unpipe', function () {
          console.log('\n');
          resolve('Process Completed!');
        })
        .on('error', function (err) {
        reject(err);
      });
    });

  }).catch(function (err) {
    throw err;
  }).nodeify(callback);

};
