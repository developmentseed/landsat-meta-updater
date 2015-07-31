/* global describe, it, after */
'use strict';

var _ = require('lodash');
var expect = require('chai').expect;
var async = require('async');
var client = require('../libs/connections.js').elasticsearch;
var es = require('../libs/es/updater.js');

var record1 = {
  sceneID: 'scene1',
  row: 10,
  path: 100,
  lowerRightCornerLongitude: -20.74929,
  lowerLeftCornerLatitude: 81.83357,
  upperLeftCornerLongitude: -13.37295,
  lowerLeftCornerLongitude: -25.34162,
  upperRightCornerLongitude: -10.53945,
  upperLeftCornerLatitude: 82.46215,
  lowerRightCornerLatitude: 80.25769,
  upperRightCornerLatitude: 80.78312
};

var record2 = {
  sceneID: 'scene2',
  row: 20,
  path: 200,
  lowerRightCornerLongitude: -20.74929,
  lowerLeftCornerLatitude: 81.83357,
  upperLeftCornerLongitude: -13.37295,
  lowerLeftCornerLongitude: -25.34162,
  upperRightCornerLongitude: -10.53945,
  upperLeftCornerLatitude: 82.46215,
  lowerRightCornerLatitude: 80.25769,
  upperRightCornerLatitude: 80.78312
};

describe('Test es.js components', function () {
  this.timeout(5000);

  var testIndex = 'test';
  var testType = 'test';

  it('create index and add mapping', function (done) {
    // Create test index
    es.createIndex('test', 'test', function (err) {
      expect(err).to.be.undefined;
      done();
    });
  });

  it('test if index is created', function (done) {
    // Create test index
    client.indices.exists({index: testIndex}).then(function (exist) {
      expect(exist).to.be.true;
      done();
    }).catch(function (err) {
      expect(err).to.be.undefined;
      done();
    });
  });

  it('test if mapping is correctly set', function (done) {
    // Create test index
    client.indices.getMapping({
      index: testIndex,
      type: testType
    }).then(function (mapping) {
      expect(mapping.test.mappings.test.properties).to.have.any.keys('sceneID', 'row', 'path');
      done();
    }).catch(function (err) {
      console.log(err);
      expect(err).to.be.undefined;
      done();
    });
  });

  it('test a single upload to es', function (done) {
    // add record
    es.processSingle(_.keys(record1), _.values(record1), testIndex, testType, function (err) {
      expect(err).to.be.undefined;

      // check if record exists
      client.get({
        index: testIndex,
        type: testType,
        id: 'scene1'
      }).then(function (response) {
        expect(response._source.row).to.equal(10);
        done();
      }).catch(function (err) {
        console.log(err);
        expect(err).to.be.undefined;
        done();
      });
    });
  });

  it('test bulk upload to es', function (done) {
    // add record
    var bulk = [];

    async.waterfall([

      // First call to processBulk
      function (callback) {
        es.processBulk(
          _.keys(record1),
          _.values(record1),
          bulk,
          2,
          testIndex,
          testType,
          callback
        );
      }, function (added, skipped, bulkReturn, callback) {
        expect(added).to.be.true;
        expect(skipped).to.be.false;
        expect(bulkReturn.length).to.equal(2);

        es.processBulk(
          _.keys(record2),
          _.values(record2),
          bulkReturn,
          2,
          testIndex,
          testType,
          callback
        );
      },
      function (added, skipped, bulkReturn, callback) {
        expect(added).to.be.true;
        expect(skipped).to.be.false;
        expect(bulkReturn.length).to.equal(0);
        callback(null);
      },
      function (callback) {
        client.get({
          index: testIndex,
          type: testType,
          id: 'scene1'
        }, callback);
      },
      function (response, b, callback) {
        expect(response._source.row).to.equal(10);
        client.get({
          index: testIndex,
          type: testType,
          id: 'scene2'
        }, callback);
      }, function (response, b, callback) {
        expect(response._source.row).to.equal(20);
        callback(null);
      }
    ], function (err) {
      expect(err).to.be.null;
      done();
    });
  });

  after(function (done) {
    client.indices.delete({index: testIndex}).then(function () {
      done();
    }).catch(function (err) {
      console.log(err);
      expect(err).to.be.undefined;
      done();
    });
  });
});

describe('Test es.js main toElasticSearch method', function () {
  this.timeout(100000);
  var testIndex = 'test2';
  var testType = 'test2';

  it('add records', function (done) {
    es.toElasticSearch(__dirname + '/test.csv', testIndex, testType, 5, function (err) {
      expect(err).to.be.null;
      done();
    });
  });

  it('test if records were added', function (done) {
    client.get({
      index: testIndex,
      type: testType,
      id: 'LC81630272015162LGN00'
    }).then(function (response) {
      expect(response._source.boundingBox.coordinates[0]).to.have.length.within(4, 6);
      return expect(response._source.row).to.equal(27);
    }).then(function () {
      return client.get({
        index: testIndex,
        type: testType,
        id: 'LC81630152015162LGN00'
      });
    }).then(function (response) {
      expect(response._source.boundingBox.coordinates[0]).to.have.length.within(4, 6);
      expect(response._source.row).to.equal(15);
      done();
    }).catch(function (err) {
      console.log(err);
      expect(err).to.be.null;
      done();
    });
  });

  after(function (done) {
    client.indices.delete({index: testIndex}).then(function () {
      done();
    }).catch(function (err) {
      console.log(err);
      expect(err).to.be.undefined;
      done();
    });
  });
});
