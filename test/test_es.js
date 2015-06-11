/* global describe, it, after */
'use strict';

var expect = require('chai').expect;
var client = require('../libs/connections.js').elasticsearch;
var es = require('../libs/es.js');

var record1 = {
  sceneID: 'scene1',
  row: 10,
  path: 100
};

var record2 = {
  sceneID: 'scene2',
  row: 20,
  path: 200
};

describe('Test es.js components', function () {
  this.timeout(50000);

  var testIndex = 'test';
  var testType = 'test';

  it('create index and add mapping', function (done) {
    // Create test index
    es.createIndex('test', 'test').then(function (result) {
      expect(result).to.be.true;
      done();
    }).catch(function (err) {
      console.log(err);
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
      console.log(err);
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

  it('test single upload to es', function (done) {
    // add record
    es.processSingle(record1, testIndex, testType, function (err) {
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
    var bulk = [
      {create: {_index: testIndex, _type: testType, _id: record1.sceneID}},
      record1,
      {create: {_index: testIndex, _type: testType, _id: record2.sceneID}},
      record2
    ];

    es.processBulk(bulk, function (err) {
      expect(err).to.be.undefined;

      // check if records exist
      client.get({
        index: testIndex,
        type: testType,
        id: 'scene1'
      }).then(function (response) {
        return expect(response._source.row).to.equal(10);
      }).then(function () {
        return client.get({
          index: testIndex,
          type: testType,
          id: 'scene2'
        });
      }).then(function (response) {
        expect(response._source.row).to.equal(20);
        done();
      }).catch(function (err) {
        console.log(err);
        expect(err).to.be.undefined;
        done();
      });
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
    es.toElasticSearch(
      __dirname + '/test.csv',
      testIndex,
      testType,
      5
    ).then(function () {
      done();
    }).catch(function (err) {
      console.log(err);
      expect(err).to.be.undefined;
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
      expect(err).to.be.undefined;
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
