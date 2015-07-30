/* global describe, it, after, before */
'use strict';

var fs = require('fs-extra');
var expect = require('chai').expect;
var nock = require('nock');
var Updater = require('../index.js');
var client = require('../libs/connections.js').elasticsearch;
var MongoDb = require('../libs/connections.js').mongodb;
var Landsat = require('../libs/mongo/model.js');

describe('Test the package with elasticsearch', function () {
  this.timeout(1000000);
  var testIndex = 'test_main';
  var testType = '8';
  var downloadDir = __dirname + '/download';

  it('run', function (done) {
    var csv = fs.readFileSync(__dirname + '/test.csv', {encoding: 'utf8'});

    nock('http://landsat.usgs.gov')
      .get('/metadata_service/bulk_metadata_files/LANDSAT_8.csv')
      .reply(200, csv);

    var u = new Updater(testIndex, testType, 5, downloadDir);

    // Update Elastic Search
    u.updateEs(function (err, msg) {
      if (err) {
        console.log('Error:', err);
      } else {
        done();
      }
    });
  });

  it('test if records were added', function (done) {
    client.get({
      index: testIndex,
      type: testType,
      id: 'LC81630762015162LGN00'
    }).then(function (response) {
      expect(response._source.boundingBox.coordinates[0]).to.have.length.within(4, 6);
      return expect(response._source.row).to.equal(76);
    }).then(function () {
      return client.get({
        index: testIndex,
        type: testType,
        id: 'LC81630752015162LGN00'
      });
    }).then(function (response) {
      expect(response._source.boundingBox.coordinates[0]).to.have.length.within(4, 6);
      expect(response._source.row).to.equal(75);
      done();
    }).catch(function (err) {
      console.log(err);
      expect(err).to.be.undefined;
      done();
    });
  });

  after(function (done) {
    client.indices.delete({index: testIndex}).then(function () {
      fs.removeSync(downloadDir);
      done();
    }).catch(function (err) {
      console.log(err);
      expect(err).to.be.undefined;
      done();
    });
  });
});

describe('Test the package with mongoDB', function () {
  this.timeout(1000000);
  var testIndex = 'test_main';
  var testType = '8';
  var downloadDir = __dirname + '/download';
  var db;
  var dbUrl;

  before(function (done) {
    // Connect to MongoDb
    db = new MongoDb(process.env.DBNAME || 'landsat-api', dbUrl);
    db.start(done);
  });

  it('add records', function (done) {
    var csv = fs.readFileSync(__dirname + '/test.csv', {encoding: 'utf8'});

    nock('http://landsat.usgs.gov')
      .get('/metadata_service/bulk_metadata_files/LANDSAT_8.csv')
      .reply(200, csv);

    var u = new Updater(testIndex, testType, 5, downloadDir);

    // Update Elastic Search
    u.updateMongoDb(dbUrl, function (err, msg) {
      if (err) {
        console.log('Error:', err);
      } else {
        expect(msg).to.equal('\nProcess is complete!');
        done();
      }
    });
  });

  it('test record count', function (done) {
    Landsat.count({}, function (err, count) {
      // console.log(err);
      expect(err).to.be.null;
      expect(count).to.equal(14);
      done();
    });
  });

  it('check a record', function (done) {
    Landsat.find({sceneID: 'LC81630762015162LGN00'}, function (err, record) {
      expect(err).to.be.null;
      expect(record[0].boundingBox.coordinates[0]).to.have.length.within(4, 6);
      expect(record[0].row).to.equal(76);
      done();
    });
  });

  it('check another record', function (done) {
    Landsat.find({sceneID: 'LC81630752015162LGN00'}, function (err, record) {
      expect(err).to.be.null;
      expect(record[0].boundingBox.coordinates[0]).to.have.length.within(4, 6);
      expect(record[0].row).to.equal(75);
      done();
    });
  });

  after(function (done) {
    db.deleteDb(function () {
      done();
    });
  });
});
