/* global describe, it, after */
'use strict';

var fs = require('fs-extra');
var expect = require('chai').expect;
var nock = require('nock');
var Updater = require('../index.js');
var client = require('../libs/connections.js').elasticsearch;

describe('Test the package', function () {
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
