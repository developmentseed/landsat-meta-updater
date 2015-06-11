'use strict';

var fs = require('fs-extra');
var join = require('path').join;
var request = require('request');
var progress = require('request-progress');
var Promise = require('bluebird');
var es = require('./libs/es.js');

var Updater = function (esIndex, esType, bulkSize) {
  // Globals
  this.url = 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_8.csv';
  this.downloadFolder = join(__dirname, 'download');
  this.csvFile = join(this.downloadFolder, 'landsat.csv');
  this.esIndex = esIndex;
  this.esType = esType;
  this.bulkSize = bulkSize;
};

Updater.prototype.download = function (url) {
  var self = this;

  return new Promise(function (resolve, reject) {
    fs.mkdirsSync(self.downloadFolder);

    // Check modification date of the csv file
    // If it is less than 12 hours skip
    fs.stat(self.csvFile, function (err, stats) {
      var elapsed = (Date.now() - Date.parse(stats.mtime)) / 1000 / 60 / 60;

      if (elapsed < 12) {
        console.log('Meta file was downloaded less than 12 hours ago!');
        resolve();
      } else {
        progress(request(url), {
            delay: 1000      // Only start to emit after 1000ms delay
        })
        .on('progress', function (state) {
          var size = Math.floor(state.received / Math.pow(1024, 2)).toFixed(2);
          process.stdout.write('Received size:              ' + size + 'MB \r');
        })
        .pipe(fs.createWriteStream(self.csvFile))
        .on('error', function (err) {
          reject(err);
        })
        .on('close', function () {
          console.log('\n Download Completed!');
          resolve();
        });
      }
    });
  });
};

Updater.prototype.updateEs = function (cb) {
  console.log('Downloading landsat.csv from NASA ...');

  var self = this;

  this.download(self.url)
    .then(function () {
      return es.toElasticSearch(
        join(__dirname, 'download', 'landsat.csv'),
        self.esIndex,
        self.esType,
        self.bulkSize
      );
    }).then(function (msg) {
      return msg;
    }).catch(function (err) {
      throw err;
    }).nodeify(cb);
};

module.exports = Updater;
