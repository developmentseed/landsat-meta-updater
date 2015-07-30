'use strict';

var fs = require('fs-extra');
var join = require('path').join;
var request = require('request');
var progress = require('request-progress');
var Promise = require('bluebird');
var async = require('async');
var es = require('./libs/es/updater.js');
var MongoDb = require('./libs/connections.js').mongodb;
var mongoUpdater = require('./libs/mongo/updater.js');

var Updater = function (esIndex, esType, bulkSize, downloadFolder) {
  // Globals
  this.url = 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_8.csv';
  this.downloadFolder = downloadFolder || join(__dirname, 'download');
  this.csvFile = join(this.downloadFolder, 'landsat.csv');
  this.esIndex = esIndex;
  this.esType = esType;
  this.bulkSize = bulkSize;
};

// Downloads the url to the given path
var downloadCsv = function (url, path, callback) {
  progress(request(url), {
      delay: 1000      // Only start to emit after 1000ms delay
  })
  .on('progress', function (state) {
    var size = Math.floor(state.received / Math.pow(1024, 2)).toFixed(2);
    process.stdout.write('Received size:              ' + size + 'MB \r');
  })
  .pipe(fs.createWriteStream(path))
  .on('error', function (err) {
    callback(err);
  })
  .on('close', function () {
    console.log('\n Download Completed!');
    callback(null);
  });
};

Updater.prototype.download = function (cb) {
  var self = this;

  async.waterfall([
    // Create download directory
    function (callback) {
      fs.mkdirsSync(self.downloadFolder);
      callback(null);
    },
    // // Check when was the last time the file was downloaded
    function (callback) {
      fs.stat(self.csvFile, function (err, stats) {
        if (err) {
          if (err.code !== 'ENOENT') {
            callback(err);
          } else {
            stats = {mtime: '2010-01-01'};
          }
        }
        callback(null, stats);
      });
    },
    // // Download the file
    function (stats, callback) {
      var elapsed = (Date.now() - Date.parse(stats.mtime)) / 1000 / 60 / 60;
      if (elapsed < 12) {
        console.log('Meta file was downloaded less than 12 hours ago!');
        callback(null);
      } else {
        downloadCsv(self.url, self.csvFile, callback);
      }
    }
  ], function (err, result) {
    cb(err, result);
  });
};

Updater.prototype.updateEs = function (cb) {
  console.log('Downloading landsat.csv from NASA ...');

  var self = this;

  this.download()
    .then(function () {
      return es.toElasticSearch(
        self.csvFile,
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

Updater.prototype.updateMongoDb = function (dbURL, cb) {
  console.log('Downloading landsat.csv from NASA ...');

  var self = this;

  async.waterfall([
    // Download landsat meta csv file
    function (callback) {
      self.download(callback);
    },

    // Connect to MongoDb
    function (result, callback) {
      var db = new MongoDb(process.env.DBNAME || 'landsat-api', dbURL);
      db.start(callback);
    },

    // Add new records to MongoDB
    function (callback) {
      console.log();
      mongoUpdater.toMongoDb(self.csvFile, self.bulkSize, callback);
    }
  ], function (err, result) {
    cb(err, result);
  });
};

module.exports = Updater;
