'use strict';

var Updater = require('../index.js');
var MongoDb = require('../libs/connections.js').mongodb;
var async = require('async');

var u = new Updater('landsat', '8', 1000);

var dbUrl = 'mongodb://localhost/landsat-api' || process.env.DBURL;

async.waterfall([
  // Connect to MongoDb
  function (callback) {
    var db = new MongoDb(process.env.DBNAME || 'landsat-api', dbUrl);
    db.start(callback);
  },

  // Update MongoDB
  function (callback) {
    u.updateMongoDb(dbUrl, callback);
  }
], function (err, msg) {
  if (err) {
    console.log('Error:', err);
  }
  console.log(msg);
  process.exit();
});
