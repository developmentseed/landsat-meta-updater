'use strict';

var Updater = require('../index.js');
var async = require('async');

var u = new Updater('landsat', '8', 1000);

async.waterfall([

  // Update Elastic Search
  function (callback) {
    u.updateEs(callback);
  }
], function (err, msg) {
  if (err) {
    console.log('Error:', err);
  }
  console.log(msg);
  process.exit();
});
