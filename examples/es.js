'use strict';

var Updater = require('../index.js');

var u = new Updater('landsat', '8', 1000);

u.updateEs(function (err, msg) {
  console.log('Error:', err);
  console.log(msg);
  process.exit();
});
