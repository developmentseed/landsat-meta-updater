'use strict';

var prs = require('./pathsRows.json');

var pathNormalizer = function (value) {
  // Make sure value is in string
  value = String(value);

  while (value.length < 3) {
    value = '0' + value;
  }

  return value;
};

module.exports.pathNormalizer = pathNormalizer;

var country = function (row, path) {
  var countries = prs[pathNormalizer(row) + pathNormalizer(path)];

  if (countries) {
    return countries;
  } else {
    return [{'name': 'Ocean'}];
  }
};

module.exports.country = country;
