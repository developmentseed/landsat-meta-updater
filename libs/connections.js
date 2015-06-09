'use strict';

var elasticsearch = require('elasticsearch');

module.exports.elasticsearch = new elasticsearch.Client({
  host: process.env.ES_HOST || 'localhost:9200',

  // Note that this doesn't abort the query.
  requestTimeout: process.env.ES_TIMEOUT || 50000  // milliseconds
});
