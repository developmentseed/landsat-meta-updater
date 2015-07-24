'use strict';

var elasticsearch = require('elasticsearch');
var mongoose = require('mongoose');
var _ = require('lodash');

// Connection to Elastic Search
module.exports.elasticsearch = new elasticsearch.Client({
  host: process.env.ES_HOST || 'localhost:9200',

  // Note that this doesn't abort the query.
  requestTimeout: process.env.ES_TIMEOUT || 50000  // milliseconds
});

// Connection to MongoDB
var Mongodb = function (dbName, dbUri) {
  if (!_.isEmpty(dbUri)) {
    this.dbUri = dbUri;
  } else {
    this.dbUri = 'mongodb://localhost/' + dbName;
  }

  mongoose.connect(this.dbUri);
  this.db = mongoose.connection;
};

Mongodb.prototype.start = function (cb) {
  this.db.on('error', console.error.bind(console, 'connection error:'));
  this.db.once('open', function () {
    console.log('connected');
    if (cb) {
      cb();
    }
  });
};

Mongodb.prototype.deleteDb = function (cb) {
  this.db.db.dropDatabase(function (err) {
    if (err) {
      console.log(err);
    }
    mongoose.connection.close();
    cb();
  });
};

module.exports.mongodb = Mongodb;
