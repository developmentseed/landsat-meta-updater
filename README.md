## landsat-meta-updater

[![Build Status](https://travis-ci.org/developmentseed/landsat-meta-updater.svg?branch=master)](https://travis-ci.org/developmentseed/landsat-meta-updater)

A downloader for landsat metadata with auto update for Elasticsearch (MongoDB to be added soon).

### Requirements

- Elastic Search 1.6+ if used with Elastic Search
- MongoDB if used with MongoDB.

### Installation

    $ npm install

### Usage

```javascript

var Updater = require('landsat-meta-updater');

var u = new Updater('landsat', '8', 200);

//Update Elastic Search
u.updateEs(function(err, msg) {
  if (err) {
    console.log(err);
  }
  console.log(msg);
});

```

The number 200 (bulksize) shows the size of the bulk objects used for elasticsearch bulk update.

You can play around with the bulksize to speed up the update process. If `null` is passed, the updater adds records individually to Elastic Search (not recommended).

Running the updater is an expensive operation for the first time because Elastic Search creates geo-indices for each image's bounding box.
