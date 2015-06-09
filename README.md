## landsat-meta-downloader

A downloader for landsat metadata with auto update for Elasticsearch or Mongodb.

### Installation

    $ npm install

### Usage

```javascript

var Updater = require('landsat-meta-updater');

var u = new Updater('landsat', '8');

//Update Elastic Search
u.updateEs(function(err, msg) {
  if (err) {
    console.log(err);
  }
  console.log(msg);
});

```
