'use strict';

var mongoose = require('mongoose');

var landsatSchema = new mongoose.Schema({
    sceneID: {type: String, unique: true, required: true, dropDups: true },
    sensor: String,
    receivingStation: String,
    dayOrNight: String,
    row: Number,
    path: Number,
    sunAzimuth: Number,
    sunElevation: Number,
    cloudCoverFull: Number,
    sceneStartTime: Date,
    sceneStopTime: Date,
    acquisitionDate: Date,
    boundingBox: {type: mongoose.Schema.Types.Mixed, index: '2dsphere'},
    extras: mongoose.Schema.Types.Mixed
});

module.exports = mongoose.model('landsat', landsatSchema);
