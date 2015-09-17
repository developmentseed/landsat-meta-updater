'use strict';

var mongoose = require('mongoose');

var landsatSchema = new mongoose.Schema({
    sceneID: {type: String, unique: true, required: true, dropDups: true },
    sensor: String,
    receivingStation: String,
    dayOrNight: {type: String, index: true},
    row: {type: Number, index: true},
    path: {type: Number, index: true},
    sunAzimuth: {type: Number, index: true},
    sunElevation: {type: Number, index: true},
    cloudCoverFull: {type: Number, index: true},
    sceneStartTime: {type: Date, index: true},
    sceneStopTime: {type: Date, index: true},
    acquisitionDate: {type: Date, index: true},
    browseAvailable: String,
    browseURL: String,
    upperLeftCornerLatitude: Number,
    upperLeftCornerLongitude: Number,
    upperRightCornerLatitude: Number,
    upperRightCornerLongitude: Number,
    lowerLeftCornerLatitude: Number,
    lowerLeftCornerLongitude: Number,
    lowerRightCornerLatitude: Number,
    lowerRightCornerLongitude: Number,
    sceneCenterLatitude: Number,
    sceneCenterLongitude: Number,
    cloudCover: Number,
    FULL_UL_QUAD_CCA: String,
    FULL_UR_QUAD_CCA: String,
    FULL_LL_QUAD_CCA: String,
    FULL_LR_QUAD_CCA: String,
    flightPath: String,
    lookAngle: String,
    imageQuality1: Number,
    imageQuality2: Number,
    gainBand1: String,
    gainBand2: String,
    gainBand3: String,
    gainBand4: String,
    gainBand5: String,
    gainBand6H: String,
    gainBand6L: String,
    gainBand7: String,
    gainBand8: String,
    gainChangeBand1: String,
    gainChangeBand2: String,
    gainChangeBand3: String,
    gainChangeBand4: String,
    gainChangeBand5: String,
    gainChangeBand6H: String,
    gainChangeBand6L: String,
    gainChangeBand7: String,
    gainChangeBand8: String,
    satelliteNumber: String,
    DATA_TYPE_L1: String,
    DATE_ACQUIRED_GAP_FILL: String,
    DATA_TYPE_L0RP: String,
    DATUM: String,
    ELEVATION_SOURCE: String,
    ELLIPSOID: String,
    EPHEMERIS_TYPE: String,
    FALSE_EASTING: String,
    FALSE_NORTHING: String,
    GAP_FILL: String,
    GROUND_CONTROL_POINTS_MODEL: String,
    GROUND_CONTROL_POINTS_VERIFY: String,
    GEOMETRIC_RMSE_MODEL: String,
    GEOMETRIC_RMSE_MODEL_X: Number,
    GEOMETRIC_RMSE_MODEL_Y: Number,
    GEOMETRIC_RMSE_VERIFY: String,
    GRID_CELL_SIZE_PANCHROMATIC: String,
    GRID_CELL_SIZE_REFLECTIVE: String,
    GRID_CELL_SIZE_THERMAL: String,
    MAP_PROJECTION_L1: String,
    MAP_PROJECTION_L0RA: String,
    ORIENTATION: String,
    OUTPUT_FORMAT: String,
    PANCHROMATIC_LINES: String,
    PANCHROMATIC_SAMPLES: String,
    L1_AVAILABLE: String,
    REFLECTIVE_LINES: String,
    REFLECTIVE_SAMPLES: String,
    RESAMPLING_OPTION: String,
    SCAN_GAP_INTERPOLATION: String,
    THERMAL_LINES: String,
    THERMAL_SAMPLES: String,
    TRUE_SCALE_LAT: String,
    UTM_ZONE: String,
    VERTICAL_LON_FROM_POLE: String,
    PRESENT_BAND_1: String,
    PRESENT_BAND_2: String,
    PRESENT_BAND_3: String,
    PRESENT_BAND_4: String,
    PRESENT_BAND_5: String,
    PRESENT_BAND_6: String,
    PRESENT_BAND_7: String,
    PRESENT_BAND_8: String,
    NADIR_OFFNADIR: String,
    countries: mongoose.Schema.Types.Mixed,
    boundingBox: {type: mongoose.Schema.Types.Mixed, index: '2dsphere'}
});

landsatSchema.index({sceneCenterLatitude: 1, sceneCenterLongitude: 1});
landsatSchema.index({acquisitionDate: 1, sceneCenterLatitude: 1, sceneCenterLongitude: 1});
landsatSchema.index({dayOrNight: 1, acquisitionDate: 1, cloudCoverFull: 1,
                     sceneCenterLatitude: 1, sceneCenterLongitude: 1});
landsatSchema.index({acquisitionDate: 1, cloudCoverFull: 1, sceneCenterLatitude: 1, sceneCenterLongitude: 1});

module.exports = mongoose.model('landsat', landsatSchema);
