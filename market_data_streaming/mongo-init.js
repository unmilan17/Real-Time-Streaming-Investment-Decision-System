// MongoDB Initialization Script for Market Data
// This script runs when MongoDB container starts for the first time

// Switch to the market_db database
db = db.getSiblingDB('market_db');

print('========================================');
print('Initializing Market Database');
print('========================================');

// Create the ohlcv_raw collection for raw OHLCV data
db.createCollection('ohlcv_raw');

// Create indexes for ohlcv_raw collection
db.ohlcv_raw.createIndex({ "timestamp": -1 });
db.ohlcv_raw.createIndex({ "symbol": 1 });
db.ohlcv_raw.createIndex({ "symbol": 1, "timestamp": -1 });

print('✅ Collection created: ohlcv_raw');
print('✅ Indexes created for ohlcv_raw');

// Create the market_features collection for computed features
db.createCollection('market_features');

// Create indexes for market_features collection
db.market_features.createIndex({ "timestamp": -1 });
db.market_features.createIndex({ "symbol": 1 });
db.market_features.createIndex({ "symbol": 1, "timestamp": -1 });
db.market_features.createIndex({ "window_end": -1 });

print('✅ Collection created: market_features');
print('✅ Indexes created for market_features');

// Insert a test document to verify everything works
db.ohlcv_raw.insertOne({
    symbol: "TEST",
    timestamp: new Date().getTime(),
    open: 100.0,
    high: 105.0,
    low: 99.0,
    close: 102.0,
    volume: 1000,
    initialized_at: new ISODate()
});

db.market_features.insertOne({
    symbol: "TEST",
    timestamp: new Date().getTime(),
    window_end: new Date().getTime(),
    sma_5: 100.0,
    vwap_5: 101.0,
    initialized_at: new ISODate()
});

print('✅ Test documents inserted');

// Show the document counts
var rawCount = db.ohlcv_raw.countDocuments({});
var featuresCount = db.market_features.countDocuments({});
print('📊 ohlcv_raw document count: ' + rawCount);
print('📊 market_features document count: ' + featuresCount);

print('========================================');
print('Market Database Initialization Complete');
print('========================================');
