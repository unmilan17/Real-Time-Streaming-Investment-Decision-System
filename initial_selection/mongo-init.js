// MongoDB Initialization Script
// This script runs when MongoDB container starts for the first time

// Switch to the news_database
db = db.getSiblingDB('news_database');

print('========================================');
print('Initializing News Database');
print('========================================');

// Create the news_collection if it doesn't exist
db.createCollection('news_collection');

// Create indexes for better query performance
db.news_collection.createIndex({ "symbol": 1 });
db.news_collection.createIndex({ "published_at": -1 });
db.news_collection.createIndex({ "source": 1 });
db.news_collection.createIndex({ "category": 1 });

// Create a compound index for common queries
db.news_collection.createIndex({ "symbol": 1, "published_at": -1 });

// Insert a test document to verify everything works
db.news_collection.insertOne({
    symbol: "TEST",
    title: "MongoDB Initialization Test",
    description: "This is a test document created during initialization",
    url: "https://example.com",
    published_at: new ISODate(),
    source: "init-script",
    category: "test",
    initialized_at: new ISODate()
});

print('✅ Database initialized successfully');
print('✅ Collection created: news_collection');
print('✅ Indexes created');
print('✅ Test document inserted');

// Show the document count
var count = db.news_collection.countDocuments({});
print('📊 Current document count: ' + count);

print('========================================');