#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              🍃 MONGODB DATA VIEWER                            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Check if MongoDB is running
if ! docker-compose ps mongodb | grep -q "Up"; then
    echo "❌ MongoDB is not running!"
    echo "Start it with: ./start-all.sh"
    exit 1
fi

echo "📊 Database: news_database"
echo "📂 Collection: news_collection"
echo ""

# Get count
echo "📈 Total Documents:"
count=$(docker exec mongodb mongosh --quiet --eval "db.getSiblingDB('news_database').news_collection.countDocuments({})" 2>/dev/null)
echo "   └─ $count documents"
echo ""

# Show sample documents
echo "📄 Sample Documents (latest 5):"
echo ""
docker exec mongodb mongosh --quiet --eval "
db.getSiblingDB('news_database').news_collection.find().sort({_id: -1}).limit(5).forEach(doc => {
    print('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    print('Symbol:      ' + doc.symbol);
    print('Title:       ' + doc.title);
    print('Source:      ' + doc.source);
    print('Published:   ' + doc.published_at);
    print('URL:         ' + doc.url);
    print('');
});
" 2>/dev/null

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Show statistics by symbol
echo "📊 Statistics by Symbol:"
docker exec mongodb mongosh --quiet --eval "
db.getSiblingDB('news_database').news_collection.aggregate([
    { \$group: { _id: '\$symbol', count: { \$sum: 1 } } },
    { \$sort: { count: -1 } },
    { \$limit: 10 }
]).forEach(doc => {
    print('   ' + doc._id + ': ' + doc.count + ' documents');
});
" 2>/dev/null

echo ""
echo "💡 Access MongoDB Web UI:"
echo "   └─ http://localhost:8081 (admin/admin)"
echo ""