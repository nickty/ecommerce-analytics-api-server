import express from 'express';
import { MongoClient } from 'mongodb';
import cors from 'cors';
import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';

// Load environment variables
dotenv.config();

const app = express();
const PORT = process.env.PORT || 3002;

// Middleware
app.use(cors());
app.use(express.json());

// MongoDB connection
const mongoUri = process.env.MONGODB_URI || 'mongodb://localhost:27017/ecommerce-analytics';
let db;

// Kafka configuration for real-time updates
const kafka = new Kafka({
  clientId: 'api-server',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'api-servers' });

// Connect to MongoDB
async function connectToMongo() {
  try {
    const client = new MongoClient(mongoUri);
    await client.connect();
    console.log('Connected to MongoDB');
    db = client.db();
  } catch (error) {
    console.error('Failed to connect to MongoDB:', error);
    process.exit(1);
  }
}

// Connect to Kafka for real-time metrics
async function connectToKafka() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'analytics-metrics', fromBeginning: false });
    console.log('Connected to Kafka and subscribed to analytics-metrics');
  } catch (error) {
    console.error('Failed to connect to Kafka:', error);
  }
}

// API Routes

// Dashboard overview
app.get('/api/dashboard/overview', async (req, res) => {
  try {
    // Get today's date
    const today = new Date().toISOString().split('T')[0];
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
    
    // Get daily metrics
    const todayPageViews = await db.collection('metrics').findOne({ name: 'daily_page_views', date: today });
    const yesterdayPageViews = await db.collection('metrics').findOne({ name: 'daily_page_views', date: yesterday });
    
    const todaySales = await db.collection('metrics').findOne({ name: 'daily_sales', date: today });
    const yesterdaySales = await db.collection('metrics').findOne({ name: 'daily_sales', date: yesterday });
    
    // Get total users
    const totalUsers = await db.collection('user_analytics').countDocuments();
    
    // Get active sessions in last 30 minutes
    const activeSessionsCount = await db.collection('sessions').countDocuments({
      lastActive: { $gte: new Date(Date.now() - 1800000).toISOString() }
    });
    
    // Calculate conversion rate
    const conversionRate = todaySales && todayPageViews ? 
      ((todaySales.count || 0) / (todayPageViews.count || 1) * 100).toFixed(2) : 0;
    
    // Get top products
    const topProducts = await db.collection('product_analytics')
      .find()
      .sort({ views: -1 })
      .limit(5)
      .toArray();
    
    // Get recent orders
    const recentOrders = await db.collection('orders')
      .find()
      .sort({ timestamp: -1 })
      .limit(5)
      .toArray();
    
    res.json({
      pageViews: {
        today: todayPageViews?.count || 0,
        yesterday: yesterdayPageViews?.count || 0,
        change: todayPageViews && yesterdayPageViews ? 
          (((todayPageViews.count || 0) - (yesterdayPageViews.count || 0)) / (yesterdayPageViews.count || 1) * 100).toFixed(2) : 0
      },
      sales: {
        today: {
          count: todaySales?.count || 0,
          revenue: todaySales?.revenue || 0
        },
        yesterday: {
          count: yesterdaySales?.count || 0,
          revenue: yesterdaySales?.revenue || 0
        },
        change: todaySales && yesterdaySales ? 
          (((todaySales.revenue || 0) - (yesterdaySales.revenue || 0)) / (yesterdaySales.revenue || 1) * 100).toFixed(2) : 0
      },
      users: {
        total: totalUsers,
        active: activeSessionsCount
      },
      conversionRate,
      topProducts,
      recentOrders
    });
  } catch (error) {
    console.error('Error fetching dashboard overview:', error);
    res.status(500).json({ error: 'Failed to fetch dashboard data' });
  }
});

// Product analytics
app.get('/api/products/analytics', async (req, res) => {
  try {
    const { sort = 'views', limit = 20, category } = req.query;
    
    const query = category ? { category } : {};
    const sortOption = {};
    
    // Validate sort field
    if (['views', 'cartAdds', 'viewToCartRate', 'price'].includes(sort)) {
      sortOption[sort] = -1;
    } else {
      sortOption.views = -1; // Default sort
    }
    
    const products = await db.collection('product_analytics')
      .find(query)
      .sort(sortOption)
      .limit(parseInt(limit))
      .toArray();
    
    // Get product categories for filtering
    const categories = await db.collection('product_analytics').distinct('category');
    
    res.json({
      products,
      categories
    });
  } catch (error) {
    console.error('Error fetching product analytics:', error);
    res.status(500).json({ error: 'Failed to fetch product data' });
  }
});

// User analytics
app.get('/api/users/analytics', async (req, res) => {
  try {
    const { limit = 20 } = req.query;
    
    // Get top users by spending
    const topUsers = await db.collection('user_analytics')
      .find()
      .sort({ totalSpent: -1 })
      .limit(parseInt(limit))
      .toArray();
    
    // Get new users in last 24 hours
    const newUsers = await db.collection('user_analytics').countDocuments({
      firstSeen: { $gte: new Date(Date.now() - 86400000).toISOString() }
    });
    
    // Get user retention data (users who came back after first visit)
    const returningUsers = await db.collection('user_analytics').countDocuments({
      $expr: { $gt: [{ $size: { $ifNull: ["$viewedProducts", []] } }, 1] }
    });
    
    res.json({
      topUsers,
      newUsers,
      returningUsers,
      totalUsers: await db.collection('user_analytics').countDocuments()
    });
  } catch (error) {
    console.error('Error fetching user analytics:', error);
    res.status(500).json({ error: 'Failed to fetch user data' });
  }
});

// Sales analytics
app.get('/api/sales/analytics', async (req, res) => {
  try {
    const { period = 'daily' } = req.query;
    
    // Get sales data for the specified period
    const now = new Date();
    let startDate;
    let groupBy;
    
    if (period === 'hourly') {
      startDate = new Date(now.getTime() - 24 * 60 * 60 * 1000); // Last 24 hours
      groupBy = { $dateToString: { format: '%Y-%m-%dT%H:00', date: { $toDate: '$timestamp' } } };
    } else if (period === 'daily') {
      startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000); // Last 30 days
      groupBy = { $dateToString: { format: '%Y-%m-%d', date: { $toDate: '$timestamp' } } };
    } else if (period === 'weekly') {
      startDate = new Date(now.getTime() - 12 * 7 * 24 * 60 * 60 * 1000); // Last 12 weeks
      groupBy = { 
        $concat: [
          { $toString: { $year: { $toDate: '$timestamp' } } },
          '-W',
          { $toString: { $week: { $toDate: '$timestamp' } } }
        ]
      };
    } else if (period === 'monthly') {
      startDate = new Date(now.getTime() - 12 * 30 * 24 * 60 * 60 * 1000); // Last 12 months
      groupBy = { $dateToString: { format: '%Y-%m', date: { $toDate: '$timestamp' } } };
    }
    
    const salesData = await db.collection('orders')
      .aggregate([
        { 
          $match: { 
            timestamp: { $gte: startDate.toISOString() } 
          } 
        },
        {
          $group: {
            _id: groupBy,
            count: { $sum: 1 },
            revenue: { $sum: '$total' },
            items: { $sum: '$items' }
          }
        },
        { $sort: { _id: 1 } }
      ])
      .toArray();
    
    // Get payment method breakdown
    const paymentMethods = await db.collection('orders')
      .aggregate([
        {
          $group: {
            _id: '$paymentMethod',
            count: { $sum: 1 },
            revenue: { $sum: '$total' }
          }
        }
      ])
      .toArray();
    
    res.json({
      salesData,
      paymentMethods
    });
  } catch (error) {
    console.error('Error fetching sales analytics:', error);
    res.status(500).json({ error: 'Failed to fetch sales data' });
  }
});

// Search analytics
app.get('/api/search/analytics', async (req, res) => {
  try {
    // Get top search terms
    const topSearchTerms = await db.collection('search_analytics')
      .find()
      .sort({ count: -1 })
      .limit(20)
      .toArray();
    
    // Get searches with no results
    const zeroResultSearches = await db.collection('searches')
      .aggregate([
        { $match: { results: 0 } },
        { $group: { _id: '$query', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 10 }
      ])
      .toArray();
    
    res.json({
      topSearchTerms,
      zeroResultSearches
    });
  } catch (error) {
    console.error('Error fetching search analytics:', error);
    res.status(500).json({ error: 'Failed to fetch search data' });
  }
});

// Real-time metrics
app.get('/api/realtime/metrics', async (req, res) => {
  try {
    const lastHour = new Date(Date.now() - 3600000).toISOString().slice(0, 16);
    
    const metrics = await db.collection('real_time_metrics')
      .find({ timestamp: { $gte: lastHour } })
      .sort({ timestamp: 1 })
      .toArray();
    
    res.json(metrics);
  } catch (error) {
    console.error('Error fetching real-time metrics:', error);
    res.status(500).json({ error: 'Failed to fetch real-time data' });
  }
});

// Start the server
async function startServer() {
  await connectToMongo();
  await connectToKafka();
  
  // Start consuming real-time metrics (for WebSocket implementation if needed)
  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      // This could be used to push updates to connected clients via WebSockets
      console.log('Received real-time metrics update');
    },
  });
  
  app.listen(PORT, () => {
    console.log(`API server running on port ${PORT}`);
  });
}

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received, closing HTTP server and Kafka consumer');
  await consumer.disconnect();
  process.exit(0);
});

// Start the server
startServer().catch(error => {
  console.error('Failed to start server:', error);
  process.exit(1);
});

console.log('API server initialized');