const Redis = require("ioredis");
require("dotenv").config();

let redis;

if (process.env.REDIS_URL) {
  // Use Redis Cloud connection string
  redis = new Redis(process.env.REDIS_URL);
  console.log("Using Redis Cloud connection");
} else {
  // Fallback to local Redis (useful for local Docker)
  redis = new Redis({
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: process.env.REDIS_PORT || 6379,
  });
  console.log("Using local Redis connection");
}

redis.on("connect", () => console.log("✅ Connected to Redis"));
redis.on("error", (err) => console.error("❌ Redis Error:", err));

module.exports = redis;
