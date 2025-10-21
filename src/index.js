require("dotenv").config();
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const movieRoutes = require("../routes/movieRoutes");
const redis = require("../config/redis"); // initializes Redis connection

const app = express();

// CORS configuration
app.use(
  cors({
    origin: process.env.CORS_ORIGIN || "*", // allow all origins by default
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

// Middleware
app.use(bodyParser.json());
app.use("/", movieRoutes);

// Health check endpoint
app.get("/health", async (req, res) => {
  try {
    const ping = await redis.ping();
    res.json({ status: "ok", redis: ping });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

// Export for Vercel (serverless)
module.exports = app;

// Local development server
if (require.main === module) {
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () =>
    console.log(`🚀 Eventura Autocomplete running on port ${PORT}`)
  );
}
