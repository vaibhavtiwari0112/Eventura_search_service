const express = require("express");
const router = express.Router();
const {
  indexMovie,
  indexMoviesBatch,
  incrementPopularity,
  autocomplete,
} = require("../services/movieService");

// ✅ Index single movie
router.post("/movie", async (req, res) => {
  try {
    await indexMovie(req.body);
    res.json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to index movie" });
  }
});

// ✅ Batch index multiple movies
router.post("/movies/batch", async (req, res) => {
  try {
    const { movies } = req.body;
    if (!Array.isArray(movies) || movies.length === 0) {
      return res
        .status(400)
        .json({ error: "Movies must be a non-empty array" });
    }

    await indexMoviesBatch(movies);
    res.json({ message: `${movies.length} movies indexed successfully` });
  } catch (err) {
    console.error("Batch insert error:", err);
    res.status(500).json({ error: "Failed to index movies batch" });
  }
});

// ✅ Increment popularity
router.post("/movie/:id/view", async (req, res) => {
  try {
    await incrementPopularity(req.params.id);
    res.json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to increment popularity" });
  }
});

// ✅ Autocomplete
router.get("/autocomplete", async (req, res) => {
  try {
    const suggestions = await autocomplete(
      req.query.q || "",
      req.query.limit || 5
    );
    res.json({ suggestions });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to autocomplete" });
  }
});

module.exports = router;
