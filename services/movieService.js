const redis = require("../config/redis");

const TITLE_ZSET = "movies:titles";
const POP_ZSET = "movies:popularity";
const RELEASE_ZSET = "movies:release";
const CACHE_PREFIX = "cache:autocomplete:";

function normalize(str) {
  return (str || "").trim().toLowerCase();
}

function lexRange(prefix) {
  const min = `[${prefix}`;
  const max = `[${prefix}\xff`;
  return { min, max };
}

/* -------------------------------------------
   Index a single movie
-------------------------------------------- */
async function indexMovie(movie) {
  if (!movie?.id || !movie?.title) return;

  const titleLower = normalize(movie.title);
  const member = `${titleLower}|${movie.id}`;
  const pipeline = redis.pipeline();
  const now = movie.releaseUnix || Date.now();

  pipeline.zadd(TITLE_ZSET, 0, member);
  pipeline.zadd(POP_ZSET, 0, movie.id);
  pipeline.zadd(RELEASE_ZSET, now, movie.id);
  pipeline.hmset(`movie:${movie.id}`, movie);

  await pipeline.exec();
}

/* -------------------------------------------
   Batch index multiple movies efficiently
-------------------------------------------- */
async function indexMoviesBatch(movies = []) {
  if (!Array.isArray(movies) || movies.length === 0) return;

  const pipeline = redis.pipeline();
  const now = Date.now();

  for (const movie of movies) {
    if (!movie?.id || !movie?.title) continue;
    const titleLower = normalize(movie.title);
    const member = `${titleLower}|${movie.id}`;
    pipeline.zadd(TITLE_ZSET, 0, member);
    pipeline.zadd(POP_ZSET, 0, movie.id);
    pipeline.zadd(RELEASE_ZSET, movie.releaseUnix || now, movie.id);
    pipeline.hmset(`movie:${movie.id}`, movie);
  }

  await pipeline.exec();
}

/* -------------------------------------------
   Increment popularity for ranking
-------------------------------------------- */
async function incrementPopularity(movieId) {
  await redis.zincrby(POP_ZSET, 1, movieId);
}

/* -------------------------------------------
   Optimized Autocomplete
   - Uses Redis cache (10s)
   - Fetches metadata including description
   - Weighted scoring: prefix + popularity + recency
-------------------------------------------- */
async function autocomplete(query, limit = 5) {
  const q = normalize(query);
  const cacheKey = `${CACHE_PREFIX}${q || "trending"}`;

  // Step 1: Cache check
  const cached = await redis.get(cacheKey);
  if (cached) return JSON.parse(cached);

  // Step 2: Empty query = trending fallback
  if (!q) {
    const topIds = await redis.zrevrange(POP_ZSET, 0, limit - 1);
    if (!topIds.length) return [];
    const pipeline = redis.pipeline();
    topIds.forEach((id) => pipeline.hgetall(`movie:${id}`));
    const raw = await pipeline.exec();
    const movies = raw.map(([err, data]) => data).filter(Boolean);
    await redis.setex(cacheKey, 10, JSON.stringify(movies));
    return movies;
  }

  // Step 3: Range match by prefix
  const { min, max } = lexRange(q);
  const members = await redis.zrangebylex(TITLE_ZSET, min, max, "LIMIT", 0, 30);
  if (!members.length) return [];

  const ids = [...new Set(members.map((m) => m.split("|")[1]))];

  // Step 4: Single pipeline fetch (meta + pop + release + description + duration)
  const pipeline = redis.pipeline();
  ids.forEach((id) =>
    pipeline.hmget(
      `movie:${id}`,
      "title",
      "poster_url",
      "rating",
      "genres",
      "releaseUnix",
      "description",
      "duration_minutes"
    )
  );
  ids.forEach((id) => pipeline.zscore(POP_ZSET, id));

  const raw = await pipeline.exec();
  const now = Date.now();
  const results = [];

  for (let i = 0; i < ids.length; i++) {
    const [
      title,
      poster_url,
      rating,
      genres,
      releaseUnix,
      description,
      duration,
    ] = raw[i][1] || [];
    const pop = parseFloat(raw[ids.length + i][1] || 0);
    if (!title) continue;
    results.push({
      id: ids[i],
      title,
      poster_url,
      rating,
      genres,
      pop,
      release: parseFloat(releaseUnix || now),
      description,
      duration_minutes: parseInt(duration) || 0,
    });
  }

  // Step 5: Rank by prefix match, popularity, recency
  const maxPop = Math.max(...results.map((r) => r.pop), 1);
  const maxDelta = Math.max(...results.map((r) => now - (r.release || now)), 1);

  const ranked = results
    .map((r) => {
      const prefixScore = r.title?.toLowerCase().startsWith(q) ? 1 : 0.5;
      const recency = 1 - (now - r.release) / maxDelta;
      const score =
        0.65 * prefixScore + 0.25 * (r.pop / maxPop) + 0.1 * recency;
      return { ...r, score };
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);

  // Step 6: Cache results (10s)
  await redis.setex(cacheKey, 10, JSON.stringify(ranked));

  return ranked;
}

module.exports = {
  indexMovie,
  indexMoviesBatch,
  incrementPopularity,
  autocomplete,
};
