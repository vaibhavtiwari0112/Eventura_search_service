const redis = require("../config/redis");

const TITLE_ZSET = "movies:titles";
const POP_ZSET = "movies:popularity";
const RELEASE_ZSET = "movies:release";

function normalize(str) {
  return (str || "").trim().toLowerCase();
}

function lexRange(prefix) {
  const min = `[${prefix}`;
  const max = `[${prefix}\xff`;
  return { min, max };
}

/**
 * Index a single movie into Redis
 */
async function indexMovie(movie) {
  if (!movie?.id || !movie?.title) return;

  const titleLower = normalize(movie.title);
  const member = `${titleLower}|${movie.id}`;
  const pipeline = redis.pipeline();

  pipeline.zadd(TITLE_ZSET, 0, member);
  pipeline.zadd(POP_ZSET, 0, movie.id);
  pipeline.zadd(RELEASE_ZSET, movie.releaseUnix || Date.now(), movie.id);
  pipeline.hmset(`movie:${movie.id}`, movie);

  await pipeline.exec();
}

/**
 * Optimized: Batch index multiple movies using Redis pipeline
 */
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

async function incrementPopularity(movieId) {
  await redis.zincrby(POP_ZSET, 1, movieId);
}

/**
 * Autocomplete optimized with multi-field ranking
 */
async function autocomplete(query, limit = 5) {
  const q = normalize(query);
  const { min, max } = lexRange(q);

  // Early return: if no query, return top trending by popularity
  if (!q) {
    const topIds = await redis.zrevrange(POP_ZSET, 0, limit - 1);
    if (!topIds.length) return [];
    const pipeline = redis.pipeline();
    topIds.forEach((id) => pipeline.hgetall(`movie:${id}`));
    const raw = await pipeline.exec();
    return raw.map(([err, movie]) => movie).filter(Boolean);
  }

  const members = await redis.zrangebylex(
    TITLE_ZSET,
    min,
    max,
    "LIMIT",
    0,
    100
  );
  if (!members.length) return [];

  const ids = [...new Set(members.map((m) => m.split("|")[1]))];
  const pipeline = redis.pipeline();
  ids.forEach((id) => {
    pipeline.hgetall(`movie:${id}`);
    pipeline.zscore(POP_ZSET, id);
    pipeline.zscore(RELEASE_ZSET, id);
  });

  const raw = await pipeline.exec();
  const now = Date.now();
  const results = [];

  for (let i = 0; i < ids.length; i++) {
    const meta = raw[i * 3][1];
    if (!meta || !meta.title) continue;
    const pop = parseFloat(raw[i * 3 + 1][1] || 0);
    const release = parseFloat(raw[i * 3 + 2][1] || 0);
    results.push({ id: ids[i], meta, pop, release });
  }

  const maxPop = Math.max(...results.map((r) => r.pop), 1);
  const maxDelta = Math.max(...results.map((r) => now - (r.release || now)), 1);

  const ranked = results.map((r) => {
    const prefixScore = r.meta.title?.toLowerCase().startsWith(q) ? 1 : 0.5;
    const recency = 1 - (now - r.release) / maxDelta;
    const score = 0.6 * prefixScore + 0.3 * (r.pop / maxPop) + 0.1 * recency;
    return { ...r.meta, score };
  });

  ranked.sort((a, b) => b.score - a.score);
  return ranked.slice(0, limit);
}

module.exports = {
  indexMovie,
  indexMoviesBatch,
  incrementPopularity,
  autocomplete,
};
