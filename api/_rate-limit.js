const buckets = new Map();

function cleanText(value, max = 255) {
  return String(value || '').trim().slice(0, max);
}

function getClientIp(req) {
  const trustedHeaderCandidates = [
    req?.headers?.['cf-connecting-ip'],
    req?.headers?.['x-vercel-forwarded-for'],
    req?.headers?.['fly-client-ip'],
    req?.headers?.['fastly-client-ip'],
    req?.headers?.['x-real-ip']
  ];
  for (const candidate of trustedHeaderCandidates) {
    const value = cleanText(candidate || '', 120);
    if (value) return value;
  }

  // Fall back to the right-most XFF value (closest proxy hop) to reduce spoofability.
  const forwardedFor = cleanText(req?.headers?.['x-forwarded-for'] || '', 500);
  const parts = forwardedFor
    .split(',')
    .map((part) => cleanText(part, 120))
    .filter(Boolean);
  if (parts.length) return parts[parts.length - 1];

  const socketIp = cleanText(req?.socket?.remoteAddress || req?.connection?.remoteAddress || '', 120);
  return socketIp || 'unknown';
}

function evictExpired(nowMs) {
  for (const [key, entry] of buckets.entries()) {
    if (!entry || entry.resetAtMs <= nowMs) buckets.delete(key);
  }
}

function applyInMemoryRateLimit({ key, limit, windowMs, nowMs = Date.now() }) {
  evictExpired(nowMs);
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 1;
  const safeWindowMs = Number.isFinite(windowMs) && windowMs > 0 ? Math.floor(windowMs) : 60_000;
  const bucketKey = cleanText(key, 300) || 'default';
  const current = buckets.get(bucketKey);

  if (!current || current.resetAtMs <= nowMs) {
    const next = { count: 1, resetAtMs: nowMs + safeWindowMs };
    buckets.set(bucketKey, next);
    return {
      allowed: true,
      limit: safeLimit,
      remaining: Math.max(0, safeLimit - 1),
      resetAtMs: next.resetAtMs,
      retryAfterSeconds: Math.ceil(safeWindowMs / 1000)
    };
  }

  if (current.count >= safeLimit) {
    return {
      allowed: false,
      limit: safeLimit,
      remaining: 0,
      resetAtMs: current.resetAtMs,
      retryAfterSeconds: Math.max(1, Math.ceil((current.resetAtMs - nowMs) / 1000))
    };
  }

  current.count += 1;
  buckets.set(bucketKey, current);
  return {
    allowed: true,
    limit: safeLimit,
    remaining: Math.max(0, safeLimit - current.count),
    resetAtMs: current.resetAtMs,
    retryAfterSeconds: Math.max(1, Math.ceil((current.resetAtMs - nowMs) / 1000))
  };
}

async function applyPostgresRateLimit({ sql, key, limit, windowMs, nowMs = Date.now() }) {
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 1;
  const safeWindowMs = Number.isFinite(windowMs) && windowMs > 0 ? Math.floor(windowMs) : 60_000;
  const bucketKey = cleanText(key, 300) || 'default';
  const windowStartMs = Math.floor(nowMs / safeWindowMs) * safeWindowMs;
  const resetAtMs = windowStartMs + safeWindowMs;
  const windowStartIso = new Date(windowStartMs).toISOString();

  try {
    const rows = await sql`
      INSERT INTO api_rate_limits (
        limiter_key,
        window_start,
        request_count,
        updated_at
      )
      VALUES (
        ${bucketKey},
        ${windowStartIso}::timestamptz,
        1,
        NOW()
      )
      ON CONFLICT (limiter_key, window_start) DO UPDATE
      SET
        request_count = api_rate_limits.request_count + 1,
        updated_at = NOW()
      RETURNING request_count::int as "count"
    `;
    const count = Number(rows?.[0]?.count || 0);
    const allowed = count > 0 && count <= safeLimit;
    const remaining = allowed
      ? Math.max(0, safeLimit - count)
      : 0;

    return {
      allowed,
      limit: safeLimit,
      remaining,
      resetAtMs,
      retryAfterSeconds: Math.max(1, Math.ceil((resetAtMs - nowMs) / 1000))
    };
  } catch (error) {
    if (String(error?.message || '').toLowerCase().includes('api_rate_limits')) {
      const schemaError = new Error('Schema not ready: missing table api_rate_limits');
      schemaError.statusCode = 503;
      throw schemaError;
    }
    throw error;
  }
}

async function applyRateLimit({ key, limit, windowMs, nowMs = Date.now(), sql = null }) {
  if (sql) {
    return applyPostgresRateLimit({ sql, key, limit, windowMs, nowMs });
  }
  return applyInMemoryRateLimit({ key, limit, windowMs, nowMs });
}

function setRateLimitHeaders(res, rate) {
  res.setHeader('X-RateLimit-Limit', String(rate.limit));
  res.setHeader('X-RateLimit-Remaining', String(rate.remaining));
  res.setHeader('X-RateLimit-Reset', String(Math.floor(rate.resetAtMs / 1000)));
  if (!rate.allowed) {
    res.setHeader('Retry-After', String(rate.retryAfterSeconds));
  }
}

module.exports = {
  getClientIp,
  applyRateLimit,
  setRateLimitHeaders
};
