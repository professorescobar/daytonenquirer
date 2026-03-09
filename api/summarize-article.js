const dns = require('dns').promises;
const net = require('net');
const { neon } = require('@neondatabase/serverless');
const { getClientIp, applyRateLimit, setRateLimitHeaders } = require('./_rate-limit');

const SUMMARIZE_RATE_LIMIT_PER_MINUTE = 20;
const SUMMARIZE_FETCH_TIMEOUT_MS = 10_000;

function isPrivateIpv4(ip) {
  const parts = String(ip || '').split('.').map((part) => Number(part));
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return false;
  if (parts[0] === 10) return true;
  if (parts[0] === 127) return true;
  if (parts[0] === 0) return true;
  if (parts[0] === 169 && parts[1] === 254) return true;
  if (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) return true;
  if (parts[0] === 192 && parts[1] === 168) return true;
  return false;
}

function isPrivateIpv6(ip) {
  const normalized = String(ip || '').toLowerCase();
  if (!normalized) return false;
  if (normalized === '::1') return true;
  if (normalized.startsWith('fc') || normalized.startsWith('fd')) return true;
  if (normalized.startsWith('fe80:')) return true;
  return false;
}

function isPrivateOrLoopbackIp(ip) {
  const family = net.isIP(ip);
  if (family === 4) return isPrivateIpv4(ip);
  if (family === 6) return isPrivateIpv6(ip);
  return false;
}

function isBlockedHostname(hostname) {
  const value = String(hostname || '').trim().toLowerCase();
  if (!value) return true;
  if (value === 'localhost') return true;
  if (value.endsWith('.local') || value.endsWith('.internal')) return true;
  return false;
}

async function validatePublicHttpUrl(rawUrl) {
  const value = String(rawUrl || '').trim();
  if (!value) {
    const error = new Error('URL is required');
    error.statusCode = 400;
    throw error;
  }

  let parsed;
  try {
    parsed = new URL(value);
  } catch (_) {
    const error = new Error('Invalid URL');
    error.statusCode = 400;
    throw error;
  }

  if (!['http:', 'https:'].includes(parsed.protocol)) {
    const error = new Error('Only http/https URLs are allowed');
    error.statusCode = 400;
    throw error;
  }

  if (isBlockedHostname(parsed.hostname)) {
    const error = new Error('URL host is not allowed');
    error.statusCode = 400;
    throw error;
  }

  if (isPrivateOrLoopbackIp(parsed.hostname)) {
    const error = new Error('Private network targets are not allowed');
    error.statusCode = 400;
    throw error;
  }

  let addresses = [];
  try {
    addresses = await dns.lookup(parsed.hostname, { all: true, verbatim: true });
  } catch (_) {
    const error = new Error('Unable to resolve URL host');
    error.statusCode = 400;
    throw error;
  }

  if (!Array.isArray(addresses) || !addresses.length) {
    const error = new Error('Unable to resolve URL host');
    error.statusCode = 400;
    throw error;
  }

  if (addresses.some((entry) => isPrivateOrLoopbackIp(entry?.address))) {
    const error = new Error('Private network targets are not allowed');
    error.statusCode = 400;
    throw error;
  }

  return parsed.toString();
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    res.setHeader('Allow', ['POST']);
    return res.status(405).json({ error: `Method ${req.method} Not Allowed` });
  }

  const body = req.body || {};
  const urlInput = String(body.url || '').trim();
  const title = body.title;
  const source = body.source;
  const description = body.description;

  try {
    const sql = process.env.DATABASE_URL ? neon(process.env.DATABASE_URL) : null;
    const clientIp = getClientIp(req);
    const rate = await applyRateLimit({
      key: `summarize-article:${clientIp}`,
      limit: SUMMARIZE_RATE_LIMIT_PER_MINUTE,
      windowMs: 60_000,
      sql
    });
    setRateLimitHeaders(res, rate);
    if (!rate.allowed) {
      return res.status(429).json({
        error: 'Rate limit exceeded',
        details: 'Too many summary requests. Please wait and try again.'
      });
    }

    const safeUrl = await validatePublicHttpUrl(urlInput);

    // If we already have a description from RSS, use that
    if (description && description.length > 50) {
      return res.status(200).json({
        summary: description,
        title,
        source,
        originalUrl: safeUrl
      });
    }

    // Otherwise fetch the article and extract the beginning
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), SUMMARIZE_FETCH_TIMEOUT_MS);
    let articleResponse;
    try {
      articleResponse = await fetch(safeUrl, { signal: controller.signal });
    } finally {
      clearTimeout(timeoutId);
    }
    if (!articleResponse.ok) {
      throw new Error(`Upstream fetch failed (${articleResponse.status})`);
    }
    const html = await articleResponse.text();
    
    // Simple text extraction
    const text = html
      .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
      .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, '')
      .replace(/<[^>]+>/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    // Extract first 2-3 sentences (approximately 200-300 chars)
    const summary = text.slice(0, 300).split('. ').slice(0, 2).join('. ') + '.';

    res.status(200).json({
      summary,
      title,
      source,
      originalUrl: safeUrl
    });

  } catch (err) {
    console.error('Summarize error:', err);
    const isAbort = String(err?.name || '').toLowerCase() === 'aborterror';
    const statusCode = Number(err?.statusCode || 0) === 400
      ? 400
      : Number(err?.statusCode || 0) === 503
        ? 503
        : isAbort
          ? 504
          : 500;
    res.status(statusCode).json({
      error: statusCode === 400 || statusCode === 503
        ? err.message
        : statusCode === 504
          ? 'Upstream fetch timed out'
          : 'Failed to load article',
      summary: description || 'Unable to load article preview.',
      title,
      source,
      originalUrl: urlInput
    });
  }
};
