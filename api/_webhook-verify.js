const crypto = require('crypto');

function asString(value) {
  if (Array.isArray(value)) return String(value[0] || '');
  if (value == null) return '';
  return String(value);
}

function getHeader(req, name) {
  return asString(req.headers?.[name.toLowerCase()] || '');
}

function getRawBody(req) {
  if (typeof req.body === 'string') return req.body;
  if (Buffer.isBuffer(req.body)) return req.body.toString('utf8');
  if (req.body && typeof req.body === 'object') return JSON.stringify(req.body);
  return '';
}

function timingSafeEqual(a, b) {
  const left = Buffer.from(asString(a));
  const right = Buffer.from(asString(b));
  if (left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

function normalizeSignature(signature) {
  const raw = asString(signature).trim();
  if (!raw) return '';
  return raw.startsWith('sha256=') ? raw.slice('sha256='.length) : raw;
}

function verifyWithHmac(req, secret) {
  const headerCandidates = [
    getHeader(req, 'x-kit-signature'),
    getHeader(req, 'x-convertkit-signature'),
    getHeader(req, 'x-webhook-signature'),
    getHeader(req, 'x-signature')
  ].map(normalizeSignature).filter(Boolean);

  if (headerCandidates.length === 0) return { ok: false };

  const rawBody = getRawBody(req);
  const expected = crypto.createHmac('sha256', secret).update(rawBody).digest('hex');
  const matched = headerCandidates.some((candidate) => timingSafeEqual(candidate, expected));
  if (!matched) return { ok: false };

  return { ok: true, verifiedBy: 'hmac_sha256' };
}

function verifyWithSharedSecret(req, secret) {
  const provided = asString(
    req.query?.secret ||
    getHeader(req, 'x-kit-webhook-secret') ||
    getHeader(req, 'x-webhook-secret')
  ).trim();

  if (!provided) return { ok: false };
  if (!timingSafeEqual(provided, secret)) return { ok: false };

  return { ok: true, verifiedBy: 'shared_secret' };
}

function verifyKitWebhook(req) {
  const secret = asString(process.env.KIT_WEBHOOK_SECRET).trim();
  if (!secret) {
    return {
      ok: false,
      status: 500,
      error: 'Missing KIT_WEBHOOK_SECRET env var'
    };
  }

  const hmacResult = verifyWithHmac(req, secret);
  if (hmacResult.ok) return { ok: true, verifiedBy: hmacResult.verifiedBy };

  const sharedSecretResult = verifyWithSharedSecret(req, secret);
  if (sharedSecretResult.ok) {
    return { ok: true, verifiedBy: sharedSecretResult.verifiedBy };
  }

  return {
    ok: false,
    status: 401,
    error: 'Invalid webhook signature/secret'
  };
}

module.exports = { verifyKitWebhook };
