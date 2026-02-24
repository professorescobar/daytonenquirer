function getKitConfig() {
  const apiKey = String(process.env.KIT_API_KEY || '').trim();
  if (!apiKey) throw new Error('Missing KIT_API_KEY env var');
  const baseUrl = String(process.env.KIT_API_BASE_URL || 'https://api.kit.com/v4').replace(/\/+$/, '');
  const defaultTagId = Number(process.env.KIT_NEWSLETTER_TAG_ID || '');
  return {
    apiKey,
    baseUrl,
    defaultTagId: Number.isInteger(defaultTagId) && defaultTagId > 0 ? defaultTagId : null
  };
}

function isValidEmail(email) {
  const value = String(email || '').trim();
  if (!value || value.length > 320) return false;
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function getClientIp(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').trim();
  if (forwarded) return forwarded.split(',')[0].trim();
  return String(req.socket?.remoteAddress || '').trim();
}

async function verifyTurnstile(token, ip) {
  const secret = String(process.env.TURNSTILE_SECRET_KEY || '').trim();
  if (!secret) return { ok: true };
  if (!token) return { ok: false, message: 'Please verify you are human.' };

  const form = new URLSearchParams();
  form.set('secret', secret);
  form.set('response', token);
  if (ip) form.set('remoteip', ip);

  const res = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: form
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data?.success) {
    return {
      ok: false,
      message: 'Verification failed. Please try again.'
    };
  }

  return { ok: true };
}

async function requestKit(config, path, body) {
  const res = await fetch(`${config.baseUrl}${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Kit-Api-Key': config.apiKey
    },
    body: JSON.stringify(body || {})
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const error = new Error(data.error || data.message || `Kit request failed (${res.status})`);
    error.status = res.status;
    error.payload = data;
    throw error;
  }

  return data;
}

function extractSubscriberId(payload) {
  const candidates = [
    payload?.subscriber?.id,
    payload?.id,
    payload?.data?.id,
    payload?.data?.subscriber?.id
  ];

  for (const item of candidates) {
    const id = Number(item);
    if (Number.isInteger(id) && id > 0) return id;
  }
  return null;
}

async function createOrUpsertSubscriber(config, email) {
  try {
    return await requestKit(config, '/subscribers', {
      email_address: email,
      state: 'active'
    });
  } catch (error) {
    if (error.status === 409) {
      return { alreadyExists: true };
    }
    throw error;
  }
}

async function attachTag(config, subscriberId, tagId) {
  if (!subscriberId || !tagId) return;

  try {
    await requestKit(config, `/subscribers/${encodeURIComponent(subscriberId)}/tags`, {
      tag_id: tagId
    });
  } catch (error) {
    console.warn('Newsletter tag attach skipped:', error.message);
  }
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const config = getKitConfig();
    const email = String(req.body?.email || '').trim().toLowerCase();
    const honeypot = String(req.body?.company || '').trim();
    const turnstileToken = String(req.body?.turnstileToken || '').trim();

    if (honeypot) {
      return res.status(200).json({ ok: true });
    }

    if (!isValidEmail(email)) {
      return res.status(400).json({ error: 'Please enter a valid email address.' });
    }

    const turnstileCheck = await verifyTurnstile(turnstileToken, getClientIp(req));
    if (!turnstileCheck.ok) {
      return res.status(400).json({ error: turnstileCheck.message });
    }

    const upsertResult = await createOrUpsertSubscriber(config, email);
    const subscriberId = extractSubscriberId(upsertResult);

    if (config.defaultTagId) {
      await attachTag(config, subscriberId, config.defaultTagId);
    }

    return res.status(200).json({
      ok: true,
      alreadyExists: !!upsertResult?.alreadyExists
    });
  } catch (error) {
    console.error('Newsletter subscribe error:', error);
    return res.status(500).json({ error: 'Failed to subscribe. Please try again.' });
  }
};
