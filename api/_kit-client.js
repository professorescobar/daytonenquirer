const KIT_API_BASE_URL = (process.env.KIT_API_BASE_URL || 'https://api.kit.com/v4').replace(/\/+$/, '');

function getApiKey() {
  const key = String(process.env.KIT_API_KEY || '').trim();
  if (!key) throw new Error('Missing KIT_API_KEY env var');
  return key;
}

function getSenderEmail() {
  const email = String(process.env.KIT_SENDER_EMAIL || '').trim();
  if (!email) {
    throw new Error('Missing KIT_SENDER_EMAIL env var');
  }
  return email;
}

async function requestKit(path, options = {}) {
  const res = await fetch(`${KIT_API_BASE_URL}${path}`, {
    method: options.method || 'GET',
    headers: {
      'X-Kit-Api-Key': getApiKey(),
      'Content-Type': 'application/json',
      ...(options.headers || {})
    },
    body: options.body ? JSON.stringify(options.body) : undefined
  });

  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const message = body.error || body.message || `Kit request failed (${res.status})`;
    const details = body.details || body.errors || null;
    const error = new Error(details ? `${message}: ${JSON.stringify(details)}` : message);
    error.status = res.status;
    error.payload = body;
    throw error;
  }

  return body;
}

function buildSubscriberFilter(campaign) {
  const segmentIds = Array.isArray(campaign.segmentIds) ? campaign.segmentIds : [];
  const tagIds = Array.isArray(campaign.tagIds) ? campaign.tagIds : [];
  const all = [];

  if (segmentIds.length > 0) {
    all.push({ type: 'segment', ids: segmentIds });
  }

  if (tagIds.length > 0) {
    all.push({ type: 'tag', ids: tagIds });
  }

  if (all.length === 0) {
    all.push({ type: 'all_subscribers' });
  }

  return [{ all, any: [], none: [] }];
}

function normalizeCampaignHtml(campaign) {
  const html = String(campaign.contentHtml || '').trim();
  if (html) return html;
  const text = String(campaign.contentText || '').trim();
  if (text) {
    return `<p>${text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</p>`;
  }
  return '<p>New edition from The Dayton Enquirer.</p>';
}

function normalizeCampaignText(campaign) {
  const text = String(campaign.contentText || '').trim();
  if (text) return text;
  const fallback = String(campaign.contentHtml || '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return fallback || 'New edition from The Dayton Enquirer.';
}

async function createBroadcast(campaign) {
  const nowIso = new Date().toISOString();
  const payload = {
    public: false,
    email_address: getSenderEmail(),
    content: {
      html: normalizeCampaignHtml(campaign),
      text: normalizeCampaignText(campaign)
    },
    description: String(campaign.description || campaign.title || 'Newsletter campaign'),
    published_at: nowIso,
    send_at: campaign.sendAt || nowIso,
    thumbnail_alt: 'The Dayton Enquirer',
    thumbnail_url: 'https://i.imgur.com/iB8mETG.png',
    subscriber_filter: buildSubscriberFilter(campaign),
    subject: String(campaign.subject || campaign.title || 'The Dayton Enquirer'),
    preview_text: String(campaign.previewText || '')
  };

  const result = await requestKit('/broadcasts', {
    method: 'POST',
    body: payload
  });

  return result;
}

async function getBroadcast(id) {
  return requestKit(`/broadcasts/${encodeURIComponent(id)}`);
}

async function getBroadcastStats(id) {
  return requestKit(`/broadcasts/${encodeURIComponent(id)}/stats`);
}

module.exports = {
  createBroadcast,
  getBroadcast,
  getBroadcastStats
};
