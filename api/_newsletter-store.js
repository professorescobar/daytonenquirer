const { neon } = require('@neondatabase/serverless');

function getSql() {
  if (!process.env.DATABASE_URL) {
    throw new Error('Missing DATABASE_URL env var');
  }
  return neon(process.env.DATABASE_URL);
}

async function ensureNewsletterTables(sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS newsletter_campaigns (
      id SERIAL PRIMARY KEY,
      title TEXT NOT NULL,
      subject TEXT NOT NULL DEFAULT '',
      preview_text TEXT DEFAULT '',
      description TEXT DEFAULT '',
      content_html TEXT DEFAULT '',
      content_text TEXT DEFAULT '',
      segment_ids JSONB DEFAULT '[]'::jsonb,
      tag_ids JSONB DEFAULT '[]'::jsonb,
      status TEXT NOT NULL DEFAULT 'draft',
      kit_broadcast_id BIGINT,
      kit_status TEXT,
      kit_progress INTEGER DEFAULT 0,
      send_at TIMESTAMP,
      sent_at TIMESTAMP,
      last_synced_at TIMESTAMP,
      created_by TEXT DEFAULT 'admin_ui',
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_newsletter_campaigns_created_at
    ON newsletter_campaigns(created_at DESC)
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_newsletter_campaigns_status
    ON newsletter_campaigns(status)
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS newsletter_events (
      id SERIAL PRIMARY KEY,
      campaign_id INTEGER REFERENCES newsletter_campaigns(id) ON DELETE CASCADE,
      provider TEXT NOT NULL,
      event_type TEXT NOT NULL,
      payload_json JSONB NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `;

  await sql`
    CREATE INDEX IF NOT EXISTS idx_newsletter_events_campaign_created
    ON newsletter_events(campaign_id, created_at DESC)
  `;
}

function normalizeIntArray(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => Number(item))
      .filter((num) => Number.isInteger(num) && num > 0);
  }
  const raw = String(value || '').trim();
  if (!raw) return [];
  return raw
    .split(',')
    .map((item) => Number(item.trim()))
    .filter((num) => Number.isInteger(num) && num > 0);
}

function asText(value, fallback = '') {
  const out = String(value == null ? '' : value).trim();
  return out || fallback;
}

function safeJsonArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_) {
      return [];
    }
  }
  return [];
}

function mapCampaign(row) {
  if (!row) return null;
  return {
    id: row.id,
    title: row.title,
    subject: row.subject || '',
    previewText: row.previewText || '',
    description: row.description || '',
    contentHtml: row.contentHtml || '',
    contentText: row.contentText || '',
    segmentIds: safeJsonArray(row.segmentIds),
    tagIds: safeJsonArray(row.tagIds),
    status: row.status,
    kitBroadcastId: row.kitBroadcastId == null ? null : Number(row.kitBroadcastId),
    kitStatus: row.kitStatus || null,
    kitProgress: Number(row.kitProgress || 0),
    sendAt: row.sendAt || null,
    sentAt: row.sentAt || null,
    lastSyncedAt: row.lastSyncedAt || null,
    createdBy: row.createdBy || 'admin_ui',
    createdAt: row.createdAt,
    updatedAt: row.updatedAt
  };
}

async function listCampaigns(sql, options = {}) {
  const status = asText(options.status, 'all');
  const limit = Math.min(Math.max(Number(options.limit || 25), 1), 200);
  const rows = await sql`
    SELECT
      id,
      title,
      subject,
      preview_text as "previewText",
      description,
      content_html as "contentHtml",
      content_text as "contentText",
      segment_ids as "segmentIds",
      tag_ids as "tagIds",
      status,
      kit_broadcast_id as "kitBroadcastId",
      kit_status as "kitStatus",
      kit_progress as "kitProgress",
      send_at as "sendAt",
      sent_at as "sentAt",
      last_synced_at as "lastSyncedAt",
      created_by as "createdBy",
      created_at as "createdAt",
      updated_at as "updatedAt"
    FROM newsletter_campaigns
    WHERE (${status} = 'all' OR status = ${status})
    ORDER BY created_at DESC
    LIMIT ${limit}
  `;
  return rows.map(mapCampaign);
}

async function getCampaignById(sql, id) {
  const numericId = Number(id);
  if (!Number.isInteger(numericId) || numericId <= 0) return null;
  const rows = await sql`
    SELECT
      id,
      title,
      subject,
      preview_text as "previewText",
      description,
      content_html as "contentHtml",
      content_text as "contentText",
      segment_ids as "segmentIds",
      tag_ids as "tagIds",
      status,
      kit_broadcast_id as "kitBroadcastId",
      kit_status as "kitStatus",
      kit_progress as "kitProgress",
      send_at as "sendAt",
      sent_at as "sentAt",
      last_synced_at as "lastSyncedAt",
      created_by as "createdBy",
      created_at as "createdAt",
      updated_at as "updatedAt"
    FROM newsletter_campaigns
    WHERE id = ${numericId}
    LIMIT 1
  `;
  return mapCampaign(rows[0]);
}

async function getCampaignByKitBroadcastId(sql, kitBroadcastId) {
  const numericId = Number(kitBroadcastId);
  if (!Number.isInteger(numericId) || numericId <= 0) return null;
  const rows = await sql`
    SELECT
      id,
      title,
      subject,
      preview_text as "previewText",
      description,
      content_html as "contentHtml",
      content_text as "contentText",
      segment_ids as "segmentIds",
      tag_ids as "tagIds",
      status,
      kit_broadcast_id as "kitBroadcastId",
      kit_status as "kitStatus",
      kit_progress as "kitProgress",
      send_at as "sendAt",
      sent_at as "sentAt",
      last_synced_at as "lastSyncedAt",
      created_by as "createdBy",
      created_at as "createdAt",
      updated_at as "updatedAt"
    FROM newsletter_campaigns
    WHERE kit_broadcast_id = ${numericId}
    LIMIT 1
  `;
  return mapCampaign(rows[0]);
}

async function createCampaign(sql, payload = {}) {
  const title = asText(payload.title, `Newsletter ${new Date().toLocaleString('en-US')}`);
  const subject = asText(payload.subject);
  const previewText = asText(payload.previewText);
  const description = asText(payload.description);
  const contentHtml = String(payload.contentHtml || '');
  const contentText = String(payload.contentText || '');
  const segmentIds = normalizeIntArray(payload.segmentIds);
  const tagIds = normalizeIntArray(payload.tagIds);
  const sendAt = asText(payload.sendAt) || null;

  const rows = await sql`
    INSERT INTO newsletter_campaigns (
      title,
      subject,
      preview_text,
      description,
      content_html,
      content_text,
      segment_ids,
      tag_ids,
      send_at
    )
    VALUES (
      ${title},
      ${subject},
      ${previewText},
      ${description},
      ${contentHtml},
      ${contentText},
      ${JSON.stringify(segmentIds)},
      ${JSON.stringify(tagIds)},
      ${sendAt}
    )
    RETURNING
      id,
      title,
      subject,
      preview_text as "previewText",
      description,
      content_html as "contentHtml",
      content_text as "contentText",
      segment_ids as "segmentIds",
      tag_ids as "tagIds",
      status,
      kit_broadcast_id as "kitBroadcastId",
      kit_status as "kitStatus",
      kit_progress as "kitProgress",
      send_at as "sendAt",
      sent_at as "sentAt",
      last_synced_at as "lastSyncedAt",
      created_by as "createdBy",
      created_at as "createdAt",
      updated_at as "updatedAt"
  `;
  return mapCampaign(rows[0]);
}

async function updateCampaign(sql, id, payload = {}) {
  const current = await getCampaignById(sql, id);
  if (!current) return null;

  const title = asText(payload.title, current.title);
  const subject = payload.subject == null ? current.subject : asText(payload.subject);
  const previewText = payload.previewText == null ? current.previewText : asText(payload.previewText);
  const description = payload.description == null ? current.description : asText(payload.description);
  const contentHtml = payload.contentHtml == null ? current.contentHtml : String(payload.contentHtml || '');
  const contentText = payload.contentText == null ? current.contentText : String(payload.contentText || '');
  const segmentIds = payload.segmentIds == null ? current.segmentIds : normalizeIntArray(payload.segmentIds);
  const tagIds = payload.tagIds == null ? current.tagIds : normalizeIntArray(payload.tagIds);
  const sendAt = payload.sendAt === undefined ? current.sendAt : (asText(payload.sendAt) || null);

  const rows = await sql`
    UPDATE newsletter_campaigns
    SET
      title = ${title},
      subject = ${subject},
      preview_text = ${previewText},
      description = ${description},
      content_html = ${contentHtml},
      content_text = ${contentText},
      segment_ids = ${JSON.stringify(segmentIds)},
      tag_ids = ${JSON.stringify(tagIds)},
      send_at = ${sendAt},
      updated_at = NOW()
    WHERE id = ${Number(id)}
    RETURNING
      id,
      title,
      subject,
      preview_text as "previewText",
      description,
      content_html as "contentHtml",
      content_text as "contentText",
      segment_ids as "segmentIds",
      tag_ids as "tagIds",
      status,
      kit_broadcast_id as "kitBroadcastId",
      kit_status as "kitStatus",
      kit_progress as "kitProgress",
      send_at as "sendAt",
      sent_at as "sentAt",
      last_synced_at as "lastSyncedAt",
      created_by as "createdBy",
      created_at as "createdAt",
      updated_at as "updatedAt"
  `;
  return mapCampaign(rows[0]);
}

async function updateCampaignDelivery(sql, id, fields = {}) {
  const numericId = Number(id);
  if (!Number.isInteger(numericId) || numericId <= 0) return null;
  const status = asText(fields.status, 'draft');
  const kitStatus = asText(fields.kitStatus) || null;
  const kitProgress = Math.max(0, Math.min(100, Number(fields.kitProgress || 0)));
  const kitBroadcastId = fields.kitBroadcastId == null ? null : Number(fields.kitBroadcastId);
  const sentAt = fields.sentAt || null;
  const lastSyncedAt = fields.lastSyncedAt || new Date().toISOString();
  const rows = await sql`
    UPDATE newsletter_campaigns
    SET
      status = ${status},
      kit_status = ${kitStatus},
      kit_progress = ${kitProgress},
      kit_broadcast_id = COALESCE(${kitBroadcastId}, kit_broadcast_id),
      sent_at = COALESCE(${sentAt}, sent_at),
      last_synced_at = ${lastSyncedAt},
      updated_at = NOW()
    WHERE id = ${numericId}
    RETURNING
      id,
      title,
      subject,
      preview_text as "previewText",
      description,
      content_html as "contentHtml",
      content_text as "contentText",
      segment_ids as "segmentIds",
      tag_ids as "tagIds",
      status,
      kit_broadcast_id as "kitBroadcastId",
      kit_status as "kitStatus",
      kit_progress as "kitProgress",
      send_at as "sendAt",
      sent_at as "sentAt",
      last_synced_at as "lastSyncedAt",
      created_by as "createdBy",
      created_at as "createdAt",
      updated_at as "updatedAt"
  `;
  return mapCampaign(rows[0]);
}

async function appendCampaignEvent(sql, fields = {}) {
  const campaignId = Number(fields.campaignId);
  if (!Number.isInteger(campaignId) || campaignId <= 0) return;
  const provider = asText(fields.provider, 'kit');
  const eventType = asText(fields.eventType, 'unknown');
  const payload = fields.payload && typeof fields.payload === 'object' ? fields.payload : {};
  await sql`
    INSERT INTO newsletter_events (campaign_id, provider, event_type, payload_json)
    VALUES (${campaignId}, ${provider}, ${eventType}, ${JSON.stringify(payload)})
  `;
}

module.exports = {
  getSql,
  ensureNewsletterTables,
  listCampaigns,
  getCampaignById,
  getCampaignByKitBroadcastId,
  createCampaign,
  updateCampaign,
  updateCampaignDelivery,
  appendCampaignEvent
};
