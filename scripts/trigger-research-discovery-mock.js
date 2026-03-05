require('dotenv').config();

function cleanText(value, max = 5000) {
  return String(value || '').trim().slice(0, max);
}

async function main() {
  const signalId = Number(process.argv[2] || 0);
  if (!Number.isFinite(signalId) || signalId < 1) {
    throw new Error('Usage: node scripts/trigger-research-discovery-mock.js <signalId>');
  }

  const endpoint = cleanText(process.env.INNGEST_EVENT_URL, 1000);
  if (!endpoint) throw new Error('Missing INNGEST_EVENT_URL');
  const key = cleanText(process.env.INNGEST_EVENT_KEY, 500);

  const payload = {
    name: 'signal.research_discovery.mock',
    data: {
      signalId
    }
  };

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(key ? { Authorization: `Bearer ${key}` } : {})
    },
    body: JSON.stringify(payload)
  });

  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Inngest event send failed ${response.status}: ${body.slice(0, 300)}`);
  }

  console.log(JSON.stringify({ ok: true, status: response.status, payload }, null, 2));
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
