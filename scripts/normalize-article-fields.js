const fs = require('fs');

const FILE = 'content/custom-articles.json';

function stripTags(text) {
  return String(text || '').replace(/<[^>]*>/g, ' ');
}

function cleanWhitespace(text) {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
}

function buildSummary(longText, maxChars = 260) {
  const plain = cleanWhitespace(stripTags(longText));
  if (!plain) return '';

  const sentences = plain.split(/(?<=[.!?])\s+/).filter(Boolean);
  if (!sentences.length) {
    return plain.length > maxChars ? `${plain.slice(0, maxChars - 3).trim()}...` : plain;
  }

  let summary = '';
  for (const sentence of sentences) {
    const next = summary ? `${summary} ${sentence}` : sentence;
    if (next.length > maxChars) break;
    summary = next;
    if (summary.length >= 160) break;
  }

  if (!summary) {
    summary = plain.length > maxChars ? `${plain.slice(0, maxChars - 3).trim()}...` : plain;
  }

  return summary;
}

function looksLikeLongBody(text) {
  const t = String(text || '');
  return t.length > 500 || t.includes('\n\n');
}

function normalizeArticles(articles) {
  let movedBodyCount = 0;
  let alreadyStructuredCount = 0;
  let unchangedCount = 0;

  const normalized = articles.map((article) => {
    const description = String(article.description || '').trim();
    const content = String(article.content || '').trim();

    if (!description && !content) {
      unchangedCount += 1;
      return article;
    }

    if (!content && looksLikeLongBody(description)) {
      movedBodyCount += 1;
      return {
        ...article,
        description: buildSummary(description),
        content: description
      };
    }

    if (content) {
      alreadyStructuredCount += 1;
      return {
        ...article,
        description: buildSummary(description || content),
        content
      };
    }

    unchangedCount += 1;
    return article;
  });

  return {
    normalized,
    stats: {
      total: articles.length,
      movedBodyCount,
      alreadyStructuredCount,
      unchangedCount
    }
  };
}

function main() {
  const raw = fs.readFileSync(FILE, 'utf8');
  const articles = JSON.parse(raw);
  const { normalized, stats } = normalizeArticles(articles);
  fs.writeFileSync(FILE, `${JSON.stringify(normalized, null, 2)}\n`);
  console.log('Normalization complete:', stats);
}

main();
