module.exports = async (req, res) => {
  try {
    const { url, title, source, description } = req.body;

    if (!url) {
      return res.status(400).json({ error: 'URL is required' });
    }

    // If we already have a description from RSS, use that
    if (description && description.length > 50) {
      return res.status(200).json({
        summary: description,
        title,
        source,
        originalUrl: url
      });
    }

    // Otherwise fetch the article and extract the beginning
    const articleResponse = await fetch(url);
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
      originalUrl: url
    });

  } catch (err) {
    console.error('Summarize error:', err);
    res.status(500).json({ 
      error: 'Failed to load article',
      summary: description || 'Unable to load article preview.',
      title,
      source,
      originalUrl: url
    });
  }
};