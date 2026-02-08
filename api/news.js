export default async function handler(req, res) {
  const url = `https://gnews.io/api/v4/top-headlines?country=us&token=f64ccb8988099959940262b663ed0c24`;

  try {
    const response = await fetch(url);
    const data = await response.json();
    res.status(200).json(data);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch news" });
  }
}
