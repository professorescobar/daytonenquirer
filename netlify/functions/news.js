export async function handler() {
  const response = await fetch(
    "https://gnews.io/api/v4/top-headlines?country=us&token=" + process.env.GNEWS_API_KEY
  );

  const data = await response.json();

  return {
    statusCode: 200,
    headers: {
      "Access-Control-Allow-Origin": "*"
    },
    body: JSON.stringify(data)
  };
}