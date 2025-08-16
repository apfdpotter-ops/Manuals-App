// /api/data.js
export const config = { runtime: 'edge' };

export default async function handler(req) {
  const DATA_URL = process.env.DATA_URL;
  if (!DATA_URL) {
    return new Response(JSON.stringify({ error: 'DATA_URL not set' }), {
      status: 500,
      headers: { 'content-type': 'application/json; charset=utf-8' }
    });
  }

  try {
    const upstream = await fetch(DATA_URL, { cache: 'no-store' });
    if (!upstream.ok) {
      return new Response(JSON.stringify({ error: `Upstream ${upstream.status}` }), {
        status: upstream.status,
        headers: { 'content-type': 'application/json; charset=utf-8' }
      });
    }
    const body = await upstream.text(); // pass JSON through
    return new Response(body, {
      status: 200,
      headers: { 'content-type': 'application/json; charset=utf-8' }
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: { 'content-type': 'application/json; charset=utf-8' }
    });
  }
}
