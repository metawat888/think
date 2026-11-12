// Vercel Serverless API for Cross-Device Whiteboard Persistence
// Automatically uses Vercel KV / Upstash Redis if connected in Vercel Dashboard, with in-memory fallback.
const memoryRooms = global.__CLASSBOARD_ROOMS__ || (global.__CLASSBOARD_ROOMS__ = {});

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const room = (req.query.room || 'CLASSBOARD-MAIN-2026').replace(/[^a-zA-Z0-9_-]/g, '_');
  const kvUrl = process.env.KV_REST_API_URL;
  const kvToken = process.env.KV_REST_API_TOKEN;

  try {
    if (req.method === 'GET') {
      if (kvUrl && kvToken) {
        const r = await fetch(`${kvUrl}/get/board_${room}`, {
          headers: { Authorization: `Bearer ${kvToken}` }
        });
        const j = await r.json();
        if (j && j.result) {
          return res.status(200).json(typeof j.result === 'string' ? JSON.parse(j.result) : j.result);
        }
      }
      return res.status(200).json(memoryRooms[room] || { posts: [], deletedIds: [] });
    }

    if (req.method === 'POST' || req.method === 'PUT') {
      const incoming = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
      memoryRooms[room] = incoming;
      if (kvUrl && kvToken) {
        await fetch(`${kvUrl}/set/board_${room}`, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${kvToken}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(incoming)
        });
      }
      return res.status(200).json({ ok: true });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    return res.status(200).json(memoryRooms[room] || { posts: [], deletedIds: [] });
  }
}
