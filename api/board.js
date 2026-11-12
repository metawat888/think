// Vercel Serverless API for Cross-Device Permanent Whiteboard Persistence (Additive CRDT Merge)
const memoryRooms = global.__CLASSBOARD_ROOMS__ || (global.__CLASSBOARD_ROOMS__ = {});

function mergeRoomData(existing, incoming) {
  if (!existing || !Array.isArray(existing.posts)) return incoming;
  if (!incoming || typeof incoming !== 'object') return existing;
  const deletedSet = new Set([...(existing.deletedIds || []), ...(incoming.deletedIds || [])]);
  const postMap = new Map();
  for (const p of existing.posts) {
    if (p && p.id && !deletedSet.has(p.id)) postMap.set(p.id, p);
  }
  if (Array.isArray(incoming.posts)) {
    for (const rp of incoming.posts) {
      if (!rp || !rp.id || deletedSet.has(rp.id)) continue;
      const lp = postMap.get(rp.id);
      if (!lp || (rp.updatedAtMs || 0) >= (lp.updatedAtMs || 0)) {
        const mergedComments = new Map();
        for (const c of [...(lp?.comments || []), ...(rp.comments || [])]) {
          if (c && c.id && !deletedSet.has(c.id)) mergedComments.set(c.id, c);
        }
        postMap.set(rp.id, {
          ...lp,
          ...rp,
          likes: Math.max(lp?.likes || 0, rp.likes || 0),
          hearts: Math.max(lp?.hearts || 0, rp.hearts || 0),
          image: rp.image || lp?.image || '',
          comments: Array.from(mergedComments.values())
        });
      }
    }
  }
  return {
    ...existing,
    ...incoming,
    deletedIds: Array.from(deletedSet),
    posts: Array.from(postMap.values())
  };
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const room = String(req.query.room || '101').trim().toUpperCase().replace(/[^A-Z0-9_-]/g, '_');
  const kvUrl = process.env.KV_REST_API_URL;
  const kvToken = process.env.KV_REST_API_TOKEN;

  try {
    let current = memoryRooms[room] || { posts: [], deletedIds: [] };
    if (kvUrl && kvToken) {
      const r = await fetch(`${kvUrl}/get/board_${room}`, { headers: { Authorization: `Bearer ${kvToken}` } });
      const j = await r.json();
      if (j && j.result) current = typeof j.result === 'string' ? JSON.parse(j.result) : j.result;
    }

    if (req.method === 'GET') {
      return res.status(200).json(current);
    }

    if (req.method === 'POST' || req.method === 'PUT') {
      const incoming = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
      const merged = mergeRoomData(current, incoming);
      memoryRooms[room] = merged;
      if (kvUrl && kvToken) {
        await fetch(`${kvUrl}/set/board_${room}`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${kvToken}`, 'Content-Type': 'application/json' },
          body: JSON.stringify(merged)
        });
      }
      return res.status(200).json({ ok: true, count: merged.posts.length });
    }
    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    return res.status(200).json(memoryRooms[room] || { posts: [], deletedIds: [] });
  }
}
