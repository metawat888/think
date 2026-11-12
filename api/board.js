// Vercel Serverless API Connected to Turso LibSQL (libsql://ailsed-metawat888.aws-ap-southeast-1.turso.io)
const DEFAULT_TURSO_URL = process.env.TURSO_DATABASE_URL || 'libsql://ailsed-metawat888.aws-ap-southeast-1.turso.io';
const memoryRooms = global.__THINK_BOARD_ROOMS__ || (global.__THINK_BOARD_ROOMS__ = {});

function getTursoHttpEndpoint(urlStr) {
  return String(urlStr || DEFAULT_TURSO_URL)
    .trim()
    .replace(/^libsql:\/\//i, 'https://')
    .replace(/^wss?:\/\//i, 'https://')
    .replace(/\/+$/, '') + '/v2/pipeline';
}

async function tursoPipeline(statements, authToken) {
  const endpoint = getTursoHttpEndpoint(DEFAULT_TURSO_URL);
  const headers = { 'Content-Type': 'application/json' };
  const token = authToken || process.env.TURSO_AUTH_TOKEN || '';
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const requests = statements.map(s => ({
    type: 'execute',
    stmt: {
      sql: s.sql,
      args: (s.args || []).map(a => {
        if (typeof a === 'number') return { type: 'integer', value: String(Math.trunc(a)) };
        if (a === null || a === undefined) return { type: 'null' };
        return { type: 'text', value: String(a) };
      })
    }
  }));
  requests.push({ type: 'close' });

  const res = await fetch(endpoint, {
    method: 'POST',
    headers,
    body: JSON.stringify({ requests })
  });
  if (!res.ok) return null;
  return await res.json();
}

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
          youtubeUrl: rp.youtubeUrl || lp?.youtubeUrl || '',
          youtubeId: rp.youtubeId || lp?.youtubeId || '',
          webRunUrl: rp.webRunUrl || lp?.webRunUrl || '',
          webAutoCmd: rp.webAutoCmd !== undefined ? rp.webAutoCmd : (lp?.webAutoCmd || ''),
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
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-turso-token');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const room = String(req.query.room || 'DEFAULT').trim().toUpperCase();
  const clientToken = req.headers['x-turso-token'] || '';

  try {
    await tursoPipeline([
      {
        sql: `CREATE TABLE IF NOT EXISTS think_posts (
          id TEXT PRIMARY KEY,
          room_code TEXT NOT NULL,
          post_json TEXT NOT NULL,
          updated_at INTEGER NOT NULL,
          deleted INTEGER NOT NULL DEFAULT 0
        )`
      }
    ], clientToken);

    let current = memoryRooms[room] || { posts: [], deletedIds: [] };

    const dbRead = await tursoPipeline([
      {
        sql: `SELECT id, post_json, updated_at, deleted FROM think_posts WHERE room_code = ?`,
        args: [room]
      }
    ], clientToken);

    if (dbRead && dbRead.results && dbRead.results[0]?.response?.result?.rows) {
      const rows = dbRead.results[0].response.result.rows;
      const dbPosts = [];
      const dbDeleted = [];
      for (const r of rows) {
        const idVal = r[0]?.value;
        const jsonVal = r[1]?.value;
        const delVal = Number(r[3]?.value || 0);
        if (delVal === 1 && idVal) dbDeleted.push(idVal);
        else if (jsonVal) {
          try { dbPosts.push(JSON.parse(jsonVal)); } catch (e) {}
        }
      }
      current = mergeRoomData(current, { posts: dbPosts, deletedIds: dbDeleted });
      memoryRooms[room] = current;
    }

    if (req.method === 'GET') {
      return res.status(200).json(current);
    }

    if (req.method === 'POST' || req.method === 'PUT') {
      const incoming = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
      const merged = mergeRoomData(current, incoming);
      memoryRooms[room] = merged;

      const stmts = [];
      for (const p of merged.posts) {
        stmts.push({
          sql: `INSERT INTO think_posts (id, room_code, post_json, updated_at, deleted)
                VALUES (?, ?, ?, ?, 0)
                ON CONFLICT(id) DO UPDATE SET
                  post_json = excluded.post_json,
                  updated_at = excluded.updated_at,
                  deleted = 0
                WHERE excluded.updated_at >= think_posts.updated_at`,
          args: [p.id, room, JSON.stringify(p), p.updatedAtMs || Date.now()]
        });
      }
      for (const delId of merged.deletedIds || []) {
        stmts.push({
          sql: `UPDATE think_posts SET deleted = 1, updated_at = ? WHERE id = ?`,
          args: [Date.now(), delId]
        });
      }
      if (stmts.length > 0) {
        await tursoPipeline(stmts, clientToken);
      }

      return res.status(200).json({ ok: true, count: merged.posts.length });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    return res.status(200).json(memoryRooms[room] || { posts: [], deletedIds: [] });
  }
}
