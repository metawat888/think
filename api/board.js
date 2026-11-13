// Vercel Serverless API Connected to Turso LibSQL (libsql://ailsed-metawat888.aws-ap-southeast-1.turso.io)
// Enforces permanent tombstones in think_tombstones + think_posts + Upstash KV + memory.
const DEFAULT_TURSO_URL = process.env.TURSO_DATABASE_URL || 'libsql://ailsed-metawat888.aws-ap-southeast-1.turso.io';
const memoryRooms = global.__THINK_BOARD_ROOMS__ || (global.__THINK_BOARD_ROOMS__ = {});
const globalTombstones = global.__THINK_TOMBSTONES__ || (global.__THINK_TOMBSTONES__ = new Set());

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
  if (!existing || !Array.isArray(existing.posts)) existing = { posts: [], deletedIds: [] };
  if (!incoming || typeof incoming !== 'object') return existing;

  const deletedSet = new Set([
    ...globalTombstones,
    ...(existing.deletedIds || []),
    ...(incoming.deletedIds || [])
  ]);
  deletedSet.forEach(id => { if (id) globalTombstones.add(String(id)); });

  const postMap = new Map();
  for (const p of existing.posts) {
    if (p && p.id && !deletedSet.has(String(p.id))) postMap.set(String(p.id), p);
  }
  if (Array.isArray(incoming.posts)) {
    for (const rp of incoming.posts) {
      if (!rp || !rp.id || deletedSet.has(String(rp.id))) continue;
      const sId = String(rp.id);
      const lp = postMap.get(sId);
      if (!lp || (rp.updatedAtMs || 0) >= (lp.updatedAtMs || 0)) {
        const mergedComments = new Map();
        for (const c of [...(lp?.comments || []), ...(rp.comments || [])]) {
          if (c && c.id && !deletedSet.has(String(c.id))) mergedComments.set(String(c.id), c);
        }
        postMap.set(sId, {
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

  const room = String(req.query.room || 'DEFAULT').trim().toUpperCase().replace(/[^A-Z0-9_-]/g, '_');
  const clientToken = req.headers['x-turso-token'] || '';
  const kvUrl = process.env.KV_REST_API_URL;
  const kvToken = process.env.KV_REST_API_TOKEN;

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
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS think_tombstones (
          id TEXT PRIMARY KEY,
          room_code TEXT NOT NULL,
          deleted_at INTEGER NOT NULL
        )`
      }
    ], clientToken);

    let current = memoryRooms[room] || { posts: [], deletedIds: Array.from(globalTombstones) };

    if (kvUrl && kvToken) {
      try {
        const r = await fetch(`${kvUrl}/get/board_${room}`, { headers: { Authorization: `Bearer ${kvToken}` } });
        const j = await r.json();
        if (j && j.result) {
          const kvData = typeof j.result === 'string' ? JSON.parse(j.result) : j.result;
          current = mergeRoomData(current, kvData);
        }
      } catch (e) {}
    }

    const dbRead = await tursoPipeline([
      { sql: `SELECT id FROM think_tombstones WHERE room_code = ?`, args: [room] },
      { sql: `SELECT id, post_json, updated_at, deleted FROM think_posts WHERE room_code = ?`, args: [room] }
    ], clientToken);

    if (dbRead && Array.isArray(dbRead.results)) {
      const dbPosts = [];
      const dbDeleted = [];
      const tombRows = dbRead.results[0]?.response?.result?.rows || [];
      for (const r of tombRows) {
        if (r[0]?.value) {
          dbDeleted.push(String(r[0].value));
          globalTombstones.add(String(r[0].value));
        }
      }
      const postRows = dbRead.results[1]?.response?.result?.rows || [];
      for (const r of postRows) {
        const idVal = r[0]?.value ? String(r[0].value) : '';
        const jsonVal = r[1]?.value;
        const delVal = Number(r[3]?.value || 0);
        if (delVal === 1 && idVal) {
          dbDeleted.push(idVal);
          globalTombstones.add(idVal);
        } else if (jsonVal && idVal && !globalTombstones.has(idVal)) {
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

      if (kvUrl && kvToken) {
        try {
          await fetch(`${kvUrl}/set/board_${room}`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${kvToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify(merged)
          });
        } catch (e) {}
      }

      const stmts = [];
      for (const delId of merged.deletedIds || []) {
        stmts.push({
          sql: `INSERT INTO think_tombstones (id, room_code, deleted_at)
                VALUES (?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET deleted_at = excluded.deleted_at`,
          args: [delId, room, Date.now()]
        });
        stmts.push({
          sql: `INSERT INTO think_posts (id, room_code, post_json, updated_at, deleted)
                VALUES (?, ?, '{}', ?, 1)
                ON CONFLICT(id) DO UPDATE SET
                  post_json = '{}',
                  updated_at = excluded.updated_at,
                  deleted = 1`,
          args: [delId, room, Date.now()]
        });
      }
      for (const p of merged.posts) {
        if (globalTombstones.has(String(p.id))) continue;
        stmts.push({
          sql: `INSERT INTO think_posts (id, room_code, post_json, updated_at, deleted)
                VALUES (?, ?, ?, ?, 0)
                ON CONFLICT(id) DO UPDATE SET
                  post_json = excluded.post_json,
                  updated_at = excluded.updated_at
                WHERE think_posts.deleted = 0 AND excluded.updated_at >= think_posts.updated_at`,
          args: [p.id, room, JSON.stringify(p), p.updatedAtMs || Date.now()]
        });
      }
      if (stmts.length > 0) {
        await tursoPipeline(stmts, clientToken);
      }

      return res.status(200).json({ ok: true, count: merged.posts.length });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    return res.status(200).json(memoryRooms[room] || { posts: [], deletedIds: Array.from(globalTombstones) });
  }
}
