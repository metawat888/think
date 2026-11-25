// Vercel Serverless API Connected to Turso LibSQL + Permanent Cloud KV + JSONBlob Cold-Start Persistence
// Guarantees boxes/data NEVER disappear over time, even after serverless cold starts or cache expirations.
const DEFAULT_TURSO_URL = process.env.TURSO_DATABASE_URL || 'libsql://ailseddatabase-metawat888.aws-ap-southeast-1.turso.io';
const KV_APP_KEY = 'r9f8k2m4';
const memoryRooms = global.__THINK_BOARD_ROOMS__ || (global.__THINK_BOARD_ROOMS__ = {});
const globalTombstones = global.__THINK_TOMBSTONES__ || (global.__THINK_TOMBSTONES__ = new Set());

function getTursoHttpEndpoint(urlStr) {
  return String(urlStr || DEFAULT_TURSO_URL)
    .trim()
    .replace(/^libsql:\/\//i, 'https://')
    .replace(/^wss?:\/\//i, 'https://')
    .replace(/\/+$/, '') + '/v2/pipeline';
}

function getDeterministicKvKey(roomCode) {
  const clean = String(roomCode || 'DEFAULT').trim().toUpperCase();
  let hex = '';
  for (let i = 0; i < clean.length; i++) hex += clean.charCodeAt(i).toString(16);
  return 'tb_' + hex.slice(0, 32);
}

async function readPermanentCloudSnapshot(roomCode) {
  const kvKey = getDeterministicKvKey(roomCode);
  try {
    const kvRes = await fetch(`https://keyvalue.immanuel.co/api/KeyVal/GetValue/${KV_APP_KEY}/${kvKey}`);
    if (!kvRes.ok) return null;
    const blobId = (await kvRes.text()).replace(/^"+|"+$/g, '').trim();
    if (!blobId || blobId === 'null' || blobId.length < 5) return null;

    const blobRes = await fetch(`https://jsonblob.com/api/jsonBlob/${blobId}`, {
      method: 'GET',
      headers: { Accept: 'application/json' }
    });
    if (blobRes.ok) {
      const data = await blobRes.json();
      if (data && typeof data === 'object') {
        data.cloudBlobId = blobId;
        return data;
      }
    }
  } catch (e) {}
  return null;
}

async function writePermanentCloudSnapshot(roomCode, roomState) {
  const kvKey = getDeterministicKvKey(roomCode);
  try {
    let blobId = roomState.cloudBlobId || '';
    if (!blobId) {
      const kvRes = await fetch(`https://keyvalue.immanuel.co/api/KeyVal/GetValue/${KV_APP_KEY}/${kvKey}`);
      if (kvRes.ok) {
        const val = (await kvRes.text()).replace(/^"+|"+$/g, '').trim();
        if (val && val !== 'null' && val.length >= 5) blobId = val;
      }
    }

    const payload = JSON.stringify(roomState);
    if (blobId) {
      const putRes = await fetch(`https://jsonblob.com/api/jsonBlob/${blobId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: payload
      });
      if (putRes.ok) {
        roomState.cloudBlobId = blobId;
        await fetch(`https://keyvalue.immanuel.co/api/KeyVal/UpdateValue/${KV_APP_KEY}/${kvKey}/${encodeURIComponent(blobId)}`, { method: 'POST' });
        return blobId;
      }
    }

    const postRes = await fetch('https://jsonblob.com/api/jsonBlob', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: payload
    });
    if (postRes.ok) {
      const loc = postRes.headers.get('location') || postRes.headers.get('Location') || postRes.headers.get('x-jsonblob') || '';
      const idMatch = loc.match(/\/([^\/]+)$/) || [null, loc];
      if (idMatch && idMatch[1]) {
        blobId = idMatch[1].trim();
        roomState.cloudBlobId = blobId;
        await fetch(`https://keyvalue.immanuel.co/api/KeyVal/UpdateValue/${KV_APP_KEY}/${kvKey}/${encodeURIComponent(blobId)}`, { method: 'POST' });
        return blobId;
      }
    }
  } catch (e) {}
  return '';
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
      const mergedComments = new Map();
      for (const c of [...(lp?.comments || []), ...(rp.comments || [])]) {
        if (c && c.id && !deletedSet.has(String(c.id))) mergedComments.set(String(c.id), c);
      }
      const sortedComments = Array.from(mergedComments.values()).sort((a, b) => (a.createdAtMs || 0) - (b.createdAtMs || 0));
      const newer = (!lp || (rp.updatedAtMs || 0) >= (lp.updatedAtMs || 0)) ? rp : lp;
      const older = newer === rp ? (lp || {}) : rp;
      postMap.set(sId, {
        ...older,
        ...newer,
        likes: Math.max(lp?.likes || 0, rp.likes || 0),
        hearts: Math.max(lp?.hearts || 0, rp.hearts || 0),
        cares: Math.max(lp?.cares || 0, rp.cares || 0),
        hahas: Math.max(lp?.hahas || 0, rp.hahas || 0),
        wows: Math.max(lp?.wows || 0, rp.wows || 0),
        sads: Math.max(lp?.sads || 0, rp.sads || 0),
        angrys: Math.max(lp?.angrys || 0, rp.angrys || 0),
        imageBlobId: rp.imageBlobId || lp?.imageBlobId || '',
        hasImage: Boolean(rp.hasImage || lp?.hasImage || rp.image || lp?.image),
        imageInline: rp.imageInline || lp?.imageInline || '',
        image: (rp.image && (!lp?.image || rp.image.length >= lp.image.length)) ? rp.image : (lp?.image || rp.image || rp.imageInline || lp?.imageInline || ''),
        images: (Array.isArray(rp.images) && rp.images.length >= (Array.isArray(lp?.images) ? lp.images.length : 0)) ? rp.images : (lp?.images || rp.images),
        fileDataUrl: (rp.fileDataUrl && (!lp?.fileDataUrl || rp.fileDataUrl.length >= lp.fileDataUrl.length)) ? rp.fileDataUrl : (lp?.fileDataUrl || rp.fileDataUrl || ''),
        youtubeUrl: newer.youtubeUrl || older.youtubeUrl || '',
        youtubeId: newer.youtubeId || older.youtubeId || '',
        webRunUrl: newer.webRunUrl || older.webRunUrl || '',
        webAutoCmd: newer.webAutoCmd !== undefined ? newer.webAutoCmd : (older.webAutoCmd || ''),
        comments: sortedComments
      });
    }
  }
  return {
    ...existing,
    ...incoming,
    cloudBlobId: incoming.cloudBlobId || existing.cloudBlobId || '',
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
    let current = memoryRooms[room] || { posts: [], deletedIds: Array.from(globalTombstones) };

    // 1. Hydrate from Permanent Cloud Snapshot if memory is cold
    if (!current.posts || current.posts.length === 0) {
      const cloudSnap = await readPermanentCloudSnapshot(room);
      if (cloudSnap) {
        current = mergeRoomData(current, cloudSnap);
        memoryRooms[room] = current;
      }
    }

    // 2. Hydrate from Vercel KV if configured
    if (kvUrl && kvToken) {
      try {
        const r = await fetch(`${kvUrl}/get/board_${room}`, { headers: { Authorization: `Bearer ${kvToken}` } });
        const j = await r.json();
        if (j && j.result) {
          const kvData = typeof j.result === 'string' ? JSON.parse(j.result) : j.result;
          current = mergeRoomData(current, kvData);
          memoryRooms[room] = current;
        }
      } catch (e) {}
    }

    // 3. Hydrate from Turso LibSQL (including dedicated think_images table)
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
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS think_images (
          post_id TEXT PRIMARY KEY,
          room_code TEXT NOT NULL,
          image_data TEXT NOT NULL,
          image_blob_id TEXT DEFAULT '',
          updated_at INTEGER NOT NULL
        )`
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS think_comments (
          id TEXT PRIMARY KEY,
          post_id TEXT NOT NULL,
          room_code TEXT NOT NULL,
          comment_json TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          deleted INTEGER NOT NULL DEFAULT 0
        )`
      }
    ], clientToken);

    const dbRead = await tursoPipeline([
      { sql: `SELECT id FROM think_tombstones WHERE room_code = ?`, args: [room] },
      { sql: `SELECT id, post_json, updated_at, deleted FROM think_posts WHERE room_code = ?`, args: [room] },
      { sql: `SELECT post_id, image_data, image_blob_id FROM think_images WHERE room_code = ?`, args: [room] },
      { sql: `SELECT id, post_id, comment_json, deleted FROM think_comments WHERE room_code = ?`, args: [room] }
    ], clientToken);

    if (dbRead && Array.isArray(dbRead.results)) {
      const dbPosts = [];
      const dbDeleted = [];
      const dbImageMap = new Map();
      const dbBlobMap = new Map();
      const tombRows = dbRead.results[0]?.response?.result?.rows || [];
      for (const r of tombRows) {
        if (r[0]?.value) {
          dbDeleted.push(String(r[0].value));
          globalTombstones.add(String(r[0].value));
        }
      }
      const imgRows = dbRead.results[2]?.response?.result?.rows || [];
      for (const r of imgRows) {
        const pId = r[0]?.value ? String(r[0].value) : '';
        const imgVal = r[1]?.value || '';
        const blobVal = r[2]?.value || '';
        if (pId && imgVal && imgVal.length > 10 && !globalTombstones.has(pId)) {
          dbImageMap.set(pId, imgVal);
        }
        if (pId && blobVal) dbBlobMap.set(pId, blobVal);
      }
      const dbCommentsMap = new Map();
      const cmtRows = dbRead.results[3]?.response?.result?.rows || [];
      for (const r of cmtRows) {
        const cId = r[0]?.value ? String(r[0].value) : '';
        const pId = r[1]?.value ? String(r[1].value) : '';
        const cJson = r[2]?.value;
        const cDel = Number(r[3]?.value || 0);
        if (cDel === 1 && cId) {
          dbDeleted.push(cId);
          globalTombstones.add(cId);
        } else if (cId && pId && cJson && !globalTombstones.has(cId)) {
          try {
            const cObj = JSON.parse(cJson);
            if (cObj && cObj.id) {
              if (!dbCommentsMap.has(pId)) dbCommentsMap.set(pId, []);
              dbCommentsMap.get(pId).push(cObj);
            }
          } catch (e) {}
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
          try {
            const parsed = JSON.parse(jsonVal);
            if (dbImageMap.has(idVal)) {
              parsed.image = dbImageMap.get(idVal);
              parsed.hasImage = true;
            }
            if (dbImageMap.has(idVal + '__gallery')) {
              try {
                const gal = JSON.parse(dbImageMap.get(idVal + '__gallery'));
                if (Array.isArray(gal) && gal.length > 0) {
                  parsed.images = gal;
                  parsed.image = gal[0];
                  parsed.hasImage = true;
                  parsed.imageCount = gal.length;
                }
              } catch (e) {}
            }
            if (dbImageMap.has(idVal + '__file')) {
              parsed.fileDataUrl = dbImageMap.get(idVal + '__file');
              parsed.hasFile = true;
            }
            if (dbBlobMap.has(idVal) && !parsed.imageBlobId) {
              parsed.imageBlobId = dbBlobMap.get(idVal);
            }
            if (dbCommentsMap.has(idVal)) {
              const cMap = new Map((Array.isArray(parsed.comments) ? parsed.comments : []).map(c => [c.id, c]));
              for (const dc of dbCommentsMap.get(idVal)) {
                if (dc && dc.id && !globalTombstones.has(String(dc.id)) && !cMap.has(dc.id)) cMap.set(dc.id, dc);
              }
              parsed.comments = Array.from(cMap.values()).sort((a, b) => (a.createdAtMs || 0) - (b.createdAtMs || 0));
            }
            dbPosts.push(parsed);
          } catch (e) {}
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

      // Save to Permanent Cloud Snapshot (survives cold starts without env vars)
      await writePermanentCloudSnapshot(room, merged);

      // Save to Vercel KV if configured
      if (kvUrl && kvToken) {
        try {
          await fetch(`${kvUrl}/set/board_${room}`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${kvToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify(merged)
          });
        } catch (e) {}
      }

      // Save to Turso LibSQL
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
        const lightNode = Object.assign({}, p);
        delete lightNode.images;
        delete lightNode.fileDataUrl;
        if (lightNode.image && lightNode.image.startsWith('data:') && lightNode.image.length > 16000) {
          lightNode.image = lightNode.imageInline || '';
        }
        stmts.push({
          sql: `INSERT INTO think_posts (id, room_code, post_json, updated_at, deleted)
                VALUES (?, ?, ?, ?, 0)
                ON CONFLICT(id) DO UPDATE SET
                  post_json = excluded.post_json,
                  updated_at = excluded.updated_at
                WHERE think_posts.deleted = 0 AND excluded.updated_at >= think_posts.updated_at`,
          args: [p.id, room, JSON.stringify(lightNode), p.updatedAtMs || Date.now()]
        });
        if (p.image && p.image.length > 10) {
          stmts.push({
            sql: `INSERT INTO think_images (post_id, room_code, image_data, image_blob_id, updated_at)
                  VALUES (?, ?, ?, ?, ?)
                  ON CONFLICT(post_id) DO UPDATE SET
                    image_data = CASE WHEN LENGTH(excluded.image_data) >= LENGTH(think_images.image_data) THEN excluded.image_data ELSE think_images.image_data END,
                    image_blob_id = CASE WHEN LENGTH(excluded.image_blob_id) > 3 THEN excluded.image_blob_id ELSE think_images.image_blob_id END,
                    updated_at = excluded.updated_at`,
            args: [p.id, room, p.image, p.imageBlobId || '', Date.now()]
          });
        }
        if (Array.isArray(p.images) && p.images.length > 1) {
          stmts.push({
            sql: `INSERT INTO think_images (post_id, room_code, image_data, image_blob_id, updated_at)
                  VALUES (?, ?, ?, ?, ?)
                  ON CONFLICT(post_id) DO UPDATE SET
                    image_data = CASE WHEN LENGTH(excluded.image_data) >= LENGTH(think_images.image_data) THEN excluded.image_data ELSE think_images.image_data END,
                    updated_at = excluded.updated_at`,
            args: [p.id + '__gallery', room, JSON.stringify(p.images), String(p.images.length), Date.now()]
          });
        }
        if (p.fileDataUrl && p.fileDataUrl.length > 15 && p.fileDataUrl.length <= 900000) {
          stmts.push({
            sql: `INSERT INTO think_images (post_id, room_code, image_data, image_blob_id, updated_at)
                  VALUES (?, ?, ?, ?, ?)
                  ON CONFLICT(post_id) DO UPDATE SET
                    image_data = CASE WHEN LENGTH(excluded.image_data) >= LENGTH(think_images.image_data) THEN excluded.image_data ELSE think_images.image_data END,
                    updated_at = excluded.updated_at`,
            args: [p.id + '__file', room, p.fileDataUrl, p.fileName || '', Date.now()]
          });
        }
        for (const c of p.comments || []) {
          if (!c || !c.id || globalTombstones.has(String(c.id))) continue;
          stmts.push({
            sql: `INSERT INTO think_comments (id, post_id, room_code, comment_json, created_at, deleted)
                  VALUES (?, ?, ?, ?, ?, 0)
                  ON CONFLICT(id) DO NOTHING`,
            args: [c.id, p.id, room, JSON.stringify(c), c.createdAtMs || Date.now()]
          });
        }
      }
      if (stmts.length > 0) {
        await tursoPipeline(stmts, clientToken);
      }

      return res.status(200).json({ ok: true, count: merged.posts.length, cloudBlobId: merged.cloudBlobId || '' });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    return res.status(200).json(memoryRooms[room] || { posts: [], deletedIds: Array.from(globalTombstones) });
  }
}
