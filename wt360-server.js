const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { Client } = require('pg');

try { process.loadEnvFile && process.loadEnvFile(); } catch (e) { /* no .env */ }

const PORT = +(process.env.PORT || 3225);
const HOST = process.env.HOST || '0.0.0.0';

function dbConfig() {
  return {
    host: process.env.PGHOST || 'arl-community-developer.postgres.database.azure.com',
    user: process.env.PGUSER || 'deputy.coo@akijresource.com',
    port: +(process.env.PGPORT || 5432),
    database: process.env.WT360_DATABASE || process.env.PGDATABASE || 'ArlOpexDB',
    password: process.env.PGPASSWORD,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 15000,
  };
}

const TABLES = {
  users: {
    pk: 'id',
    cols: ['id','enroll','name','section','role','type','status','created_at','updated_at','is_deleted'],
    types: 'id text, enroll text, name text, section text, role text, type text, status text, created_at timestamptz, updated_at timestamptz, is_deleted boolean',
  },
  roles: {
    pk: 'id',
    cols: ['id','section','name'],
    types: 'id text, section text, name text',
  },
  dict: {
    pk: 'id',
    cols: ['id','section','role','title','est_min','priority','frequency'],
    types: 'id text, section text, role text, title text, est_min integer, priority text, frequency text',
  },
  tasks: {
    pk: 'id',
    cols: ['id','code','title','description','section','role','assignee_id','supervisor_id','est_min','actual_min','priority','shift','due_date','frequency','status','created_at','updated_at','timer_start','attachments','comments','is_cross','assigned_by_id','assigned_by_name','from_section'],
    types: 'id text, code text, title text, description text, section text, role text, assignee_id text, supervisor_id text, est_min integer, actual_min integer, priority text, shift text, due_date text, frequency text, status text, created_at timestamptz, updated_at timestamptz, timer_start bigint, attachments jsonb, comments jsonb, is_cross boolean, assigned_by_id text, assigned_by_name text, from_section text',
  },
  notify: {
    pk: 'id',
    cols: ['id','at','to_user','title','body','is_read'],
    types: 'id text, at timestamptz, to_user text, title text, body text, is_read boolean',
  },
};
const DATA_TABLES = ['users','roles','dict','tasks','notify'];
const SQL = { users:'hrml_users', roles:'hrml_roles', dict:'hrml_dict', tasks:'hrml_tasks', notify:'hrml_notify', settings:'hrml_settings', seq:'hrml_seq' };

async function loadDB() {
  const client = new Client(dbConfig());
  await client.connect();
  try {
    const db = {};
    for (const t of DATA_TABLES) {
      db[t] = (await client.query(`SELECT * FROM ${SQL[t]} ORDER BY id`)).rows;
    }
    db.settings = (await client.query(`SELECT key, value FROM ${SQL.settings} ORDER BY key`)).rows;
    db.seq = (await client.query(`SELECT key, value FROM ${SQL.seq} ORDER BY key`)).rows;
    const modRows = (await client.query(`SELECT value FROM ${SQL.seq} WHERE key = 'mod'`)).rows;
    return { ok: true, db, rev: modRows.length ? Number(modRows[0].value) : 0 };
  } finally {
    await client.end();
  }
}

async function getRev() {
  const client = new Client(dbConfig());
  await client.connect();
  try {
    const r = await client.query(`SELECT value FROM ${SQL.seq} WHERE key = 'mod'`);
    return { ok: true, rev: r.rows.length ? Number(r.rows[0].value) : 0 };
  } finally {
    await client.end();
  }
}

async function saveChanges(upserts, deletes, settings, seq) {
  const client = new Client(dbConfig());
  await client.connect();
  try {
    await client.query('BEGIN');
    for (const t of DATA_TABLES) {
      const def = TABLES[t];
      const rows = (upserts && upserts[t]) || [];
      if (rows.length) {
        const j = JSON.stringify(rows);
        const colList = def.cols.join(', ');
        const setClause = def.cols
          .filter((col) => col !== def.pk && col !== 'created_at')
          .map((col) => `${col}=EXCLUDED.${col}`)
          .join(', ');
        await client.query(
          `INSERT INTO ${SQL[t]} (${colList})
           SELECT ${colList} FROM jsonb_to_recordset($1::jsonb) AS x(${def.types})
           ON CONFLICT (${def.pk}) DO UPDATE SET ${setClause}`,
          [j]
        );
      }
      const ids = (deletes && deletes[t]) || [];
      if (ids.length) {
        await client.query(`DELETE FROM ${SQL[t]} WHERE id = ANY($1::text[])`, [ids.map(String)]);
      }
    }
    for (const s of (settings || [])) {
      if (!s || s.key == null) continue;
      await client.query(
        `INSERT INTO ${SQL.settings}(key, value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value`,
        [String(s.key), s.value == null ? null : Number(s.value)]
      );
    }
    for (const s of (seq || [])) {
      if (!s || s.key == null) continue;
      await client.query(
        `INSERT INTO ${SQL.seq}(key, value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value`,
        [String(s.key), s.value == null ? null : Number(s.value)]
      );
    }
    const mod = Date.now();
    await client.query(
      `INSERT INTO ${SQL.seq}(key, value) VALUES('mod', $1) ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value`,
      [mod]
    );
    await client.query('COMMIT');
    return { ok: true, rev: mod };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    await client.end();
  }
}

function send(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type,Content-Encoding',
    'Cache-Control': 'no-store',
  });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > 25 * 1024 * 1024) {
        reject(new Error('payload too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      try {
        let buf = Buffer.concat(chunks);
        const enc = String(req.headers['content-encoding'] || '').toLowerCase();
        if (enc === 'gzip') {
          try { buf = zlib.gunzipSync(buf); } catch (e) { /* already decompressed upstream */ }
        } else if (enc === 'deflate') {
          try { buf = zlib.inflateSync(buf); } catch (e) { /* already decompressed upstream */ }
        }
        resolve(buf.toString('utf8'));
      } catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8',
};

function serveStatic(res, pathname) {
  let rel = pathname === '/' || pathname === '/wt360' ? '/index.html' : pathname;
  rel = rel.split('?')[0];
  const file = path.join(__dirname, rel);
  if (!file.startsWith(__dirname)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }
  fs.readFile(file, (err, buf) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }
    const ext = path.extname(file).toLowerCase();
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(buf);
  });
}

const server = http.createServer(async (req, res) => {
  const pathname = (req.url || '/').split('?')[0];

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,Content-Encoding',
    });
    res.end();
    return;
  }

  if (pathname === '/api/wt360' || pathname === '/api/wt360/') {
    if (req.method === 'GET') {
      try {
        send(res, 200, await loadDB());
      } catch (e) {
        send(res, 500, { ok: false, error: e.message });
      }
      return;
    }

    if (req.method === 'POST') {
      let body;
      try {
        body = await readBody(req);
      } catch (e) {
        send(res, 413, { ok: false, error: e.message });
        return;
      }
      let parsed;
      try {
        parsed = JSON.parse(body || '{}');
      } catch (e) {
        send(res, 400, { ok: false, error: 'invalid JSON body' });
        return;
      }
      const action = parsed.action;
      try {
        if (action === 'ping') {
          send(res, 200, { ok: true, pong: true, time: Date.now() });
        } else if (action === 'rev') {
          send(res, 200, await getRev());
        } else if (action === 'load') {
          send(res, 200, await loadDB());
        } else if (action === 'save') {
          send(res, 200, await saveChanges(parsed.upserts, parsed.deletes, parsed.settings, parsed.seq));
        } else {
          send(res, 400, { ok: false, error: 'unknown action: ' + action });
        }
      } catch (e) {
        send(res, 500, { ok: false, error: e.message });
      }
      return;
    }

    send(res, 405, { ok: false, error: 'method not allowed' });
    return;
  }

  serveStatic(res, pathname);
});

server.listen(PORT, HOST, () => {
  console.log(`WorkTrac360 server running at http://localhost:${PORT}`);
  console.log(`Backend: ${dbConfig().database} @ ${dbConfig().host}`);
});
