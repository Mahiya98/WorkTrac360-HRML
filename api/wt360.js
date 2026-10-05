const { Client } = require('pg');
const zlib = require('zlib');

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

// Whitelisted tables + their columns (prevents SQL injection).
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

function client() {
  return new Client(dbConfig());
}

async function loadDB() {
  const c = client();
  await c.connect();
  try {
    const db = {};
    for (const t of DATA_TABLES) {
      db[t] = (await c.query(`SELECT * FROM ${SQL[t]} ORDER BY id`)).rows;
    }
    db.settings = (await c.query(`SELECT key, value FROM ${SQL.settings} ORDER BY key`)).rows;
    db.seq = (await c.query(`SELECT key, value FROM ${SQL.seq} ORDER BY key`)).rows;
    const modRows = (await c.query(`SELECT value FROM ${SQL.seq} WHERE key = 'mod'`)).rows;
    return { ok: true, db, rev: modRows.length ? Number(modRows[0].value) : 0 };
  } finally {
    await c.end();
  }
}

async function getRev() {
  const c = client();
  await c.connect();
  try {
    const r = await c.query(`SELECT value FROM ${SQL.seq} WHERE key = 'mod'`);
    return { ok: true, rev: r.rows.length ? Number(r.rows[0].value) : 0 };
  } finally {
    await c.end();
  }
}

async function saveChanges(upserts, deletes, settings, seq) {
  const c = client();
  await c.connect();
  try {
    await c.query('BEGIN');

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
        await c.query(
          `INSERT INTO ${SQL[t]} (${colList})
           SELECT ${colList} FROM jsonb_to_recordset($1::jsonb) AS x(${def.types})
           ON CONFLICT (${def.pk}) DO UPDATE SET ${setClause}`,
          [j]
        );
      }
      const ids = (deletes && deletes[t]) || [];
      if (ids.length) {
        await c.query(`DELETE FROM ${SQL[t]} WHERE id = ANY($1::text[])`, [ids.map(String)]);
      }
    }

    for (const s of (settings || [])) {
      if (!s || s.key == null) continue;
      await c.query(
        `INSERT INTO ${SQL.settings}(key, value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value`,
        [String(s.key), s.value == null ? null : Number(s.value)]
      );
    }
    for (const s of (seq || [])) {
      if (!s || s.key == null) continue;
      await c.query(
        `INSERT INTO ${SQL.seq}(key, value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value`,
        [String(s.key), s.value == null ? null : Number(s.value)]
      );
    }

    const mod = Date.now();
    await c.query(
      `INSERT INTO ${SQL.seq}(key, value) VALUES('mod', $1) ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value`,
      [mod]
    );

    await c.query('COMMIT');
    return { ok: true, rev: mod };
  } catch (e) {
    await c.query('ROLLBACK');
    throw e;
  } finally {
    await c.end();
  }
}

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,Content-Encoding',
};

function json(res, status, obj) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  for (const [k, v] of Object.entries(CORS)) res.setHeader(k, v);
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > 25 * 1024 * 1024) {
        reject(new Error('payload too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
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

module.exports = async (req, res) => {
  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    for (const [k, v] of Object.entries(CORS)) res.setHeader(k, v);
    res.end();
    return;
  }

  if (req.method === 'GET') {
    try {
      json(res, 200, await loadDB());
    } catch (e) {
      json(res, 500, { ok: false, error: e.message });
    }
    return;
  }

  if (req.method === 'POST') {
    let body;
    try {
      body = await readBody(req);
    } catch (e) {
      json(res, 413, { ok: false, error: e.message });
      return;
    }

    let parsed;
    try {
      parsed = JSON.parse(body || '{}');
    } catch (e) {
      json(res, 400, { ok: false, error: 'invalid JSON body' });
      return;
    }

    const action = parsed.action;
    try {
      if (action === 'ping') {
        json(res, 200, { ok: true, pong: true, time: Date.now() });
      } else if (action === 'rev') {
        json(res, 200, await getRev());
      } else if (action === 'save') {
        json(res, 200, await saveChanges(parsed.upserts, parsed.deletes, parsed.settings, parsed.seq));
      } else if (action === 'load') {
        json(res, 200, await loadDB());
      } else {
        json(res, 400, { ok: false, error: 'unknown action: ' + action });
      }
    } catch (e) {
      json(res, 500, { ok: false, error: e.message });
    }
    return;
  }

  json(res, 405, { ok: false, error: 'method not allowed' });
};
