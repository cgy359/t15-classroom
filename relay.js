/* ==========================================================================
   15.2《电流和电路》· 云端课堂中继
   --------------------------------------------------------------------------
   干什么：让「学生扫码进课堂」不再依赖本机服务与公网隧道。
   把它部署到任意免费云平台（Render / Railway / Fly.io / 自己的服务器）之后：

     教师端（本地双击的单文件，file:// 就能用）
        │
        └──wss──>  本服务  <──wss──  学生手机 / 听课教师
                     │
                     └─ 同时托管学生端页面 /student.html

   于是不管学生在教室 WiFi、手机流量还是校外网，只要能上网就能扫码进课堂；
   教师端也不用装 Node、不用解压服务、不用放行防火墙、不用开隧道。

   协议上与本地 server.js 完全一致（/api/info、/api/bus、/ws），
   前端不必为云端模式写一套新逻辑。

   零依赖：只用 Node 自带模块（含一个精简的 WebSocket 实现）。

   环境变量：
     PORT      监听端口（云平台会自己给，默认 3000）
     ROOM      默认房间号（默认 T15，教师端可以用 ?room= 覆盖）
     PIN       课堂口令（可选；设了之后学生要输入口令才能进）
     SITE_DIR  学生端页面所在目录（默认本文件所在目录）
   ========================================================================== */
'use strict';
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = parseInt(process.env.PORT || '3000', 10);
const DEF_ROOM = (process.env.ROOM || 'T15').toUpperCase();
const PIN = (process.env.PIN || '').trim();
const SITE_DIR = process.env.SITE_DIR || __dirname;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

/* ---------------- 房间与消息总线 ---------------- */
const rooms = new Map();

function getRoom(name) {
  const n = String(name || DEF_ROOM).toUpperCase();
  let r = rooms.get(n);
  if (!r) {
    r = { name: n, seq: 0, msgs: [], clients: new Set(), teacher: false, born: Date.now() };
    rooms.set(n, r);
  }
  return r;
}

function busPush(room, msg) {
  const r = getRoom(room);
  msg.__i = ++r.seq;
  r.msgs.push({ i: r.seq, m: msg });
  if (r.msgs.length > 300) r.msgs.splice(0, r.msgs.length - 300);
  return msg.__i;
}

function busSince(room, since) {
  const r = getRoom(room);
  const s = parseInt(since || '0', 10) || 0;
  return r.msgs.filter(x => x.i > s);
}

/* 学生报到 / 心跳 → 维护在线名单；教师端据此显示「N 人在线」 */
const rosterCache = new Map();   /* room -> Map(id -> {name,role,at}) */
function touch(room, id, name, role) {
  let m = rosterCache.get(room);
  if (!m) { m = new Map(); rosterCache.set(room, m); }
  /* 心跳（pong）不带 role —— 沿用之前登记的身份，别把听课教师冲成学生 */
  const old = m.get(id);
  m.set(id, {
    name: name || (old && old.name) || '',
    role: role || (old && old.role) || '',
    at: Date.now()
  });
}
function rosterList(room) {
  const m = rosterCache.get(room);
  if (!m) return [];
  const now = Date.now();
  const out = [];
  m.forEach((v, k) => {
    if (now - v.at > 60000) { m.delete(k); return; }   /* 60 秒没心跳算离开 */
    out.push({ id: k, name: v.name, role: v.role });
  });
  return out;
}
function pushRoster(room) {
  const all = rosterList(room);
  const stus = all.filter(x => x.role !== 'teacher' && x.role !== 'audit');
  const auds = all.filter(x => x.role === 'audit');
  const msg = {
    type: '__roster', count: stus.length, audit: auds.length,
    names: stus.map(x => x.name).filter(Boolean),
    audits: auds.map(x => x.name).filter(Boolean),
    /* 与本地 server.js 保持一致：带 role 的完整名单，教师端据此区分学生 / 听课教师 */
    students: all.map(x => ({ id: x.id, name: x.name, role: x.role || '' }))
  };
  busPush(room, msg);
  /* 两条通道都要推：HTTP 轮询靠总线，WebSocket 靠广播 */
  broadcast(room, msg, null);
}

/* ---------------- WebSocket（零依赖实现） ---------------- */
function sendFrame(sock, str) {
  const payload = Buffer.from(String(str), 'utf8');
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  head[0] = 0x81;                       /* FIN + 文本帧 */
  try { sock.write(Buffer.concat([head, payload])); } catch (e) {}
}
function sendPong(sock, payload) {
  const p = payload || Buffer.alloc(0);
  const len = p.length;
  let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  head[0] = 0x8a;
  try { sock.write(Buffer.concat([head, p])); } catch (e) {}
}

function broadcast(roomName, msg, except) {
  const r = getRoom(roomName);
  const txt = JSON.stringify(msg);
  r.clients.forEach(cl => {
    if (cl === except) return;
    sendFrame(cl.sock, txt);
  });
}

function onWsData(cl, chunk) {
  cl.buf = Buffer.concat([cl.buf, chunk]);
  for (;;) {
    if (cl.buf.length < 2) return;
    const b0 = cl.buf[0], b1 = cl.buf[1];
    const fin = (b0 & 0x80) !== 0;
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) {
      if (cl.buf.length < 4) return;
      len = cl.buf.readUInt16BE(2); off = 4;
    } else if (len === 127) {
      if (cl.buf.length < 10) return;
      len = Number(cl.buf.readBigUInt64BE(2)); off = 10;
    }
    let mask = null;
    if (masked) {
      if (cl.buf.length < off + 4) return;
      mask = cl.buf.slice(off, off + 4); off += 4;
    }
    if (cl.buf.length < off + len) return;
    let payload = cl.buf.slice(off, off + len);
    if (masked && mask) {
      const out = Buffer.alloc(len);
      for (let i = 0; i < len; i++) out[i] = payload[i] ^ mask[i % 4];
      payload = out;
    }
    cl.buf = cl.buf.slice(off + len);

    if (opcode === 0x8) {                       /* close */
      try { cl.sock.end(); } catch (e) {}
      return;
    }
    if (opcode === 0x9) { sendPong(cl.sock, payload); continue; }   /* ping */
    if (opcode === 0xa) { continue; }                                /* pong */

    /* 文本帧（含分片续帧） */
    if (opcode === 0x1) cl.frag = [];
    if (opcode === 0x1 || opcode === 0x0) {
      cl.frag.push(payload);
      if (!fin) continue;
      const text = Buffer.concat(cl.frag).toString('utf8');
      cl.frag = [];
      handleWsMsg(cl, text);
    }
  }
}

function handleWsMsg(cl, text) {
  let m = null;
  try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || !m.type) return;
  const room = (m.room || cl.room || DEF_ROOM).toUpperCase();
  cl.room = room;

  /* 报到 / 心跳 → 记进在线名单 */
  if (m.type === 'i-am-here' || m.type === '__hello' || m.type === 'pong' || m.type === 'hello') {
    const id = m.__own || (cl.id + '-' + (m.name || ''));
    /* 身份以报文里自带的 role 为准：听课端（?role=audit）连 WebSocket 时
       用的是 student 通道（它要收老师广播），真实身份写在 i-am-here 里 */
    const rl = String(m.role || cl.role || '').toLowerCase();
    touch(room, id, m.name || cl.name, rl);
    if (cl.role === 'teacher') getRoom(room).teacher = true;
    busPush(room, m);
    broadcast(room, m, cl);
    pushRoster(room);
    return;
  }
  busPush(room, m);
  broadcast(room, m, cl);
}

function wsUpgrade(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) { try { socket.destroy(); } catch (e) {} return; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  try {
    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
    );
  } catch (e) { return; }

  const u = new URL(req.url, 'http://x');
  const role = (u.searchParams.get('role') || 'student').toLowerCase();
  const room = (u.searchParams.get('room') || DEF_ROOM).toUpperCase();
  const name = u.searchParams.get('name') || '';
  const pin = u.searchParams.get('pin') || '';

  if (PIN && role !== 'teacher' && pin !== PIN) {
    sendFrame(socket, JSON.stringify({ type: '__denied', msg: '课堂口令不对' }));
    setTimeout(() => { try { socket.end(); } catch (e) {} }, 300);
    return;
  }

  const cl = {
    id: 'w' + Math.random().toString(36).slice(2, 9),
    sock: socket, room: room, role: role, name: name,
    buf: Buffer.alloc(0), frag: []
  };
  getRoom(room).clients.add(cl);
  if (role === 'teacher') getRoom(room).teacher = true;

  sendFrame(socket, JSON.stringify({ type: '__ready', room: room, role: role }));
  pushRoster(room);

  socket.setNoDelay && socket.setNoDelay(true);
  socket.on('data', c => { try { onWsData(cl, c); } catch (e) {} });
  socket.on('error', () => { getRoom(room).clients.delete(cl); });
  socket.on('close', () => {
    getRoom(room).clients.delete(cl);
    pushRoster(room);
  });
  /* 保活：20 秒 ping 一次，防止云平台掐掉空闲连接 */
  const hb = setInterval(() => {
    try { sendFrame(socket, JSON.stringify({ type: '__ping', t: Date.now() })); } catch (e) {}
  }, 20000);
  socket.on('close', () => clearInterval(hb));
}

/* ---------------- HTTP ---------------- */
function selfUrl(req) {
  const host = (req.headers['x-forwarded-host'] || req.headers.host || '').split(',')[0].trim();
  const proto = (req.headers['x-forwarded-proto'] || '').split(',')[0].trim() ||
                (host && /\.onrender\.com|\.fly\.dev|\.railway\.app|up\.railway\.app/.test(host) ? 'https' : 'http');
  return host ? (proto + '://' + host) : '';
}

function json(res, o) {
  const b = JSON.stringify(o);
  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(b),
    'Cache-Control': 'no-store'
  });
  res.end(b);
}

const MIME = {
  '.html': 'text/html; charset=utf-8', '.htm': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
  '.gif': 'image/gif', '.svg': 'image/svg+xml', '.mp4': 'video/mp4',
  '.webp': 'image/webp', '.ico': 'image/x-icon', '.txt': 'text/plain; charset=utf-8'
};

function serveFile(res, file) {
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('404');
      return;
    }
    const type = MIME[path.extname(file).toLowerCase()] || 'application/octet-stream';
    res.writeHead(200, {
      'Content-Type': type, 'Content-Length': st.size,
      'Cache-Control': 'public, max-age=300'
    });
    fs.createReadStream(file).pipe(res);
  });
}

/* 学生端页面：优先用自包含的 student-standalone.html，其次 student.html */
function studentPage(cb) {
  const cands = ['student-standalone.html', 'student.html'];
  for (const c of cands) {
    const f = path.join(SITE_DIR, c);
    if (fs.existsSync(f)) return cb(f);
  }
  cb('');
}

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://' + (req.headers.host || 'localhost'));
  const p = decodeURIComponent(u.pathname);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  /* 健康检查（云平台探活用） */
  if (p === '/healthz' || p === '/health') { json(res, { ok: true, up: process.uptime() }); return; }

  if (p === '/api/info') {
    const room = (u.searchParams.get('room') || DEF_ROOM).toUpperCase();
    const r = getRoom(room);
    json(res, {
      ok: true, port: PORT, room: room, cloud: true,
      publicUrl: selfUrl(req),
      ips: [], ipsAll: [],                 /* 云端模式没有局域网地址，也不需要 */
      needPin: !!PIN,
      hasTeacher: r.teacher,
      students: rosterList(room).filter(x => x.role !== 'teacher' && x.role !== 'audit').length,
      online: rosterList(room).length
    });
    return;
  }

  /* 消息总线：WebSocket 不通时的兜底通道（部分网络会拦 ws，只放行普通 HTTPS） */
  if (p === '/api/bus') {
    const room = (u.searchParams.get('room') || DEF_ROOM).toUpperCase();
    if (req.method === 'POST') {
      let body = '';
      req.on('data', c => { body += c; if (body.length > 4e6) req.destroy(); });
      req.on('end', () => {
        let o = null;
        try { o = JSON.parse(body); } catch (e) { o = null; }
        const msg = (o && o.msg && o.msg.type) ? o.msg : (o && o.type ? o : null);
        if (!msg) { json(res, { ok: false }); return; }
        const i = busPush(room, msg);
        if (msg.type === 'i-am-here' || msg.type === '__hello' || msg.type === 'pong') {
          touch(room, msg.__own || ('h' + Math.random().toString(36).slice(2, 8)),
                msg.name, msg.role);
          pushRoster(room);
        }
        broadcast(room, msg, null);
        json(res, { ok: true, i: i });
      });
      return;
    }
    json(res, {
      ok: true, room: room, seq: getRoom(room).seq,
      msgs: busSince(room, u.searchParams.get('since') || 0),
      roster: rosterList(room)
    });
    return;
  }

  /* 课堂名单（调试 / 听课端用） */
  if (p === '/api/roster') {
    const room = (u.searchParams.get('room') || DEF_ROOM).toUpperCase();
    json(res, { ok: true, room: room, list: rosterList(room) });
    return;
  }

  /* 首页：直接进学生端（房间号可带 room=） */
  if (p === '/' || p === '') {
    const room = (u.searchParams.get('room') || DEF_ROOM).toUpperCase();
    res.writeHead(302, { Location: '/student.html?room=' + encodeURIComponent(room) });
    res.end();
    return;
  }
  /* /audit.html → 听课教师（只读观摩）入口 */
  if (p === '/audit.html' || p === '/audit') {
    const room = (u.searchParams.get('room') || DEF_ROOM).toUpperCase();
    res.writeHead(302, {
      Location: '/student.html?room=' + encodeURIComponent(room) + '&role=audit'
    });
    res.end();
    return;
  }

  /* 静态文件 */
  let file = p.replace(/^\/+/, '');
  if (file === 'student.html') {
    studentPage(f => {
      if (!f) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('缺少 student-standalone.html'); return; }
      serveFile(res, f);
    });
    return;
  }
  const abs = path.join(SITE_DIR, file);
  if (abs.indexOf(SITE_DIR) === 0 && fs.existsSync(abs) && fs.statSync(abs).isFile()) {
    serveFile(res, abs);
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('404 ' + p);
});

server.on('upgrade', (req, socket) => {
  if ((req.headers.upgrade || '').toLowerCase() !== 'websocket') {
    try { socket.destroy(); } catch (e) {}
    return;
  }
  wsUpgrade(req, socket);
});

server.on('error', (e) => {
  if (e && e.code === 'EADDRINUSE') {
    console.log('');
    console.log('  [×] 端口 ' + PORT + ' 已经被占用了。');
    console.log('      换一个端口再试，例如：  PORT=8787 node relay.js');
  } else {
    console.log('  [×] 启动失败：' + (e && e.message));
  }
  process.exit(1);
});

server.listen(PORT, '0.0.0.0', () => {
  const line = '─'.repeat(58);
  console.log('');
  console.log('  ┌' + line + '┐');
  console.log('  │  15.2 云端课堂中继已启动                                 │');
  console.log('  └' + line + '┘');
  console.log('');
  console.log('  端口：' + PORT + '    默认房间：' + DEF_ROOM + (PIN ? '    口令：' + PIN : ''));
  console.log('  学生端页面目录：' + SITE_DIR);
  console.log('');
  console.log('  学生扫码地址（任意网络都能进）：');
  console.log('    http://<你的地址>/student.html?room=' + DEF_ROOM);
  console.log('  听课教师（只读观摩）：');
  console.log('    http://<你的地址>/audit.html?room=' + DEF_ROOM);
  console.log('');
  console.log('  把「你的地址」填进教师端的云端地址里，点「🔗 连接课堂」即可出码。');
  console.log('');
});

/* 定时清理超过 6 小时没人用的房间，避免内存堆积 */
const cleaner = setInterval(() => {
  const now = Date.now();
  rooms.forEach((r, k) => {
    if (r.clients.size === 0 && now - r.born > 6 * 3600e3) rooms.delete(k);
  });
}, 30 * 60e3);
if (cleaner.unref) cleaner.unref();
