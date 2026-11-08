/* ============================================================
   GAME OF AP · SERVIDOR ONLINE (sem dependências, só Node 18+)
   - Serve o jogo (index.html) e o WebSocket na MESMA porta/endereço
   - Contas (ID de 6 números), amigos, convites, times (2X2) e relay
   ============================================================ */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const GAME_FILE = path.join(__dirname, 'index.html');
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

/* ---------------- contas (salvas em arquivo) ---------------- */
let acc = {}; // id -> {id,name,tok,friends:[ids]}
try { acc = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')) || {}; } catch (e) { acc = {}; }
let dirty = false;
setInterval(() => {
  if (!dirty) return;
  dirty = false;
  try { fs.writeFileSync(DATA_FILE, JSON.stringify(acc)); } catch (e) { /* disco só leitura: tudo bem */ }
}, 5000);

const online = {};   // id -> Conn
const partyOf = {};  // id -> {members:[a,b],host}
const invites = {};  // 'para:de' -> timestamp

const cleanName = n => String(n == null ? '' : n).replace(/[^\p{L}\p{N} _.\-]/gu, '').trim().slice(0, 12) || 'JOGADOR';
const newId = () => { let i; do { i = String(Math.floor(Math.random() * 1e6)).padStart(6, '0'); } while (acc[i]); return i; };
const isId = v => typeof v === 'string' && /^\d{6}$/.test(v);

/* ---------------- WebSocket (RFC 6455) feito à mão ---------------- */
class Conn {
  constructor(sock) {
    this.s = sock; this.buf = Buffer.alloc(0); this.fr = null;
    this.alive = true; this.user = null; this.n = 0; this.nt = Date.now(); this.closed = false;
    sock.setNoDelay(true);
    sock.on('data', d => {
      this.buf = Buffer.concat([this.buf, d]);
      if (this.buf.length > 1 << 20) return this.close();
      this.parse();
    });
    sock.on('close', () => this.gone());
    sock.on('error', () => { });
  }
  parse() {
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = !!(b[0] & 128), op = b[0] & 15, mask = !!(b[1] & 128);
      let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 65536) return this.close();
      const need = off + (mask ? 4 : 0) + len;
      if (b.length < need) return;
      let p = b.subarray(off + (mask ? 4 : 0), need);
      if (mask) { const m = b.subarray(off, off + 4); p = Buffer.from(p); for (let i = 0; i < p.length; i++) p[i] ^= m[i & 3]; }
      this.buf = b.subarray(need);
      this.frame(op, fin, p);
      if (this.closed) return;
    }
  }
  frame(op, fin, p) {
    if (op === 8) return this.close();
    if (op === 9) return this.raw(10, p);
    if (op === 10) { this.alive = true; return; }
    if (op === 1 || op === 0) {
      if (op === 1) this.fr = [p]; else if (this.fr) this.fr.push(p);
      if (fin && this.fr) { const t = Buffer.concat(this.fr).toString('utf8'); this.fr = null; this.msg(t); }
    }
  }
  raw(op, p) {
    if (this.closed || this.s.destroyed) return;
    const l = p.length;
    const h = l < 126 ? Buffer.from([128 | op, l]) : l < 65536 ? Buffer.from([128 | op, 126, l >> 8, l & 255]) : null;
    if (!h) return;
    try { this.s.write(Buffer.concat([h, p])); } catch (e) { }
  }
  send(o) { this.raw(1, Buffer.from(typeof o === 'string' ? o : JSON.stringify(o))); }
  close() { if (this.closed) return; try { this.raw(8, Buffer.alloc(0)); this.s.end(); } catch (e) { } setTimeout(() => { try { this.s.destroy(); } catch (e) { } }, 500); this.gone(); }
  gone() { if (this.closed) return; this.closed = true; disconnect(this); }
  msg(t) {
    const now = Date.now();
    if (now - this.nt > 1000) { this.nt = now; this.n = 0; }
    if (++this.n > 150) return;            // anti-flood
    let m; try { m = JSON.parse(t); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    handle(this, m);
  }
}

/* ---------------- lógica do jogo online ---------------- */
const send = (id, o) => { const c = online[id]; if (c) c.send(o); };
const note = (id, m) => send(id, { t: 'note', m });

function friendList(id) {
  const a = acc[id]; if (!a) return [];
  return a.friends.filter(f => acc[f]).map(f => ({ id: f, name: acc[f].name, online: !!online[f] }));
}
const pushFriends = id => { if (online[id]) send(id, { t: 'friends', list: friendList(id) }); };
const pushFriendsOfMine = id => { const a = acc[id]; if (a) a.friends.forEach(pushFriends); };

function partyMsg(p) { return { t: 'party', host: p.host, members: p.members.map(i => ({ id: i, name: acc[i] ? acc[i].name : '?' })) }; }
function leaveParty(id, tell) {
  const p = partyOf[id]; if (!p) return;
  for (const m of p.members) { delete partyOf[m]; if (m !== id || tell) send(m, { t: 'pleft' }); }
}

function hello(c, m) {
  let id = isId(m.id) ? m.id : null;
  const tok = typeof m.token === 'string' ? m.token.slice(0, 64) : '';
  if (tok.length < 8) return c.close();
  const name = cleanName(m.name);
  if (!id || (acc[id] && acc[id].tok !== tok)) id = newId();   // ID tomado por outra pessoa → gera outro
  if (!acc[id]) acc[id] = { id, name, tok, friends: [] };
  acc[id].name = name;
  // amigos que o aparelho lembra (útil quando o servidor reinicia e perde o arquivo)
  if (Array.isArray(m.fr)) for (const f of m.fr.slice(0, 100)) {
    if (isId(f) && f !== id && acc[f] && !acc[id].friends.includes(f)) {
      acc[id].friends.push(f);
      if (!acc[f].friends.includes(id)) acc[f].friends.push(id);
    }
  }
  dirty = true;
  const old = online[id];
  if (old && old !== c) { old.user = null; old.send({ t: 'kicked' }); }   // outro aparelho entrou na conta
  c.user = id; online[id] = c;
  c.send({ t: 'welcome', id });
  pushFriends(id);
  if (partyOf[id]) send(id, partyMsg(partyOf[id]));
  pushFriendsOfMine(id);
}

function handle(c, m) {
  if (m.t === 'hello') return hello(c, m);
  const me = c.user; if (!me || online[me] !== c) return;
  switch (m.t) {
    case 'r': {                                   // relay para o parceiro do time
      const p = partyOf[me]; if (!p) return;
      const o = p.members.find(x => x !== me);
      if (o && online[o]) online[o].send(JSON.stringify({ t: 'r', d: m.d }));
      break;
    }
    case 'addfriend': {
      const f = m.id;
      if (!isId(f)) return c.send({ t: 'friendadd', ok: false, err: 'ID inválido.' });
      if (f === me) return c.send({ t: 'friendadd', ok: false, err: 'Esse é o seu ID.' });
      if (!acc[f]) return c.send({ t: 'friendadd', ok: false, err: 'ID não encontrado. Peça para o amigo abrir o jogo online.' });
      if (acc[me].friends.includes(f)) return c.send({ t: 'friendadd', ok: false, err: 'Vocês já são amigos.' });
      acc[me].friends.push(f); if (!acc[f].friends.includes(me)) acc[f].friends.push(me);
      dirty = true;
      c.send({ t: 'friendadd', ok: true, name: acc[f].name });
      pushFriends(me); pushFriends(f); note(f, acc[me].name + ' adicionou você como amigo!');
      break;
    }
    case 'invite': {
      const to = m.to;
      if (!isId(to) || !acc[me].friends.includes(to)) return note(me, 'Esse jogador não é seu amigo.');
      if (!online[to]) return note(me, 'Seu amigo está offline.');
      const p = partyOf[me]; if (p && p.members.length >= 2) return note(me, 'Seu time já está cheio.');
      invites[to + ':' + me] = Date.now();
      send(to, { t: 'invite', from: me, name: acc[me].name });
      break;
    }
    case 'accept': {
      const from = m.from, k = me + ':' + from, t0 = invites[k];
      delete invites[k];
      if (!isId(from) || !t0 || Date.now() - t0 > 60000) return note(me, 'O convite expirou.');
      if (!online[from]) return note(me, 'Quem convidou saiu do jogo.');
      if (partyOf[from] && !partyOf[from].members.includes(me)) return note(me, 'O time já está cheio.');
      leaveParty(me, false);
      const p = partyOf[from] || { members: [from, me], host: from };
      if (!p.members.includes(me)) p.members.push(me);
      partyOf[from] = partyOf[me] = p;
      p.members.forEach(i => send(i, partyMsg(p)));
      break;
    }
    case 'decline': {
      const from = m.from; delete invites[me + ':' + from];
      if (isId(from)) note(from, acc[me].name + ' recusou o convite.');
      break;
    }
    case 'leave': leaveParty(me, false); break;
    case 'start': {
      const p = partyOf[me];
      if (!p || p.host !== me) return;
      if (p.members.length < 2 || !p.members.every(i => online[i])) return note(me, 'Falta um jogador no time.');
      p.members.forEach(i => send(i, { t: 'start' }));
      break;
    }
  }
}

function disconnect(c) {
  const id = c.user;
  if (!id || online[id] !== c) return;
  delete online[id];
  leaveParty(id, false);
  pushFriendsOfMine(id);
}

/* ---------------- HTTP: entrega o jogo e responde /health ---------------- */
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (url === '/' || url === '/index.html') {
    fs.readFile(GAME_FILE, 'utf8', (err, html) => {
      if (err) { res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('index.html não encontrado ao lado do server.js'); }
      const proto = String(req.headers['x-forwarded-proto'] || 'http').split(',')[0].trim() === 'https' ? 'wss' : 'ws';
      const host = String(req.headers['x-forwarded-host'] || req.headers.host || '').split(',')[0].trim();
      // o jogo servido por aqui já sai apontando para este mesmo servidor
      html = html.replace("const SERVER_URL='';", "const SERVER_URL='" + proto + "://" + host.replace(/[^\w.:\-]/g, '') + "';");
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(html);
    });
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('404');
});

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (String(req.headers.upgrade || '').toLowerCase() !== 'websocket' || !key) return sock.destroy();
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  if (Object.keys(online).length > 800) return sock.destroy();
  const c = new Conn(sock);
  const t = setTimeout(() => { if (!c.user) c.close(); }, 15000);   // precisa dar "hello" em 15s
  sock.on('close', () => clearTimeout(t));
});

// ping a cada 25s (mantém a conexão viva na hospedagem e derruba conexões mortas)
setInterval(() => {
  for (const id in online) {
    const c = online[id];
    if (!c.alive) { c.close(); continue; }
    c.alive = false; c.raw(9, Buffer.alloc(0));
  }
  const now = Date.now();
  for (const k in invites) if (now - invites[k] > 120000) delete invites[k];
}, 25000);

server.listen(PORT, () => console.log('Game Of Ap online na porta ' + PORT));
