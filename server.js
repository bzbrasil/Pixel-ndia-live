// Pixelândia — servidor da live
// Lê a live do TikTok pelo @ do perfil, hospeda a vila e repassa os eventos
// (comentários, presentes, seguidores, curtidas) para a vila em tempo real.
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
let USERNAME = (process.env.TIKTOK_USERNAME || '').replace(/^@/, '').trim();
const SIGN_KEY = process.env.EULER_API_KEY || process.env.SIGN_API_KEY || '';
const MOCK = process.env.MOCK === '1';
const PAGE = fs.existsSync(path.join(__dirname, 'public', 'index.html')) ? path.join(__dirname, 'public', 'index.html') : path.join(__dirname, 'index.html');

/* ---------------- WebSocket mínimo (sem dependências) ---------------- */
const clients = new Set();
function wsSend(sock, obj) {
  const data = Buffer.from(JSON.stringify(obj));
  let header;
  if (data.length < 126) header = Buffer.from([0x81, data.length]);
  else if (data.length < 65536) { header = Buffer.alloc(4); header[0] = 0x81; header[1] = 126; header.writeUInt16BE(data.length, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x81; header[1] = 127; header.writeBigUInt64BE(BigInt(data.length), 2); }
  try { sock.write(Buffer.concat([header, data])); } catch (e) { /* ignora */ }
}
function broadcast(event, data) { for (const s of clients) wsSend(s, { event, data }); }
let lastStatus = 'Servidor iniciado';
function status(msg) { lastStatus = msg; console.log('[status]', msg); broadcast('status', { msg }); }

function handleUpgrade(req, sock) {
  if (!req.url.startsWith('/ws')) { sock.destroy(); return; }
  const key = req.headers['sec-websocket-key'];
  if (!key) { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  clients.add(sock);
  wsSend(sock, { event: 'status', data: { msg: lastStatus } });
  let buf = Buffer.alloc(0);
  sock.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const op = buf[0] & 0x0f, masked = buf[1] & 0x80;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      const mOff = off; if (masked) off += 4;
      if (buf.length < off + len) return;
      let payload = buf.slice(off, off + len);
      if (masked) { const m = buf.slice(mOff, mOff + 4); payload = Buffer.from(payload.map((b, i) => b ^ m[i % 4])); }
      buf = buf.slice(off + len);
      if (op === 0x8) { try { sock.end(Buffer.from([0x88, 0])); } catch (e) {} clients.delete(sock); return; }
      if (op === 0x9) { try { sock.write(Buffer.concat([Buffer.from([0x8a, payload.length]), payload])); } catch (e) {} continue; }
      if (op === 0x1) {
        try {
          const msg = JSON.parse(payload.toString());
          if (msg.type === 'connect' && msg.username) { USERNAME = String(msg.username).replace(/^@/, '').trim(); startTikTok(true); }
        } catch (e) {}
      }
    }
  });
  const bye = () => clients.delete(sock);
  sock.on('close', bye); sock.on('end', bye); sock.on('error', bye);
}
// mantém as conexões vivas
setInterval(() => { for (const s of clients) { try { s.write(Buffer.from([0x89, 0])); } catch (e) {} } }, 25000);

/* ---------------- TikTok ---------------- */
let conn = null, retryTimer = null, attempt = 0;
const pick = (...v) => v.find(x => x !== undefined && x !== null && x !== '');
function userOf(d) {
  const u = d.user || {};
  return { uniqueId: pick(d.uniqueId, u.uniqueId, u.unique_id), nickname: pick(d.nickname, u.nickname, d.uniqueId, u.uniqueId) };
}
function makeConnection(username) {
  const lib = require('tiktok-live-connector');
  const opts = { enableExtendedGiftInfo: true, processInitialData: false };
  if (SIGN_KEY) opts.signApiKey = SIGN_KEY;
  if (lib.TikTokLiveConnection) return new lib.TikTokLiveConnection(username, opts); // versão 2.x
  return new lib.WebcastPushConnection(username, opts); // versão 1.x
}
function startTikTok(force) {
  if (MOCK) { startMock(); return; }
  if (!USERNAME) { status('Falta o @ do TikTok (variável TIKTOK_USERNAME)'); return; }
  if (retryTimer) { clearTimeout(retryTimer); retryTimer = null; }
  if (conn) { try { conn.disconnect(); } catch (e) {} conn = null; }
  try { conn = makeConnection(USERNAME); }
  catch (e) { status('Erro ao carregar a biblioteca do TikTok: ' + e.message); return; }
  conn.on('chat', d => { const u = userOf(d); broadcast('chat', { ...u, comment: d.comment || '' }); });
  conn.on('gift', d => {
    const u = userOf(d), g = d.giftDetails || {}, x = d.extendedGiftInfo || {};
    broadcast('gift', {
      ...u,
      giftId: pick(d.giftId, g.id),
      giftName: pick(d.giftName, g.giftName, x.name, 'presente'),
      diamondCount: pick(d.diamondCount, g.diamondCount, x.diamond_count, 1),
      repeatCount: pick(d.repeatCount, 1),
      repeatEnd: d.repeatEnd !== undefined ? !!d.repeatEnd : true,
      giftType: pick(d.giftType, g.giftType, 0),
    });
  });
  conn.on('follow', d => broadcast('follow', userOf(d)));
  conn.on('like', d => broadcast('like', { ...userOf(d), likeCount: pick(d.likeCount, 1) }));
  conn.on('share', d => broadcast('share', userOf(d)));
  conn.on('streamEnd', () => { status('A live terminou. Tentando de novo em 30s…'); schedule(30); });
  conn.on('disconnected', () => { status('Desconectado do TikTok. Tentando de novo…'); schedule(15); });
  conn.on('error', e => console.log('[tiktok erro]', e && e.message ? e.message : e));
  status('Conectando na live de @' + USERNAME + '…');
  conn.connect().then(() => { attempt = 0; status('✔ Conectado na live de @' + USERNAME); })
    .catch(e => {
      const m = (e && e.message) ? e.message : String(e);
      status(/offline|not live|LIVE has ended|isn't online|UserOfflineError/i.test(m)
        ? '@' + USERNAME + ' não está ao vivo. Tentando de novo em 30s…'
        : 'Não conectou (' + m.slice(0, 120) + '). Tentando de novo…');
      schedule(30);
    });
}
function schedule(sec) {
  if (retryTimer) return;
  attempt++;
  retryTimer = setTimeout(() => { retryTimer = null; startTikTok(); }, Math.min(120, sec + attempt * 5) * 1000);
}
// modo de teste: gera eventos falsos para ver a vila funcionando sem live
function startMock() {
  status('✔ MODO TESTE: eventos simulados');
  const names = ['ana_clara', 'joao.p', 'bia_gamer', 'davi22', 'lu.costa', 'kaue_oficial', 'mel.br', 'theo07'];
  const msgs = ['oi vila', 'quero ser fazendeiro', 'caçador', 'médica', 'mineiro', 'lenhador', '1', '2', 'bora'];
  setInterval(() => {
    const u = names[Math.floor(Math.random() * names.length)];
    const r = Math.random();
    if (r < 0.7) broadcast('chat', { uniqueId: u, nickname: u, comment: msgs[Math.floor(Math.random() * msgs.length)] });
    else if (r < 0.9) broadcast('gift', { uniqueId: u, nickname: u, giftId: 5655, giftName: 'Rose', diamondCount: 1, repeatCount: 1, repeatEnd: true, giftType: 1 });
    else broadcast('gift', { uniqueId: u, nickname: u, giftId: 5827, giftName: 'Heart Me', diamondCount: 5, repeatCount: 1, repeatEnd: true, giftType: 1 });
  }, 2500);
}

/* ---------------- HTTP ---------------- */
const STATE_FILE = path.join(require('os').tmpdir(), 'pixelandia-state.json');
let villageState = null;
const DIARY_FILE = path.join(require('os').tmpdir(), 'pixelandia-diario.json');
let diaryLog = [];
try { diaryLog = JSON.parse(fs.readFileSync(DIARY_FILE, 'utf8')) || []; } catch (e) {}
try { villageState = fs.readFileSync(STATE_FILE, 'utf8'); } catch (e) {}
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/diario' && req.method === 'POST') {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 1e6) req.destroy(); });
    req.on('end', () => {
      try { const d = JSON.parse(body); for (const l of (d.lines || [])) if (l && l.x) diaryLog.push({ t: +l.t || Date.now(), x: String(l.x).slice(0, 300) }); if (diaryLog.length > 8000) diaryLog.splice(0, diaryLog.length - 8000); fs.writeFile(DIARY_FILE, JSON.stringify(diaryLog), () => {}); res.writeHead(200); res.end('ok'); }
      catch (e) { res.writeHead(400); res.end('erro'); }
    });
    return;
  }
  if (url === '/diario') {
    const fmt = new Intl.DateTimeFormat('pt-BR', { timeZone: 'America/Sao_Paulo', day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const txt = diaryLog.length ? diaryLog.map(l => fmt.format(new Date(l.t)) + '  ' + l.x).join('\n') : '(o diário está vazio: ele começa a anotar quando a vila de live (?live=1) está aberta)';
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end('DIÁRIO DA PIXELÂNDIA\n\n' + txt + '\n');
    return;
  }
  if (url === '/diario/limpar' && req.method === 'POST') { diaryLog = []; fs.writeFile(DIARY_FILE, '[]', () => {}); res.writeHead(200); res.end('ok'); return; }
  if (url === '/state') {
    if (req.method === 'POST') {
      let body = '';
      req.on('data', c => { body += c; if (body.length > 3e6) req.destroy(); });
      req.on('end', () => {
        try { const d = JSON.parse(body); if (!d || !Array.isArray(d.r)) throw 0; villageState = body; fs.writeFile(STATE_FILE, body, () => {}); res.writeHead(200, { 'Content-Type': 'application/json' }); res.end('{"ok":true}'); }
        catch (e) { res.writeHead(400); res.end('{"ok":false}'); }
      });
      return;
    }
    res.writeHead(villageState ? 200 : 404, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(villageState || 'null');
    return;
  }
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('ok · ' + lastStatus); return; }
  if (url === '/' || url === '/index.html') {
    fs.readFile(PAGE, 'utf8', (err, html) => {
      if (err) { res.writeHead(500); res.end('Arquivo da vila não encontrado em public/index.html'); return; }
      html = html.replace('<head>', '<head><script>window.VILA_SERVER=' + JSON.stringify({ user: USERNAME }) + ';</script>');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(html);
    });
    return;
  }
  const STATIC = { '/manifest.json': 'application/manifest+json', '/icon-192.png': 'image/png', '/icon-512.png': 'image/png', '/icon-maskable-512.png': 'image/png', '/favicon.ico': 'image/png' };
  if (STATIC[url]) {
    fs.readFile(path.join(__dirname, url === '/favicon.ico' ? 'icon-192.png' : url.slice(1)), (err, buf) => {
      if (err) { res.writeHead(404); res.end(); return; }
      res.writeHead(200, { 'Content-Type': STATIC[url], 'Cache-Control': 'public, max-age=3600' }); res.end(buf);
    });
    return;
  }
  res.writeHead(404); res.end('não encontrado');
});
server.on('upgrade', handleUpgrade);
server.listen(PORT, () => {
  console.log('Pixelândia rodando na porta ' + PORT);
  startTikTok();
});
