const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MAX_PLAYERS = parseInt(process.env.MAX_PLAYERS || '64', 10);
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'brasilrp2026';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const STATS_FILE = path.join(DATA_DIR, 'stats.json');
const ADMIN_HTML_FILE = path.join(__dirname, 'admin.html');

// Sessoes de admin em memoria: token -> expiresAt
const adminSessions = new Map();
const SESSION_TTL_MS = 24 * 60 * 60 * 1000;

// Jogadores conectados via WebSocket: id -> playerObj
const players = new Map();
let nextId = 1;

// Estado persistente de analytics e usuarios
function createDefaultStore() {
  return {
    createdAt: Date.now(),
    totals: {
      pageViews: 0,
      downloadClicks: 0,
      shareClicks: 0,
      gameSessions: 0,
      chatMessages: 0,
      peakOnline: 0,
      peakOnlineAt: null
    },
    uniqueVisitors: {}, // visitorId -> { id, firstSeen, lastSeen, visits, downloads, shares, device, os, browser, referrer, ip, screen }
    users: {},          // userKey -> { key, name, namesHistory, firstSeen, lastSeen, sessions, totalPlaytimeSec, lastDevice, lastOs, lastVersion, lastOta, lastIp, messagesSent, maxSpeed }
    daily: {},          // YYYY-MM-DD -> { pageViews, downloads, shares, gameSessions, peakOnline, uniqueVisitors: [], uniquePlayers: [] }
    hourly: {},         // YYYY-MM-DDTHH -> { pageViews, downloads, gameSessions, peakOnline }
    referrers: {},      // host -> count
    devices: {},        // OS -> count
    recentChats: [],    // [{ ts, from, name, text, scope }]
    recentEvents: []    // [{ ts, type, label, detail, ip }]
  };
}

let store = createDefaultStore();
let storeDirty = false;

function loadStore() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    if (fs.existsSync(STATS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(STATS_FILE, 'utf8'));
      store = Object.assign(createDefaultStore(), parsed);
      store.totals = Object.assign(createDefaultStore().totals, parsed.totals || {});
      console.log('[BrasilRP Server] Estatisticas carregadas de', STATS_FILE);
    }
  } catch (err) {
    console.error('[BrasilRP Server] Falha ao carregar stats.json:', err.message);
  }
}

function saveStoreNow() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    const tmpFile = STATS_FILE + '.tmp';
    fs.writeFileSync(tmpFile, JSON.stringify(store), 'utf8');
    fs.renameSync(tmpFile, STATS_FILE);
    storeDirty = false;
  } catch (err) {
    console.error('[BrasilRP Server] Falha ao salvar stats.json:', err.message);
  }
}

function markDirty() {
  storeDirty = true;
}

loadStore();
setInterval(() => {
  if (storeDirty) saveStoreNow();
}, 10000);

process.on('SIGTERM', () => { saveStoreNow(); process.exit(0); });
process.on('SIGINT', () => { saveStoreNow(); process.exit(0); });

// Helpers de data (fuso BRT UTC-3 aproximado para agrupamento diario/horario legivel)
function brDateObj(ts = Date.now()) {
  return new Date(ts - 3 * 60 * 60 * 1000);
}
function dayKey(ts = Date.now()) {
  return brDateObj(ts).toISOString().slice(0, 10);
}
function hourKey(ts = Date.now()) {
  return brDateObj(ts).toISOString().slice(0, 13);
}

function getDailyBucket(ts = Date.now()) {
  const k = dayKey(ts);
  if (!store.daily[k]) {
    store.daily[k] = {
      pageViews: 0,
      downloads: 0,
      shares: 0,
      gameSessions: 0,
      peakOnline: 0,
      uniqueVisitors: [],
      uniquePlayers: []
    };
    // Mantem no maximo 35 dias
    const keys = Object.keys(store.daily).sort();
    while (keys.length > 35) {
      delete store.daily[keys.shift()];
    }
  }
  return store.daily[k];
}

function getHourlyBucket(ts = Date.now()) {
  const k = hourKey(ts);
  if (!store.hourly[k]) {
    store.hourly[k] = {
      pageViews: 0,
      downloads: 0,
      gameSessions: 0,
      peakOnline: 0
    };
    // Mantem no maximo 48 horas
    const keys = Object.keys(store.hourly).sort();
    while (keys.length > 48) {
      delete store.hourly[keys.shift()];
    }
  }
  return store.hourly[k];
}

function pushUnique(arr, val, limit = 5000) {
  if (!val) return;
  if (!arr.includes(val) && arr.length < limit) arr.push(val);
}

function logEvent(type, label, detail = '', ip = '') {
  store.recentEvents.unshift({
    ts: Date.now(),
    type,
    label: String(label || '').slice(0, 80),
    detail: String(detail || '').slice(0, 160),
    ip: maskIp(ip)
  });
  if (store.recentEvents.length > 150) {
    store.recentEvents.length = 150;
  }
  markDirty();
}

function getClientIp(req) {
  const xf = req.headers['x-forwarded-for'];
  if (xf) return String(xf).split(',')[0].trim();
  const realIp = req.headers['x-real-ip'];
  if (realIp) return String(realIp).trim();
  return (req.socket && req.socket.remoteAddress) || 'unknown';
}

function maskIp(ip) {
  if (!ip) return '-';
  const clean = String(ip).replace(/^::ffff:/, '');
  if (clean.includes('.')) {
    const parts = clean.split('.');
    if (parts.length === 4) return `${parts[0]}.${parts[1]}.${parts[2]}.***`;
  }
  return clean.slice(0, 16);
}

function parseUserAgent(ua = '') {
  const s = String(ua || '');
  let os = 'Outro';
  if (/Android/i.test(s)) os = 'Android';
  else if (/iPhone|iPad|iPod/i.test(s)) os = 'iOS';
  else if (/Windows/i.test(s)) os = 'Windows';
  else if (/Mac OS X|Macintosh/i.test(s)) os = 'macOS';
  else if (/Linux/i.test(s)) os = 'Linux';

  let browser = 'Outro';
  if (/wv\)|BrasilRP/i.test(s)) browser = 'App Brasil RP';
  else if (/Edg\//i.test(s)) browser = 'Edge';
  else if (/OPR\/|Opera/i.test(s)) browser = 'Opera';
  else if (/Chrome\//i.test(s)) browser = 'Chrome';
  else if (/Firefox\//i.test(s)) browser = 'Firefox';
  else if (/Safari\//i.test(s)) browser = 'Safari';

  // Tenta extrair modelo Android (ex: "Android 14; SM-S918B")
  let device = os;
  const m = s.match(/Android\s[\d.]+;\s*([^;)]+)/i);
  if (m && m[1]) {
    device = m[1].replace(/\sBuild\/.*/i, '').trim().slice(0, 32);
  }
  return { os, browser, device };
}

function normalizeReferrer(ref = '') {
  const raw = String(ref || '').trim();
  if (!raw) return 'Direto';
  try {
    const u = new URL(raw);
    const host = u.hostname.replace(/^www\./, '').toLowerCase();
    if (!host || host === 'luh5826-byte.github.io') return 'Direto / Interno';
    return host.slice(0, 50);
  } catch (_) {
    return raw.slice(0, 50);
  }
}

function updatePeakOnline() {
  const count = players.size;
  const now = Date.now();
  if (count > store.totals.peakOnline) {
    store.totals.peakOnline = count;
    store.totals.peakOnlineAt = now;
    markDirty();
  }
  const d = getDailyBucket(now);
  if (count > d.peakOnline) { d.peakOnline = count; markDirty(); }
  const h = getHourlyBucket(now);
  if (count > h.peakOnline) { h.peakOnline = count; markDirty(); }
}

function upsertUserRecord(player, isNewSession = false) {
  const key = (player.deviceId || ('name:' + player.name)).slice(0, 64);
  player.userKey = key;
  const now = Date.now();
  let u = store.users[key];
  if (!u) {
    u = {
      key,
      name: player.name,
      namesHistory: [player.name],
      firstSeen: now,
      lastSeen: now,
      sessions: 0,
      totalPlaytimeSec: 0,
      lastDevice: player.device || 'Android',
      lastOs: player.os || 'Android',
      lastVersion: player.appVersion || '-',
      lastOta: player.otaVersion || '-',
      lastIp: maskIp(player.ip),
      messagesSent: 0,
      maxSpeed: 0
    };
    store.users[key] = u;
  }
  u.lastSeen = now;
  if (player.name && player.name !== u.name) {
    u.name = player.name;
    if (!u.namesHistory.includes(player.name)) {
      u.namesHistory.unshift(player.name);
      if (u.namesHistory.length > 6) u.namesHistory.length = 6;
    }
  }
  if (player.device) u.lastDevice = player.device;
  if (player.os) u.lastOs = player.os;
  if (player.appVersion) u.lastVersion = player.appVersion;
  if (player.otaVersion) u.lastOta = player.otaVersion;
  if (player.ip) u.lastIp = maskIp(player.ip);
  if (isNewSession) {
    u.sessions += 1;
    store.totals.gameSessions += 1;
    const d = getDailyBucket(now);
    d.gameSessions += 1;
    pushUnique(d.uniquePlayers, key);
    const h = getHourlyBucket(now);
    h.gameSessions += 1;
  }
  markDirty();
  return u;
}

// Helpers HTTP / Auth
function sendJson(res, code, obj) {
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  });
  res.end(JSON.stringify(obj));
}

function readJsonBody(req, maxBytes = 32768) {
  return new Promise((resolve) => {
    let body = '';
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size <= maxBytes) body += chunk.toString('utf8');
    });
    req.on('end', () => {
      if (!body) return resolve({});
      try { resolve(JSON.parse(body)); } catch (_) { resolve({}); }
    });
    req.on('error', () => resolve({}));
  });
}

function createAdminToken() {
  const token = crypto.randomBytes(24).toString('hex');
  adminSessions.set(token, Date.now() + SESSION_TTL_MS);
  return token;
}

function isAuthorizedAdmin(req, urlObj) {
  const auth = req.headers['authorization'] || '';
  let token = '';
  if (auth.startsWith('Bearer ')) token = auth.slice(7).trim();
  else if (urlObj && urlObj.searchParams.get('token')) token = urlObj.searchParams.get('token').trim();
  if (!token) return false;
  const exp = adminSessions.get(token);
  if (!exp) return false;
  if (Date.now() > exp) {
    adminSessions.delete(token);
    return false;
  }
  return true;
}

// Helpers WebSocket Multiplayer
function safeSend(ws, obj) {
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify(obj)); } catch (_) {}
  }
}

function broadcast(obj, exceptId = null) {
  const payload = JSON.stringify(obj);
  for (const [id, p] of players.entries()) {
    if (id === exceptId) continue;
    if (p.ws && p.ws.readyState === 1) {
      try { p.ws.send(payload); } catch (_) {}
    }
  }
}

function publicPlayer(p) {
  return {
    id: p.id,
    name: p.name,
    charId: p.charId || 'cool_man',
    carType: p.carType || '',
    x: p.x,
    y: p.y,
    z: p.z,
    rotY: p.rotY,
    speed: p.speed,
    inCar: p.inCar,
    speaking: p.speaking
  };
}

function adminPlayer(p) {
  return {
    id: p.id,
    userKey: p.userKey,
    name: p.name,
    x: Math.round((p.x || 0) * 10) / 10,
    y: Math.round((p.y || 0) * 10) / 10,
    z: Math.round((p.z || 0) * 10) / 10,
    speedKmh: Math.round(Math.abs(p.speed || 0) * 3.6),
    inCar: !!p.inCar,
    speaking: !!p.speaking,
    connectedAt: p.connectedAt,
    sessionSec: Math.floor((Date.now() - p.connectedAt) / 1000),
    device: p.device || 'Android',
    os: p.os || 'Android',
    appVersion: p.appVersion || '-',
    otaVersion: p.otaVersion || '-',
    ip: maskIp(p.ip)
  };
}

function buildAdminDashboardData() {
  const now = Date.now();
  const todayKey = dayKey(now);
  const today = store.daily[todayKey] || {
    pageViews: 0,
    downloads: 0,
    shares: 0,
    gameSessions: 0,
    peakOnline: 0,
    uniqueVisitors: [],
    uniquePlayers: []
  };

  // Ultimas 24 horas ordenadas
  const hourlySeries = [];
  for (let i = 23; i >= 0; i--) {
    const k = hourKey(now - i * 3600 * 1000);
    const b = store.hourly[k] || { pageViews: 0, downloads: 0, gameSessions: 0, peakOnline: 0 };
    hourlySeries.push({
      hour: k.slice(11, 13) + 'h',
      key: k,
      pageViews: b.pageViews,
      downloads: b.downloads,
      gameSessions: b.gameSessions,
      peakOnline: b.peakOnline
    });
  }

  // Ultimos 14 dias ordenados
  const dailySeries = [];
  for (let i = 13; i >= 0; i--) {
    const k = dayKey(now - i * 86400 * 1000);
    const b = store.daily[k] || {
      pageViews: 0, downloads: 0, shares: 0, gameSessions: 0, peakOnline: 0,
      uniqueVisitors: [], uniquePlayers: []
    };
    dailySeries.push({
      day: k.slice(8, 10) + '/' + k.slice(5, 7),
      key: k,
      pageViews: b.pageViews,
      uniqueVisitors: (b.uniqueVisitors || []).length,
      downloads: b.downloads,
      gameSessions: b.gameSessions,
      uniquePlayers: (b.uniquePlayers || []).length,
      peakOnline: b.peakOnline
    });
  }

  const onlineList = [];
  for (const p of players.values()) onlineList.push(adminPlayer(p));

  const usersList = Object.values(store.users)
    .sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0))
    .slice(0, 300);

  const visitorsList = Object.values(store.uniqueVisitors)
    .sort((a, b) => (b.lastSeen || 0) - (a.lastSeen || 0))
    .slice(0, 200);

  const mem = process.memoryUsage();

  return {
    server: {
      name: 'Brasil RP Server',
      status: 'online',
      now,
      uptimeSec: Math.floor(process.uptime()),
      maxPlayers: MAX_PLAYERS,
      memoryMb: Math.round(mem.rss / 1024 / 1024),
      startedAt: now - Math.floor(process.uptime() * 1000)
    },
    summary: {
      playersOnline: players.size,
      peakOnline: store.totals.peakOnline,
      peakOnlineAt: store.totals.peakOnlineAt,
      totalUsers: Object.keys(store.users).length,
      totalPageViews: store.totals.pageViews,
      totalUniqueVisitors: Object.keys(store.uniqueVisitors).length,
      totalDownloads: store.totals.downloadClicks,
      totalShares: store.totals.shareClicks,
      totalGameSessions: store.totals.gameSessions,
      totalChatMessages: store.totals.chatMessages,
      todayPageViews: today.pageViews,
      todayUniqueVisitors: (today.uniqueVisitors || []).length,
      todayDownloads: today.downloads,
      todayGameSessions: today.gameSessions,
      todayUniquePlayers: (today.uniquePlayers || []).length,
      todayPeakOnline: today.peakOnline
    },
    onlinePlayers: onlineList,
    users: usersList,
    visitors: visitorsList,
    hourlySeries,
    dailySeries,
    referrers: store.referrers,
    devices: store.devices,
    recentChats: store.recentChats.slice(0, 60),
    recentEvents: store.recentEvents.slice(0, 80)
  };
}

// Servidor HTTP
const server = http.createServer(async (req, res) => {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Max-Age': '86400'
    });
    res.end();
    return;
  }

  let urlObj;
  try {
    urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  } catch (_) {
    sendJson(res, 400, { error: 'URL invalida' });
    return;
  }

  const pathname = urlObj.pathname;

  // 1. Health check e status publico (usado pelo Render e pelo site)
  if (pathname === '/' || pathname === '/api/status') {
    sendJson(res, 200, {
      name: 'Brasil RP Server',
      status: 'online',
      playersOnline: players.size,
      maxPlayers: MAX_PLAYERS,
      peakOnline: store.totals.peakOnline,
      totalUsers: Object.keys(store.users).length,
      totalPageViews: store.totals.pageViews,
      totalDownloads: store.totals.downloadClicks,
      uptimeSec: Math.floor(process.uptime())
    });
    return;
  }

  // 2. Endpoint de telemetria e acessos da pagina web / jogo
  if (pathname === '/api/track' && req.method === 'POST') {
    const body = await readJsonBody(req);
    const event = String(body.event || 'page_view').slice(0, 32);
    const ip = getClientIp(req);
    const uaInfo = parseUserAgent(body.userAgent || req.headers['user-agent'] || '');
    const refHost = normalizeReferrer(body.referrer || req.headers['referer'] || '');
    const visitorId = String(body.visitorId || '').trim().slice(0, 64) ||
      crypto.createHash('sha1').update(ip + '|' + uaInfo.os + '|' + uaInfo.browser).digest('hex').slice(0, 16);
    const now = Date.now();
    const d = getDailyBucket(now);
    const h = getHourlyBucket(now);

    let v = store.uniqueVisitors[visitorId];
    if (!v) {
      v = {
        id: visitorId,
        firstSeen: now,
        lastSeen: now,
        visits: 0,
        downloads: 0,
        shares: 0,
        device: uaInfo.device,
        os: uaInfo.os,
        browser: uaInfo.browser,
        referrer: refHost,
        ip: maskIp(ip),
        screen: String(body.screen || '').slice(0, 24)
      };
      store.uniqueVisitors[visitorId] = v;
      // Limite de seguranca para memoria/disco
      const vKeys = Object.keys(store.uniqueVisitors);
      if (vKeys.length > 5000) {
        const oldest = vKeys.sort((a, b) => store.uniqueVisitors[a].lastSeen - store.uniqueVisitors[b].lastSeen)[0];
        delete store.uniqueVisitors[oldest];
      }
    }
    v.lastSeen = now;
    if (body.screen) v.screen = String(body.screen).slice(0, 24);

    if (event === 'page_view') {
      store.totals.pageViews += 1;
      v.visits += 1;
      d.pageViews += 1;
      h.pageViews += 1;
      pushUnique(d.uniqueVisitors, visitorId);
      store.referrers[refHost] = (store.referrers[refHost] || 0) + 1;
      store.devices[uaInfo.os] = (store.devices[uaInfo.os] || 0) + 1;
      logEvent('web', 'Visita no site', `${uaInfo.device} (${uaInfo.browser}) • Origem: ${refHost}`, ip);
    } else if (event === 'download_click') {
      store.totals.downloadClicks += 1;
      v.downloads += 1;
      d.downloads += 1;
      h.downloads += 1;
      logEvent('download', 'Download do APK', `${uaInfo.device} • ${String(body.version || 'latest').slice(0, 24)}`, ip);
    } else if (event === 'share_click') {
      store.totals.shareClicks += 1;
      v.shares += 1;
      d.shares += 1;
      logEvent('share', 'Compartilhou o site', `${uaInfo.device}`, ip);
    } else if (event === 'game_open') {
      const fakePlayer = {
        deviceId: String(body.deviceId || visitorId).slice(0, 64),
        name: String(body.playerName || 'Jogador').slice(0, 18),
        device: String(body.device || uaInfo.device).slice(0, 32),
        os: uaInfo.os,
        appVersion: String(body.appVersion || '-').slice(0, 20),
        otaVersion: String(body.otaVersion || '-').slice(0, 20),
        ip
      };
      upsertUserRecord(fakePlayer, false);
      logEvent('game', 'Abriu o jogo', `${fakePlayer.name} • v${fakePlayer.appVersion} (OTA ${fakePlayer.otaVersion})`, ip);
    }

    markDirty();
    sendJson(res, 200, { ok: true, playersOnline: players.size });
    return;
  }

  // 3. Painel Web Admin (/admin ou /dashboard)
  if (pathname === '/admin' || pathname === '/admin/' || pathname === '/dashboard') {
    try {
      const html = fs.readFileSync(ADMIN_HTML_FILE, 'utf8');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      res.end(html);
    } catch (err) {
      sendJson(res, 500, { error: 'admin.html nao encontrado' });
    }
    return;
  }

  // 4. Login Admin
  if (pathname === '/api/admin/login' && req.method === 'POST') {
    const body = await readJsonBody(req);
    const pwd = String(body.password || '');
    if (pwd && pwd === ADMIN_PASSWORD) {
      const token = createAdminToken();
      sendJson(res, 200, { ok: true, token, expiresInMs: SESSION_TTL_MS });
    } else {
      sendJson(res, 401, { ok: false, error: 'Senha incorreta.' });
    }
    return;
  }

  // 5. Logout Admin
  if (pathname === '/api/admin/logout' && req.method === 'POST') {
    const auth = req.headers['authorization'] || '';
    if (auth.startsWith('Bearer ')) adminSessions.delete(auth.slice(7).trim());
    sendJson(res, 200, { ok: true });
    return;
  }

  // 6. Rotas protegidas do Admin
  if (pathname.startsWith('/api/admin/')) {
    if (!isAuthorizedAdmin(req, urlObj)) {
      sendJson(res, 401, { ok: false, error: 'Nao autorizado.' });
      return;
    }

    if (pathname === '/api/admin/stats' && req.method === 'GET') {
      sendJson(res, 200, buildAdminDashboardData());
      return;
    }

    if (pathname === '/api/admin/announce' && req.method === 'POST') {
      const body = await readJsonBody(req);
      const text = String(body.text || '').trim().slice(0, 180);
      if (!text) return sendJson(res, 400, { ok: false, error: 'Mensagem vazia.' });
      const msgObj = {
        type: 'chat',
        from: 'admin',
        name: '📢 [ADMIN]',
        text,
        scope: 'global',
        x: 0,
        z: 0,
        ts: Date.now()
      };
      broadcast(msgObj);
      store.recentChats.unshift({ ts: msgObj.ts, from: 'admin', name: msgObj.name, text, scope: 'global' });
      if (store.recentChats.length > 100) store.recentChats.length = 100;
      logEvent('admin', 'Aviso global enviado', text, getClientIp(req));
      sendJson(res, 200, { ok: true, sentTo: players.size });
      return;
    }

    if (pathname === '/api/admin/kick' && req.method === 'POST') {
      const body = await readJsonBody(req);
      const targetId = String(body.id || '');
      const reason = String(body.reason || 'Desconectado pelo administrador.').slice(0, 100);
      const target = players.get(targetId);
      if (!target) return sendJson(res, 404, { ok: false, error: 'Jogador nao encontrado online.' });
      safeSend(target.ws, { type: 'error', message: reason });
      try { target.ws.close(4000, reason); } catch (_) {}
      logEvent('admin', 'Jogador expulso', `${target.name} (${target.id}) • ${reason}`, getClientIp(req));
      sendJson(res, 200, { ok: true });
      return;
    }

    if (pathname === '/api/admin/export' && req.method === 'GET') {
      res.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Disposition': `attachment; filename="brasilrp-stats-${dayKey()}.json"`,
        'Access-Control-Allow-Origin': '*'
      });
      res.end(JSON.stringify(store, null, 2));
      return;
    }

    if (pathname === '/api/admin/reset-stats' && req.method === 'POST') {
      store = createDefaultStore();
      saveStoreNow();
      logEvent('admin', 'Estatisticas zeradas', 'Reset manual pelo painel', getClientIp(req));
      sendJson(res, 200, { ok: true });
      return;
    }
  }

  sendJson(res, 404, { error: 'Rota nao encontrada' });
});

// Servidor WebSocket Multiplayer
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  if (players.size >= MAX_PLAYERS) {
    safeSend(ws, { type: 'error', message: 'Servidor lotado.' });
    ws.close();
    return;
  }

  const ip = getClientIp(req);
  const uaInfo = parseUserAgent(req.headers['user-agent'] || '');
  const id = 'p' + (nextId++);
  const now = Date.now();

  const player = {
    id,
    ws,
    name: 'Cidadao_' + id.slice(1),
    charId: 'cool_man',
    carType: '',
    deviceId: '',
    userKey: '',
    device: uaInfo.device,
    os: uaInfo.os,
    appVersion: '-',
    otaVersion: '-',
    ip,
    x: 0,
    y: 0,
    z: 0,
    rotY: 0,
    speed: 0,
    inCar: false,
    speaking: false,
    connectedAt: now,
    lastSeen: now,
    sessionCounted: false
  };
  players.set(id, player);
  updatePeakOnline();

  // Envia boas-vindas e lista de jogadores ja conectados
  const others = [];
  for (const [oid, op] of players.entries()) {
    if (oid !== id) others.push(publicPlayer(op));
  }
  safeSend(ws, {
    type: 'welcome',
    id,
    players: others,
    onlineCount: players.size
  });

  broadcast({
    type: 'player_join',
    player: publicPlayer(player),
    onlineCount: players.size
  }, id);

  ws.on('message', (raw) => {
    player.lastSeen = Date.now();
    let msg;
    try {
      msg = JSON.parse(raw.toString('utf8'));
    } catch (_) {
      return;
    }
    if (!msg || typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'join': {
        const clean = String(msg.name || '').trim().slice(0, 18);
        if (clean) player.name = clean;
        if (msg.charId) player.charId = String(msg.charId).trim().slice(0, 32);
        if (msg.deviceId) player.deviceId = String(msg.deviceId).trim().slice(0, 64);
        if (msg.device) player.device = String(msg.device).trim().slice(0, 32);
        if (msg.appVersion) player.appVersion = String(msg.appVersion).trim().slice(0, 20);
        if (msg.otaVersion) player.otaVersion = String(msg.otaVersion).trim().slice(0, 20);

        const isNew = !player.sessionCounted;
        player.sessionCounted = true;
        upsertUserRecord(player, isNew);
        if (isNew) {
          logEvent('join', `${player.name} entrou`, `${player.device} • Online: ${players.size}`, ip);
        }

        broadcast({
          type: 'player_update',
          player: publicPlayer(player)
        }, id);
        break;
      }

      case 'state': {
        if (Number.isFinite(msg.x)) player.x = msg.x;
        if (Number.isFinite(msg.y)) player.y = msg.y;
        if (Number.isFinite(msg.z)) player.z = msg.z;
        if (Number.isFinite(msg.rotY)) player.rotY = msg.rotY;
        if (typeof msg.charId === 'string' && msg.charId) player.charId = msg.charId.slice(0, 32);
        if (typeof msg.carType === 'string') player.carType = msg.carType.slice(0, 40);
        if (Number.isFinite(msg.speed)) {
          player.speed = msg.speed;
          const kmh = Math.round(Math.abs(msg.speed) * 3.6);
          if (player.userKey && store.users[player.userKey] && kmh > (store.users[player.userKey].maxSpeed || 0)) {
            store.users[player.userKey].maxSpeed = kmh;
            markDirty();
          }
        }
        player.inCar = !!msg.inCar;
        player.speaking = !!msg.speaking;
        break;
      }

      case 'chat': {
        const text = String(msg.text || '').trim().slice(0, 160);
        if (!text) return;
        const scope = msg.scope === 'local' ? 'local' : 'global';
        const ts = Date.now();
        store.totals.chatMessages += 1;
        if (player.userKey && store.users[player.userKey]) {
          store.users[player.userKey].messagesSent = (store.users[player.userKey].messagesSent || 0) + 1;
        }
        store.recentChats.unshift({ ts, from: id, name: player.name, text, scope });
        if (store.recentChats.length > 100) store.recentChats.length = 100;
        markDirty();

        broadcast({
          type: 'chat',
          from: id,
          name: player.name,
          text,
          scope,
          x: player.x,
          z: player.z,
          ts
        });
        break;
      }

      // Sinalizacao WebRTC para chat de voz por proximidade
      case 'rtc_offer':
      case 'rtc_answer':
      case 'rtc_ice': {
        const target = players.get(msg.to);
        if (target) {
          safeSend(target.ws, {
            type: msg.type,
            from: id,
            data: msg.data
          });
        }
        break;
      }

      case 'ping': {
        safeSend(ws, { type: 'pong', ts: msg.ts || Date.now() });
        break;
      }
    }
  });

  function cleanupPlayer() {
    if (!players.has(id)) return;
    const durSec = Math.max(0, Math.floor((Date.now() - player.connectedAt) / 1000));
    if (player.userKey && store.users[player.userKey]) {
      store.users[player.userKey].totalPlaytimeSec = (store.users[player.userKey].totalPlaytimeSec || 0) + durSec;
      store.users[player.userKey].lastSeen = Date.now();
      markDirty();
    } else if (!player.sessionCounted && durSec >= 3) {
      upsertUserRecord(player, true);
      if (store.users[player.userKey]) {
        store.users[player.userKey].totalPlaytimeSec += durSec;
      }
    }
    players.delete(id);
    logEvent('leave', `${player.name} saiu`, `Sessao: ${durSec}s • Online: ${players.size}`, ip);
    broadcast({
      type: 'player_leave',
      id,
      onlineCount: players.size
    });
  }

  ws.on('close', cleanupPlayer);
  ws.on('error', () => {
    try { ws.close(); } catch (_) {}
  });
});

// Broadcast consolidado de posicoes a 15 Hz (economiza bateria e dados no celular)
setInterval(() => {
  if (players.size <= 1) return;
  const list = [];
  for (const p of players.values()) {
    list.push(publicPlayer(p));
  }
  const payload = JSON.stringify({ type: 'snapshot', players: list });
  for (const p of players.values()) {
    if (p.ws && p.ws.readyState === 1) {
      try { p.ws.send(payload); } catch (_) {}
    }
  }
}, 66);

// Limpeza de conexoes travadas e sessoes admin expiradas
setInterval(() => {
  const now = Date.now();
  for (const [id, p] of players.entries()) {
    if (now - p.lastSeen > 60000) {
      try { p.ws.terminate(); } catch (_) {}
      players.delete(id);
      broadcast({ type: 'player_leave', id, onlineCount: players.size });
    }
  }
  for (const [tok, exp] of adminSessions.entries()) {
    if (now > exp) adminSessions.delete(tok);
  }
}, 20000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[BrasilRP Server] Online na porta ${PORT} | Painel Admin: http://localhost:${PORT}/admin`);
});
