const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MAX_PLAYERS = 64;

const players = new Map(); // id -> { id, ws, name, x, y, z, rotY, speed, inCar, speaking, lastSeen }
let nextId = 1;

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
    x: p.x,
    y: p.y,
    z: p.z,
    rotY: p.rotY,
    speed: p.speed,
    inCar: p.inCar,
    speaking: p.speaking
  };
}

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*'
  });
  res.end(JSON.stringify({
    name: 'Brasil RP Server',
    status: 'online',
    playersOnline: players.size,
    maxPlayers: MAX_PLAYERS,
    uptimeSec: Math.floor(process.uptime())
  }));
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  if (players.size >= MAX_PLAYERS) {
    safeSend(ws, { type: 'error', message: 'Servidor lotado.' });
    ws.close();
    return;
  }

  const id = 'p' + (nextId++);
  const player = {
    id,
    ws,
    name: 'Cidadao_' + id.slice(1),
    x: 0,
    y: 0,
    z: 0,
    rotY: 0,
    speed: 0,
    inCar: false,
    speaking: false,
    lastSeen: Date.now()
  };
  players.set(id, player);

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
        if (Number.isFinite(msg.speed)) player.speed = msg.speed;
        player.inCar = !!msg.inCar;
        player.speaking = !!msg.speaking;
        break;
      }

      case 'chat': {
        const text = String(msg.text || '').trim().slice(0, 160);
        if (!text) return;
        const scope = msg.scope === 'local' ? 'local' : 'global';
        broadcast({
          type: 'chat',
          from: id,
          name: player.name,
          text,
          scope,
          x: player.x,
          z: player.z,
          ts: Date.now()
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

  ws.on('close', () => {
    players.delete(id);
    broadcast({
      type: 'player_leave',
      id,
      onlineCount: players.size
    });
  });

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

// Limpeza de conexoes travadas
setInterval(() => {
  const now = Date.now();
  for (const [id, p] of players.entries()) {
    if (now - p.lastSeen > 60000) {
      try { p.ws.terminate(); } catch (_) {}
      players.delete(id);
      broadcast({ type: 'player_leave', id, onlineCount: players.size });
    }
  }
}, 20000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[BrasilRP Server] Online na porta ${PORT}`);
});
