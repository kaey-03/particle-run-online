'use strict';
/* ════════════════════════════════════════════════════════════
   粒子梦境 Particle Run · 双人在线版 — 服务端
   职责：
   1. 托管前端静态页面（public/）
   2. 房间管理：创建 / 加入 / 满员拒绝 / 无人自动清理
   3. 竞速状态：种子下发、开始时间、收集判定、坠毁结算
   4. 断线宽限 + 重连恢复（8 秒内回到原房间原进度）
   说明：赛道内容由客户端用同一种子确定性生成，服务器只做
   「谁收集了什么 / 谁坠毁了 / 分数」的权威仲裁。
   ════════════════════════════════════════════════════════════ */

const express = require('express');
const path = require('path');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
app.use(express.static(path.join(__dirname, '..', 'public')));
app.get('/healthz', (_req, res) => res.json({ ok: true, rooms: rooms.size }));

const server = http.createServer(app);
const io = new Server(server, {
  serveClient: true,                     // 自动在 /socket.io/socket.io.js 提供客户端脚本
  pingInterval: 8000,
  pingTimeout: 4000,
  transports: ['websocket', 'polling'],  // WebSocket 优先，必要时自动回退
});

const PORT = process.env.PORT || 3000;   // 部署平台会注入 PORT，本地默认 3000
const GRACE_MS = 8000;                   // 断线宽限：8 秒内重连可恢复

const rooms = new Map();                 // code -> room

/* ---------- 工具 ---------- */
function genCode() {
  let c;
  do { c = String(Math.floor(100000 + Math.random() * 900000)); } while (rooms.has(c));
  return c;
}
const genPid = () => Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);

function roomOf(sock) { return sock.data.roomCode ? rooms.get(sock.data.roomCode) : null; }
function playerOf(room, sock) {
  for (const p of room.players.values()) if (p.socketId === sock.id) return p;
  return null;
}
function publicPlayers(room) {
  return [...room.players.values()].map(p => ({
    slot: p.slot, connected: p.connected, alive: p.alive, score: p.score,
  }));
}
function scoresMap(room) {
  const out = {};
  for (const p of room.players.values()) out[p.slot] = p.score;
  return out;
}
function broadcastRoom(room) {
  io.to(room.code).emit('room:update', { players: publicPlayers(room), state: room.state });
}

/* ---------- 竞速流程 ---------- */
function startRace(room) {
  room.state = 'running';
  room.seed = (Math.random() * 0xffffffff) >>> 0;
  room.startAt = Date.now() + 4600;      // 预留同步时钟 + 倒计时
  room.collected = new Set();
  room.againVotes = new Set();
  for (const p of room.players.values()) { p.alive = true; p.score = 0; }
  io.to(room.code).emit('race:start', { seed: room.seed, startAt: room.startAt });
}

function finishRace(room, note) {
  if (room.state !== 'running') return;
  room.state = 'over';
  room.againVotes = new Set();
  const results = [...room.players.values()]
    .map(p => ({ slot: p.slot, score: p.score, alive: p.alive }))
    .sort((a, b) => a.slot - b.slot);
  io.to(room.code).emit('race:over', { results, note: note || null });
}

/* 当房间重新凑齐两人（大厅阶段）时自动开赛 */
function maybeAutoStart(room) {
  if (room.state !== 'lobby') return;
  const ps = [...room.players.values()];
  if (ps.length === 2 && ps.every(p => p.connected)) startRace(room);
}

/* ---------- 离开 / 断线 ---------- */
function leaveRoom(sock, notify = true) {
  const room = roomOf(sock);
  sock.data.roomCode = null;
  if (!room) return;
  const p = playerOf(room, sock);
  sock.leave(room.code);
  if (!p) return;
  if (p.dcTimer) { clearTimeout(p.dcTimer); p.dcTimer = null; }
  room.players.delete(p.slot);
  room.againVotes.delete(p.slot);
  if (notify) {
    io.to(room.code).emit('peer:left', { slot: p.slot });
    broadcastRoom(room);
  }
  if (room.players.size === 0) rooms.delete(room.code);
}

function onDisconnect(sock) {
  const room = roomOf(sock);
  sock.data.roomCode = null;
  if (!room) return;
  const p = playerOf(room, sock);
  if (!p) return;
  p.connected = false;
  p.dcTimer = setTimeout(() => {
    if (p.connected) return;                       // 已重连
    room.players.delete(p.slot);
    room.againVotes.delete(p.slot);
    io.to(room.code).emit('peer:left', { slot: p.slot });
    if (room.players.size === 0) { rooms.delete(room.code); return; }
    if (room.state === 'running') {
      // 对方彻底掉线：让仍在奔跑的一方直接获胜结算
      finishRace(room, 'opponent_left');
    } else {
      room.state = 'lobby';
      broadcastRoom(room);
    }
  }, GRACE_MS);
  broadcastRoom(room);                             // 立刻把「已断线」告知对方
}

/* ---------- 连接处理 ---------- */
io.on('connection', (sock) => {
  sock.on('sync:ping', (t, ack) => { if (typeof ack === 'function') ack({ t, now: Date.now() }); });

  /* 创建房间：生成者固定为 1 号位 */
  sock.on('room:create', (ack) => {
    if (typeof ack !== 'function') return;
    leaveRoom(sock);
    const code = genCode();
    const room = {
      code, state: 'lobby', seed: 0, startAt: 0,
      players: new Map(), collected: new Set(), againVotes: new Set(),
    };
    const p = { slot: 1, playerId: genPid(), socketId: sock.id, connected: true, alive: true, score: 0, dcTimer: null };
    room.players.set(1, p);
    rooms.set(code, room);
    sock.join(code);
    sock.data.roomCode = code;
    ack({ ok: true, code, slot: 1, playerId: p.playerId });
  });

  /* 加入房间：2 号位优先；满员 / 比赛中拒绝 */
  sock.on('room:join', (data, ack) => {
    if (typeof ack !== 'function') return;
    const code = String((data && data.code) || '').trim();
    const room = rooms.get(code);
    if (!room) return ack({ ok: false, reason: 'not_found' });
    if (room.state === 'running') return ack({ ok: false, reason: 'running' });
    let slot = null;
    if (!room.players.has(2)) slot = 2;
    else if (!room.players.has(1)) slot = 1;
    if (!slot) return ack({ ok: false, reason: 'full' });
    leaveRoom(sock);
    const p = { slot, playerId: genPid(), socketId: sock.id, connected: true, alive: true, score: 0, dcTimer: null };
    room.players.set(slot, p);
    room.againVotes.clear();
    sock.join(room.code);
    sock.data.roomCode = room.code;
    broadcastRoom(room);
    ack({ ok: true, code: room.code, slot, playerId: p.playerId });
    maybeAutoStart(room);
  });

  /* 刷新 / 短暂断线后的重连：恢复原座位、分数、生死状态 */
  sock.on('room:rejoin', (data, ack) => {
    if (typeof ack !== 'function') return;
    const code = String((data && data.code) || '').trim();
    const playerId = String((data && data.playerId) || '');
    const room = rooms.get(code);
    if (!room) return ack({ ok: false, reason: 'not_found' });
    const p = [...room.players.values()].find(x => x.playerId === playerId);
    if (!p) return ack({ ok: false, reason: 'not_found' });

    const old = io.sockets.sockets.get(p.socketId);
    if (old && old.id !== sock.id) {               // 旧连接还在：踢掉旧窗口
      old.leave(room.code);
      old.data.roomCode = null;
      old.emit('room:kicked', { reason: 'rejoined' });
    }
    if (p.dcTimer) { clearTimeout(p.dcTimer); p.dcTimer = null; }
    p.socketId = sock.id;
    p.connected = true;
    sock.join(room.code);
    sock.data.roomCode = room.code;
    broadcastRoom(room);
    ack({
      ok: true, code: room.code, slot: p.slot, playerId,
      state: room.state, alive: p.alive, score: p.score,
    });
    if (room.state === 'running') {
      sock.emit('race:start', { seed: room.seed, startAt: room.startAt, resume: true });
    } else {
      maybeAutoStart(room);
    }
  });

  /* ---- 比赛内事件（服务器只做转发 + 仲裁） ---- */
  sock.on('p:jump', (data) => {
    const room = roomOf(sock); if (!room) return;
    sock.to(room.code).emit('o:jump', { down: !!(data && data.down) });
  });

  sock.on('p:state', (s) => {
    const room = roomOf(sock); if (!room || room.state !== 'running') return;
    const p = playerOf(room, sock); if (!p) return;
    s = s || {};
    if (Number.isFinite(s.score)) p.score = Math.max(p.score, s.score | 0);
    sock.to(room.code).emit('o:state', {
      h: +s.h || 0, vh: +s.vh || 0,
      grounded: !!s.grounded, alive: !!s.alive, score: p.score,
    });
  });

  sock.on('p:pick', (data) => {
    const room = roomOf(sock); if (!room || room.state !== 'running') return;
    const p = playerOf(room, sock); if (!p) return;
    const id = (data && data.id) | 0;
    if (room.collected.has(id)) return;            // 已被任何人收集 → 忽略（防重复计数）
    room.collected.add(id);
    p.score += 10;
    io.to(room.code).emit('race:picked', { id, slot: p.slot, scores: scoresMap(room) });
  });

  sock.on('p:crash', (data) => {
    const room = roomOf(sock); if (!room || room.state !== 'running') return;
    const p = playerOf(room, sock); if (!p || !p.alive) return;
    p.alive = false;
    if (data && Number.isFinite(data.score)) p.score = Math.max(p.score, data.score | 0);
    io.to(room.code).emit('race:crashed', { slot: p.slot });
    if ([...room.players.values()].every(x => !x.alive)) finishRace(room);
  });

  sock.on('race:again', () => {
    const room = roomOf(sock); if (!room || room.state !== 'over') return;
    const p = playerOf(room, sock); if (!p) return;
    room.againVotes.add(p.slot);
    const ps = [...room.players.values()];
    if (ps.length === 2 && ps.every(x => x.connected) && room.againVotes.size === 2) startRace(room);
  });

  sock.on('room:leave', () => leaveRoom(sock));
  sock.on('disconnect', () => onDisconnect(sock));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[particle-run] 服务已启动: http://localhost:${PORT}`);
});
