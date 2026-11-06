'use strict';
const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { pingInterval: 5000, pingTimeout: 9000 });
app.use(express.static(path.join(__dirname, 'public')));

const CFG = {
  HOURS: 5,
  HOUR_MS: 7000,
  FORCE_STEAL_AT: 6300,          // السرقة الإجبارية قبل إغلاق العينين بلحظات
  PARTNER_MS: 10000,
  DISCUSS_MS: 420000,            // 7 دقائق
  VOTE_UNLOCK_MS: 210000,        // 3:30
  VOTE_MS: 30000,
  MIN_PLAYERS: 4,
  MAX_PLAYERS: 12,
  HOST_GRACE_MS: 15000,
  ROOM_TTL_MS: 10 * 60 * 1000,
};
const AVATARS = ['🦊', '🐺', '🦉', '🐯', '🦁', '🐼', '🐸', '🦄', '🐙', '🦅', '🐲', '🦈', '🐻', '🦇'];
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const rooms = new Map();

/* ---------------- أدوات ---------------- */
const rnd = (n) => Math.floor(Math.random() * n);
const pick = (arr) => arr[rnd(arr.length)];
const clean = (s, n) => String(s || '').replace(/[<>]/g, '').trim().slice(0, n);

function newCode() {
  let c;
  do { c = Array.from({ length: 5 }, () => CODE_CHARS[rnd(CODE_CHARS.length)]).join(''); } while (rooms.has(c));
  return c;
}

function makeRoom(code) {
  return {
    code, hostId: null, players: new Map(), phase: 'lobby', round: 0,
    hour: 0, endsAt: 0, gemOnTable: true, theft: null, theftKey: 0,
    criminalId: null, partnerId: null,
    voteUnlocked: false, voteUnlockAt: 0, wantVote: new Set(), votes: new Map(),
    chat: [], voice: new Set(), result: null, timers: [], hostTimer: null, cleanup: null,
  };
}

function schedule(room, ms, fn) {
  const t = setTimeout(() => {
    room.timers = room.timers.filter((x) => x !== t);
    fn();
  }, ms);
  room.timers.push(t);
}
function clearTimers(room) { room.timers.forEach(clearTimeout); room.timers = []; }

const connected = (room) => [...room.players.values()].filter((p) => p.connected);
const pub = (p) => ({ id: p.id, name: p.name, avatar: p.avatar });

/* ---------------- العرض الخاص بكل لاعب ---------------- */
function roleOf(room, p) {
  if (p.id === room.criminalId) return 'criminal';
  if (p.id === room.partnerId) return 'partner';
  return 'citizen';
}

function viewFor(room, pid) {
  const me = room.players.get(pid);
  const inGame = room.phase !== 'lobby';
  const ended = room.phase === 'end';
  const v = {
    now: Date.now(), code: room.code, phase: room.phase, round: room.round,
    hostId: room.hostId, minPlayers: CFG.MIN_PLAYERS, endsAt: room.endsAt,
    players: [...room.players.values()].map((p) => ({
      ...pub(p), connected: p.connected, score: p.score,
      ready: room.phase === 'reveal' ? p.ready : undefined,
      wantVote: room.phase === 'discussion' ? room.wantVote.has(p.id) : undefined,
      voted: room.phase === 'voting' ? room.votes.has(p.id) : undefined,
      ...(ended ? { role: roleOf(room, p), hour: p.hour } : {}),
    })),
    me: { id: me.id, name: me.name, avatar: me.avatar, isHost: room.hostId === me.id },
  };
  if (inGame) {
    v.me.role = me.id === room.criminalId ? 'criminal' : 'citizen';
    v.me.hour = me.hour;
    v.me.isPartner = room.partnerId === me.id;
    if (me.id === room.criminalId && room.partnerId) {
      v.me.partnerName = room.players.get(room.partnerId).name;
    }
  }

  if (room.phase === 'hours') {
    v.hour = room.hour;
    v.awake = me.hour === room.hour;
    if (v.awake) {
      v.present = [...room.players.values()].filter((p) => p.hour === room.hour).map(pub);
      v.gemOnTable = room.gemOnTable;
      v.canSteal = me.id === room.criminalId && room.gemOnTable;
      v.theftKey = room.theftKey;
      if (room.theft && room.theft.hour === room.hour) {
        if (room.theft.witnessIds.includes(me.id)) v.witnessOf = room.players.get(room.theft.thiefId).name;
        if (room.theft.thiefId === me.id) v.iStole = true;
      }
    }
  }
  if (room.phase === 'partner') {
    if (me.id === room.criminalId) {
      v.partnerOptions = [...room.players.values()].filter((p) => p.id !== room.criminalId).map(pub);
    }
  }
  if (room.phase === 'discussion') {
    v.voteUnlocked = room.voteUnlocked;
    v.voteUnlockAt = room.voteUnlockAt;
    v.iWantVote = room.wantVote.has(pid);
    v.chat = room.chat;
  }
  if (room.phase === 'voting') {
    v.myVote = room.votes.get(pid) || null;
    v.chat = room.chat;
  }
  if (ended) v.result = room.result;
  return v;
}

function broadcast(room) {
  for (const p of room.players.values()) {
    if (p.connected && p.socketId) io.to(p.socketId).emit('state', viewFor(room, p.id));
  }
}

/* ---------------- سير اللعبة ---------------- */
function startRound(room) {
  clearTimers(room);
  room.players.forEach((p, id) => { if (!p.connected) room.players.delete(id); });
  const list = [...room.players.values()];
  room.round += 1;
  room.phase = 'reveal';
  room.criminalId = pick(list).id;
  room.partnerId = null;
  room.gemOnTable = true;
  room.theft = null;
  room.theftKey = 0;
  room.hour = 0;
  room.endsAt = 0;
  room.voteUnlocked = false;
  room.wantVote = new Set();
  room.votes = new Map();
  room.chat = [];
  room.voice = new Set();
  room.result = null;
  // كل لاعب يسحب رقماً عشوائياً مستقلاً (قد تكون هناك ساعة فارغة، وقد يتشارك أكثر من لاعب نفس الساعة)
  list.forEach((p) => { p.hour = 1 + rnd(CFG.HOURS); p.ready = false; });
  broadcast(room);
}

function checkAllReady(room) {
  if (room.phase !== 'reveal') return;
  const c = connected(room);
  if (c.length > 0 && c.every((p) => p.ready)) runHour(room, 1);
}

function runHour(room, h) {
  clearTimers(room);
  room.phase = 'hours';
  room.hour = h;
  room.endsAt = Date.now() + CFG.HOUR_MS;
  broadcast(room);
  schedule(room, CFG.FORCE_STEAL_AT, () => {
    const crim = room.players.get(room.criminalId);
    if (crim && crim.hour === h && room.gemOnTable) steal(room, crim);
  });
  schedule(room, CFG.HOUR_MS, () => (h < CFG.HOURS ? runHour(room, h + 1) : startPartner(room)));
}

function steal(room, thief) {
  room.gemOnTable = false;
  room.theftKey += 1;
  room.theft = {
    hour: room.hour,
    thiefId: thief.id,
    witnessIds: [...room.players.values()].filter((p) => p.hour === room.hour && p.id !== thief.id).map((p) => p.id),
  };
  broadcast(room);
}

function startPartner(room) {
  clearTimers(room);
  room.phase = 'partner';
  room.endsAt = Date.now() + CFG.PARTNER_MS;
  broadcast(room);
  schedule(room, CFG.PARTNER_MS, () => {
    const citizens = [...room.players.values()].filter((p) => p.id !== room.criminalId);
    const pool = citizens.filter((p) => p.connected);
    room.partnerId = pick(pool.length ? pool : citizens).id;
    startDiscussion(room);
  });
}

function startDiscussion(room) {
  clearTimers(room);
  room.phase = 'discussion';
  room.endsAt = Date.now() + CFG.DISCUSS_MS;
  room.voteUnlockAt = Date.now() + CFG.VOTE_UNLOCK_MS;
  room.voteUnlocked = false;
  room.wantVote = new Set();
  broadcast(room);
  schedule(room, CFG.VOTE_UNLOCK_MS, () => { room.voteUnlocked = true; broadcast(room); });
  schedule(room, CFG.DISCUSS_MS, () => startVoting(room));
}

function checkEarlyVote(room) {
  if (room.phase !== 'discussion' || !room.voteUnlocked) return;
  const c = connected(room);
  if (c.length > 0 && c.every((p) => room.wantVote.has(p.id))) startVoting(room);
}

function startVoting(room) {
  clearTimers(room);
  room.phase = 'voting';
  room.endsAt = Date.now() + CFG.VOTE_MS;
  room.votes = new Map();
  broadcast(room);
  schedule(room, CFG.VOTE_MS, () => endGame(room));
}

function checkAllVoted(room) {
  if (room.phase !== 'voting') return;
  const c = connected(room);
  if (c.length > 0 && c.every((p) => room.votes.has(p.id))) endGame(room);
}

function endGame(room) {
  clearTimers(room);
  const tally = {};
  room.votes.forEach((target) => { tally[target] = (tally[target] || 0) + 1; });
  const max = Math.max(0, ...Object.values(tally));
  const leaders = max > 0 ? Object.keys(tally).filter((id) => tally[id] === max) : [];
  // المواطنون يفوزون فقط إذا انفرد المجرم الحقيقي بأعلى الأصوات؛ التعادل يعني فوز المجرم والشريك
  const citizensWin = leaders.length === 1 && leaders[0] === room.criminalId;
  room.players.forEach((p) => {
    const crimTeam = p.id === room.criminalId || p.id === room.partnerId;
    if (citizensWin ? !crimTeam : crimTeam) p.score += 1;
  });
  const votes = {};
  room.votes.forEach((t, v) => { votes[v] = t; });
  room.result = {
    winner: citizensWin ? 'citizens' : 'criminals',
    criminalId: room.criminalId, partnerId: room.partnerId,
    tally, votes, leaders,
  };
  room.phase = 'end';
  room.endsAt = 0;
  broadcast(room);
}

/* ---------------- الانضمام ---------------- */
function addOrResume(room, sock, playerId, name) {
  let p = room.players.get(playerId);
  if (p) {
    p.connected = true;
    p.socketId = sock.id;
    if (name) p.name = name;
  } else {
    if (room.phase !== 'lobby') return { error: 'اللعبة بدأت بالفعل في هذه الغرفة' };
    if (room.players.size >= CFG.MAX_PLAYERS) return { error: 'الغرفة ممتلئة' };
    const used = new Set([...room.players.values()].map((x) => x.avatar));
    const avatar = AVATARS.find((a) => !used.has(a)) || pick(AVATARS);
    p = { id: playerId, name: name || 'لاعب', avatar, connected: true, socketId: sock.id, role: null, hour: 0, ready: false, score: 0 };
    room.players.set(playerId, p);
    if (!room.hostId) room.hostId = playerId;
  }
  clearTimeout(room.cleanup);
  sock.data = { code: room.code, playerId };
  sock.join(room.code);
  return { ok: true, code: room.code };
}

function reassignHostIfNeeded(room) {
  const host = room.players.get(room.hostId);
  if (host && host.connected) return;
  const next = connected(room)[0];
  if (next) { room.hostId = next.id; broadcast(room); }
}

function leaveVoice(room, pid) {
  if (room.voice.delete(pid)) io.to(room.code).emit('voice:left', { id: pid });
}

/* ---------------- Socket.io ---------------- */
io.on('connection', (sock) => {
  const ctx = () => {
    const { code, playerId } = sock.data || {};
    const room = rooms.get(code);
    if (!room || !room.players.has(playerId)) return {};
    return { room, me: room.players.get(playerId) };
  };

  sock.on('room:create', (d, ack) => {
    if (typeof ack !== 'function') return;
    const playerId = clean(d && d.playerId, 40);
    const name = clean(d && d.name, 14);
    if (!playerId || !name) return ack({ ok: false, error: 'اكتب اسمك أولاً' });
    const room = makeRoom(newCode());
    rooms.set(room.code, room);
    const r = addOrResume(room, sock, playerId, name);
    ack(r.ok ? r : { ok: false, error: r.error });
    broadcast(room);
  });

  sock.on('room:join', (d, ack) => {
    if (typeof ack !== 'function') return;
    const playerId = clean(d && d.playerId, 40);
    const name = clean(d && d.name, 14);
    const code = clean(d && d.code, 5).toUpperCase();
    const room = rooms.get(code);
    if (!room) return ack({ ok: false, error: 'الغرفة غير موجودة' });
    if (!playerId) return ack({ ok: false, error: 'معرّف غير صالح' });
    const existing = room.players.has(playerId);
    if (!existing && !name) return ack({ ok: false, error: 'اكتب اسمك أولاً' });
    const r = addOrResume(room, sock, playerId, name);
    if (!r.ok) return ack({ ok: false, error: r.error });
    ack(r);
    broadcast(room);
  });

  sock.on('room:leave', () => {
    const { room, me } = ctx();
    if (!room) return;
    leaveVoice(room, me.id);
    if (room.phase === 'lobby') room.players.delete(me.id);
    else me.connected = false;
    sock.leave(room.code);
    sock.data = {};
    reassignHostIfNeeded(room);
    broadcast(room);
  });

  sock.on('game:start', () => {
    const { room, me } = ctx();
    if (!room || room.hostId !== me.id || room.phase !== 'lobby') return;
    if (connected(room).length < CFG.MIN_PLAYERS) return;
    startRound(room);
  });

  sock.on('game:replay', () => {
    const { room, me } = ctx();
    if (!room || room.hostId !== me.id || room.phase !== 'end') return;
    if (connected(room).length < CFG.MIN_PLAYERS) return;
    room.voice.forEach((id) => io.to(room.code).emit('voice:left', { id }));
    startRound(room);
  });

  sock.on('player:ready', () => {
    const { room, me } = ctx();
    if (!room || room.phase !== 'reveal') return;
    me.ready = true;
    broadcast(room);
    checkAllReady(room);
  });

  sock.on('gem:steal', () => {
    const { room, me } = ctx();
    if (!room || room.phase !== 'hours') return;
    if (me.id !== room.criminalId || me.hour !== room.hour || !room.gemOnTable) return;
    steal(room, me);
  });

  sock.on('partner:choose', (d) => {
    const { room, me } = ctx();
    if (!room || room.phase !== 'partner' || me.id !== room.criminalId) return;
    const t = room.players.get(d && d.id);
    if (!t || t.id === room.criminalId) return;
    room.partnerId = t.id;
    startDiscussion(room);
  });

  sock.on('vote:early', (d) => {
    const { room, me } = ctx();
    if (!room || room.phase !== 'discussion' || !room.voteUnlocked) return;
    if (d && d.want === false) room.wantVote.delete(me.id); else room.wantVote.add(me.id);
    broadcast(room);
    checkEarlyVote(room);
  });

  sock.on('vote:cast', (d) => {
    const { room, me } = ctx();
    if (!room || room.phase !== 'voting') return;
    const t = room.players.get(d && d.id);
    if (!t || t.id === me.id) return;
    room.votes.set(me.id, t.id);
    broadcast(room);
    checkAllVoted(room);
  });

  sock.on('chat:send', (d) => {
    const { room, me } = ctx();
    if (!room || (room.phase !== 'discussion' && room.phase !== 'voting')) return;
    const text = clean(d && d.text, 200);
    if (!text) return;
    const msg = { id: Date.now() + '-' + rnd(1e6), from: me.id, name: me.name, avatar: me.avatar, text };
    room.chat.push(msg);
    if (room.chat.length > 100) room.chat.shift();
    for (const p of connected(room)) io.to(p.socketId).emit('chat:msg', msg);
  });

  /* ---- إشارات WebRTC (دردشة صوتية P2P) ---- */
  sock.on('voice:join', (_d, ack) => {
    const { room, me } = ctx();
    if (!room || typeof ack !== 'function') return;
    const peers = [...room.voice].filter((id) => id !== me.id && room.players.get(id) && room.players.get(id).connected);
    room.voice.add(me.id);
    ack({ peers });
  });
  sock.on('voice:leave', () => {
    const { room, me } = ctx();
    if (room) leaveVoice(room, me.id);
  });
  sock.on('rtc:signal', (d) => {
    const { room, me } = ctx();
    if (!room || !d) return;
    const to = room.players.get(d.to);
    if (to && to.connected) io.to(to.socketId).emit('rtc:signal', { from: me.id, data: d.data });
  });

  sock.on('disconnect', () => {
    const { room, me } = ctx();
    if (!room || me.socketId !== sock.id) return;
    me.connected = false;
    leaveVoice(room, me.id);
    if (room.hostId === me.id) {
      clearTimeout(room.hostTimer);
      room.hostTimer = setTimeout(() => reassignHostIfNeeded(room), CFG.HOST_GRACE_MS);
    }
    broadcast(room);
    checkAllReady(room);
    checkEarlyVote(room);
    checkAllVoted(room);
    if (connected(room).length === 0) {
      room.cleanup = setTimeout(() => { clearTimers(room); rooms.delete(room.code); }, CFG.ROOM_TTL_MS);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`لعبة المجرم والمواطنين تعمل على http://localhost:${PORT}`));
