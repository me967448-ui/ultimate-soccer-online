'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { EventEmitter } = require('node:events');

// Small RFC 6455 WebSocket peer implemented with Node built-ins, so this package
// does not require a third-party runtime dependency. Browser JSON text frames are supported.

const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || '0.0.0.0';
const HTML_FILE = path.join(__dirname, 'Pitch-Legends-Online.html');
const DB_FILE = path.join(process.env.DATA_DIR || __dirname, 'online-profiles.json');
const TEAM_KEYS = new Set(['blue','royal','green','gold','red','black','white','orange','coastal','violet','desert']);
const FORMATIONS = new Set(['4-3-3','4-4-2','4-2-3-1','3-5-2','4-1-4-1','4-5-1','3-4-3','5-3-2','5-4-1','4-1-2-1-2']);
const BASE_XI = ['base_gk','base_rb','base_cb1','base_cb2','base_lb','base_cdm','base_cm1','base_cm2','base_rw','base_st','base_lw'];
const DEFAULT_PLAYER_POSITIONS = [
  {x:8,y:50},{x:18,y:24},{x:16,y:42},{x:16,y:58},{x:18,y:76},
  {x:26,y:50},{x:31,y:34},{x:31,y:66},{x:39,y:20},{x:44,y:50},{x:39,y:80}
];
const DIVISIONS = [
  {key:'amateur',name:'Amateur',min:0,color:'#a7b0bf',icon:'⚪',style:'Starter tier'},
  {key:'normal',name:'Normal',min:24,color:'#62c0ff',icon:'🔹',style:'Rising competitor'},
  {key:'bronze-pro',name:'Bronze Pro',min:60,color:'#c88b55',icon:'🥉',style:'Bronze division'},
  {key:'silver-pro',name:'Silver Pro',min:120,color:'#c6d0df',icon:'🥈',style:'Silver division'},
  {key:'gold-pro',name:'Gold Pro',min:198,color:'#ffd166',icon:'🥇',style:'Gold division'},
  {key:'legend',name:'Legend',min:294,color:'#c084fc',icon:'💎',style:'Elite division'},
  {key:'world-class',name:'World Class',min:420,color:'#fb7185',icon:'👑',style:'Top global division'}
];
const PLAYER_MARKET = new Map([
  ['market_vargas',{id:'market_vargas',name:'Leo Vargas',position:'ST',rating:88,cost:900}],
  ['market_blaze',{id:'market_blaze',name:'Kai Blaze',position:'RW',rating:86,cost:760}],
  ['market_mateo',{id:'market_mateo',name:'Mateo Cross',position:'LW',rating:87,cost:820}],
  ['market_nova',{id:'market_nova',name:'Rico Nova',position:'ST',rating:90,cost:1150}],
  ['market_maestro',{id:'market_maestro',name:'Leo Maestro',position:'AMF',rating:89,cost:980}],
  ['market_vale',{id:'market_vale',name:'Mika Vale',position:'CM',rating:85,cost:690}],
  ['market_anchor',{id:'market_anchor',name:'Dani Anchor',position:'CDM',rating:88,cost:880}],
  ['market_fortress',{id:'market_fortress',name:'Noah Fortress',position:'CB',rating:89,cost:970}],
  ['market_jet',{id:'market_jet',name:'Ari Jet',position:'RB',rating:84,cost:620}],
  ['market_wall',{id:'market_wall',name:'Sam Wall',position:'GK',rating:87,cost:790}],
  ['market_arrow',{id:'market_arrow',name:'Eden Arrow',position:'LW',rating:87,cost:830}],
  ['market_silk',{id:'market_silk',name:'Tariq Silk',position:'CB',rating:85,cost:650}],
  ['market_frost',{id:'market_frost',name:'Sami Frost',position:'LB',rating:83,cost:590}],
  ['market_costa',{id:'market_costa',name:'Juno Costa',position:'AMF',rating:91,cost:1250}]
]);
const MANAGER_MARKET = new Map([
  ['coach_maestro',{id:'coach_maestro',name:'Marco Reed',rating:82,cost:500,style:'Possession'}],
  ['coach_falcon',{id:'coach_falcon',name:'D. Falcon',rating:86,cost:750,style:'High Press'}],
  ['coach_moreno',{id:'coach_moreno',name:'A. Moreno',rating:88,cost:900,style:'Counter Attack'}],
  ['coach_tanaka',{id:'coach_tanaka',name:'Y. Tanaka',rating:84,cost:620,style:'Compact Defence'}],
  ['coach_duarte',{id:'coach_duarte',name:'Luis Duarte',rating:90,cost:1250,style:'Balanced Elite'}]
]);

class WebSocketPeer extends EventEmitter {
  constructor(socket, head = Buffer.alloc(0)) {
    super();
    this.socket = socket;
    this.readyState = WebSocketPeer.OPEN;
    this.buffer = Buffer.alloc(0);
    this.fragmentOpcode = 0;
    this.fragmentBuffers = [];
    socket.on('data', chunk => this._onData(chunk));
    socket.on('close', () => this._finish());
    socket.on('end', () => this._finish());
    socket.on('error', err => this.emit('error', err));
    if (head && head.length) this._onData(head);
  }
  send(value) {
    if (this.readyState !== WebSocketPeer.OPEN) throw new Error('WebSocket is not open');
    this.socket.write(this._frame(0x1, Buffer.from(String(value), 'utf8')));
  }
  ping() { if (this.readyState === WebSocketPeer.OPEN) this.socket.write(this._frame(0x9, Buffer.alloc(0))); }
  close(code = 1000, reason = '') {
    if (this.readyState !== WebSocketPeer.OPEN) return;
    this.readyState = WebSocketPeer.CLOSING;
    const msg = Buffer.from(String(reason).slice(0,100));
    const payload = Buffer.alloc(2 + msg.length); payload.writeUInt16BE(code,0); msg.copy(payload,2);
    try { this.socket.end(this._frame(0x8,payload)); } catch (_) { this.socket.destroy(); }
    setTimeout(()=>{if(!this.socket.destroyed)this.socket.destroy();},800).unref();
  }
  _frame(opcode, payload) {
    const len = payload.length;
    let header;
    if (len < 126) { header = Buffer.alloc(2); header[1] = len; }
    else if (len < 65536) { header = Buffer.alloc(4); header[1] = 126; header.writeUInt16BE(len,2); }
    else { header = Buffer.alloc(10); header[1] = 127; header.writeBigUInt64BE(BigInt(len),2); }
    header[0] = 0x80 | opcode;
    return Buffer.concat([header,payload]);
  }
  _onData(chunk) {
    this.buffer = Buffer.concat([this.buffer,chunk]);
    while (this.buffer.length >= 2) {
      const b0=this.buffer[0], b1=this.buffer[1], fin=!!(b0&0x80), opcode=b0&0x0f, masked=!!(b1&0x80);
      let len=b1&0x7f, offset=2;
      if (len===126) { if(this.buffer.length<4)return; len=this.buffer.readUInt16BE(2);offset=4; }
      else if (len===127) { if(this.buffer.length<10)return; const wide=this.buffer.readBigUInt64BE(2);if(wide>1024n*1024n){this.close(1009,'Message too large');return;}len=Number(wide);offset=10; }
      const maskBytes=masked?4:0;
      if(this.buffer.length<offset+maskBytes+len)return;
      let payload=this.buffer.subarray(offset+maskBytes,offset+maskBytes+len);
      if(masked){payload=Buffer.from(payload);const mask=this.buffer.subarray(offset,offset+4);for(let i=0;i<payload.length;i++)payload[i]^=mask[i%4];}
      this.buffer=this.buffer.subarray(offset+maskBytes+len);
      if(opcode===0x8){if(this.readyState===WebSocketPeer.OPEN){this.readyState=WebSocketPeer.CLOSING;try{this.socket.end(this._frame(0x8,payload));}catch(_){this.socket.destroy();}}else this.socket.end();this._finish();return;}
      if(opcode===0x9){if(this.readyState===WebSocketPeer.OPEN)this.socket.write(this._frame(0xA,payload));continue;}
      if(opcode===0xA)continue;
      if(opcode===0x1||opcode===0x2){
        if(!fin){this.fragmentOpcode=opcode;this.fragmentBuffers=[payload];continue;}
        this.emit('message',Buffer.from(payload));continue;
      }
      if(opcode===0x0&&this.fragmentOpcode){
        this.fragmentBuffers.push(payload);
        if(fin){const complete=Buffer.concat(this.fragmentBuffers);this.fragmentBuffers=[];this.emit('message',complete);this.fragmentOpcode=0;}
      }
    }
  }
  _finish(){if(this.readyState===WebSocketPeer.CLOSED)return;this.readyState=WebSocketPeer.CLOSED;this.emit('close');}
}
WebSocketPeer.CONNECTING=0;WebSocketPeer.OPEN=1;WebSocketPeer.CLOSING=2;WebSocketPeer.CLOSED=3;

function loadDb() {
  try { const value = JSON.parse(fs.readFileSync(DB_FILE,'utf8')); return value && value.profiles ? value : {profiles:{}}; }
  catch (_) { return {profiles:{}}; }
}
let db = loadDb();
const online = new Map(); // profile ID -> socket
const wsClients = new Set();
const invitations = new Map(); // invitation id -> {id,from,to,mode,createdAt,timer}
const matchRooms = new Map(); // room id -> {id,hostId,guestId,mode,status,settings,finished}
const playerRoom = new Map(); // profile id -> room id
const rankedQueue = [];

function divisionForPoints(value) {
  const points = Number.isFinite(Number(value)) ? Number(value) : 0;
  let division = DIVISIONS[0];
  for (const row of DIVISIONS) if (points >= row.min) division = row;
  return {...division, points};
}
function derivedClubStats(profile) {
  const owned = Array.isArray(profile.ownedPlayers) ? profile.ownedPlayers : [];
  const lineup = Array.isArray(profile.lineup) && profile.lineup.length === 11 ? profile.lineup : BASE_XI;
  const ratings = lineup.map(id => {
    if (BASE_XI.includes(id)) return ({base_gk:66,base_rb:64,base_cb1:65,base_cb2:65,base_lb:64,base_cdm:66,base_cm1:65,base_cm2:65,base_rw:66,base_st:67,base_lw:66})[id] || 65;
    return PLAYER_MARKET.get(id)?.rating || 65;
  });
  const squadRating = Math.round(ratings.reduce((a,b)=>a+b,0)/Math.max(1,ratings.length));
  // Value is a game-currency estimate based on the full roster, signed talent and manager.
  const rosterRatings = [...BASE_XI.map(id=>({id,rating:({base_gk:66,base_rb:64,base_cb1:65,base_cb2:65,base_lb:64,base_cdm:66,base_cm1:65,base_cm2:65,base_rw:66,base_st:67,base_lw:66})[id]})), ...owned.map(id=>({id,rating:PLAYER_MARKET.get(id)?.rating || 0}))];
  const rosterValue = rosterRatings.reduce((sum,x)=>sum + Math.max(0,Number(x.rating)||0)*10000,0);
  const manager = MANAGER_MARKET.get(profile.managerId);
  const clubValue = rosterValue + (manager ? manager.rating*1000 : 0);
  return {squadRating,clubValue,division:divisionForPoints(profile.rankPoints)};
}
function safeProfile(profile) {
  if (!profile) return null;
  const { authToken, ...out } = profile;
  return {...out, rankPoints:Number(profile.rankPoints)||0, rankedWins:Number(profile.rankedWins)||0,
    rankedLosses:Number(profile.rankedLosses)||0, rankedDraws:Number(profile.rankedDraws)||0,
    ...derivedClubStats(profile)};
}
function playerSummary(profile) {
  if (!profile) return null;
  const d = divisionForPoints(profile.rankPoints);
  const stats = derivedClubStats(profile);
  return {id:profile.id,name:profile.name,clubName:profile.clubName,teamKey:profile.teamKey,
    formation:profile.formation,status:'online',rankPoints:Number(profile.rankPoints)||0,division:d,
    squadRating:stats.squadRating,clubValue:stats.clubValue,matches:profile.matches||0,wins:profile.wins||0};
}
function rankingSnapshot() {
  return Object.values(db.profiles).map(playerSummary).sort((a,b)=>b.rankPoints-a.rankPoints||b.squadRating-a.squadRating||a.name.localeCompare(b.name)).slice(0,20);
}
function broadcastRankings() {
  const message={type:'rank:leaderboard',players:rankingSnapshot()};
  for(const ws of online.values()) send(ws,message);
}
function findRoom(id) { const roomId=playerRoom.get(id); return roomId ? matchRooms.get(roomId) : null; }
function sendToProfile(id,payload) { send(online.get(id),payload); }
function publicRoom(room, forId) {
  const host=db.profiles[room.hostId], guest=db.profiles[room.guestId];
  return {roomId:room.id,mode:room.mode,role:forId===room.hostId?'host':'guest',hostId:room.hostId,
    homeProfile:safeProfile(host),awayProfile:safeProfile(guest),settings:room.settings||{duration:30}};
}
function cleanupRoom(room, reason='', notify=true) {
  if(!room) return;
  matchRooms.delete(room.id);
  playerRoom.delete(room.hostId); playerRoom.delete(room.guestId);
  if(notify) {
    sendToProfile(room.hostId,{type:'match:ended',roomId:room.id,message:reason||'The online match room has ended.'});
    sendToProfile(room.guestId,{type:'match:ended',roomId:room.id,message:reason||'The online match room has ended.'});
  }
}
function makeRoom(hostId, guestId, mode) {
  const roomId=crypto.randomBytes(12).toString('hex');
  const room={id:roomId,hostId,guestId,mode,status:'settings',settings:{duration:30},finished:false,createdAt:Date.now()};
  matchRooms.set(roomId,room);playerRoom.set(hostId,roomId);playerRoom.set(guestId,roomId);
  sendToProfile(hostId,{type:'match:settings',...publicRoom(room,hostId)});
  sendToProfile(guestId,{type:'match:settings',...publicRoom(room,guestId)});
  return room;
}
function removeFromRankedQueue(id) { for(let i=rankedQueue.length-1;i>=0;i--) if(rankedQueue[i]===id) rankedQueue.splice(i,1); }
function closeProfileSessions(id) {
  removeFromRankedQueue(id);
  for(const [inviteId,inv] of invitations) {
    if(inv.from===id||inv.to===id) {
      clearTimeout(inv.timer);invitations.delete(inviteId);
      const other=inv.from===id?inv.to:inv.from;
      sendToProfile(other,{type:'match:invite-expired',inviteId,message:'The player is no longer available.'});
    }
  }
  const room=findRoom(id);
  if(room) cleanupRoom(room,'Opponent disconnected. The match has been closed.',true);
}
function persist() {
  try {
    fs.mkdirSync(path.dirname(DB_FILE), {recursive:true});
    const temp = DB_FILE + '.tmp';
    fs.writeFileSync(temp, JSON.stringify(db, null, 2));
    fs.renameSync(temp, DB_FILE);
  } catch (err) { console.error('Could not save online profile database:', err.message); }
}
function send(ws, payload) {
  if (ws && ws.readyState === WebSocketPeer.OPEN) {
    try { ws.send(JSON.stringify(payload)); } catch (_) {}
  }
}
function sendError(ws, message) { send(ws, {type:'online:error', message}); }
function cleanText(value, fallback, max=28) {
  const text = String(value == null ? '' : value).replace(/[<>\u0000-\u001f]/g,' ').replace(/\s+/g,' ').trim().slice(0,max);
  return text || fallback;
}
function newProfile(id, name) {
  const playerName = cleanText(name, 'Guest Player', 24);
  return {
    id,
    authToken: crypto.randomBytes(28).toString('hex'),
    name: playerName,
    clubName: `${playerName}'s FC`.slice(0,28),
    teamKey: 'blue',
    formation: '4-3-3',
    coins: 3000,
    ownedPlayers: [],
    managerId: null,
    lineup: [...BASE_XI],
    playerPositions: DEFAULT_PLAYER_POSITIONS.map(x=>({...x})),
    rankPoints: 0,
    rankedWins: 0,
    rankedLosses: 0,
    rankedDraws: 0,
    matches: 0,
    wins: 0,
    goals: 0,
    lastDailyClaim: 0,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
}
function lobbySnapshot() {
  const result = [];
  for (const [id, socket] of online.entries()) {
    if (socket.readyState !== WebSocketPeer.OPEN || !socket.profileId) continue;
    const p = db.profiles[id];
    if (!p) continue;
    result.push(playerSummary(p));
  }
  result.sort((a,b)=>b.rankPoints-a.rankPoints||a.name.localeCompare(b.name));
  return result;
}

function broadcastLobby() {
  const payload = {type:'lobby:state',players:lobbySnapshot()};
  for (const ws of online.values()) send(ws,payload);
}
function sendProfile(ws, type, message='') {
  const p = db.profiles[ws.profileId];
  if (p) send(ws,{type,ok:true,profile:safeProfile(p),message});
}
function defaultLineup(profile) {
  if (!Array.isArray(profile.lineup) || profile.lineup.length !== 11) profile.lineup = [...BASE_XI];
  if (!Array.isArray(profile.ownedPlayers)) profile.ownedPlayers = [];
  if (!Array.isArray(profile.playerPositions) || profile.playerPositions.length !== 11) profile.playerPositions = DEFAULT_PLAYER_POSITIONS.map(x=>({...x}));
  profile.playerPositions = profile.playerPositions.map((pos,i)=>({x:Math.max(4,Math.min(48,Number(pos?.x)||DEFAULT_PLAYER_POSITIONS[i].x)),y:Math.max(8,Math.min(92,Number(pos?.y)||DEFAULT_PLAYER_POSITIONS[i].y))}));
  if (!Number.isFinite(profile.rankPoints)) profile.rankPoints = 0;
  if (!Number.isFinite(profile.rankedWins)) profile.rankedWins = 0;
  if (!Number.isFinite(profile.rankedLosses)) profile.rankedLosses = 0;
  if (!Number.isFinite(profile.rankedDraws)) profile.rankedDraws = 0;
  profile.ownedPlayers = profile.ownedPlayers.filter((id,i,a)=>PLAYER_MARKET.has(id)&&a.indexOf(id)===i);
  const allowed = new Set([...BASE_XI,...profile.ownedPlayers]);
  profile.lineup = profile.lineup.map(id=>allowed.has(id)?id:BASE_XI[0]);
  // Ensure one unique footballer per slot.
  for (let i=0;i<profile.lineup.length;i++) {
    if (profile.lineup.slice(0,i).includes(profile.lineup[i])) profile.lineup[i] = BASE_XI[i];
  }
  if (!TEAM_KEYS.has(profile.teamKey)) profile.teamKey='blue';
  if (!FORMATIONS.has(profile.formation)) profile.formation='4-3-3';
  if (!Number.isFinite(profile.coins)) profile.coins=3000;
}

const server = http.createServer((req,res)=>{
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname === '/api/health') {
    const players = lobbySnapshot();
    res.writeHead(200, {'content-type':'application/json; charset=utf-8','cache-control':'no-store','access-control-allow-origin':'*'});
    res.end(JSON.stringify({ok:true,playersOnline:players.length,players:players.map(p=>({name:p.name,clubName:p.clubName,teamKey:p.teamKey})),service:'Ultimate Soccer Online Hub'}));
    return;
  }
  if (url.pathname === '/favicon.ico') { res.writeHead(204); res.end(); return; }
  if (url.pathname === '/' || url.pathname === '/index.html' || url.pathname === '/Pitch-Legends-Online.html') {
    fs.readFile(HTML_FILE,(err,data)=>{
      if(err){res.writeHead(500,{'content-type':'text/plain; charset=utf-8'});res.end('Game HTML is missing from this package.');return;}
      res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-cache','x-content-type-options':'nosniff'});res.end(data);
    });
    return;
  }
  res.writeHead(404,{'content-type':'text/plain; charset=utf-8'});res.end('Not found');
});
server.on('upgrade',(req,socket,head)=>{
  const key=req.headers['sec-websocket-key'];
  if(!key || req.headers.upgrade?.toLowerCase()!=='websocket'){socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');socket.destroy();return;}
  const accept=crypto.createHash('sha1').update(String(key)+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
  const ws=new WebSocketPeer(socket,head);wsClients.add(ws);ws.on('close',()=>wsClients.delete(ws));
  handleConnection(ws);
});
function handleConnection(ws){
  ws.profileId = null;
  const authTimer = setTimeout(()=>{if(!ws.profileId){sendError(ws,'Connect handshake timed out. Please reconnect.');ws.close(1008,'Handshake timeout');}},10000);
  ws.on('message',(raw)=>{
    let msg;
    try { msg = JSON.parse(String(raw)); } catch (_) { sendError(ws,'Invalid message format.'); return; }
    if (!msg || typeof msg.type !== 'string') return;
    if (msg.type === 'hello') {
      if (ws.profileId) return;
      clearTimeout(authTimer);
      const id = String(msg.id||'').replace(/[^a-zA-Z0-9_-]/g,'').slice(0,80);
      if (id.length < 8) { sendError(ws,'This guest profile ID is invalid. Reload the game and try again.');ws.close(1008,'Invalid profile');return; }
      let profile = db.profiles[id];
      if (profile) {
        if (!msg.token || String(msg.token)!==String(profile.authToken||'')) {
          sendError(ws,'This profile is already protected by a local guest token. Use the original browser profile to reconnect.');ws.close(1008,'Guest token required');return;
        }
        profile.name = cleanText(msg.name,profile.name||'Guest Player',24);
        profile.clubName = cleanText(msg.clubName,`${profile.name}'s FC`,28);
        profile.teamKey = TEAM_KEYS.has(msg.teamKey) ? msg.teamKey : (profile.teamKey||'blue');
        profile.formation = FORMATIONS.has(msg.formation) ? msg.formation : (profile.formation||'4-3-3');
        defaultLineup(profile);
      } else {
        profile = newProfile(id,msg.name);
        profile.clubName = cleanText(msg.clubName,profile.clubName,28);
        profile.teamKey = TEAM_KEYS.has(msg.teamKey) ? msg.teamKey : 'blue';
        profile.formation = FORMATIONS.has(msg.formation) ? msg.formation : '4-3-3';
        db.profiles[id] = profile;
      }
      profile.updatedAt = new Date().toISOString();
      ws.profileId = id;
      const existing = online.get(id);
      if (existing && existing !== ws) { try{existing.close(4001,'Profile opened in another tab');}catch(_){} }
      online.set(id,ws);
      persist();
      send(ws,{type:'welcome',profile:safeProfile(profile),token:profile.authToken,players:lobbySnapshot(),leaderboard:rankingSnapshot(),market:{players:[...PLAYER_MARKET.values()],managers:[...MANAGER_MARKET.values()]}});
      broadcastLobby();broadcastRankings();
      return;
    }
    const id = ws.profileId;
    const profile = id && db.profiles[id];
    if (!profile || online.get(id)!==ws) { sendError(ws,'Connect to the server before changing your club.'); return; }
    if (msg.type === 'profile:update') {
      profile.name = cleanText(msg.name,profile.name,24);
      profile.clubName = cleanText(msg.clubName,`${profile.name}'s FC`,28);
      profile.teamKey = TEAM_KEYS.has(msg.teamKey) ? msg.teamKey : profile.teamKey;
      profile.formation = FORMATIONS.has(msg.formation) ? msg.formation : profile.formation;
      profile.updatedAt = new Date().toISOString();persist();sendProfile(ws,'profile:updated','Club profile saved to the online server.');broadcastLobby();broadcastRankings();return;
    }
    if (msg.type === 'market:buy') {
      const itemId = String(msg.itemId||'');
      if (msg.kind === 'player') {
        const item = PLAYER_MARKET.get(itemId);
        if (!item) {send(ws,{type:'market:result',ok:false,message:'That player is not available in the current market.'});return;}
        if ((profile.ownedPlayers||[]).includes(item.id)) {send(ws,{type:'market:result',ok:false,message:'You already own this player.'});return;}
        if ((profile.ownedPlayers||[]).length >= 18) {send(ws,{type:'market:result',ok:false,message:'Your club roster is full. Release space before signing another player.'});return;}
        if (profile.coins < item.cost) {send(ws,{type:'market:result',ok:false,message:'Not enough club coins for this signing.'});return;}
        profile.coins -= item.cost;profile.ownedPlayers.push(item.id);profile.updatedAt=new Date().toISOString();persist();sendProfile(ws,'market:result',`${item.name} signed for ${item.cost} coins.`);broadcastLobby();return;
      }
      if (msg.kind === 'manager') {
        const item = MANAGER_MARKET.get(itemId);
        if (!item) {send(ws,{type:'market:result',ok:false,message:'That manager is not available.'});return;}
        if (profile.managerId === item.id) {send(ws,{type:'market:result',ok:false,message:'This manager is already active at your club.'});return;}
        const old = MANAGER_MARKET.get(profile.managerId);
        const refund = old ? Math.floor(old.cost * .5) : 0;
        if (profile.coins + refund < item.cost) {send(ws,{type:'market:result',ok:false,message:'Not enough club coins to hire this manager.'});return;}
        profile.coins = profile.coins + refund - item.cost;profile.managerId=item.id;profile.updatedAt=new Date().toISOString();persist();sendProfile(ws,'market:result',`${item.name} appointed manager${refund?` · ${refund} coins refunded for the previous manager`:''}.`);broadcastLobby();return;
      }
      send(ws,{type:'market:result',ok:false,message:'Unknown market category.'});return;
    }
    if (msg.type === 'daily:claim') {
      const now=Date.now(), previous=Number(profile.lastDailyClaim)||0, interval=24*60*60*1000;
      if(now-previous<interval){const hours=Math.max(1,Math.ceil((interval-(now-previous))/3600000));sendError(ws,`Daily coins were already claimed. Come back in about ${hours} hour${hours===1?'':'s'}.`);return;}
      profile.coins=(Number(profile.coins)||0)+250;profile.lastDailyClaim=now;profile.updatedAt=new Date().toISOString();persist();sendProfile(ws,'profile:updated','Daily reward claimed: +250 club coins.');return;
    }
    if (msg.type === 'lineup:update') {
      const slot = Number(msg.slot), playerId = String(msg.playerId||'');
      const permitted = new Set([...BASE_XI,...(profile.ownedPlayers||[])]);
      if (!Number.isInteger(slot)||slot<0||slot>10||!permitted.has(playerId)) {send(ws,{type:'lineup:updated',ok:false,message:'Invalid lineup change.'});return;}
      defaultLineup(profile);
      const previous = profile.lineup[slot];
      const otherSlot = profile.lineup.indexOf(playerId);
      if (otherSlot >= 0 && otherSlot !== slot) profile.lineup[otherSlot] = previous;
      profile.lineup[slot] = playerId;profile.updatedAt=new Date().toISOString();persist();sendProfile(ws,'lineup:updated','Starting XI updated and saved.');return;
    }
    if (msg.type === 'lineup:reset') {
      profile.lineup=[...BASE_XI];profile.updatedAt=new Date().toISOString();persist();sendProfile(ws,'lineup:updated','Starting XI reset to the academy lineup.');return;
    }
    if (msg.type === 'formation:positions') {
      if (!Array.isArray(msg.positions) || msg.positions.length!==11 || msg.positions.some(v=>!v||!Number.isFinite(Number(v.x))||!Number.isFinite(Number(v.y)))) {sendError(ws,'Invalid tactical positions.');return;}
      profile.playerPositions=msg.positions.map(pos=>({x:Math.max(4,Math.min(48,Number(pos.x))),y:Math.max(8,Math.min(92,Number(pos.y)))}));
      profile.updatedAt=new Date().toISOString();persist();sendProfile(ws,'lineup:updated','Your custom formation is saved to your online club.');return;
    }
    if (msg.type === 'match:invite') {
      const targetId=String(msg.targetId||'');
      if(targetId===id){sendError(ws,'You cannot invite your own club.');return;}
      const targetSocket=online.get(targetId),targetProfile=db.profiles[targetId];
      if(!targetSocket||targetSocket.readyState!==WebSocketPeer.OPEN||!targetProfile){sendError(ws,'That player is no longer online.');return;}
      if(findRoom(id)||findRoom(targetId)){sendError(ws,'One of these players is already in a match.');return;}
      if([...invitations.values()].some(inv=>inv.from===id&&inv.to===targetId)){sendError(ws,'An invitation is already pending for this player.');return;}
      const inviteId=crypto.randomBytes(10).toString('hex');
      const inv={id:inviteId,from:id,to:targetId,mode:'casual',createdAt:Date.now(),timer:null};
      inv.timer=setTimeout(()=>{if(!invitations.has(inviteId))return;invitations.delete(inviteId);sendToProfile(inv.from,{type:'match:invite-expired',inviteId,message:'The invitation expired.'});sendToProfile(inv.to,{type:'match:invite-expired',inviteId,message:'The invitation expired.'});},60000);if(inv.timer.unref)inv.timer.unref();invitations.set(inviteId,inv);
      sendToProfile(targetId,{type:'match:invite-received',inviteId,mode:'casual',from:playerSummary(profile)});
      send(ws,{type:'match:invite-sent',inviteId,target:playerSummary(targetProfile),message:`Invitation sent to ${targetProfile.name}.`});return;
    }
    if (msg.type === 'match:invite-answer') {
      const inviteId=String(msg.inviteId||''),inv=invitations.get(inviteId);
      if(!inv||inv.to!==id){sendError(ws,'This invitation is no longer available.');return;}
      clearTimeout(inv.timer);invitations.delete(inviteId);
      if(!msg.accept){sendToProfile(inv.from,{type:'match:invite-declined',inviteId,player:profile.name,message:`${profile.name} declined your invitation.`});send(ws,{type:'match:invite-answer',ok:true,message:'Invitation declined.'});return;}
      if(!online.has(inv.from)||!online.has(inv.to)||findRoom(inv.from)||findRoom(inv.to)){sendError(ws,'The inviting player is no longer available.');return;}
      makeRoom(inv.from,inv.to,'casual');return;
    }
    if (msg.type === 'match:queue') {
      if(findRoom(id)){sendError(ws,'You are already in an online match.');return;}
      const existingIndex=rankedQueue.findIndex(otherId=>otherId!==id&&online.has(otherId)&&!findRoom(otherId));
      if(existingIndex>=0){const opponentId=rankedQueue.splice(existingIndex,1)[0];removeFromRankedQueue(id);makeRoom(opponentId,id,'ranked');return;}
      removeFromRankedQueue(id);rankedQueue.push(id);send(ws,{type:'match:queue-state',queued:true,message:'Searching for a ranked opponent worldwide…'});return;
    }
    if (msg.type === 'match:queue-cancel') {removeFromRankedQueue(id);send(ws,{type:'match:queue-state',queued:false,message:'Ranked matchmaking cancelled.'});return;}
    if (msg.type === 'match:settings-submit') {
      const room=matchRooms.get(String(msg.roomId||''));
      if(!room||room.hostId!==id||room.status!=='settings'){sendError(ws,'Only the host can confirm these match settings.');return;}
      const duration=['30','60','90'].includes(String(msg.settings?.duration))?Number(msg.settings.duration):30;
      room.settings={duration};room.status='playing';
      const payload={type:'match:begin',roomId:room.id,mode:room.mode,settings:room.settings,homeProfile:safeProfile(db.profiles[room.hostId]),awayProfile:safeProfile(db.profiles[room.guestId])};
      sendToProfile(room.hostId,payload);sendToProfile(room.guestId,payload);return;
    }
    if (msg.type === 'match:input') {
      const room=matchRooms.get(String(msg.roomId||''));if(!room||room.status!=='playing')return;
      if(id!==room.guestId)return;
      if(msg.inputKind==='keys'&&Array.isArray(msg.keys))sendToProfile(room.hostId,{type:'match:input',roomId:room.id,inputKind:'keys',keys:msg.keys.filter(k=>['w','a','s','d',' ','arrowup','arrowdown','arrowleft','arrowright'].includes(k)).slice(0,9)});
      else if(msg.inputKind==='action'&&['x','f','g','i','c','e','1','2','3','4'].includes(String(msg.action)))sendToProfile(room.hostId,{type:'match:input',roomId:room.id,inputKind:'action',action:String(msg.action)});
      return;
    }
    if (msg.type === 'match:snapshot') {
      const room=matchRooms.get(String(msg.roomId||''));if(!room||room.status!=='playing'||id!==room.hostId)return;
      if(typeof msg.snapshot==='object'&&msg.snapshot)sendToProfile(room.guestId,{type:'match:snapshot',roomId:room.id,snapshot:msg.snapshot});return;
    }
    if (msg.type === 'match:finish') {
      const room=matchRooms.get(String(msg.roomId||''));if(!room||room.hostId!==id||room.status!=='playing'||room.finished)return;
      room.finished=true;room.status='finished';
      const homeScore=Math.max(0,Math.min(99,Math.floor(Number(msg.homeScore)||0))),awayScore=Math.max(0,Math.min(99,Math.floor(Number(msg.awayScore)||0)));
      const hp=db.profiles[room.hostId],gp=db.profiles[room.guestId];
      const homeDelta=room.mode==='ranked'?(homeScore>awayScore?6:homeScore<awayScore?-4:0):0;
      const awayDelta=room.mode==='ranked'?(awayScore>homeScore?6:awayScore<homeScore?-4:0):0;
      if(hp){hp.matches=(hp.matches||0)+1;hp.goals=(hp.goals||0)+homeScore;if(homeScore>awayScore)hp.wins=(hp.wins||0)+1;if(room.mode==='ranked'){hp.rankPoints=(Number(hp.rankPoints)||0)+homeDelta;if(homeDelta>0)hp.rankedWins=(hp.rankedWins||0)+1;else if(homeDelta<0)hp.rankedLosses=(hp.rankedLosses||0)+1;else hp.rankedDraws=(hp.rankedDraws||0)+1;}hp.updatedAt=new Date().toISOString();}
      if(gp){gp.matches=(gp.matches||0)+1;gp.goals=(gp.goals||0)+awayScore;if(awayScore>homeScore)gp.wins=(gp.wins||0)+1;if(room.mode==='ranked'){gp.rankPoints=(Number(gp.rankPoints)||0)+awayDelta;if(awayDelta>0)gp.rankedWins=(gp.rankedWins||0)+1;else if(awayDelta<0)gp.rankedLosses=(gp.rankedLosses||0)+1;else gp.rankedDraws=(gp.rankedDraws||0)+1;}gp.updatedAt=new Date().toISOString();}
      persist();
      sendToProfile(room.hostId,{type:'match:result',roomId:room.id,mode:room.mode,homeScore,awayScore,mySide:'home',pointsDelta:homeDelta,profile:safeProfile(hp),opponent:safeProfile(gp)});
      sendToProfile(room.guestId,{type:'match:result',roomId:room.id,mode:room.mode,homeScore,awayScore,mySide:'away',pointsDelta:awayDelta,profile:safeProfile(gp),opponent:safeProfile(hp)});
      broadcastLobby();broadcastRankings();return;
    }
    if (msg.type === 'match:leave') {const room=matchRooms.get(String(msg.roomId||''));if(room&&(room.hostId===id||room.guestId===id)){cleanupRoom(room,'The match room was closed.',true);}return;}
    if (msg.type === 'ping') {send(ws,{type:'pong',at:Date.now()});return;}
    sendError(ws,'Unknown online action.');
  });
  ws.on('close',()=>{
    clearTimeout(authTimer);
    if(ws.profileId && online.get(ws.profileId)===ws){closeProfileSessions(ws.profileId);online.delete(ws.profileId);broadcastLobby();broadcastRankings();}
  });
  ws.on('error',(err)=>console.error('WebSocket error:',err.message));
}
const keepAlive = setInterval(()=>{for(const ws of wsClients){if(ws.readyState===WebSocketPeer.OPEN)ws.ping();}},25000);
server.listen(PORT,HOST,()=>{
  console.log(`Ultimate Soccer Online Hub listening on http://${HOST==='0.0.0.0'?'localhost':HOST}:${PORT}`);
  console.log(`Profiles are saved in: ${DB_FILE}`);
});
function shutdown(){clearInterval(keepAlive);for(const ws of wsClients){try{ws.close(1001,'Server shutting down');}catch(_){}}server.close(()=>process.exit(0));setTimeout(()=>process.exit(0),1500).unref();}
process.on('SIGINT',shutdown);process.on('SIGTERM',shutdown);
