import http from 'node:http';
import {randomInt} from 'node:crypto';
import {WebSocketServer, WebSocket} from 'ws';

// Room state is deliberately temporary. A restart closes rooms; no voice is recorded.
const rooms = new Map();
const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const server = http.createServer((req,res) => {
  res.writeHead(req.url === '/health' ? 200 : 404, {'Content-Type':'application/json'});
  res.end(JSON.stringify(req.url === '/health' ? {ok:true,service:'the-hum-relay',protocol:1} : {error:'not found'}));
});
const wss = new WebSocketServer({server,maxPayload:65548,perMessageDeflate:false});
const send = (ws,obj) => {if(ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));};
const fail = (ws,message) => {send(ws,{type:'error',message}); ws.close(1008,message);};
function cleanup(ws) {
  const room=ws.room;
  if(!room || room.peers.get(ws.id)!==ws) return;
  room.peers.delete(ws.id);
  if(ws.id===1) {
    rooms.delete(room.code);
    for(const peer of room.peers.values()) {send(peer,{type:'error',message:'The host left the room.'});peer.close();}
    room.peers.clear();
  } else for(const peer of room.peers.values()) send(peer,{type:'left',id:ws.id});
}
wss.on('connection',ws => {
  ws.alive=true; ws.tokens=256000; ws.last=Date.now(); ws.controls=0;
  const deadline=setTimeout(()=>{if(!ws.room)ws.close(1008,'Room handshake timed out');},15000);
  ws.on('pong',()=>ws.alive=true);
  ws.on('error',()=>{});
  ws.on('close',()=>{clearTimeout(deadline);cleanup(ws);});
  ws.on('message',(data,binary)=>{
    const now=Date.now();
    ws.tokens=Math.min(256000,ws.tokens+(now-ws.last)*128);ws.last=now;
    ws.tokens-=data.length;
    if(ws.tokens<0 || ws.bufferedAmount>1048576)return fail(ws,'Connection sending too much data.');
    if(binary) {
      if(!ws.room || data.length<13)return fail(ws,'Invalid packet.');
      const target=data.readInt32LE(0),channel=data[8],mode=data[9];
      if(channel>2 || mode>2)return fail(ws,'Invalid channel.');
      // Clients send only to the authoritative game host.
      if(ws.id!==1 && target!==1)return fail(ws,'Invalid destination.');
      const packet=Buffer.from(data);packet.writeInt32LE(ws.id,4);
      for(const [id,peer] of ws.room.peers) {
        if(id===ws.id || (target>0 && id!==target) || (target<0 && id===-target))continue;
        if(peer.readyState===WebSocket.OPEN && peer.bufferedAmount<1048576)peer.send(packet,{binary:true});
        else peer.close(1013,'Connection too slow');
      }
      return;
    }
    if(data.length>512 || ++ws.controls>12)return fail(ws,'Invalid control message.');
    let msg;try{msg=JSON.parse(data.toString());}catch{return fail(ws,'Invalid JSON.');}
    if(!msg || msg.protocol!==1)return fail(ws,'Game version mismatch.');
    if(ws.room) {
      if(msg.type==='kick' && ws.id===1 && Number.isInteger(msg.id) && msg.id!==1)ws.room.peers.get(msg.id)?.close(1008,'Removed by host');
      return;
    }
    if(msg.type==='create') {
      if(rooms.size>=100)return fail(ws,'Relay full. Try later.');
      let code;do{code=Array.from({length:8},()=>alphabet[randomInt(alphabet.length)]).join('');}while(rooms.has(code));
      ws.room={code,peers:new Map(),nextId:2};ws.id=1;rooms.set(code,ws.room);
    } else if(msg.type==='join') {
      if(typeof msg.code!=='string' || !/^[A-Z2-9]{8}$/.test(msg.code))return fail(ws,'Check the eight-character room code.');
      ws.room=rooms.get(msg.code);
      if(!ws.room)return fail(ws,'Room not found. Ask the host for a new code.');
      if(ws.room.peers.size>=4){ws.room=null;return fail(ws,'Room is full (four players).');}
      ws.id=ws.room.nextId++;
    } else return fail(ws,'Invalid room request.');
    const others=[...ws.room.peers.keys()];
    ws.room.peers.set(ws.id,ws);
    send(ws,{type:'welcome',id:ws.id,code:ws.room.code,peers:others});
    for(const id of others)send(ws.room.peers.get(id),{type:'joined',id:ws.id});
    clearTimeout(deadline);
  });
});
const heartbeat=setInterval(()=>{
  for(const ws of wss.clients){if(!ws.alive){ws.terminate();continue;}ws.alive=false;ws.ping();}
},25000);
server.on('close',()=>clearInterval(heartbeat));
server.listen(Number(process.env.PORT || 10000),'0.0.0.0',()=>console.log('THE HUM relay listening'));
process.on('SIGTERM',()=>{for(const ws of wss.clients)ws.close(1001,'Relay restarting');wss.close();server.close();});
