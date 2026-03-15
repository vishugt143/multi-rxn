// worker.js - Reaction Worker (module)
export default {
  async fetch(request, env, ctx) {
    const FIREBASE_DB_URL = env.FIREBASE_DB_URL; // set in Worker env
    const FIREBASE_SECRET = env.FIREBASE_SECRET; // set in Worker env
    const DEFAULT_DELAY_BETWEEN_REACTIONS = Number(env.DEFAULT_DELAY_BETWEEN_REACTIONS || 800);
    const INITIAL_WAIT_BEFORE_REACTIONS = Number(env.INITIAL_WAIT_BEFORE_REACTIONS || 700);

    const url = new URL(request.url);
    const parts = url.pathname.split('/').filter(Boolean);
    let masterKey = parts.length >= 2 && parts[0] === 'webhook' ? parts[1] : null;

    if (request.method === 'GET' || request.method === 'HEAD') {
      return new Response(`Reaction Worker. masterKey: ${masterKey||'none'}`, { headers:{'Content-Type':'text/plain'}});
    }
    if (request.method === 'OPTIONS') {
      return new Response(null, { status:204, headers:{'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'POST,OPTIONS','Access-Control-Allow-Headers':'Content-Type'}});
    }
    if (request.method !== 'POST') return new Response('Method Not Allowed', { status:405 });

    let update;
    try { update = await request.json(); } catch (e) { return new Response('Bad Request', { status:400 }); }

    if (!masterKey) masterKey = url.searchParams.get('master') || null;

    if (update.channel_post) {
      const chat = update.channel_post.chat;
      const chatId = chat.username ? `@${chat.username}` : chat.id;
      const messageId = update.channel_post.message_id;
      ctx.waitUntil(processChannelPost({FIREBASE_DB_URL,FIREBASE_SECRET,DEFAULT_DELAY_BETWEEN_REACTIONS,INITIAL_WAIT_BEFORE_REACTIONS, masterKey, chatId, messageId}));
    }

    return new Response('OK', { status:200 });
  }
};

async function processChannelPost(opts) {
  const { FIREBASE_DB_URL, FIREBASE_SECRET, DEFAULT_DELAY_BETWEEN_REACTIONS, INITIAL_WAIT_BEFORE_REACTIONS, masterKey, chatId, messageId } = opts;
  if (!masterKey) { console.warn('no masterKey'); return; }

  // Helper to fetch from Firebase Realtime DB
  async function fb_get(path) {
    const u = `${FIREBASE_DB_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
    const r = await fetch(u);
    if (!r.ok) { console.warn('fb_get failed', r.status); return null; }
    return await r.json().catch(()=>null);
  }
  async function fb_patch(path, data) {
    const u = `${FIREBASE_DB_URL}/${path}.json?auth=${FIREBASE_SECRET}`;
    await fetch(u, { method:'PATCH', headers:{'Content-Type':'application/json'}, body: JSON.stringify(data) });
  }

  try {
    const assignmentsObj = await fb_get('assignments') || {};
    const relevant = [];
    for (const id in assignmentsObj) {
      const a = assignmentsObj[id];
      if (!a || !a.enabled) continue;
      if (a.masterKey !== masterKey) continue;
      if (Number(a.channel_id) !== Number(chatId)) continue;
      if (a.last_message_id && Number(a.last_message_id) === Number(messageId)) continue;
      relevant.push(Object.assign({ id }, a));
    }
    if (relevant.length === 0) { console.log('no assignments'); return; }

    const slavesObj = await fb_get('slaves') || {};
    const slaves = [];
    for (const k in slavesObj) {
      const s = slavesObj[k];
      if (s && s.enabled && s.token) slaves.push({ id:k, token:s.token });
    }
    if (slaves.length === 0) { console.warn('no slaves'); return; }

    await sleep(INITIAL_WAIT_BEFORE_REACTIONS);

    for (const a of relevant) {
      const reactions = Array.isArray(a.reactions) && a.reactions.length ? a.reactions : ['🔥'];
      const count = Math.max(1, Math.min(slaves.length, Number(a.count) || 1));
      const chosen = pickRandomUnique(slaves.map(s=>s.token), count);

      for (const token of chosen) {
        const emoji = reactions[Math.floor(Math.random()*reactions.length)];
        await sendReactionWithRetries(token, chatId, messageId, emoji);
        await sleep(DEFAULT_DELAY_BETWEEN_REACTIONS);
      }

      await fb_patch(`assignments/${a.id}`, { last_message_id: Number(messageId) });
      console.log('processed assignment', a.id, messageId);
    }

  } catch (err) {
    console.error('processChannelPost error', err);
  }
}

async function sendReactionWithRetries(token, chatId, messageId, emoji) {
  const url = `https://api.telegram.org/bot${token}/setMessageReaction`;
  let attempt=0, backoff=800;
  while (attempt < 5) {
    attempt++;
    try {
      const res = await fetch(url, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ chat_id: chatId, message_id: messageId, reaction: [{ type:'emoji', emoji }] })
      });
      if (res.ok) { console.log('reaction ok', mask(token), emoji); return true; }
      const body = await safeJson(res);
      console.warn('reaction error', res.status, body);
      if (res.status === 429) { await sleep(backoff); backoff *= 2; continue; }
      if (res.status === 400 && body && body.description && body.description.toLowerCase().includes('message')) { await sleep(1000); continue; }
      return false;
    } catch (err) {
      console.error('network', err);
      await sleep(backoff); backoff *= 2;
    }
  }
  return false;
}

function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }
function pickRandomUnique(arr,n){ const c=arr.slice(); const out=[]; while(out.length<n && c.length){ const i=Math.floor(Math.random()*c.length); out.push(c.splice(i,1)[0]); } return out; }
function mask(t){ return t ? `${t.slice(0,6)}...${t.slice(-6)}` : 'empty'; }
async function safeJson(res){ try { return await res.json(); } catch(e) { return null; } }