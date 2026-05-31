require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const tmi = require('tmi.js');
const axios = require('axios');
const { io: ioClient } = require('socket.io-client');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// ── NATÍV WEBSOCKET szerver a /ws útvonalon ──
const { WebSocketServer, WebSocket } = require('ws');
const wss = new WebSocketServer({ server, path: '/ws' });

app.use(express.static(__dirname));

const PORT = process.env.PORT || 3000;

// ── MESSAGE HISTORY (szerver memóriában, visszajátszáshoz) ──
const MESSAGE_HISTORY_LIMIT = 300;
const messageHistory = [];

function addToHistory(msg) {
  messageHistory.push(msg);
  if (messageHistory.length > MESSAGE_HISTORY_LIMIT) messageHistory.shift();
}

// ── BROADCAST: natív WS klienseknek ──
function broadcast(platform, type, data) {
  const msg = { platform, msgType: type, ...data, ts: Date.now() };
  addToHistory(msg);

  const packet = JSON.stringify({ type: 'message', data: msg });
  wss.clients.forEach(client => {
    if (client.readyState === 1) client.send(packet);
  });

  const label = type === 'chat'
    ? `[${platform.toUpperCase()}] ${data.username}: ${data.text}`
    : `[${platform.toUpperCase()} ALERT] ${data.alertType}: ${data.text}`;
  console.log(label);
}

// ── ÚJ WS KLIENS → küldjük a history-t ──
wss.on('connection', (wsClient) => {
  console.log(`[WS] Kliens csatlakozva (history: ${messageHistory.length} üzenet)`);
  if (messageHistory.length > 0) {
    wsClient.send(JSON.stringify({ type: 'history', data: messageHistory }));
  }
  wsClient.on('close', () => console.log('[WS] Kliens lecsatlakozva'));
  wsClient.on('error', (e) => console.error('[WS] Hiba:', e.message));
});

// ══════════════════════════════════════════
// TWITCH
// ══════════════════════════════════════════
let twitchClient = null;

function startTwitch() {
  const channel = process.env.TWITCH_CHANNEL;
  const username = process.env.TWITCH_USERNAME;
  const token = process.env.TWITCH_OAUTH_TOKEN;

  if (!channel || !username || !token || token === 'oauth:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') {
    console.log('[Twitch] Nincs beállítva – kihagyva.');
    return;
  }

  if (twitchClient) {
    try { twitchClient.disconnect(); } catch(e) {}
  }

  twitchClient = new tmi.Client({
    options: { debug: false },
    identity: { username, password: token },
    channels: [channel]
  });

  twitchClient.connect().catch(err => console.error('[Twitch] Kapcsolódási hiba:', err));

  twitchClient.on('message', (ch, tags, message, self) => {
    if (self) return;
    const badges = [];
    if (tags.badges?.subscriber) badges.push('sub');
    if (tags.badges?.moderator) badges.push('mod');
    if (tags.badges?.vip) badges.push('vip');
    broadcast('twitch', 'chat', {
      username: tags['display-name'] || tags.username,
      text: message,
      color: tags.color || '#9146ff',
      badges
    });
  });

  twitchClient.on('connected', () => console.log('[Twitch] Csatlakozva:', channel));
  twitchClient.on('disconnected', (reason) => {
    console.log('[Twitch] Lecsatlakozva:', reason);
    setTimeout(startTwitch, 5000);
  });
}

// ══════════════════════════════════════════
// KICK - Pusher WebSocket (valós idejű chat)
// ══════════════════════════════════════════
let kickWs = null;
let kickReconnectTimer = null;
let kickChatroomId = null;
let kickChannel = null;

async function startKick() {
  const channel = process.env.KICK_CHANNEL;
  if (!channel || channel === '' || channel === 'csatornanev') {
    console.log('[Kick] Nincs beállítva – kihagyva.');
    return;
  }
  kickChannel = channel;

  console.log('[Kick] Csatorna adatok lekérése:', channel);

  try {
    const res = await axios.get(`https://kick.com/api/v2/channels/${channel}`, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://kick.com/'
      },
      timeout: 10000
    });

    const chatroomId = res.data?.chatroom?.id;
    if (!chatroomId) {
      console.error('[Kick] Nem található chatroom ID. Újrapróba 60mp múlva...');
      setTimeout(startKick, 60000);
      return;
    }

    kickChatroomId = chatroomId;
    console.log(`[Kick] Chatroom ID: ${chatroomId} – Pusher WS csatlakozás indítása...`);
    connectKickPusher(chatroomId);

  } catch (err) {
    console.error('[Kick] API hiba:', err.response?.status, err.message);
    setTimeout(startKick, 60000);
  }
}

function connectKickPusher(chatroomId) {
  if (kickWs) {
    try { kickWs.terminate(); } catch(e) {}
    kickWs = null;
  }
  if (kickReconnectTimer) { clearTimeout(kickReconnectTimer); kickReconnectTimer = null; }

  const pusherUrl = 'wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679?protocol=7&client=js&version=8.4.0-rc2&flash=false';

  try {
    kickWs = new WebSocket(pusherUrl, {
      headers: {
        'Origin': 'https://kick.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      }
    });
  } catch(e) {
    console.error('[Kick] WebSocket létrehozási hiba:', e.message);
    kickReconnectTimer = setTimeout(() => connectKickPusher(chatroomId), 10000);
    return;
  }

  let pingInterval = null;

  kickWs.on('open', () => {
    console.log('[Kick] Pusher WebSocket csatlakozva');
  });

  kickWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);

      if (msg.event === 'pusher:connection_established') {
        console.log(`[Kick] Pusher kapcsolat OK – feliratkozás: chatrooms.${chatroomId}.v2`);
        kickWs.send(JSON.stringify({
          event: 'pusher:subscribe',
          data: { auth: '', channel: `chatrooms.${chatroomId}.v2` }
        }));

        if (pingInterval) clearInterval(pingInterval);
        pingInterval = setInterval(() => {
          if (kickWs && kickWs.readyState === WebSocket.OPEN) {
            kickWs.send(JSON.stringify({ event: 'pusher:ping', data: {} }));
          }
        }, 30000);
      }

      if (msg.event === 'pusher_internal:subscription_succeeded') {
        console.log('[Kick] Chatroom csatornára feliratkozva – üzenetek fogadása folyamatban');
      }

      if (msg.event === 'App\\Events\\ChatMessageEvent') {
        let data;
        try { data = typeof msg.data === 'string' ? JSON.parse(msg.data) : msg.data; }
        catch(e) { return; }

        const sender = data.sender || data;
        const badges = [];
        if (sender.identity?.badges) {
          sender.identity.badges.forEach(b => {
            const t = (b.type || '').toLowerCase();
            if (t === 'subscriber' || t === 'sub') badges.push('sub');
            if (t === 'moderator' || t === 'mod') badges.push('mod');
            if (t === 'vip') badges.push('vip');
            if (t === 'broadcaster' || t === 'owner') badges.push('mod');
          });
        }

        broadcast('kick', 'chat', {
          username: sender.username || sender.slug || 'Ismeretlen',
          text: data.content || data.message || '',
          color: sender.identity?.color || '#53fc18',
          badges
        });
      }

      if (msg.event === 'App\\Events\\SubscriptionEvent' || msg.event === 'App\\Events\\GiftedSubscriptionsEvent') {
        let data;
        try { data = typeof msg.data === 'string' ? JSON.parse(msg.data) : msg.data; }
        catch(e) { return; }

        broadcast('kick', 'alert', {
          alertType: 'FELIRATKOZÁS',
          icon: '⭐',
          username: data.username || data.gifter_username || '',
          text: data.gifter_username
            ? `${data.gifter_username} ajándékozott ${data.gifted_quantity || 1} sub-ot!`
            : `${data.username} feliratkozott!`
        });
      }

      if (msg.event === 'App\\Events\\RaidEvent') {
        let data;
        try { data = typeof msg.data === 'string' ? JSON.parse(msg.data) : msg.data; }
        catch(e) { return; }

        broadcast('kick', 'alert', {
          alertType: 'RAID',
          icon: '⚔️',
          username: data.host_username || data.raider || '',
          text: `${data.host_username || data.raider || 'Valaki'} ${data.viewers || ''} néző raiddel érkezett!`
        });
      }

    } catch(e) {}
  });

  kickWs.on('close', (code, reason) => {
    console.log(`[Kick] WebSocket lezárva (${code}) – újracsatlakozás 10mp múlva...`);
    if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
    kickReconnectTimer = setTimeout(() => connectKickPusher(chatroomId), 10000);
  });

  kickWs.on('error', (err) => {
    console.error('[Kick] WebSocket hiba:', err.message);
    if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
  });
}

// ══════════════════════════════════════════
// YOUTUBE - Data API v3 TÖBB API KULCSSAL
// ══════════════════════════════════════════
let ytPageToken = null;
let ytPollTimeout = null;
let currentLiveChatId = null;
let currentApiKeyIndex = 0;
let youtubeApiKeys = [];

async function callYoutubeApi(url, params) {
  if (youtubeApiKeys.length === 0) {
    throw new Error('Nincs YouTube API kulcs');
  }

  let lastError = null;
  
  for (let attempt = 0; attempt < youtubeApiKeys.length; attempt++) {
    const apiKey = youtubeApiKeys[(currentApiKeyIndex + attempt) % youtubeApiKeys.length];
    
    try {
      const response = await axios.get(url, {
        params: { ...params, key: apiKey },
        timeout: 10000
      });
      
      currentApiKeyIndex = (currentApiKeyIndex + attempt) % youtubeApiKeys.length;
      if (attempt > 0) {
        console.log(`[YouTube] API kulcs váltás: sikeres a ${apiKey.substring(0, 15)}... használatával`);
      }
      return response;
      
    } catch (err) {
      const status = err.response?.status;
      const errorMsg = err.response?.data?.error?.message || err.message;
      const isQuotaError = status === 403 && (errorMsg.includes('quota') || errorMsg.includes('dailyLimit'));
      
      if (isQuotaError) {
        console.log(`[YouTube] ⚠️ API kulcs kvótája kimerült: ${apiKey.substring(0, 15)}... (${attempt + 1}/${youtubeApiKeys.length})`);
        lastError = err;
        continue;
      }
      
      throw err;
    }
  }
  
  console.error('[YouTube] ❌ MINDEN API KULCS KVÓTÁJA KIMERÜLT!');
  throw new Error('ALL_API_KEYS_QUOTA_EXHAUSTED');
}

async function startYoutube() {
  const apiKeysEnv = process.env.YOUTUBE_API_KEY || process.env.YOUTUBE_API_KEYS;
  
  if (!apiKeysEnv) {
    console.log('[YouTube] Nincs API kulcs beállítva – kihagyva.');
    return;
  }
  
  youtubeApiKeys = apiKeysEnv.split(';').filter(k => k.trim().length > 0 && !k.startsWith('AIzaXXX'));
  console.log(`[YouTube] ✅ Betöltve ${youtubeApiKeys.length} API kulcs`);
  
  if (youtubeApiKeys.length === 0) {
    console.log('[YouTube] Nincs érvényes API kulcs – kihagyva.');
    return;
  }
  
  console.log(`[YouTube] Első API kulcs: ${youtubeApiKeys[0].substring(0, 15)}...`);
  
  let channelIdentifier = process.env.YOUTUBE_CHANNEL;
  let videoId = process.env.YOUTUBE_VIDEO_ID;

  if (videoId && videoId !== '') {
    console.log('[YouTube] Direkt video ID használata:', videoId);
    await getLiveChatIdFromVideoId(videoId);
    return;
  }
  
  if (!channelIdentifier || channelIdentifier === '') {
    console.log('[YouTube] Nincs csatorna vagy video ID beállítva – kihagyva.');
    return;
  }
  
  console.log('[YouTube] Csatorna keresés:', channelIdentifier);
  
  try {
    let handle = channelIdentifier;
    if (handle.startsWith('@')) {
      handle = handle.substring(1);
    }
    
    let channelId = null;
    
    try {
      const searchRes = await callYoutubeApi('https://www.googleapis.com/youtube/v3/search', {
        part: 'snippet',
        q: handle,
        type: 'channel',
        maxResults: 1
      });
      
      if (searchRes.data.items && searchRes.data.items.length > 0) {
        channelId = searchRes.data.items[0].snippet.channelId;
        console.log('[YouTube] Csatorna ID megtalálva (search):', channelId);
      }
    } catch (err) {
      if (err.message === 'ALL_API_KEYS_QUOTA_EXHAUSTED') throw err;
      console.log('[YouTube] Search API hiba:', err.message);
    }
    
    if (!channelId) {
      try {
        const channelRes = await callYoutubeApi('https://www.googleapis.com/youtube/v3/channels', {
          part: 'id',
          forHandle: handle
        });
        
        if (channelRes.data.items && channelRes.data.items.length > 0) {
          channelId = channelRes.data.items[0].id;
          console.log('[YouTube] Csatorna ID megtalálva (forHandle):', channelId);
        }
      } catch (err) {
        if (err.message === 'ALL_API_KEYS_QUOTA_EXHAUSTED') throw err;
        console.log('[YouTube] Channels API hiba:', err.message);
      }
    }
    
    if (!channelId) {
      console.log('[YouTube] Nem található csatorna a megadott névvel:', channelIdentifier);
      setTimeout(startYoutube, 60000);
      return;
    }
    
    const liveRes = await callYoutubeApi('https://www.googleapis.com/youtube/v3/search', {
      part: 'snippet',
      channelId: channelId,
      type: 'video',
      eventType: 'live',
      maxResults: 5
    });
    
    if (!liveRes.data.items || liveRes.data.items.length === 0) {
      console.log('[YouTube] Nincs élő stream a csatornán. Újrapróba 60mp múlva...');
      setTimeout(startYoutube, 60000);
      return;
    }
    
    const liveVideoId = liveRes.data.items[0].id.videoId;
    console.log('[YouTube] Élő video ID:', liveVideoId);
    
    await getLiveChatIdFromVideoId(liveVideoId);
    
  } catch (err) {
    if (err.message === 'ALL_API_KEYS_QUOTA_EXHAUSTED') {
      console.error('[YouTube] Összes API kulcs kvótája kimerült - 1 óra múlva újrapróba');
      setTimeout(startYoutube, 3600000);
    } else {
      console.error('[YouTube] Hiba:', err.response?.data?.error?.message || err.message);
      setTimeout(startYoutube, 60000);
    }
  }
}

async function getLiveChatIdFromVideoId(videoId) {
  try {
    const vidRes = await callYoutubeApi('https://www.googleapis.com/youtube/v3/videos', {
      part: 'liveStreamingDetails',
      id: videoId
    });
    
    const liveChatId = vidRes.data?.items?.[0]?.liveStreamingDetails?.activeLiveChatId;
    if (!liveChatId) {
      console.log('[YouTube] A video nem élő vagy nincs live chat. Újrapróba 30mp múlva...');
      setTimeout(startYoutube, 30000);
      return;
    }
    
    console.log('[YouTube] Live chat ID:', liveChatId);
    currentLiveChatId = liveChatId;
    ytPageToken = null;
    pollYoutube(liveChatId);
    
  } catch (err) {
    if (err.message === 'ALL_API_KEYS_QUOTA_EXHAUSTED') {
      console.error('[YouTube] Kvóta kimerült - 1 óra múlva újrapróba');
      setTimeout(startYoutube, 3600000);
    } else {
      console.error('[YouTube] Video info hiba:', err.response?.data?.error?.message || err.message);
      setTimeout(startYoutube, 30000);
    }
  }
}

async function pollYoutube(liveChatId) {
  try {
    const res = await callYoutubeApi('https://www.googleapis.com/youtube/v3/liveChat/messages', {
      liveChatId,
      part: 'snippet,authorDetails',
      pageToken: ytPageToken || undefined,
      maxResults: 200
    });

    const items = res.data.items || [];
    if (ytPageToken) {
      items.forEach(item => {
        const snippet = item.snippet;
        const author = item.authorDetails;
        if (snippet.type === 'textMessageEvent') {
          const badges = [];
          if (author.isChatModerator) badges.push('mod');
          if (author.isChatOwner) badges.push('vip');
          if (author.isChatSponsor) badges.push('sub');
          broadcast('youtube', 'chat', {
            username: author.displayName,
            text: snippet.textMessageDetails?.messageText || '',
            color: '#ff0000',
            badges
          });
        } else if (snippet.type === 'superChatEvent') {
          broadcast('youtube', 'alert', {
            alertType: 'SUPER CHAT',
            icon: '💛',
            username: author.displayName,
            text: `${author.displayName} küldött ${snippet.superChatDetails?.amountDisplayString || ''}: "${snippet.superChatDetails?.userComment || ''}"`
          });
        } else if (snippet.type === 'newSponsorEvent') {
          broadcast('youtube', 'alert', {
            alertType: 'ÚJ TAG',
            icon: '⭐',
            username: author.displayName,
            text: `${author.displayName} csatlakozott a csatornához!`
          });
        }
      });
    }

    ytPageToken = res.data.nextPageToken;
    // ✅ MÓDOSÍTVA: 20 másodpercre (volt 15)
    const pollMs = Math.max(res.data.pollingIntervalMillis || 5000, 20000);
    if (ytPollTimeout) clearTimeout(ytPollTimeout);
    ytPollTimeout = setTimeout(() => pollYoutube(liveChatId), pollMs);
  } catch (err) {
    if (err.message === 'ALL_API_KEYS_QUOTA_EXHAUSTED') {
      console.error('[YouTube] Poll: Összes API kulcs kvótája kimerült - 1 óra múlva újrapróba');
      clearTimeout(ytPollTimeout);
      setTimeout(startYoutube, 3600000);
    } else if (err.response?.status === 404 || err.response?.data?.error?.code === 404) {
      console.log('[YouTube] Live chat véget ért, újracsatlakozás...');
      clearTimeout(ytPollTimeout);
      setTimeout(startYoutube, 30000);
    } else {
      console.error('[YouTube] Poll hiba:', err.response?.data?.error?.message || err.message);
      if (ytPollTimeout) clearTimeout(ytPollTimeout);
      ytPollTimeout = setTimeout(() => pollYoutube(liveChatId), 30000);
    }
  }
}

// ══════════════════════════════════════════
// STREAMLABS
// ══════════════════════════════════════════
let slClient = null;

function startStreamlabs() {
  const token = process.env.STREAMLABS_SOCKET_TOKEN;
  if (!token || token === '') {
    console.log('[Streamlabs] Nincs beállítva – kihagyva.');
    return;
  }

  if (slClient) {
    try { slClient.disconnect(); } catch(e) {}
  }

  slClient = ioClient(`https://sockets.streamlabs.com?token=${token}`, {
    transports: ['websocket']
  });

  slClient.on('connect', () => console.log('[Streamlabs] Csatlakozva'));

  slClient.on('event', data => {
    if (!data || !data.type || !data.message) return;
    const msg = Array.isArray(data.message) ? data.message[0] : data.message;
    if (!msg) return;

    if (data.type === 'donation') {
      broadcast('streamlabs', 'alert', {
        alertType: 'DONÁCIÓ',
        icon: '💰',
        username: msg.name || 'Ismeretlen',
        text: `${msg.name} küldött ${msg.formatted_amount || msg.amount || ''}: "${msg.message || ''}"`
      });
    } else if (data.type === 'subscription') {
      broadcast('streamlabs', 'alert', {
        alertType: 'FELIRATKOZÁS',
        icon: '⭐',
        username: msg.name || '',
        text: `${msg.name} feliratkozott! (${msg.months || 1} hónap)`
      });
    } else if (data.type === 'resub') {
      broadcast('streamlabs', 'alert', {
        alertType: 'RESUB',
        icon: '🔄',
        username: msg.name || '',
        text: `${msg.name} újra feliratkozott! (${msg.months} hónap)`
      });
    } else if (data.type === 'follow') {
      broadcast('streamlabs', 'alert', {
        alertType: 'KÖVETÉS',
        icon: '❤️',
        username: msg.name || '',
        text: `Új követő: ${msg.name}`
      });
    } else if (data.type === 'raid') {
      broadcast('streamlabs', 'alert', {
        alertType: 'RAID',
        icon: '⚔️',
        username: msg.name || '',
        text: `${msg.name} ${msg.raiders || ''} néző raiddel érkezett!`
      });
    } else if (data.type === 'bits') {
      broadcast('streamlabs', 'alert', {
        alertType: 'BITS',
        icon: '💜',
        username: msg.name || '',
        text: `${msg.name} adott ${msg.amount} bitet!`
      });
    }
  });

  slClient.on('disconnect', () => {
    console.log('[Streamlabs] Lecsatlakozva, újracsatlakozás...');
    setTimeout(startStreamlabs, 5000);
  });
}

// ══════════════════════════════════════════
// STREAMELEMENTS
// ══════════════════════════════════════════
let seToken = null;
let seWs = null;
let seReconnectTimer = null;

async function startStreamElements() {
  const clientId = process.env.STREAMELEMENTS_CLIENT_ID;
  const clientSecret = process.env.STREAMELEMENTS_CLIENT_SECRET;
  const channelId = process.env.STREAMELEMENTS_CHANNEL_ID;
  
  if (!clientId || !clientSecret || !channelId) {
    console.log('[StreamElements] Nincs beállítva – kihagyva.');
    return;
  }

  try {
    const tokenRes = await axios.post('https://api.streamelements.com/oauth2/token', {
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'client_credentials'
    });
    
    seToken = tokenRes.data.access_token;
    console.log('[StreamElements] Token megszerezve');
    connectStreamElementsWS(channelId);
    
  } catch (err) {
    console.error('[StreamElements] Token hiba:', err.response?.data || err.message);
    setTimeout(startStreamElements, 30000);
  }
}

function connectStreamElementsWS(channelId) {
  if (seWs) {
    try { seWs.close(); } catch(e) {}
  }
  
  const wsUrl = `wss://realtime.streamelements.com?token=${seToken}`;
  seWs = new WebSocket(wsUrl);
  
  seWs.on('open', () => {
    console.log('[StreamElements] WebSocket csatlakozva');
    seWs.send(JSON.stringify({
      op: 1,
      d: { channelId: channelId }
    }));
  });
  
  seWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      
      if (msg.op === 0 && msg.d?.type === 'message') {
        const data = msg.d;
        broadcast('streamelements', 'chat', {
          username: data.data?.displayName || data.data?.username || 'Ismeretlen',
          text: data.data?.message || '',
          color: data.data?.color || '#fca503',
          badges: data.data?.badges?.map(b => b.type) || []
        });
      }
      
      if (msg.op === 0 && msg.d?.type === 'event') {
        const event = msg.d;
        const eventType = event.listener?.type || 'UNKNOWN';
        
        broadcast('streamelements', 'alert', {
          alertType: eventType.toUpperCase(),
          icon: getStreamelementsIcon(eventType),
          username: event.data?.displayName || event.data?.username || 'Ismeretlen',
          text: formatStreamelementsEvent(event)
        });
      }
      
    } catch (err) {}
  });
  
  seWs.on('close', () => {
    console.log('[StreamElements] Lecsatlakozva, újracsatlakozás...');
    if (seReconnectTimer) clearTimeout(seReconnectTimer);
    seReconnectTimer = setTimeout(() => startStreamElements(), 5000);
  });
  
  seWs.on('error', (err) => {
    console.error('[StreamElements] WS hiba:', err.message);
  });
}

function getStreamelementsIcon(eventType) {
  const icons = {
    'follow': '❤️',
    'subscription': '⭐',
    'tip': '💰',
    'cheer': '💜',
    'host': '🏠',
    'raid': '⚔️'
  };
  return icons[eventType.toLowerCase()] || '🔔';
}

function formatStreamelementsEvent(event) {
  const type = event.listener?.type;
  const data = event.data || {};
  
  switch(type) {
    case 'follow':
      return `${data.displayName || data.username} követ!`;
    case 'subscription':
      return `${data.displayName} feliratkozott! (${data.tier} szint)`;
    case 'tip':
      return `${data.displayName} küldött ${data.amount} ${data.currency || 'USD'}-t!`;
    case 'cheer':
      return `${data.displayName} adott ${data.amount} bitet!`;
    default:
      return `${data.displayName || data.username}: ${data.message || ''}`;
  }
}

// ══════════════════════════════════════════
// TIKTOK
// ══════════════════════════════════════════
let tiktokLive = null;

function startTikTok() {
  const username = process.env.TIKTOK_USERNAME;
  if (!username || username === '') {
    console.log('[TikTok] Nincs beállítva – kihagyva.');
    return;
  }

  try {
    const { WebcastPushConnection } = require('tiktok-live-connector');
    
    if (tiktokLive) {
      try { tiktokLive.disconnect(); } catch(e) {}
    }
    
    tiktokLive = new WebcastPushConnection(username);

    tiktokLive.connect().then(() => {
      console.log('[TikTok] Csatlakozva:', username);
    }).catch(err => {
      console.error('[TikTok] Kapcsolódási hiba:', err.message);
      setTimeout(startTikTok, 30000);
    });

    tiktokLive.on('chat', data => {
      broadcast('tiktok', 'chat', {
        username: data.uniqueId || data.nickname || 'Ismeretlen',
        text: data.comment || '',
        color: '#ff0050',
        badges: data.userBadges?.length > 0 ? ['sub'] : []
      });
    });

    tiktokLive.on('gift', data => {
      if (data.giftType === 1 && !data.repeatEnd) return;
      broadcast('tiktok', 'alert', {
        alertType: 'GIFT',
        icon: '🎁',
        username: data.uniqueId || '',
        text: `${data.uniqueId} küldött ${data.repeatCount}x ${data.giftName} giftet!`
      });
    });

    tiktokLive.on('like', data => {
      if (data.totalLikeCount % 100 === 0) {
        broadcast('tiktok', 'alert', {
          alertType: 'LIKE',
          icon: '❤️',
          username: data.uniqueId || '',
          text: `${data.totalLikeCount} like elérve!`
        });
      }
    });

    tiktokLive.on('disconnected', () => {
      console.log('[TikTok] Lecsatlakozva, újracsatlakozás 15mp múlva...');
      setTimeout(startTikTok, 15000);
    });

  } catch (err) {
    console.log('[TikTok] tiktok-live-connector csomag nem található. Telepítsd: npm install tiktok-live-connector');
  }
}

// ══════════════════════════════════════════
// BELABOX CLOUD STREAM HEALTH MONITOR
// ══════════════════════════════════════════
let belaboxInterval = null;
let lastHealthStatus = null;

function startBelaboxMonitor() {
  const statsUrl = 'https://stats.srt.belabox.net/niKkN86OZXlau8VHf5yXqlLQe7LXtH';
  
  if (belaboxInterval) {
    clearInterval(belaboxInterval);
    belaboxInterval = null;
  }
  
  console.log('[Belabox] Stream health monitor indítva (1 másodpercenként)');
  
  belaboxInterval = setInterval(async () => {
    try {
      const response = await axios.get(statsUrl, {
        timeout: 5000,
        headers: { 'Accept': 'application/json' }
      });
      
      const data = response.data;
      if (!data || !data.publishers || !data.publishers.live) return;
      
      const publisher = data.publishers.live;
      const bitrate = publisher.bitrate || 0;
      const rtt = publisher.rtt || 0;
      const droppedPkts = publisher.dropped_pkts || 0;
      const latency = publisher.latency || 0;
      const network = publisher.network || 0;
      const connected = publisher.connected || false;
      
      let status = 'good';
      let alertType = null;
      let alertMessage = null;
      
      if (!connected) {
        status = 'critical';
        alertType = 'NOT_CONNECTED';
        alertMessage = '🔴 Nincs kapcsolat a Belabox-szal!';
      } 
      else if (bitrate === 0) {
        status = 'critical';
        alertType = 'NO_STREAM';
        alertMessage = '🔴 A stream MEGÁLLT! (0 kbps)';
      } 
      else if (bitrate < 1500) {
        status = 'critical';
        alertType = 'LOW_BITRATE';
        alertMessage = `⚠️ TÚL ALACSONY BITRÁTA: ${bitrate} kbps (min. 1500)`;
      } 
      else if (bitrate < 2500) {
        status = 'warning';
        alertType = 'MEDIUM_BITRATE';
        alertMessage = `📉 Alacsony bitráta: ${bitrate} kbps`;
      } 
      else if (droppedPkts > 100) {
        status = 'warning';
        alertType = 'DROPPED_PACKETS';
        alertMessage = `📡 Csomagvesztés: ${droppedPkts} db eldobott csomag`;
      }
      else if (rtt > 200) {
        status = 'warning';
        alertType = 'HIGH_LATENCY';
        alertMessage = `🐌 Nagy késleltetés: ${rtt} ms`;
      }
      
      const healthData = {
        timestamp: Date.now(),
        status: status,
        metrics: {
          bitrate: bitrate,
          rtt: rtt,
          droppedPkts: droppedPkts,
          latency: latency,
          network: network
        },
        alert: alertMessage ? {
          type: alertType,
          message: alertMessage
        } : null
      };
      
      const statusChanged = !lastHealthStatus || lastHealthStatus.status !== status;
      
      if (statusChanged) {
        const packet = JSON.stringify({ type: 'belabox', data: healthData });
        wss.clients.forEach(client => {
          if (client.readyState === 1) client.send(packet);
        });
      } else {
        const packet = JSON.stringify({ 
          type: 'belabox_metrics', 
          data: {
            timestamp: healthData.timestamp,
            status: healthData.status,
            metrics: healthData.metrics
          }
        });
        wss.clients.forEach(client => {
          if (client.readyState === 1) client.send(packet);
        });
      }
      
      lastHealthStatus = healthData;
      
      if (statusChanged) {
        console.log(`[Belabox] ${status.toUpperCase()}: bitrate=${bitrate} kbps, rtt=${rtt}ms, dropped=${droppedPkts}`);
      }
      
    } catch (err) {
      if (err.code === 'ECONNABORTED') {
        // Timeout - csendben
      } else if (err.response?.status === 404) {
        if (!lastHealthStatus || lastHealthStatus.lastError !== '404') {
          console.log('[Belabox] Statisztika URL nem elérhető (404)');
          if (lastHealthStatus) lastHealthStatus.lastError = '404';
        }
      } else if (err.code === 'ENOTFOUND') {
        console.warn('[Belabox] Hálózati hiba, nincs kapcsolat');
      }
    }
  }, 1000);
}

// ══════════════════════════════════════════
// SOCKET.IO
// ══════════════════════════════════════════
io.on('connection', socket => {
  console.log(`[Socket.IO] Kliens: ${socket.id}`);
  socket.on('disconnect', () => console.log('[Socket.IO] Lecsatlakozva:', socket.id));
});

// ══════════════════════════════════════════
// INDÍTÁS
// ══════════════════════════════════════════
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n⚡ Stream Monitor szerver fut:`);
  console.log(`   Helyben: http://localhost:${PORT}/streammonitor.html`);
  console.log(`   DDNS: https://streammonitor.fly.dev:${PORT}/streammonitor.html\n`);
  startTwitch();
  startKick();
  startYoutube();
  startStreamlabs();
  startStreamElements();
  startTikTok();
  startBelaboxMonitor();
});