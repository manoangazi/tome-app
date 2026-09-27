// ══════════════════════════════════════════════════════════════
//  TOME — Service Worker
//  1. Signed streaming: <audio>/<img> request ./stream/<sourceId>/<key>;
//     this worker signs the request (AWS SigV4, or Basic auth for WebDAV)
//     and forwards the Range header, so storage stays private and no
//     pre-signed URLs exist.
//  2. Offline app shell: network-first for page navigations.
//  Keys arrive from the page via postMessage and live in memory only.
// ══════════════════════════════════════════════════════════════
'use strict';

const SHELL_CACHE = 'tome-shell-v2';
const STREAM_PATH = new URL('stream/', self.registration.scope).pathname;
const ASK_TIMEOUT_MS = 3000;

let SOURCES = null;       // Map sourceId -> config
let corsWarned = false;

self.addEventListener('install', () => self.skipWaiting());

self.addEventListener('activate', (e) => e.waitUntil((async () => {
  for (const k of await caches.keys()) if (k !== SHELL_CACHE) await caches.delete(k);
  await self.clients.claim();
})()));

self.addEventListener('message', (e) => {
  const d = e.data || {};
  if (d.type === 'sources') SOURCES = d.sources ? new Map(d.sources.map(s => [s.id, s])) : null;
  if (d.type === 'clear')   SOURCES = null;
});

self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  if (url.origin === self.location.origin && url.pathname.startsWith(STREAM_PATH)) {
    e.respondWith(handleStream(e, url));
    return;
  }
  if (e.request.mode === 'navigate') e.respondWith(networkFirst(e.request));
});

// ── App shell ──

async function networkFirst(request) {
  try {
    const resp = await fetch(request);
    if (resp.ok) {
      const cache = await caches.open(SHELL_CACHE);
      cache.put(request, resp.clone());
    }
    return resp;
  } catch {
    return (await caches.match(request, { ignoreSearch: true }))
        || (await caches.match(self.registration.scope))
        || new Response('Offline', { status: 503 });
  }
}

// ── Credentials ──

// Worker was restarted and lost its keys: ask an open Tome window for them
async function ensureSources(clientId) {
  if (SOURCES) return SOURCES;
  const direct = clientId ? await self.clients.get(clientId) : null;
  const windows = direct ? [direct] : await self.clients.matchAll({ type: 'window' });
  for (const client of windows) {
    const reply = await new Promise((resolve) => {
      const ch = new MessageChannel();
      const timer = setTimeout(() => resolve(null), ASK_TIMEOUT_MS);
      ch.port1.onmessage = (ev) => { clearTimeout(timer); resolve(ev.data); };
      client.postMessage({ type: 'need-sources' }, [ch.port2]);
    });
    if (reply?.sources) {
      SOURCES = new Map(reply.sources.map(s => [s.id, s]));
      return SOURCES;
    }
  }
  return null;
}

// ── Streaming ──

const CONTENT_TYPES = {
  mp3: 'audio/mpeg', m4a: 'audio/mp4', m4b: 'audio/mp4', aac: 'audio/aac',
  ogg: 'audio/ogg', opus: 'audio/ogg', flac: 'audio/flac', wav: 'audio/wav',
  jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png',
  webp: 'image/webp', avif: 'image/avif', gif: 'image/gif',
};

async function handleStream(e, url) {
  const rest = url.pathname.slice(STREAM_PATH.length);
  const slash = rest.indexOf('/');
  if (slash === -1) return new Response('Bad stream path', { status: 400 });
  const sourceId = decodeURIComponent(rest.slice(0, slash));
  const key      = decodeURIComponent(rest.slice(slash + 1));

  const sources = await ensureSources(e.clientId || e.resultingClientId);
  const cfg = sources?.get(sourceId);
  if (!cfg) return new Response('Source locked or unknown', { status: 401 });

  const headers = {};
  const range = e.request.headers.get('range');
  if (range) headers.range = range;

  let resp;
  try {
    if (cfg.type === 'webdav') {
      // WebDAV: plain GET with the Basic auth header the page derived
      const url = cfg.url + key.split('/').map(encodeURIComponent).join('/');
      resp = await fetch(url, {
        headers: { Authorization: cfg.auth, ...headers },
        signal: e.request.signal, mode: 'cors', credentials: 'omit',
      });
    } else {
      resp = await new S3Signer(cfg).request('GET', '/' + key, { headers, signal: e.request.signal });
    }
  } catch (err) {
    // Usually CORS: bucket must allow this origin and the Authorization/Range headers
    console.warn('[tome-sw] upstream fetch failed', cfg.endpoint, err);
    return new Response(`Upstream fetch failed (${err.name}: ${err.message}) — check CORS on ${cfg.endpoint}`, { status: 502 });
  }
  if (!resp.ok) return new Response(null, { status: resp.status, statusText: resp.statusText });

  // Rebuild headers the media element needs (only CORS-exposed ones are readable)
  const out = new Headers();
  for (const n of ['content-length', 'content-range', 'accept-ranges', 'etag', 'last-modified']) {
    const v = resp.headers.get(n);
    if (v) out.set(n, v);
  }
  out.set('accept-ranges', 'bytes');
  const ext = key.split('.').pop().toLowerCase();
  const upstreamType = resp.headers.get('content-type') || '';
  out.set('content-type',
    (!upstreamType || /octet-stream|binary/i.test(upstreamType)) && CONTENT_TYPES[ext]
      ? CONTENT_TYPES[ext] : (upstreamType || 'application/octet-stream'));

  if (resp.status === 206 && !out.has('content-range') && !corsWarned) {
    corsWarned = true;
    const clients = await self.clients.matchAll({ type: 'window' });
    clients.forEach(c => c.postMessage({ type: 'cors-warning' }));
  }

  return new Response(resp.body, { status: resp.status, statusText: resp.statusText, headers: out });
}

// ══════════════════════════════════════════════════════════════
//  AWS Signature V4 — mirrors S3Client.signedRequest in tome.html
// ══════════════════════════════════════════════════════════════

class S3Signer {
  constructor({ endpoint, bucket, region, accessKey, secretKey }) {
    let ep = endpoint.replace(/\/$/, '').trim();
    if (!/^https?:\/\//i.test(ep)) ep = 'https://' + ep;
    this.endpoint  = ep;
    this.bucket    = bucket;
    this.region    = region;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.pathStyle = !ep.includes('.amazonaws.com');
  }

  _buildUrl(objectPath) {
    if (this.pathStyle) {
      const fullPath = ('/' + this.bucket + objectPath).replace(/\/\//g, '/');
      const url = new URL(fullPath, this.endpoint);
      return { url, canonicalPath: url.pathname };
    }
    const parsed = new URL(this.endpoint);
    const url    = new URL(objectPath, `${parsed.protocol}//${this.bucket}.${parsed.host}`);
    return { url, canonicalPath: url.pathname };
  }

  async _sha256(data) {
    if (typeof data === 'string') data = new TextEncoder().encode(data);
    const h = await crypto.subtle.digest('SHA-256', data);
    return Array.from(new Uint8Array(h)).map(b => b.toString(16).padStart(2, '0')).join('');
  }

  async _hmac(key, msg) {
    if (typeof key === 'string') key = new TextEncoder().encode(key);
    if (typeof msg === 'string') msg = new TextEncoder().encode(msg);
    const ck = await crypto.subtle.importKey('raw', key, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    return new Uint8Array(await crypto.subtle.sign('HMAC', ck, msg));
  }

  async _signingKey(dateStamp) {
    let k = await this._hmac(new TextEncoder().encode('AWS4' + this.secretKey), dateStamp);
    k = await this._hmac(k, this.region);
    k = await this._hmac(k, 's3');
    return this._hmac(k, 'aws4_request');
  }

  _uriEncode(str, encodeSlash = true) {
    let out = '';
    for (const ch of str) {
      if (/[A-Za-z0-9_\-~.]/.test(ch)) { out += ch; continue; }
      if (ch === '/' && !encodeSlash)    { out += ch; continue; }
      for (const b of new TextEncoder().encode(ch)) out += '%' + b.toString(16).toUpperCase().padStart(2, '0');
    }
    return out;
  }

  async request(method, path, { headers = {}, signal } = {}) {
    const amzDate   = new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z');
    const dateStamp = amzDate.slice(0, 8);
    const scope     = `${dateStamp}/${this.region}/s3/aws4_request`;

    const { url, canonicalPath } = this._buildUrl(this._uriEncode(path, false));
    const payloadHash = await this._sha256('');

    const allHeaders = { host: url.host, 'x-amz-content-sha256': payloadHash, 'x-amz-date': amzDate, ...headers };
    const signedKeys = Object.keys(allHeaders).sort();
    const signedStr  = signedKeys.join(';');
    const canonHdrs  = signedKeys.map(k => `${k}:${String(allHeaders[k]).trim()}\n`).join('');

    const canonReq  = [method, canonicalPath, '', canonHdrs, signedStr, payloadHash].join('\n');
    const strToSign = ['AWS4-HMAC-SHA256', amzDate, scope, await this._sha256(canonReq)].join('\n');
    const sig = Array.from(await this._hmac(await this._signingKey(dateStamp), strToSign))
                     .map(b => b.toString(16).padStart(2, '0')).join('');

    allHeaders['Authorization'] =
      `AWS4-HMAC-SHA256 Credential=${this.accessKey}/${scope}, SignedHeaders=${signedStr}, Signature=${sig}`;
    delete allHeaders.host;

    return fetch(url.origin + url.pathname, { method, headers: allHeaders, signal, mode: 'cors', credentials: 'omit' });
  }
}
