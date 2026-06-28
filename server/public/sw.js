/* SNAPwise service worker — offline-first.
 * Bump VERSION on any change to roll the caches. */
const VERSION = 'snapwise-v1';
const SHELL_CACHE = `${VERSION}-shell`;
const DATA_CACHE = `${VERSION}-data`;
const TILE_CACHE = `${VERSION}-tiles`;
const RUNTIME_CACHE = `${VERSION}-runtime`;
const TILE_MAX = 300; // cap cached map tiles so storage stays bounded

// Same-origin app shell precached on install. Best-effort: a single missing
// asset must not fail the whole install (see allSettled below).
const SHELL_ASSETS = [
  '/',
  '/manifest.webmanifest',
  '/icons/icon-192.png',
  '/icons/icon-512.png',
  '/icons/icon-maskable-512.png',
  '/media-webp/marker.webp',
  '/media-webp/Check.webp',
  '/media-webp/Settings.webp',
  '/media-webp/Sun.webp',
  '/media-webp/Moon.webp',
  '/media-webp/Warning.webp',
  '/media-webp/Magnifying%20Glass%20Light.webp',
  '/media-webp/Magnifying%20Glass%20Dark.webp',
  '/media-webp/icons/IconCover180x180.webp',
];

self.addEventListener('install', (event) => {
  event.waitUntil((async () => {
    const shell = await caches.open(SHELL_CACHE);
    await Promise.allSettled(SHELL_ASSETS.map((u) => shell.add(u)));
    // Warm the full store dataset so offline search works after install.
    try {
      const data = await caches.open(DATA_CACHE);
      await data.add('/stores/all');
    } catch (e) { /* client also warms this after load */ }
    await self.skipWaiting();
  })());
});

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    const keep = new Set([SHELL_CACHE, DATA_CACHE, TILE_CACHE, RUNTIME_CACHE]);
    const names = await caches.keys();
    await Promise.all(names.filter((n) => !keep.has(n)).map((n) => caches.delete(n)));
    await self.clients.claim();
  })());
});

// Rough LRU: Cache API keeps insertion order, so drop the oldest entries.
async function trimCache(name, max) {
  const cache = await caches.open(name);
  const keys = await cache.keys();
  for (let i = 0; i < keys.length - max; i++) await cache.delete(keys[i]);
}

async function cacheFirst(req, cacheName) {
  const cache = await caches.open(cacheName);
  const hit = await cache.match(req);
  if (hit) return hit;
  try {
    const resp = await fetch(req);
    if (resp && resp.ok) cache.put(req, resp.clone());
    return resp;
  } catch (e) {
    return hit || Response.error();
  }
}

async function staleWhileRevalidate(req, cacheName) {
  const cache = await caches.open(cacheName);
  const hit = await cache.match(req);
  const fetching = fetch(req)
    .then((resp) => {
      if (resp && (resp.ok || resp.type === 'opaque')) cache.put(req, resp.clone());
      return resp;
    })
    .catch(() => null);
  return hit || (await fetching) || Response.error();
}

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return; // never intercept POST (/chat), etc.

  const url = new URL(req.url);
  const sameOrigin = url.origin === self.location.origin;

  // Dynamic, per-request endpoints: let them hit the network. The client falls
  // back to the cached full dataset (/stores/all) when these fail offline.
  if (sameOrigin && (url.pathname === '/stores' || url.pathname === '/zip' ||
      url.pathname.startsWith('/zip/') || url.pathname === '/chat')) {
    return;
  }

  // Full offline dataset: serve from cache, refresh in the background.
  if (sameOrigin && url.pathname === '/stores/all') {
    event.respondWith(staleWhileRevalidate(req, DATA_CACHE));
    return;
  }

  // Page navigations: network-first so updates ship; fall back to cached shell.
  if (req.mode === 'navigate') {
    event.respondWith((async () => {
      try {
        const fresh = await fetch(req);
        const shell = await caches.open(SHELL_CACHE);
        shell.put('/', fresh.clone());
        return fresh;
      } catch (e) {
        return (await caches.match('/')) || Response.error();
      }
    })());
    return;
  }

  // Map tiles: cache-first with a bounded LRU so the map isn't blank offline.
  if (url.hostname.endsWith('basemaps.cartocdn.com')) {
    event.respondWith((async () => {
      const cache = await caches.open(TILE_CACHE);
      const hit = await cache.match(req);
      if (hit) return hit;
      try {
        const resp = await fetch(req);
        if (resp && (resp.ok || resp.type === 'opaque')) {
          cache.put(req, resp.clone());
          trimCache(TILE_CACHE, TILE_MAX);
        }
        return resp;
      } catch (e) {
        return hit || Response.error();
      }
    })());
    return;
  }

  // Same-origin static assets (media, icons): cache-first.
  if (sameOrigin) {
    event.respondWith(cacheFirst(req, SHELL_CACHE));
    return;
  }

  // Cross-origin libraries/styles/fonts (unpkg, jsdelivr, google fonts): SWR.
  event.respondWith(staleWhileRevalidate(req, RUNTIME_CACHE));
});
