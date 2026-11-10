import express from 'express';
import cors from 'cors';
import axios from 'axios';
import { Readable } from 'node:stream';
import { chromium } from 'playwright';
import { PlaywrightBlocker } from '@cliqz/adblocker-playwright';
import crossFetch from 'cross-fetch'; // el bloqueador lo necesita para bajar EasyList
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import fs from 'node:fs/promises';

/* ───────────────────────── CONFIGURACIÓN ───────────────────────── */

const CONFIG = {
  port: 4000,
  // URL del resolvedor externo (anime1v-api) corriendo en tu PC.
  resolverUrl: 'http://localhost:3000',
  apiKey: process.env.ANIME1V_API_KEY ?? 'dev-anime1v-key',
  userAgent:
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
  // Mayor puntaje = mayor prioridad.
  serverPriority: [
    { match: /mp4upload/i, score: 95 },
    { match: /^hls$|zilla/i, score: 80 },
    { match: /yourupload/i, score: 70 },
    { match: /streamwish/i, score: 60 },
    { match: /mega/i, score: 5 },
  ],
  maxConcurrentJobs: 2,       // peticiones resolviéndose a la vez (cuida la RAM)
  resolveTopN: 3,             // cuántos embeds intentamos convertir en enlace directo
  cacheTtlMs: 20 * 60 * 1000, // 20 min
};

const MEDIA_RE = /\.(mp4|m3u8|webm)(\?|$)/i;
// MEGA cifra el contenido en el cliente: no existe enlace directo reproducible en <video>.
const NOT_RESOLVABLE_RE = /mega\.nz|mega\.io/i;

/* ───────────────────────── UTILIDADES ASÍNCRONAS ───────────────────────── */

// Pausa asíncrona: "await" cede el hilo al event loop, otras peticiones siguen atendiéndose.
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (min, max) => sleep(min + Math.random() * (max - min));

// Cola simple: limita cuántos trabajos pesados corren en paralelo.
function createLimiter(max) {
  let active = 0;
  const queue = [];
  const next = () => {
    if (active >= max || queue.length === 0) return;
    active++;
    const { fn, resolve, reject } = queue.shift();
    fn().then(resolve, reject).finally(() => { active--; next(); });
  };
  return (fn) => new Promise((resolve, reject) => { queue.push({ fn, resolve, reject }); next(); });
}
const limit = createLimiter(CONFIG.maxConcurrentJobs);

// Espera (polling) a que se cumpla una condición o venza el tiempo.
async function waitFor(cond, timeoutMs, step = 200) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (cond()) return true;
    await sleep(step);
  }
  return false;
}

/* ───────────────────────── ESTADO GLOBAL ───────────────────────── */

const cache = new Map();            // urlEpisodio → { t, data }
const knownMediaHosts = new Set();  // el proxy solo sirve hosts que nosotros mismos descubrimos

/* ───────────────────────── NAVEGADOR FANTASMA ───────────────────────── */

let browser;  // UNA sola instancia de Chromium para todo el servidor
let blocker;  // adblock (EasyList + EasyPrivacy)

async function initBrowser() {
  browser = await chromium.launch({ headless: !process.env.SHOW_BROWSER });
  blocker = await PlaywrightBlocker.fromPrebuiltAdsAndTracking(crossFetch);
}

async function preparePage(page, foundMedia) {
  // 1) Adblock (registra su propio page.route internamente).
  await blocker.enableBlockingInPage(page);

  // 2) Filtro propio: el route registrado al ÚLTIMO corre primero;
  //    route.fallback() pasa la petición al adblocker.
  await page.route('**/*', (route) => {
    const type = route.request().resourceType();
    if (['image', 'font', 'media'].includes(type)) return route.abort();
    return route.fallback();
  });

  // 3) Sniffing: 'request' se dispara antes del abort, así vemos la URL del vídeo igualmente.
  page.on('request', (req) => {
    const u = req.url();
    if (/^https?:/.test(u) && (MEDIA_RE.test(u) || req.resourceType() === 'media')) foundMedia.add(u);
  });
}

function attachPopupKiller(context) {
  let own = 0; // páginas propias pendientes de aparecer en el evento
  context.on('page', (p) => {
    if (own > 0) { own--; return; }   // es nuestra: la dejamos
    p.close().catch(() => {});        // cualquier otra es un pop-up
  });
  return () => { own++; return context.newPage(); };
}

const pickMedia = (set) => [...set].find((u) => MEDIA_RE.test(u)) ?? [...set][0] ?? null;

// Ejecuta una función dentro de TODOS los frames de la página (el player suele ir en un iframe).
const inFrames = (page, fn) =>
  Promise.all(page.frames().map((f) => f.evaluate(fn).catch(() => null)));

async function resolveEmbed(openPage, embedUrl, referer) {
  const page = await openPage();
  const found = new Set();
  const log = (...a) => console.log(`[embed ${new URL(embedUrl).hostname}]`, ...a);
  try {
    await preparePage(page, found);
    const resp = await page.goto(embedUrl, { waitUntil: 'domcontentloaded', timeout: 15000, referer });
    log('HTTP', resp?.status(), '| título:', await page.title());
    await jitter(800, 1500);

    // Hasta 4 intentos: clic (el 1.º suele disparar un pop-up que cerramos)
    // + play() mudo por script (permitido sin gesto del usuario).
    for (let i = 0; i < 4 && found.size === 0; i++) {
      await page.mouse.click(683, 384).catch(() => {});
      await inFrames(page, () => {
        document.querySelectorAll('video').forEach((v) => { v.muted = true; v.play().catch(() => {}); });
      });
      await waitFor(() => found.size > 0, 2500);
    }

    // Respaldo 1: leer el src del <video> en todos los frames.
    if (found.size === 0) {
      const srcs = await inFrames(page, () => {
        const v = document.querySelector('video');
        return v?.currentSrc || v?.src || v?.querySelector('source')?.src || null;
      });
      srcs.filter((s) => s && /^https?:/.test(s)).forEach((s) => found.add(s));
    }

    // Respaldo 2: URL escrita en el HTML.
    if (found.size === 0) {
      const m = (await page.content()).match(/https?:\/\/[^"'\s\\]+\.(?:mp4|m3u8)[^"'\s\\]*/i);
      if (m) found.add(m[0]);
    }
    log('resultado:', pickMedia(found) ?? 'nada');
  } catch (err) {
    log('error:', err.message);
  } finally {
    await page.close().catch(() => {});
  }
  return pickMedia(found);
}

/* ───────────────────────── EXTRACCIÓN ───────────────────────── */

const scoreOf = (name) => CONFIG.serverPriority.find((p) => p.match.test(name))?.score ?? 10;
const qualityOf = (text) => {
  const m = String(text).match(/(1080|720|480|360)p?/);
  return m ? Number(m[1]) : 0;
};

// Pide la lista de servidores al resolvedor externo y la normaliza.
async function fetchServerList(episodeUrl) {
  const { data } = await axios.get(`${CONFIG.resolverUrl}/api/v1/anime/episode`, {
    params: { url: episodeUrl, includeMega: true, apiKey: CONFIG.apiKey },
    headers: { 'x-api-key': CONFIG.apiKey },
    timeout: 20000,
  });

  const payload = data?.data ?? data;
  const groups = payload?.servers ?? payload?.streamLinks ?? {};

  // Formato real: { sub: [...], dub: [...] } → lo aplanamos y guardamos el idioma.
  const raw = Array.isArray(groups)
    ? groups.map((s) => ({ ...s, lang: null }))
    : Object.entries(groups).flatMap(([lang, arr]) =>
        (Array.isArray(arr) ? arr : []).map((s) => ({ ...s, lang: lang.toUpperCase() })));

  const servers = raw
    .map((srv) => {
      const name = String(srv.server ?? srv.name ?? srv.title ?? 'desconocido');
      let url = srv.url ?? srv.embed ?? srv.code ?? srv.src ?? null;
      if (typeof url === 'string' && url.startsWith('//')) url = 'https:' + url;
      return { name, lang: srv.lang, url, score: scoreOf(name), quality: qualityOf(`${name} ${url}`) };
    })
    .filter((s) => typeof s.url === 'string' && /^https?:\/\//.test(s.url));

  return { provider: data?.source ?? payload?.source, servers };
}

async function buildEpisode(episodeUrl) {
  const { provider, servers } = await fetchServerList(episodeUrl);

  // Los que ya son .mp4/.m3u8 no necesitan navegador.
  for (const s of servers) {
    s.direct = MEDIA_RE.test(s.url) ? s.url : null;
  }

  // El resto (embeds) se resuelven con Playwright, solo los mejores y EN PARALELO.
  const toResolve = servers
    .filter((s) => !s.direct && !NOT_RESOLVABLE_RE.test(s.url))
    .sort((a, b) => b.score - a.score)
    .slice(0, CONFIG.resolveTopN);

  if (toResolve.length > 0) {
    const context = await browser.newContext({
      userAgent: CONFIG.userAgent,
      viewport: { width: 1366, height: 768 },
      locale: 'es-ES',
    });
    try {
      const openPage = attachPopupKiller(context);   // ← sustituye al context.on('page', ...) anterior
      await Promise.allSettled(
        toResolve.map(async (s) => { s.direct = await resolveEmbed(openPage, s.url, episodeUrl); }),
      );
    } finally {
      await context.close();
    }
  }

  const result = servers.map((s) => {
    const playable = Boolean(s.direct);
    if (playable) knownMediaHosts.add(new URL(s.direct).hostname);
    return {
      name: s.name,
      lang: s.lang,
      quality: s.quality || qualityOf(s.direct ?? '') || null,
      playable,
      embed: s.url,
      direct: s.direct,
      // URL lista para <video src>; el proxy añade Referer y soporta Range.
      src: playable
        ? `/api/proxy?u=${encodeURIComponent(s.direct)}&r=${encodeURIComponent(s.url)}`
        : null,
      score: s.score,
    };
  });

  // Orden: reproducibles primero → mayor calidad → mayor prioridad de servidor.
  result.sort((a, b) => Number(b.playable) - Number(a.playable) || (b.quality ?? 0) - (a.quality ?? 0) || b.score - a.score);

  return { success: true, provider: provider ?? 'desconocido', episodeUrl, servers: result };
}

/* ───────────────────────── API EXPRESS ───────────────────────── */

const app = express();
app.use(cors());

const PUBLIC_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), 'public');
console.log('[static] sirviendo archivos desde:', PUBLIC_DIR);

app.use(express.static(PUBLIC_DIR, {
  setHeaders: (res) => res.setHeader('Cache-Control', 'no-store'),
}));

app.get('/api/stream', async (req, res) => {
  let url;
  try {
    url = new URL(String(req.query.url ?? ''));
    if (!['http:', 'https:'].includes(url.protocol)) throw new Error('protocolo');
  } catch {
    return res.status(400).json({ error: 'Falta el parámetro url o no es válido' });
  }

  try {
    const hit = cache.get(url.href);
    if (hit && Date.now() - hit.t < CONFIG.cacheTtlMs) return res.json(hit.data);

    console.log(`[TV] Solicitando: ${url.href}`);
    // limit(): si ya hay N trabajos activos, esta petición espera en cola sin bloquear el servidor.
    const data = await limit(() => buildEpisode(url.href));

    if (data.servers.some((s) => s.playable)) {
      cache.set(url.href, { t: Date.now(), data });
    }
    res.json(data);
    } catch (err) {
        const status = err.response?.status;
        const body = err.response?.data;
        console.error('[Error Backend]:', err.message, body ? JSON.stringify(body).slice(0, 300) : '');
        res.status(502).json({
        error: 'Error al procesar los servidores de streaming.',
        detail: err.message,
        resolverStatus: status ?? null,
        });
    }
});

// Proxy con soporte de Range (necesario para avanzar/retroceder en <video>).
app.get('/api/proxy', async (req, res) => {
  const ac = new AbortController();
  res.on('close', () => ac.abort());
  try {
    const target = new URL(String(req.query.u ?? ''));
    if (!knownMediaHosts.has(target.hostname)) return res.sendStatus(403);

    const referer = String(req.query.r || target.origin + '/');
    const headers = { 'User-Agent': CONFIG.userAgent, Referer: referer };
    if (req.headers.range) headers.Range = req.headers.range;

    const upstream = await fetch(target, { headers, signal: ac.signal });
    if (!upstream.body) return res.sendStatus(502);

    const type = upstream.headers.get('content-type') ?? '';
    const isPlaylist = /\.m3u8(\?|$)/i.test(target.href) || /mpegurl/i.test(type);

    if (isPlaylist) {
      // Reescribimos cada URI de la lista para que la TV pida los segmentos
      // a ESTE proxy (con Referer) y no directamente al CDN.
      const text = await upstream.text();
      const toProxy = (u) => {
        const abs = new URL(u, target);            // resuelve rutas relativas
        knownMediaHosts.add(abs.hostname);
        return `/api/proxy?u=${encodeURIComponent(abs.href)}&r=${encodeURIComponent(referer)}`;
      };
      const out = text.split(/\r?\n/).map((line) => {
        if (!line.trim()) return line;
        if (line.startsWith('#')) return line.replace(/URI="([^"]+)"/g, (_, u) => `URI="${toProxy(u)}"`);
        return toProxy(line.trim());
      }).join('\n');
      return res.status(upstream.status).type('application/vnd.apple.mpegurl').send(out);
    }

    res.status(upstream.status);
    for (const h of ['content-type', 'content-length', 'content-range', 'accept-ranges']) {
      const v = upstream.headers.get(h);
      if (v) res.setHeader(h, v);
    }
    if (!/^video\//i.test(res.getHeader('content-type') ?? '') && /\.mp4(\?|$)/i.test(target.href)) {
      res.setHeader('content-type', 'video/mp4');
    }
    Readable.fromWeb(upstream.body).on('error', () => res.destroy()).pipe(res);
  } catch {
    if (!res.headersSent) res.sendStatus(502);
  }
});

app.get('/api/ping', (req, res) => res.json({ ok: true }));

// Reenvía /api/r/v1/anime/... al resolvedor inyectando la apiKey.
app.get('/api/r/*', async (req, res) => {
  const path = req.params[0];
  if (!/^v1\/anime\/[a-z0-9\-\/]+$/i.test(path) || path.includes('image-proxy')) {
    return res.status(400).json({ error: 'ruta no permitida' });
  }
  try {
    const r = await axios.get(`${CONFIG.resolverUrl}/api/${path}`, {
      params: { ...req.query, apiKey: CONFIG.apiKey },
      timeout: 20000,
    });
    res.json(r.data);
  } catch (err) {
    res.status(err.response?.status ?? 502).json({ error: err.message });
  }
});

// Portadas: las pide la TV a nuestro servidor y este las trae del resolvedor.
app.get('/api/img', async (req, res) => {
  try {
    const u = new URL(String(req.query.u ?? ''));
    if (u.protocol !== 'https:') return res.sendStatus(400);
    const r = await axios.get(`${CONFIG.resolverUrl}/api/v1/anime/image-proxy`, {
      params: { url: u.href, apiKey: CONFIG.apiKey },
      responseType: 'stream',
      timeout: 15000,
    });
    res.setHeader('content-type', r.headers['content-type'] ?? 'image/jpeg');
    res.setHeader('cache-control', 'public, max-age=86400');
    r.data.pipe(res);
  } catch {
    if (!res.headersSent) res.sendStatus(502);
  }
});

/* ───────────── BÚSQUEDA ───────────── */
const normText = (s) => String(s).normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();

const findList = (o) => {
  if (Array.isArray(o) && o.length && typeof o[0] === 'object') return o;
  if (o && typeof o === 'object') {
    for (const v of Object.values(o)) { const r = findList(v); if (r) return r; }
  }
  return null;
};
const toCard = (it) => ({
  id: String(it.id ?? ''),
  title: it.title ?? it.name ?? 'Sin título',
  url: it.url ?? it.slug ?? '',
  image: it.image ?? it.cover ?? it.poster ?? '',
  type: it.type ?? '',
});

// Opción A: probar los endpoints de búsqueda más comunes del resolvedor (una sola vez).
const SEARCH_CANDIDATES = [];
for (const name of ['search', 'find', 'query']) {
  for (const param of ['q', 'query', 'search', 'keyword']) SEARCH_CANDIDATES.push([name, param]);
}
let searchRoute = null;       // la combinación que funcionó
let noRemoteSearch = false;   // true si el resolvedor no tiene buscador

async function remoteSearch(q) {
  let saw404Only = true;
  for (const [name, param] of searchRoute ? [searchRoute] : SEARCH_CANDIDATES) {
    try {
      const r = await axios.get(`${CONFIG.resolverUrl}/api/v1/anime/${name}`, {
        params: { [param]: q, apiKey: CONFIG.apiKey },
        timeout: 15000,
        validateStatus: () => true,
      });
      if (r.status !== 404) saw404Only = false;
      if (r.status !== 200 || r.data?.success === false) continue;
      const items = (findList(r.data) ?? []).map(toCard).filter((c) => c.url);
      if (items.length) {
        if (!searchRoute) { searchRoute = [name, param]; console.log('[search] resolvedor:', name, param); }
        return items;
      }
    } catch { /* probamos el siguiente */ }
  }
  if (!searchRoute && saw404Only) { noRemoteSearch = true; console.log('[search] el resolvedor no tiene buscador; uso índice local'); }
  return null;
}

/* ───────────── ÍNDICE DEL CATÁLOGO, GÉNEROS Y HOME ───────────── */
const ROOT = path.dirname(fileURLToPath(import.meta.url));
const CACHE_DIR = path.join(ROOT, 'cache');
const CACHE_FILE = path.join(CACHE_DIR, 'catalog.json');
const CACHE_MAX_AGE = 6 * 3600 * 1000;   // 6 h. Para forzar recarga, borra la carpeta cache/

let db = { items: [], genres: [], built: 0 };
let indexReady = null;

const GENRE_GUESS = [
  'accion', 'aventura', 'comedia', 'drama', 'fantasia', 'romance', 'ciencia-ficcion',
  'slice-of-life', 'sobrenatural', 'misterio', 'terror', 'deportes', 'mecha', 'isekai',
  'escolares', 'shounen', 'seinen', 'shoujo', 'psicologico', 'musica', 'historico',
  'suspenso', 'magia', 'artes-marciales', 'harem', 'ecchi',
  'action', 'adventure', 'comedy', 'fantasy', 'sci-fi', 'supernatural', 'mystery', 'horror',
  'sports', 'school', 'psychological', 'music', 'historical', 'thriller', 'martial-arts',
];
const pretty = (s) => String(s).replace(/-/g, ' ').replace(/^\w/, (c) => c.toUpperCase());
const findArr = (o) => {
  if (Array.isArray(o) && o.length) return o;
  if (o && typeof o === 'object') for (const v of Object.values(o)) { const r = findArr(v); if (r) return r; }
  return null;
};

async function fetchCatalogPage(page, genre) {
  try {
    const r = await axios.get(`${CONFIG.resolverUrl}/api/v1/anime/catalog`, {
      params: { page, ...(genre ? { genre } : {}), apiKey: CONFIG.apiKey },
      timeout: 20000,
      validateStatus: () => true,
    });
    if (r.status !== 200) return { list: [], hasMore: false, echo: null };
    const d = r.data?.data;
    return { list: (d?.results ?? findList(r.data) ?? []).map(toCard), hasMore: d?.hasMore, echo: d?.genre ?? null };
  } catch {
    return { list: [], hasMore: false, echo: null };
  }
}

// Descubre qué géneros acepta el resolvedor: primero pregunta por una lista;
// si no hay, prueba nombres comunes y se queda solo con los que filtran de verdad.
async function discoverGenres() {
  const base = await fetchCatalogPage(1);
  const baseSig = base.list.slice(0, 3).map((c) => c.id).join(',');
  let cands = [];
  for (const n of ['genres', 'genre', 'categories']) {
    try {
      const r = await axios.get(`${CONFIG.resolverUrl}/api/v1/anime/${n}`, {
        params: { apiKey: CONFIG.apiKey }, timeout: 10000, validateStatus: () => true,
      });
      if (r.status !== 200 || r.data?.success === false) continue;
      const arr = findArr(r.data);
      if (!arr) continue;
      cands = arr
        .map((g) => (typeof g === 'string'
          ? { slug: g, name: pretty(g) }
          : { slug: String(g.slug ?? g.id ?? g.name ?? ''), name: String(g.name ?? g.title ?? g.label ?? pretty(g.slug ?? '')) }))
        .filter((g) => g.slug);
      if (cands.length) { console.log('[index] lista de géneros desde /' + n); break; }
    } catch { /* siguiente */ }
  }
  if (!cands.length) cands = GENRE_GUESS.map((s) => ({ slug: s, name: pretty(s) }));

  const ok = [], seen = new Set();
  for (let i = 0; i < cands.length; i += 6) {
    const chunk = cands.slice(i, i + 6);
    const res = await Promise.all(chunk.map((g) => fetchCatalogPage(1, g.slug)));
    res.forEach((r, k) => {
      if (!r.list.length) return;
      const sig = r.list.slice(0, 3).map((c) => c.id).join(',');
      if (r.echo !== chunk[k].slug && sig === baseSig) return;  // ignoró el filtro
      if (seen.has(sig)) return;                                // género duplicado
      seen.add(sig);
      ok.push(chunk[k]);
    });
  }
  return ok;
}

async function buildIndex() {
  try {   // 1) caché en disco
    const saved = JSON.parse(await fs.readFile(CACHE_FILE, 'utf8'));
    if (saved.items?.length && Date.now() - saved.built < CACHE_MAX_AGE) {
      db = saved;
      console.log(`[index] desde caché: ${db.items.length} animes, ${db.genres.length} géneros`);
      return;
    }
  } catch { /* sin caché */ }

  // 2) descarga del catálogo completo, 6 páginas en paralelo
  console.log('[index] descargando catálogo completo...');
  const seen = new Set(), items = [];
  for (let start = 1; start <= 300; start += 6) {
    const batch = await Promise.all([0, 1, 2, 3, 4, 5].map((i) => fetchCatalogPage(start + i)));
    let end = false;
    for (const b of batch) {
      for (const c of b.list) if (c.url && !seen.has(c.url)) { seen.add(c.url); items.push(c); }
      if (!b.list.length || b.hasMore === false) end = true;
    }
    if (end) break;
    await sleep(300);
  }
  if (!items.length) { console.log('[index] no se pudo descargar el catálogo'); return; }

  const genres = await discoverGenres();
  db = { items, genres, built: Date.now() };
  await fs.mkdir(CACHE_DIR, { recursive: true });
  await fs.writeFile(CACHE_FILE, JSON.stringify(db));
  console.log(`[index] listo: ${items.length} animes | géneros (${genres.length}): ${genres.map((g) => g.name).join(', ') || 'ninguno'}`);
}

async function getCatalogIndex() { await indexReady; return db.items; }   // lo usa /api/search

const shuffle = (a) => {
  a = a.slice();
  for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; }
  return a;
};

app.get('/api/home', async (req, res) => {
  await indexReady;
  const items = db.items;
  if (!items.length) return res.status(503).json({ error: 'catálogo vacío' });

  const rows = [
    { title: 'Recién agregados', items: items.slice(0, 24) },
    { title: 'Para descubrir', items: shuffle(items.slice(24)).slice(0, 24) },
  ];
  // Filas por tipo (película, ONA, OVA...) además del más común.
  const count = {};
  items.forEach((c) => { if (c.type) count[c.type] = (count[c.type] || 0) + 1; });
  Object.keys(count).sort((a, b) => count[b] - count[a]).slice(1, 3).forEach((t) => {
    if (count[t] >= 6) rows.push({ title: t, items: items.filter((c) => c.type === t).slice(0, 24) });
  });
  res.json({ total: items.length, genres: db.genres, hero: items.slice(0, 8), rows });
});

app.get('/api/list', async (req, res) => {
  await indexReady;
  const off = Math.max(0, Number(req.query.offset) || 0);
  const lim = Math.min(120, Number(req.query.limit) || 60);
  res.json({ total: db.items.length, items: db.items.slice(off, off + lim), hasMore: off + lim < db.items.length });
});

app.get('/api/genres', async (req, res) => {
  await indexReady;
  res.json({ genres: db.genres });
});

app.get('/api/search', async (req, res) => {
  const q = String(req.query.q ?? '').trim().slice(0, 80);
  if (q.length < 2) return res.json({ results: [] });
  try {
    let results = noRemoteSearch ? null : await remoteSearch(q);
    let via = 'resolvedor';
    if (!results) {
      const nq = normText(q);
      results = (await getCatalogIndex()).filter((c) => normText(c.title).includes(nq));
      via = 'catalogo-local';
    }
    res.json({ via, results: results.slice(0, 40) });
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});



app.get('/api/probe', async (req, res) => {
  const url = String(req.query.url ?? '');
  const out = {};
  for (const n of ['detail', 'details', 'info', 'anime', 'media', 'episodes', 'show', 'series']) {
    try {
      const r = await axios.get(`${CONFIG.resolverUrl}/api/v1/anime/${n}`, {
        params: { url, apiKey: CONFIG.apiKey },
        timeout: 15000,
        validateStatus: () => true,
      });
      out[n] = { status: r.status, preview: JSON.stringify(r.data).slice(0, 400) };
    } catch (e) {
      out[n] = { error: e.message };
    }
  }
  res.json(out);
});

/* ───────────────────────── ARRANQUE / CIERRE ───────────────────────── */

indexReady = buildIndex().catch((e) => { console.error('[index] error:', e.message); });

await initBrowser();
const server = app.listen(CONFIG.port, '0.0.0.0', () => {
  console.log('====================================================');
  console.log(`🚀 [PROXY TV] http://<IP-de-tu-PC>:${CONFIG.port}`);
  console.log(`🔗 Resolvedor esperado en ${CONFIG.resolverUrl}`);
  console.log('====================================================');
});

process.on('SIGINT', async () => {
  server.close();
  await browser?.close();
  process.exit(0);
});