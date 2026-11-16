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
import { fetch as undiciFetch, Agent } from 'undici';
import { spawn } from 'node:child_process';
import os from 'node:os';
import crypto from 'node:crypto';
import ffmpegPath from 'ffmpeg-static';

/* ───────────────────────── CONFIGURACIÓN ───────────────────────── */

const CONFIG = {
  port: 4000,
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
  transcode: process.env.TRANSCODE ?? 'auto',   // 'auto' = solo si hace falta | 'always' | 'never'
  maxHeight: 720,                               // altura máxima del vídeo convertido (720 o 1080)
};

const MEDIA_RE = /\.(mp4|m3u8|webm)(\?|$)/i;
// MEGA cifra el contenido en el cliente: no existe enlace directo reproducible en <video>.
const NOT_RESOLVABLE_RE = /mega\.nz|mega\.io/i;
// Conexión con más paciencia: algunos servidores de vídeo tardan más de 10 s en aceptar la conexión.
const mediaAgent = new Agent({ connect: { timeout: 30000 }, keepAliveTimeout: 30000 });
const mediaFetch = (u, o) => undiciFetch(u, { ...o, dispatcher: mediaAgent });

// Comprueba que el enlace directo responde de verdad antes de ofrecérselo a la TV.
async function probeDirect(url, referer) {
  const host = new URL(url).hostname;
  for (let i = 0; i < 2; i++) {
    try {
      const r = await mediaFetch(url, {
        headers: { 'User-Agent': CONFIG.userAgent, Referer: referer, Range: 'bytes=0-524287' },
        signal: AbortSignal.timeout(35000),
      });
      const ct = (r.headers.get('content-type') || '').toLowerCase();
      if (/^(text|image)\//.test(ct)) {
        console.log(`[probe] ${host} no devuelve un vídeo (${ct}); lo descarto`);
        await r.body?.cancel();
        return { ok: false, needs: false, hls: false };
      }
      const hls = /mpegurl/.test(ct) || /\.m3u8(\?|$)/i.test(url);
      let needs = false;
      if (r.status === 206 && !hls && /\.mp4(\?|$)/i.test(url)) {
        const info = sniffMp4(Buffer.from(await r.arrayBuffer()));
        console.log('[probe] formato:', info.text);
        needs = info.needs;
      } else {
        await r.body?.cancel();
      }
      console.log('[probe]', host, '->', r.status, ct);
      return { ok: r.status === 200 || r.status === 206, needs, hls };
    } catch (e) {
      const c = e.cause;
      console.log(`[probe] ${host} intento ${i + 1} falló: ${c?.code ?? ''} ${c?.message ?? e.message}`);
    }
  }
  return { ok: false, needs: false, hls: false };
}

function sniffMp4(buf) {
  const s = buf.toString('latin1'), out = [];
  let needs = false;
  out.push('marca=' + (s.slice(4, 8) === 'ftyp' ? s.slice(8, 12) : '?'));
  out.push(s.includes('moov') ? 'moov al inicio' : 'moov NO está al inicio');
  const i = s.indexOf('avcC');
  if (i > 0 && buf.length > i + 8) {
    const prof = buf[i + 5];
    out.push(`H.264 perfil=${prof} nivel=${(buf[i + 7] / 10).toFixed(1)}`);
    if (prof > 100) needs = true;   // 110/122/244 = 10 bits o 4:2:2: las TV viejas no lo soportan
  }
  for (const c of ['hvc1', 'hev1', 'av01', 'vp09']) if (s.includes(c)) { out.push('VIDEO=' + c); needs = true; }
  for (const c of ['mp4a', 'ac-3', 'ec-3', 'Opus']) if (s.includes(c)) out.push('audio=' + c);
  const a = s.indexOf('avc1');
  if (a > 0 && buf.length > a + 32) out.push(`${buf.readUInt16BE(a + 28)}x${buf.readUInt16BE(a + 30)}`);
  return { text: out.join(' | '), needs };
}

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

async function preparePage(page, foundMedia, dbg = false) {
  await blocker.enableBlockingInPage(page);

  await page.route('**/*', (route) => {
    const type = route.request().resourceType();
    if (['image', 'font', 'media'].includes(type)) return route.abort();
    return route.fallback();
  });

  page.on('request', (req) => {
    const u = req.url();
    if (/^https?:/.test(u) && (MEDIA_RE.test(u) || req.resourceType() === 'media')) foundMedia.add(u);
  });

  // Reconoce listas HLS y vídeos por el TIPO que declara el servidor, aunque la URL no tenga extensión.
  let nlog = 0;
  page.on('response', (r) => {
    try {
      const ct = (r.headers()['content-type'] || '').toLowerCase();
      const u = r.url();
      if (/mpegurl/.test(ct) || /^video\/(mp4|webm)/.test(ct)) foundMedia.add(u);
      if (dbg && nlog < 80 && !/^(image|font)\//.test(ct) && !/\.(css|woff2?|png|jpe?g|gif|svg|ico)(\?|$)/i.test(u)) {
        nlog++;
        console.log(`[net] ${r.status()} ${ct || '-'} ${r.request().resourceType()} ${u.slice(0, 150)}`);
      }
    } catch { /* la página ya se cerró */ }
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
  const host = new URL(embedUrl).hostname;
  const dbg = Boolean(process.env.DEBUG_EMBED) && host.includes(process.env.DEBUG_EMBED);
  const log = (...a) => console.log(`[embed ${host}]`, ...a);
  try {
    await preparePage(page, found, dbg);
    const resp = await page.goto(embedUrl, { waitUntil: 'domcontentloaded', timeout: 12000, referer });
    const title = await page.title();
    log('HTTP', resp?.status(), '| título:', title);

    // MP4Upload manda a su portada cuando el archivo ya no existe.
    if (/mp4upload/i.test(host) && (/Easy Way to Backup/i.test(title) || /^https?:\/\/(www\.)?mp4upload\.com\/?$/i.test(page.url()))) {
      log('el archivo ya no existe en este servidor');
      return null;
    }
    await jitter(800, 1500);

    for (let i = 0; i < 3 && found.size === 0; i++) {
      await page.mouse.click(683, 384).catch(() => {});
      await inFrames(page, () => {
        document.querySelectorAll('video').forEach((v) => { v.muted = true; v.play().catch(() => {}); });
      });
      await waitFor(() => found.size > 0, 2000);
    }

    // Respaldo 1: leer el src del <video> en todos los frames.
    if (found.size === 0) {
      const srcs = await inFrames(page, () => {
        const v = document.querySelector('video');
        return v?.currentSrc || v?.src || v?.querySelector('source')?.src || null;
      });
      srcs.filter((s) => s && /^https?:/.test(s)).forEach((s) => found.add(s));
    }

    // Respaldo 2: URL de vídeo escrita en el HTML. La extensión debe estar al FINAL de la ruta,
    // para no confundir el dominio "mp4upload.com" con un archivo .mp4.
    if (found.size === 0) {
      const m = (await page.content()).match(/https?:\/\/[^"'\s\\<>]+?\.(?:mp4|m3u8)(?:\?[^"'\s\\<>]*)?(?=["'\s\\<>]|$)/i);
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

async function buildEpisode(episodeUrl, skip = [], lang = '', quality = 'auto', night = false) {
  const { provider, servers: allServers } = await fetchServerList(episodeUrl);

  // Idioma pedido (SUB/DUB); si no existe, el primero disponible.
  const langs = [...new Set(allServers.map((s) => s.lang).filter(Boolean))];
  const useLang = langs.includes(lang) ? lang : (langs[0] ?? null);
  const servers = useLang ? allServers.filter((s) => s.lang === useLang) : allServers;
  const forcedH = /^\d+$/.test(quality) ? Number(quality) : 0;   // 0 = automático

  // Los que ya son .mp4/.m3u8 no necesitan navegador.
  for (const s of servers) {
    s.direct = MEDIA_RE.test(s.url) ? s.url : null;
  }

  const toResolve = servers
    .filter((s) => !s.direct && !NOT_RESOLVABLE_RE.test(s.url) && !skip.includes(s.name))
    .sort((a, b) => b.score - a.score)
    .slice(0, CONFIG.resolveTopN);

  if (toResolve.length > 0) {
    const context = await browser.newContext({
      userAgent: CONFIG.userAgent,
      viewport: { width: 1366, height: 768 },
      locale: 'es-ES',
    });
    try {
      const openPage = attachPopupKiller(context);
      for (const s of toResolve) {
        s.direct = await resolveEmbed(openPage, s.url, episodeUrl);
        if (s.direct) {
          const p = await probeDirect(s.direct, s.url);
          if (!p.ok) {
            console.log('[probe] el enlace no responde; pruebo el siguiente servidor');
            s.direct = null;
          } else {
            s.transcode = CONFIG.transcode === 'always' || (CONFIG.transcode === 'auto' && p.needs);
            s.hls = p.hls;
            if (s.transcode) console.log('[probe] este vídeo se convertirá a H.264 para la TV');
          }
        }
        if (s.direct) break;
      }
    } finally {
      await context.close();
    }
  }

  const result = servers.map((s) => {
    const playable = Boolean(s.direct);
    // Si el usuario eligió una calidad concreta (1080/720/480), siempre pasa por la conversión.
    const transcode = playable && (Boolean(s.transcode) || ((forcedH > 0 || night) && CONFIG.transcode !== 'never'));
    let src = null;
    if (playable) {
      knownMediaHosts.add(new URL(s.direct).hostname);
      const q = `u=${encodeURIComponent(s.direct)}&r=${encodeURIComponent(s.url)}`;
      src = transcode
        ? `/hls/${newHlsSession(`http://127.0.0.1:${CONFIG.port}/api/proxy?${q}`, forcedH || CONFIG.maxHeight, night)}/index.m3u8`
        : `/v/video.${(s.hls || /\.m3u8(\?|$)/i.test(s.direct)) ? 'm3u8' : 'mp4'}?${q}`;
    }
    return {
      name: s.name,
      lang: s.lang,
      quality: s.quality || qualityOf(s.direct ?? '') || null,
      playable,
      transcoded: transcode,
      embed: s.url,
      direct: s.direct,
      src,
      score: s.score,
    };
  });

  result.sort((a, b) => Number(b.playable) - Number(a.playable) || (b.quality ?? 0) - (a.quality ?? 0) || b.score - a.score);

  return { success: true, provider: provider ?? 'desconocido', episodeUrl, lang: useLang, langs, servers: result };
}

/* ───────────────────────── API EXPRESS ───────────────────────── */

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

const PUBLIC_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), 'public');
console.log('[static] sirviendo archivos desde:', PUBLIC_DIR);

app.use(express.static(PUBLIC_DIR, {
  setHeaders: (res) => res.setHeader('Cache-Control', 'no-store'),
}));

// Empieza a convertir ya el episodio siguiente, pero solo si no hay otra conversión en curso
// (así nunca le quita recursos al episodio que estás viendo).
function warmUp(data) {
  const s = data.servers.find((x) => x.playable && x.src?.startsWith('/hls/'));
  if (!s) return;
  const sess = hls.get(s.src.split('/')[2]);
  if (!sess || sess.run) return;
  const busy = [...hls.values()].some((o) => o !== sess && o.run && !o.run.done);
  if (!busy) { console.log('[hls] precalentando el siguiente episodio'); startRun(sess, 0); }
}

app.get('/api/stream', async (req, res) => {
  let url;
  try {
    url = new URL(String(req.query.url ?? ''));
    if (!['http:', 'https:'].includes(url.protocol)) throw new Error('protocolo');
  } catch {
    return res.status(400).json({ error: 'Falta el parámetro url o no es válido' });
  }
  const skip = String(req.query.skip ?? '').split(',').filter(Boolean);
  const lang = String(req.query.lang ?? '').toUpperCase().slice(0, 4);
  const q = /^(auto|\d{3,4})$/.test(String(req.query.q ?? '')) ? String(req.query.q) : 'auto';
  const night = req.query.n === '1';
  const warm = req.query.warm === '1';
  const key = url.href + '|' + skip.join(',') + '|' + lang + '|' + q + '|' + (night ? 'n' : '');

  try {
    const hit = cache.get(key);
    if (hit && Date.now() - hit.t < CONFIG.cacheTtlMs) {
      if (warm) warmUp(hit.data);
      return res.json(hit.data);
    }

    console.log(`[TV] Solicitando: ${url.href}${skip.length ? ' (sin ' + skip.join(', ') + ')' : ''}`);
    const data = await limit(() => buildEpisode(url.href, skip, lang, q, night));

    if (data.servers.length === 0) {
      return res.status(404).json({ error: 'No se encontraron servidores de video.' });
    }
    if (data.servers.some((s) => s.playable)) cache.set(key, { t: Date.now(), data });
    if (warm) warmUp(data);
    res.json(data);
  } catch (err) {
    const status = err.response?.status;
    console.error('[Error Backend]:', err.message, err.response?.data ? JSON.stringify(err.response.data).slice(0, 300) : '');
    res.status(502).json({
      error: 'Error al procesar los servidores de streaming.',
      detail: err.response?.data?.message ?? err.message,
      resolverStatus: status ?? null,
    });
  }
});

async function fetchRetry(target, opts, tries = 2) {
  let last;
  for (let i = 0; i < tries; i++) {
    try { return await mediaFetch(target, opts); }
    catch (e) {
      last = e;
      if (e.name === 'AbortError') throw e;
      const c = e.cause;
      console.log(`[proxy] intento ${i + 1} falló: ${c ? (c.code ?? '') + ' ' + (c.message ?? c) : e.message}`);
      await sleep(300);
    }
  }
  throw last;
}

app.get(['/api/proxy', '/v/:name'], async (req, res) => {
  const ac = new AbortController();
  res.on('close', () => ac.abort());
  try {
    const target = new URL(String(req.query.u ?? ''));
        console.log('[proxy]', req.method, target.hostname, 'range=' + (req.headers.range ?? '-'), '| cliente:', String(req.headers['user-agent'] ?? '-').slice(0, 70));
    if (!knownMediaHosts.has(target.hostname)) {
      console.log('[proxy] host no permitido:', target.hostname);
      return res.sendStatus(403);
    }

    const referer = String(req.query.r || target.origin + '/');
    const headers = { 'User-Agent': CONFIG.userAgent, Referer: referer };
    if (req.headers.range) headers.Range = req.headers.range;

        const upstream = await fetchRetry(target, { headers, signal: ac.signal });
    console.log(`[proxy] ${target.hostname} range=${req.headers.range ?? '-'} -> ${upstream.status} ${upstream.headers.get('content-type')} len=${upstream.headers.get('content-length')} cr=${upstream.headers.get('content-range')}`);
    if (!upstream.body) return res.sendStatus(502);

    const type = upstream.headers.get('content-type') ?? '';
    const isPlaylist = /\.m3u8(\?|$)/i.test(target.href) || /mpegurl/i.test(type);

    if (isPlaylist) {
      const text = await upstream.text();
      const toProxy = (u) => {
        const abs = new URL(u, target);
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
    for (const h of ['content-length', 'content-range', 'accept-ranges']) {
      const v = upstream.headers.get(h);
      if (v) res.setHeader(h, v);
    }
    // Siempre declaramos el tipo: muchos hosts mandan application/octet-stream.
    res.setHeader('content-type', /\.mp4(\?|$)/i.test(target.pathname + target.search) ? 'video/mp4' : (type || 'video/mp4'));
    if (!res.getHeader('accept-ranges')) res.setHeader('accept-ranges', 'bytes');

    let sent = 0;
    const t0 = Date.now();
    const body = Readable.fromWeb(upstream.body);
    body.on('data', (c) => { sent += c.length; });
    res.on('close', () => console.log(`[proxy] cierre: ${Math.round(sent / 1024)} KB enviados en ${Date.now() - t0} ms | completo=${res.writableFinished}`));
    body.on('error', () => res.destroy()).pipe(res);
  } catch (error) {
    if (error.name !== 'AbortError') console.error('[proxy] error:', error.stack || error.message);
    if (!res.headersSent) res.sendStatus(502);
  }
});

// Tiempos de opening/ending (AniSkip). Si no hay datos o falla, responde found:false y no pasa nada.
const skipCache = new Map();
app.get('/api/skip', async (req, res) => {
  const mal = Number(req.query.mal);
  const n = Number(req.query.ep);
  const len = Math.round(Number(req.query.len) || 0);
  if (!mal || !(n >= 0)) return res.json({ found: false });

  const key = `${mal}/${n}/${len}`;
  const hit = skipCache.get(key);
  if (hit && Date.now() - hit.t < 6 * 3600 * 1000) return res.json(hit.data);

  let out = { found: false };
  try {
    const types = ['op', 'ed', 'mixed-op', 'mixed-ed', 'recap'].map((t) => `types[]=${t}`).join('&');
    const r = await axios.get(`https://api.aniskip.com/v2/skip-times/${mal}/${n || 1}?${types}&episodeLength=${len}`, {
      timeout: 8000,
      validateStatus: () => true,
    });
    if (r.status === 200 && r.data?.found && Array.isArray(r.data.results)) {
      const items = r.data.results
        .map((x) => ({ type: String(x.skipType), start: Number(x.interval?.startTime), end: Number(x.interval?.endTime) }))
        .filter((x) => isFinite(x.start) && isFinite(x.end) && x.end > x.start);
      if (items.length) out = { found: true, items };
    }
    console.log(`[skip] mal=${mal} ep=${n} -> ${out.found ? out.items.map((i) => i.type).join(',') : 'sin datos'}`);
  } catch (e) {
    console.log('[skip] error:', e.message);
  }
  skipCache.set(key, { t: Date.now(), data: out });
  res.json(out);
});

app.get('/api/ping', (req, res) => res.json({ ok: true }));


app.get('/api/r/*', async (req, res) => {
  const p = req.params[0];
  if (!/^v1\/anime\/[a-z0-9\-\/]+$/i.test(p) || p.includes('image-proxy')) {
    return res.status(400).json({ error: 'ruta no permitida' });
  }
  const { kids, ...rest } = req.query;
  try {
    const r = await axios.get(`${CONFIG.resolverUrl}/api/${p}`, {
      params: { ...rest, apiKey: CONFIG.apiKey },
      timeout: 20000,
    });
    if (kids === '1' && Array.isArray(r.data?.data?.results)) {
      r.data.data.results = await kidsFilter(r.data.data.results);
    }
    res.json(r.data);
  } catch (err) {
    res.status(err.response?.status ?? 502).json({ error: err.message });
  }
});

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
  let saved = null;
  try {   // 1) caché en disco
    saved = JSON.parse(await fs.readFile(CACHE_FILE, 'utf8'));
    if (saved.items?.length && Date.now() - saved.built < CACHE_MAX_AGE) {
      db = saved;
      console.log(`[index] desde caché: ${db.items.length} animes, ${db.genres.length} géneros`);
      return;
    }
  } catch { saved = null; }

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
  if (!items.length) {
    if (saved?.items?.length) { db = saved; console.log('[index] no pude actualizar; uso la caché anterior'); }
    else { console.log('[index] no se pudo descargar el catálogo'); }
    return;
  }

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
  const kids = req.query.kids === '1';
  const items = kids ? await kidsFilter(db.items) : db.items;
  if (!items.length) return res.status(503).json({ error: 'catálogo vacío' });

  const rows = [
    { title: 'Recién agregados', items: items.slice(0, 24) },
    { title: 'Para descubrir', items: shuffle(items.slice(24)).slice(0, 24) },
  ];
  const count = {};
  items.forEach((c) => { if (c.type) count[c.type] = (count[c.type] || 0) + 1; });
  Object.keys(count).sort((a, b) => count[b] - count[a]).slice(1, 3).forEach((t) => {
    if (count[t] >= 6) rows.push({ title: t, items: items.filter((c) => c.type === t).slice(0, 24) });
  });
  const genres = kids ? db.genres.filter((g) => !KID_BLOCK.includes(g.slug)) : db.genres;
  res.json({ total: items.length, genres, hero: items.slice(0, 8), rows });
});

app.get('/api/list', async (req, res) => {
  await indexReady;
  let items = db.items;
  if (req.query.kids === '1') items = await kidsFilter(items);

  const type = String(req.query.type ?? '');
  const letter = String(req.query.letter ?? '').toUpperCase();
  const sort = String(req.query.sort ?? 'new');

  const types = {};
  items.forEach((c) => { if (c.type) types[c.type] = (types[c.type] || 0) + 1; });

  if (type) items = items.filter((c) => c.type === type);
  if (letter) {
    items = items.filter((c) => {
      const f = normText(c.title).charAt(0).toUpperCase();
      return letter === '#' ? !/[A-Z]/.test(f) : f === letter;
    });
  }
  if (sort === 'az' || sort === 'za') {
    items = items.slice().sort((a, b) => normText(a.title).localeCompare(normText(b.title)));
    if (sort === 'za') items.reverse();
  }

  const off = Math.max(0, Number(req.query.offset) || 0);
  const lim = Math.min(120, Number(req.query.limit) || 60);
  res.json({
    total: items.length,
    items: items.slice(off, off + lim),
    hasMore: off + lim < items.length,
    types: off === 0 ? Object.keys(types).sort((a, b) => types[b] - types[a]) : undefined,
  });
});

app.get('/api/genres', async (req, res) => {
  await indexReady;
  const kids = req.query.kids === '1';
  res.json({ genres: kids ? db.genres.filter((g) => !KID_BLOCK.includes(g.slug)) : db.genres });
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
    if (req.query.kids === '1') results = await kidsFilter(results);
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

app.get('/test.mp4', async (req, res) => {
  const ac = new AbortController();
  res.on('close', () => ac.abort());
  try {
    const headers = { 'User-Agent': CONFIG.userAgent };
    if (req.headers.range) headers.Range = req.headers.range;
    const up = await mediaFetch('https://www.w3schools.com/html/mov_bbb.mp4', { headers, signal: ac.signal });
    res.status(up.status);
    for (const h of ['content-length', 'content-range', 'accept-ranges']) {
      const v = up.headers.get(h);
      if (v) res.setHeader(h, v);
    }
    res.setHeader('content-type', 'video/mp4');
    if (!res.getHeader('accept-ranges')) res.setHeader('accept-ranges', 'bytes');
    Readable.fromWeb(up.body).on('error', () => res.destroy()).pipe(res);
  } catch {
    if (!res.headersSent) res.sendStatus(502);
  }
});

/* ───────────── CONVERSIÓN AL VUELO (ffmpeg → HLS), con saltos ───────────── */
const FFMPEG = process.env.FFMPEG_PATH || ffmpegPath || 'ffmpeg';
const HLS_DIR = path.join(os.tmpdir(), 'tv-hls');
const SEG = 6;      // segundos por trozo
const AHEAD = 8;    // si piden un trozo a más de 8 trozos (~48 s) de donde trabaja ffmpeg, reiniciamos ahí
const hls = new Map();
const exists = (f) => fs.access(f).then(() => true, () => false);
const segName = (i) => `seg${String(i).padStart(5, '0')}.ts`;

function newHlsSession(input, maxHeight = CONFIG.maxHeight, night = false) {
  const token = crypto.randomBytes(8).toString('hex');
  hls.set(token, {
    input, maxHeight, night, dir: path.join(HLS_DIR, token), run: null, runId: 0,
    duration: 0, durationP: null, resolveDur: null, rejectDur: null,
    lastUse: Date.now(), lastRestart: 0,
  });
  return token;
}

function stopRun(sess) {
  const r = sess.run;
  if (r && !r.done) { r.killed = true; try { r.proc?.kill('SIGKILL'); } catch { /* ya terminó */ } }
}

// Arranca (o reinicia) ffmpeg desde el trozo startIdx.
function startRun(sess, startIdx) {
  for (const o of hls.values()) if (o.run && !o.run.done) stopRun(o);   // una sola conversión activa
  sess.lastRestart = Date.now();
  const startSec = startIdx * SEG;
  const run = { id: ++sess.runId, startIdx, done: false, killed: false, ranToEnd: false, failed: false, proc: null };
  sess.run = run;
  if (!sess.durationP) {
    sess.durationP = new Promise((resolve, reject) => { sess.resolveDur = resolve; sess.rejectDur = reject; });
    sess.durationP.catch(() => {});
  }

  fs.mkdir(sess.dir, { recursive: true }).then(() => {
    if (sess.run !== run || run.killed) return;   // ya lo reemplazaron
    const args = [
      '-hide_banner', '-nostdin',
      '-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '5',
      ...(startSec ? ['-ss', String(startSec)] : []),          // salto de entrada: rápido y preciso al recodificar
      '-i', sess.input,
      '-map', '0:v:0', '-map', '0:a:0?',
      '-vf', `scale=-2:'min(${sess.maxHeight},ih)'`,
      '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23', '-maxrate', '4M', '-bufsize', '8M',
      '-profile:v', 'high', '-level', '4.0', '-pix_fmt', 'yuv420p',
      '-force_key_frames', `expr:gte(t,n_forced*${SEG})`,
      ...(sess.night ? ['-af', 'acompressor=threshold=0.089:ratio=4:attack=20:release=250:makeup=2'] : []),   // modo noche
      '-c:a', 'aac', '-b:a', '128k', '-ac', '2',
      ...(startSec ? ['-output_ts_offset', String(startSec)] : []),   // los tiempos siguen siendo los reales
      '-f', 'hls', '-hls_time', String(SEG), '-hls_list_size', '0',
      '-hls_flags', 'independent_segments+temp_file',          // un trozo solo "existe" cuando está completo
      '-start_number', String(startIdx),
      '-hls_segment_filename', path.join(sess.dir, 'seg%05d.ts'),
      path.join(sess.dir, `ff${run.id}.m3u8`),
    ];
    console.log(`[hls] conversión desde ${startSec} s (trozo ${startIdx})`);
    const proc = spawn(FFMPEG, args, { windowsHide: true });
    run.proc = proc;
    let buf = '';
    proc.stderr.on('data', (d) => {
      buf += d.toString();
      if (!sess.duration) {
        const m = buf.match(/Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/);
        if (m) { sess.duration = (+m[1]) * 3600 + (+m[2]) * 60 + parseFloat(m[3]); sess.resolveDur(); }
      }
      if (buf.length > 20000) buf = buf.slice(-6000);
    });
    proc.on('error', (e) => { run.done = true; run.failed = true; sess.rejectDur(e); });
    proc.on('close', (code) => {
      run.done = true;
      if (run.killed) return;
      if (code === 0) { run.ranToEnd = true; return; }
      run.failed = true;
      console.error('[hls] ffmpeg falló (código ' + code + '). Últimas líneas:\n' + buf.slice(-700));
      if (!sess.duration) sess.rejectDur(new Error('ffmpeg salió con código ' + code));
    });
  }).catch((e) => { run.done = true; run.failed = true; sess.rejectDur(e); });
}

// Lista de reproducción completa (con duración), para que la TV muestre la barra y pueda saltar.
app.get('/hls/:token/index.m3u8', async (req, res) => {
  const sess = hls.get(req.params.token);
  if (!sess) return res.sendStatus(404);
  sess.lastUse = Date.now();
  try {
    if (!sess.duration) {
      if (!sess.run || sess.run.done) startRun(sess, 0);
      await Promise.race([
        sess.durationP,
        sleep(60000).then(() => { throw new Error('tiempo agotado leyendo la duración'); }),
      ]);
    }
  } catch (e) {
    console.error('[hls] error:', e.message);
    return res.sendStatus(502);
  }

  const n = Math.ceil(sess.duration / SEG);
  const lines = ['#EXTM3U', '#EXT-X-VERSION:3', `#EXT-X-TARGETDURATION:${SEG + 1}`, '#EXT-X-MEDIA-SEQUENCE:0', '#EXT-X-PLAYLIST-TYPE:VOD'];
  for (let i = 0; i < n; i++) {
    const d = i === n - 1 ? sess.duration - SEG * (n - 1) : SEG;
    lines.push(`#EXTINF:${d.toFixed(3)},`, segName(i));
  }
  lines.push('#EXT-X-ENDLIST');
  res.setHeader('Cache-Control', 'no-store');
  res.type('application/vnd.apple.mpegurl').send(lines.join('\n') + '\n');
});

// Trozos de vídeo. Si piden uno lejano, reiniciamos la conversión ahí.
app.get('/hls/:token/:file', async (req, res) => {
  const sess = hls.get(req.params.token);
  if (!sess || !/^seg\d{5}\.ts$/.test(req.params.file)) return res.sendStatus(404);
  const idx = Number(req.params.file.slice(3, 8));
  const file = path.join(sess.dir, req.params.file);

  const t0 = Date.now();
  while (Date.now() - t0 < 90000) {
    sess.lastUse = Date.now();
    if (await exists(file)) {
      return res.sendFile(file, { dotfiles: 'allow', headers: { 'Content-Type': 'video/mp2t', 'Cache-Control': 'no-store' } });
    }
    const run = sess.run;
    if (!run || run.done) {
      if (run?.ranToEnd && idx >= run.startIdx) return res.sendStatus(404);   // más allá del final
      if (run?.failed) return res.sendStatus(502);
      startRun(sess, idx);                                                   // no hay conversión activa: empezamos aquí
    } else if (Date.now() - sess.lastRestart > 3000) {
      let h = run.startIdx;
      while (await exists(path.join(sess.dir, segName(h)))) h++;             // primer trozo que falta = donde trabaja ffmpeg
      if (idx < run.startIdx || idx - h > AHEAD) startRun(sess, idx);        // salto fuera de lo que se está convirtiendo
    }
    await sleep(300);
  }
  res.sendStatus(504);
});

// Limpieza: a los 10 min sin uso se detiene ffmpeg y se borran los temporales.
setInterval(() => {
  const now = Date.now();
  for (const [t, s] of hls) {
    if (now - s.lastUse > 10 * 60 * 1000) {
      stopRun(s);
      hls.delete(t);
      cache.clear();   // las respuestas guardadas apuntaban a esta sesión
      fs.rm(s.dir, { recursive: true, force: true }).catch(() => {});
    }
  }
}, 60 * 1000).unref();

function ffmpegCheck() {
  const p = spawn(FFMPEG, ['-version'], { windowsHide: true });
  let out = '';
  p.stdout.on('data', (d) => { out += d; });
  p.on('error', (e) => console.log('[hls] ffmpeg NO disponible:', e.message));
  p.on('close', (c) => { if (c === 0) console.log('[hls] ffmpeg listo:', out.split('\n')[0]); });
}

/* ───────────── PERFILES, DATOS EN LA PC, MODO INFANTIL Y NUEVOS EPISODIOS ───────────── */
const DATA_DIR = path.join(ROOT, 'data');
const PROFILES_FILE = path.join(DATA_DIR, 'profiles.json');
const COUNTS_FILE = path.join(DATA_DIR, 'episodes.json');
const pfile = (id) => path.join(DATA_DIR, `p-${id}.json`);
const validId = (id) => /^[a-z0-9]{2,12}$/.test(String(id));

async function readJson(file, fallback) {
  try { return JSON.parse(await fs.readFile(file, 'utf8')); } catch { return fallback; }
}
async function writeJson(file, obj) {
  await fs.mkdir(DATA_DIR, { recursive: true });
  const tmp = file + '.tmp';
  await fs.writeFile(tmp, JSON.stringify(obj));
  await fs.rename(tmp, file);
}
// Las escrituras sobre un mismo archivo van en fila, para que dos peticiones seguidas no se pisen.
const chains = new Map();
function serial(key, fn) {
  const prev = chains.get(key) ?? Promise.resolve();
  const next = prev.then(fn, fn);
  chains.set(key, next.catch(() => {}));
  return next;
}

/* ── Perfiles ── */
async function getProfiles() {
  let list = await readJson(PROFILES_FILE, null);
  if (!Array.isArray(list) || !list.length) {
    list = [{ id: 'main', name: 'Principal', kids: false, color: 0 }];
    await writeJson(PROFILES_FILE, list);
  }
  return list;
}

app.get('/api/profiles', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json(await getProfiles());
});

app.post('/api/profiles', async (req, res) => {
  const name = String(req.body?.name ?? '').trim().slice(0, 12);
  if (!name) return res.status(400).json({ error: 'El nombre está vacío' });
  const made = await serial('profiles', async () => {
    const list = await getProfiles();
    if (list.length >= 8) return null;
    const p = { id: crypto.randomBytes(3).toString('hex'), name, kids: Boolean(req.body?.kids), color: list.length % 6 };
    list.push(p);
    await writeJson(PROFILES_FILE, list);
    return p;
  });
  if (!made) return res.status(400).json({ error: 'Máximo 8 perfiles' });
  res.json(made);
});

app.delete('/api/profiles/:id', async (req, res) => {
  const id = req.params.id;
  if (!validId(id)) return res.sendStatus(400);
  const ok = await serial('profiles', async () => {
    const list = await getProfiles();
    if (list.length <= 1) return false;          // siempre queda al menos un perfil
    await writeJson(PROFILES_FILE, list.filter((p) => p.id !== id));
    await fs.rm(pfile(id), { force: true });
    return true;
  });
  res.json({ ok });
});

/* ── Datos de cada perfil (Mi lista, progreso, vistos, preferencias) ── */
const KEY_OK = /^(mylist|watching|pref|watched:.{1,300}|eps:.{1,300})$/;

app.get('/api/store/:pid', async (req, res) => {
  const id = req.params.pid;
  if (!validId(id) || !(await getProfiles()).some((p) => p.id === id)) return res.sendStatus(404);
  res.setHeader('Cache-Control', 'no-store');
  res.json(await readJson(pfile(id), {}));
});

app.post('/api/store/:pid', async (req, res) => {
  const id = req.params.pid;
  if (!validId(id) || !Array.isArray(req.body?.ops)) return res.sendStatus(400);
  if (!(await getProfiles()).some((p) => p.id === id)) return res.sendStatus(404);
  await serial('p-' + id, async () => {
    const data = await readJson(pfile(id), {});
    for (const op of req.body.ops.slice(0, 300)) {
      const k = String(op?.k ?? '');
      if (!KEY_OK.test(k)) continue;
      if (op.del) delete data[k];
      else if (typeof op.v === 'string' && op.v.length < 500000) data[k] = op.v;
    }
    await writeJson(pfile(id), data);
  });
  res.json({ ok: true });
});

/* ── Modo infantil: títulos de géneros no aptos (según los géneros del propio sitio) ── */
const KID_BLOCK = ['ecchi', 'harem', 'terror', 'horror', 'suspenso', 'psicologico', 'seinen'];
let blockedP = null, blockedSet = null;

function ensureBlocked() {
  if (db.blocked) return Promise.resolve(db.blocked);
  if (!blockedP) {
    blockedP = (async () => {
      await indexReady;
      const slugs = db.genres.map((g) => g.slug).filter((s) => KID_BLOCK.includes(s));
      const set = new Set();
      for (const slug of slugs) {
        for (let page = 1; page <= 40; page++) {
          const r = await fetchCatalogPage(page, slug);
          r.list.forEach((c) => set.add(c.url));
          if (!r.list.length || r.hasMore === false) break;
          await sleep(150);
        }
      }
      db.blocked = [...set];
      try { await fs.mkdir(CACHE_DIR, { recursive: true }); await fs.writeFile(CACHE_FILE, JSON.stringify(db)); } catch { /* sin permisos */ }
      console.log(`[kids] ${db.blocked.length} títulos bloqueados (${slugs.join(', ') || 'no hay géneros para bloquear'})`);
      return db.blocked;
    })();
  }
  return blockedP;
}

async function kidsFilter(arr) {
  const b = await ensureBlocked();
  if (!blockedSet || blockedSet.size !== b.length) blockedSet = new Set(b);
  return arr.filter((c) => !blockedSet.has(c.url));
}

/* ── Insignia "NUEVO": número de episodios de los animes que sigue cada perfil ── */
let counts = {};   // url del anime → { n: episodios, t: última comprobación }

async function trackedUrls() {
  const urls = new Set();
  for (const p of await getProfiles()) {
    const d = await readJson(pfile(p.id), {});
    try { JSON.parse(d.mylist || '[]').forEach((x) => x.url && urls.add(x.url)); } catch { /* dato corrupto */ }
    try { JSON.parse(d.watching || '[]').forEach((x) => x.a && urls.add(x.a)); } catch { /* dato corrupto */ }
  }
  return [...urls];
}

async function checkCount(url) {
  try {
    const r = await axios.get(`${CONFIG.resolverUrl}/api/v1/anime/info`, {
      params: { url, apiKey: CONFIG.apiKey }, timeout: 20000, validateStatus: () => true,
    });
    const d = r.data?.data;
    if (r.status !== 200 || !d) return;
    const n = Array.isArray(d.episodes) ? d.episodes.length : Number(d.totalEpisodes) || 0;
    if (n) counts[url] = { n, t: Date.now() };
  } catch { /* se reintenta en la siguiente vuelta */ }
}

async function refreshCounts() {
  counts = { ...(await readJson(COUNTS_FILE, {})), ...counts };
  const urls = await trackedUrls();
  for (const u of urls) {
    if (!counts[u] || Date.now() - counts[u].t > 3 * 3600 * 1000) { await checkCount(u); await sleep(400); }
  }
  await writeJson(COUNTS_FILE, counts);
  console.log(`[nuevos] comprobados ${urls.length} animes`);
}

app.post('/api/counts', async (req, res) => {
  const urls = Array.isArray(req.body?.urls) ? req.body.urls.slice(0, 60).map(String) : [];
  const out = {}, missing = [];
  for (const u of urls) { if (counts[u]) out[u] = counts[u].n; else missing.push(u); }
  res.json({ counts: out });
  // Los que aún no conocemos se consultan después; la próxima vez ya estarán.
  for (const u of missing.slice(0, 20)) { await checkCount(u); await sleep(300); }
  if (missing.length) writeJson(COUNTS_FILE, counts).catch(() => {});
});

/* ───────────────────────── ARRANQUE / CIERRE ───────────────────────── */

await fs.rm(HLS_DIR, { recursive: true, force: true });
ffmpegCheck();

indexReady = buildIndex().catch((e) => { console.error('[index] error:', e.message); });

// Prepara en segundo plano la lista del modo infantil y la comprobación de episodios nuevos.
indexReady.then(() => ensureBlocked()).catch(() => {});
setTimeout(() => refreshCounts().catch(() => {}), 60 * 1000).unref();
setInterval(() => refreshCounts().catch(() => {}), 3 * 3600 * 1000).unref();

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