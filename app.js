'use strict';
/* ================= settings ================= */
const NET_TARGET_MB_H = 28;     // worst-case (uncompressed) download budget per hour. Hard ceiling you set = 35.
const MIN_POLL_MS = 3e3;        // never poll faster than this
const MAX_POLL_MS = 60e3;
const OTHER_BPS = 1200;         // bytes/s reserved for the 24h snapshot + candle loads (the rest goes to price polling)
const FULL_MS = 10 * 60e3;      // full 24h snapshot (volumes, new listings) every 10 min; in between only light price polls
const RESORT_MS = 3000;         // rows are re-sorted at most every 3 s (values still update every poll)
const GAP_DIRECT = 50;          // min ms between REST requests, direct (= max 20 req/s = 1200 weight/min, Binance limit is 2400)
const GAP_PROXY = 250;          // min ms between REST requests through a public proxy
const LONG_PAUSE = 90e3;        // if Binance asks us to wait longer than this, stop sending requests and retry later
const GRACE = 2500;             // ms after a candle closes before we trust the "closed" candle from the API
const PROV_MS = 20e3;           // right after a candle closes, the last price seen before the close is used as a provisional "previous close" (this long at most)
const USE_PUBLIC_PROXIES = false; // third-party CORS proxies can serve cached/forged prices. Turn on only if Binance is unreachable directly.
const MODE_TTL = 10 * 60e3;     // after falling back to a proxy, try the direct route again after this long
const POLL_BPS = NET_TARGET_MB_H * 1e6 / 3600 - OTHER_BPS;

/* ================= network layer ================= */
const DIRECT_API = 'https://fapi.binance.com';
const CORS_PROXIES = [
  full => `https://corsproxy.io/?url=${encodeURIComponent(full)}`,
  full => `https://api.allorigins.win/raw?url=${encodeURIComponent(full)}`
];
const MODES = ['DIRECT'].concat(USE_PUBLIC_PROXIES ? CORS_PROXIES : []);
function buildUrl(mode, path) {
  const m = MODES[mode];
  if (m === 'DIRECT') return DIRECT_API + path;
  return m(DIRECT_API + path);
}
const readJSON = (k, d) => { try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch (e) { return d; } };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const net = { bytes: 0, vis: 0 };   // decoded bytes downloaded / visible seconds (for the MB/h estimate in the status tooltip)

// The route (direct / proxy) is remembered for MODE_TTL, then we try direct again (also while the page stays open).
let activeMode = 0, modeT = 0;
try {
  const m = Number(localStorage.getItem('signalApiMode')), t = Number(localStorage.getItem('signalApiModeT'));
  if (Number.isInteger(m) && m > 0 && m < MODES.length && Date.now() - t < MODE_TTL) { activeMode = m; modeT = t; }
} catch (e) {}
function setMode(m) {
  if (m === activeMode) return;
  activeMode = m; modeT = Date.now();
  try { localStorage.setItem('signalApiMode', String(m)); localStorage.setItem('signalApiModeT', String(modeT)); } catch (e) {}
}
const modeLabel = m => MODES[m] === 'DIRECT' ? 'direct' : `proxy #${m}`;

// Binance server clock (from API timestamps) so candle-close logic never depends on the device clock.
let skew = 0;
const nowMs = () => Date.now() + skew;

// Request pacing: every REST call takes a slot, so we can never burst past Binance's per-minute weight limit.
let pauseUntil = 0, nextSlot = 0;
async function gate() {
  const gap = activeMode === 0 ? GAP_DIRECT : GAP_PROXY;
  for (;;) {
    const t = Date.now();
    if (pauseUntil - t > LONG_PAUSE) {
      const e = new Error(`Binance rate limit • candle data resumes in ~${Math.ceil((pauseUntil - t) / 60e3)} min`);
      e.status = 429; e.long = true; throw e;
    }
    if (pauseUntil > t) { await sleep(pauseUntil - t); continue; }   // Binance told us to wait: wait, then queue again
    const slot = Math.max(nextSlot, t);
    nextSlot = slot + gap;
    if (slot > t) await sleep(slot - t);
    if (pauseUntil <= Date.now()) return;                              // a 429 arrived while queued → loop again
  }
}
async function fetchOnce(mode, path, ms, meta) {
  await gate();
  const ctrl = new AbortController(), to = setTimeout(() => ctrl.abort(), mode === 0 ? ms : ms * 2);
  try {
    const r = await fetch(buildUrl(mode, path), { cache: 'no-store', signal: ctrl.signal });
    const used = Number(r.headers.get('x-mbx-used-weight-1m')) || 0;  // only readable when Binance exposes it
    if (used > 1800) pauseUntil = Math.max(pauseUntil, Date.now() + 8e3);
    if (!r.ok) {
      const err = new Error(`${r.status} ${r.statusText}`); err.status = r.status;
      if (r.status === 429 || r.status === 418) {
        const ra = Number(r.headers.get('retry-after'));
        const wait = ra > 0 ? ra * 1000 : r.status === 418 ? 120e3 : 15e3;
        pauseUntil = Math.max(pauseUntil, Date.now() + wait);
      }
      throw err;
    }
    const txt = await r.text();
    net.bytes += txt.length; if (meta) meta.len = txt.length;
    return JSON.parse(txt);
  } finally { clearTimeout(to); }
}
async function getJSON(path, ms = 10e3, meta) {
  if (activeMode !== 0 && Date.now() - modeT > MODE_TTL) activeMode = 0;   // proxy fallback expired → try direct first again
  let lastErr; const log = [];
  for (let step = 0; step < MODES.length; step++) {
    const mode = (activeMode + step) % MODES.length; let modeErr;
    for (let i = 0; i < 3; i++) {
      try {
        const data = await fetchOnce(mode, path, ms, meta);
        setMode(mode);
        return data;
      } catch (x) {
        lastErr = modeErr = x;
        const s = x.status;
        if (x.long || s === 400 || s === 404) throw x;          // waiting / bad request: other routes won't help
        if (s === 429 || s === 418) { if (i < 2) continue; break; } // rate limited: we already waited, retry same route
        if (s && s < 500) break;                                   // 451 / 403 …: this route is blocked → next route
        if (!s && i >= 1) break;                                   // network error / timeout: one retry, then next route
        await sleep(500 * (i + 1));
      }
    }
    log.push(`${modeLabel(mode)}: ${modeErr?.message || 'network error (fetch blocked/failed)'}`);
    if (modeErr?.status === 429 || modeErr?.status === 418) break;  // other routes share the problem
  }
  const err = lastErr || new Error('Request failed on all connection modes');
  err.summary = log.join(' | '); throw err;
}
const concurrency = () => MODES[activeMode] === 'DIRECT' ? 5 : 2;

/* ================= constants ================= */
const TFS = ['1d', '1w', '1M']; // candle timeframes: daily / weekly / monthly
const DAY = 864e5;
const VOL_STEPS = [0, 10e6, 50e6, 100e6, 500e6, 1e9]; // min 24h USDT volume, cycled by the VOL button
const MOVE_PCT = 10; // LONG shows daily >= +10%, SHORT shows daily <= -10%
const SYMBOLS_TTL = 12 * 36e5;
const CACHE_KEY = 'signalScannerCacheV2';

const cache = readJSON(CACHE_KEY, {});
const state = {
  coins: new Map(),
  flags: new Map(readJSON('signalFlags', []).filter(e => e[1] === 1 || e[1] === 3)), // red=1, green=3
  symbols: null, symT: 0,
  sortKey: 'volume', sortDir: -1,
  filter: null,      // null | 'LONG' (daily >= +10%) | 'SHORT' (daily <= -10%)
  volIdx: 0,         // index into VOL_STEPS (0 = volume filter off)
  search: '',
  marketBusy: false, syncing: false, lastFull: 0, lastOk: 0, fails: 0
};
const rowsEl = document.getElementById('rows');
const statusText = document.getElementById('statusText');
const statusDot = document.getElementById('statusDot');
const lastUpdate = document.getElementById('lastUpdate');
const volBtn = document.getElementById('volBtn');
const longBtn = document.getElementById('longBtn');
const shortBtn = document.getElementById('shortBtn');
const searchInput = document.getElementById('searchInput');
const tableWrap = document.querySelector('.table-wrap');
const ths = [...document.querySelectorAll('th[data-sort]')];

const escapeHtml = s => String(s).replace(/[&<>'"]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[ch]));
function setStatus(t, type = '') { statusText.textContent = t; statusDot.className = `status-dot ${type}`; }
const setOk = () => setStatus(`${state.coins.size} Binance USDT perpetuals${activeMode !== 0 ? ' • via ' + modeLabel(activeMode) : ''}`, 'ok');
const saveFlags = () => { try { localStorage.setItem('signalFlags', JSON.stringify([...state.flags.entries()])); } catch (e) {} };

/* ================= formatting ================= */
const priceFmt = [2, 4, 6, 8].map(d => new Intl.NumberFormat('en-US', { minimumFractionDigits: d, maximumFractionDigits: d }));
const fmtPrice = n => {
  if (!Number.isFinite(n)) return '--';
  const a = Math.abs(n);
  return priceFmt[a >= 1 ? 0 : a >= .1 ? 1 : a >= .01 ? 2 : 3].format(n);
};
const calcPct = (o, p) => Number.isFinite(o) && o !== 0 && Number.isFinite(p) ? ((p - o) / o) * 100 : null;
const pctText = p => Number.isFinite(p) ? `${p >= 0 ? '+' : ''}${p.toFixed(2)}%` : '--';
const fmtVol = n => {
  if (!Number.isFinite(n)) return '--';
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toFixed(0);
};

/* ================= candle boundaries (UTC, like Binance: week = Monday 00:00, month = 1st 00:00) ================= */
function candleStart(tf, t) {                       // open time of the candle that contains t
  const d = new Date(t), y = d.getUTCFullYear(), m = d.getUTCMonth(), day = d.getUTCDate();
  if (tf === '1d') return Date.UTC(y, m, day);
  if (tf === '1w') return Date.UTC(y, m, day - (d.getUTCDay() + 6) % 7);
  return Date.UTC(y, m, 1);
}
function nextClose(tf, t) {                         // first boundary strictly after t
  const d = new Date(t), y = d.getUTCFullYear(), m = d.getUTCMonth(), day = d.getUTCDate();
  if (tf === '1d') return Date.UTC(y, m, day + 1);
  if (tf === '1w') return Date.UTC(y, m, day + ((8 - d.getUTCDay()) % 7 || 7)); // next Monday 00:00 UTC
  return Date.UTC(y, m + 1, 1);                                                  // 1st of next month 00:00 UTC
}

/* ================= candle % (live price vs the previous CLOSED candle's close) =================
   st = { p: previous close, nc: close time of the forming candle, pv?: provisional until this time }.
   When a candle closes, the last price seen before the close becomes a provisional previous close, so the table never
   blanks out; the exact value is fetched right after (isStale → pv). An entry that is long expired shows "--" instead. */
function liveState(c, tf, now, prev) {
  let st = c[tf];
  if (!st) return null;
  if (now >= st.nc) {
    if (now - st.nc > PROV_MS || !(prev > 0)) return null;
    st = c[tf] = { p: prev, nc: nextClose(tf, st.nc + 1), pv: st.nc + GRACE };
  }
  return st;
}
function recompute(c, prev = c.price) {
  const now = nowMs();
  let st;
  st = liveState(c, '1d', now, prev); c.pctDay = st ? calcPct(st.p, c.price) : null;
  st = liveState(c, '1w', now, prev); c.pctWeek = st ? calcPct(st.p, c.price) : null;
  st = liveState(c, '1M', now, prev); c.pctMonth = st ? calcPct(st.p, c.price) : null;
}
function newCoin(symbol, price, volume) {
  const c = { symbol, price, volume };
  for (const tf of TFS) { const cached = cache.k?.[tf]?.[symbol]; if (cached) c[tf] = cached; }
  recompute(c);
  return c;
}

/* ================= candle loading =================
   ONE request per coin gives all three timeframes: the daily candles back to the start of the month already contain
   the last closed day, the last day of the previous week and the last day of the previous month (weight stays 1).
   A coin is re-requested only when one of its candles has closed. All requests go through gate() (paced). */
const isStale = (st, now) => !st || now >= st.nc + GRACE || (st.pv && now >= st.pv);
async function loadCoin(c) {
  const now = nowMs();
  const tfs = TFS.filter(tf => isStale(c[tf], now));
  if (!tfs.length) return true;
  const today = candleStart('1d', now);
  let back = 0;                                     // how many days back the oldest boundary we need is
  for (const tf of tfs) back = Math.max(back, Math.round((today - candleStart(tf, now)) / DAY));
  const ks = await getJSON(`/fapi/v1/klines?symbol=${encodeURIComponent(c.symbol)}&interval=1d&limit=${back + 2}`);
  if (!Array.isArray(ks) || !ks.length) return false;   // nothing yet (brand-new listing) → retry later
  for (const tf of tfs) {
    const b = candleStart(tf, now);
    let p = null;                                   // close of the last daily candle that opened before this candle began
    for (let i = ks.length - 1; i >= 0; i--) if (+ks[i][0] < b) { p = +ks[i][4]; break; }
    c[tf] = { p: Number.isFinite(p) ? p : null, nc: nextClose(tf, now) };
  }
  return true;
}
function anyStale() {
  const now = nowMs(), real = Date.now();
  for (const c of state.coins.values()) {
    if (c.ra && real < c.ra) continue;
    for (const tf of TFS) if (isStale(c[tf], now)) return true;
  }
  return false;
}
function checkCandles() {
  if (state.syncing || document.hidden || !state.coins.size || !anyStale()) return;
  syncCandles();
}
async function syncCandles() {
  if (state.syncing) return;
  state.syncing = true;
  let limited = null;
  try {
    const now = nowMs(), real = Date.now();
    const todo = [];
    for (const c of state.coins.values()) if ((!c.ra || real >= c.ra) && TFS.some(tf => isStale(c[tf], now))) todo.push(c);
    todo.sort((a, b) => b.volume - a.volume);          // biggest coins first
    const total = todo.length;
    if (!total) return;
    let p = 0, n = 0;
    const worker = async () => {
      while (p < total && !limited && !document.hidden) {
        const c = todo[p++];
        try { c.ra = (await loadCoin(c)) ? 0 : Date.now() + 5 * 60e3; }
        catch (e) {
          if (e.long) { limited = e; break; }
          c.ra = Date.now() + 60e3;                    // retry this coin in a minute, don't hammer
          console.warn('klines', c.symbol, e.message || e);
        }
        recompute(c);
        if (total > 20 && ++n % 25 === 0) { setStatus(`Loading candle data… ${n}/${total}`); scheduleRender(); }
      }
    };
    if (total > 20) setStatus(`Loading candle data… 0/${total}`);
    await Promise.all(Array.from({ length: Math.min(concurrency(), total) }, worker));
    saveCache(); scheduleRender();
    if (limited) setStatus(limited.message, 'error'); else setOk();
  } finally { state.syncing = false; }
}

/* ================= persistent cache (shown instantly on next visit) ================= */
let saveTimer = 0;
function saveCache(immediate) {
  clearTimeout(saveTimer);
  if (immediate) return doSave();
  saveTimer = setTimeout(doSave, 1000);
}
function doSave() {
  const k = { '1d': {}, '1w': {}, '1M': {} }, m = {};
  for (const c of state.coins.values()) {
    for (const tf of TFS) if (c[tf]) k[tf][c.symbol] = c[tf];
    m[c.symbol] = [c.price, c.volume];
  }
  try { localStorage.setItem(CACHE_KEY, JSON.stringify({ v: 2, sym: [...state.coins.keys()], symT: state.symT, t: state.lastFull, k, m })); } catch (e) {}
}

/* ================= market data (REST) ================= */
async function ensureSymbols() {
  if (state.symbols && Date.now() - state.symT < SYMBOLS_TTL) return;
  try {
    const info = await getJSON('/fapi/v1/exchangeInfo', 30e3);
    state.symbols = new Set(info.symbols.filter(s => s.contractType === 'PERPETUAL' && s.quoteAsset === 'USDT' && s.status === 'TRADING').map(s => s.symbol));
    state.symT = Date.now();
    let pruned = false;                                // flags of delisted coins are dropped
    for (const s of [...state.flags.keys()]) if (!state.symbols.has(s)) { state.flags.delete(s); pruned = true; }
    if (pruned) saveFlags();
  } catch (e) { if (!state.symbols) throw e; }
}
function applyTicker(ts, full) {
  const allowed = state.symbols; let maxT = 0;
  if (full) for (const s of state.coins.keys()) if (!allowed.has(s)) state.coins.delete(s);
  for (const t of ts) {
    const tm = t.closeTime ?? t.time; if (tm > maxT) maxT = tm;
    if (!allowed.has(t.symbol)) continue;
    const price = Number(t.lastPrice ?? t.price);
    if (!(price > 0)) continue;
    let c = state.coins.get(t.symbol);
    if (!c) { if (!full) continue; c = newCoin(t.symbol, price, Number(t.quoteVolume)); state.coins.set(t.symbol, c); }
    else { const old = c.price; c.price = price; if (full) c.volume = Number(t.quoteVolume); recompute(c, old); }
  }
  if (maxT && activeMode === 0) skew = maxT - Date.now();   // proxies may serve cached copies, so only trust direct responses
}
let pollBytes = 40e3;                                        // running estimate of one price poll (decoded bytes)
const needFull = () => !state.symbols || Date.now() - state.lastFull >= FULL_MS;
async function refreshMarket(full) {
  if (state.marketBusy) return;
  if (!state.symbols) full = true;
  state.marketBusy = true; let ok = false;
  try {
    const symP = full ? ensureSymbols() : null;              // symbol list loads in parallel; awaited only if we have none yet
    const meta = {};
    const tsP = getJSON(full ? '/fapi/v1/ticker/24hr' : '/fapi/v1/ticker/price', full ? 20e3 : 10e3, meta);
    const [ts] = await Promise.all([tsP, state.symbols ? null : symP]);
    applyTicker(ts, full);
    if (full) state.lastFull = Date.now(); else if (meta.len) pollBytes = pollBytes * .7 + meta.len * .3;
    state.lastOk = Date.now(); state.fails = 0;
    scheduleRender(); if (full) saveCache(); ok = true;
  } catch (e) {
    console.error(e); state.fails++;
    if (state.coins.size) setStatus('Update failed • retrying automatically', 'error');
    else {
      const detail = e.summary || e.message || '';
      const hint = /451/.test(detail) ? 'Binance blocks this location (e.g. USA) — turn on a VPN set to another country (Germany, Turkey, UAE…).'
        : 'Binance is not reachable from this connection — turn on a VPN (not USA) and reload.';
      setStatus(`Could not reach Binance • ${hint}`, 'error');
      rowsEl.innerHTML = `<tr><td colspan="8" class="loading">${escapeHtml(hint)}<br><small>${escapeHtml(detail)}</small></td></tr>`;
      rowEls.clear(); lastOrder = [];
    }
  } finally { state.marketBusy = false; }
  if (ok) { if (!state.syncing) setOk(); checkCandles(); }
}

/* ================= live prices: adaptive REST polling =================
   One light request (/ticker/price, weight 2) returns every coin. The interval is derived from the measured payload so the
   worst-case (uncompressed) download stays under NET_TARGET_MB_H; real traffic is lower because responses are gzip'd.
   Nothing runs while the tab is hidden. After failures the interval backs off (x2, x4, x8). */
const live = { on: false, gen: 0, timer: 0 };
const pollDelay = () => Math.min(MAX_POLL_MS, Math.max(MIN_POLL_MS, pollBytes / POLL_BPS * 1000)) * (1 << Math.min(3, state.fails));
function stopPolling() { live.on = false; live.gen++; clearTimeout(live.timer); live.timer = 0; }
function startPolling() {
  if (document.hidden || live.on) return;
  live.on = true; const gen = ++live.gen;
  const loop = async () => {
    if (gen !== live.gen || document.hidden) return;
    try { await refreshMarket(needFull()); }
    finally { if (gen === live.gen) live.timer = setTimeout(loop, pollDelay()); }
  };
  live.timer = setTimeout(loop, pollDelay());
}

/* ================= rendering: rows are created once, only changed cells are touched ================= */
function flagCell(c) {
  const f = state.flags.get(c.symbol) || 0;
  const label = f === 1 ? 'Red flag' : f === 3 ? 'Green flag' : 'No flag';
  const cl = f === 1 ? 'red' : f === 3 ? 'green' : 'none';
  return `<div class="flag-wrap"><button class="flag-btn ${cl}" data-flag="${escapeHtml(c.symbol)}" aria-label="${label}" title="${label}">${f ? '⚑' : '⚐'}</button></div>`;
}
function makeRow(c) {
  const tr = document.createElement('tr');
  tr.innerHTML = `<td class="copy-cell"><button class="copy-btn" data-copy="${escapeHtml(c.symbol)}" title="Copy chart info">📋</button></td>` +
    `<td class="flag-cell"></td>` +
    `<td class="pair"><span class="pair-name">${escapeHtml(c.symbol.replace('USDT', '/USDT'))}</span></td>` +
    `<td class="price"></td><td class="volume"></td>` +
    `<td><div class="cell-plain"></div></td><td><div class="cell-plain"></div></td><td><div class="cell-plain"></div></td>`;
  const t = tr.children;
  tr._r = { flag: t[1], price: t[3], vol: t[4], d: t[5].firstChild, w: t[6].firstChild, m: t[7].firstChild,
            f: -1, p: undefined, v: undefined, pd: undefined, pw: undefined, pm: undefined };
  return tr;
}
function setPct(el, r, key, p) {
  if (Object.is(r[key], p)) return;
  r[key] = p; el.textContent = pctText(p); el.classList.toggle('dim', !Number.isFinite(p));
}
function paintRow(tr, c) {
  const r = tr._r;
  if (!Object.is(r.p, c.price)) { r.p = c.price; r.price.textContent = fmtPrice(c.price); }
  if (!Object.is(r.v, c.volume)) { r.v = c.volume; r.vol.textContent = fmtVol(c.volume); }
  setPct(r.d, r, 'pd', c.pctDay); setPct(r.w, r, 'pw', c.pctWeek); setPct(r.m, r, 'pm', c.pctMonth);
  const f = state.flags.get(c.symbol) || 0;
  if (r.f !== f) { r.f = f; r.flag.innerHTML = flagCell(c); }
}

const SORT_GET = {
  flag: c => state.flags.get(c.symbol) || 0,
  symbol: c => c.symbol,
  price: c => c.price,
  volume: c => c.volume,
  pctDay: c => c.pctDay,
  pctWeek: c => c.pctWeek,
  pctMonth: c => c.pctMonth
};
function passesFilter(c) {
  if (state.search && !c.symbol.includes(state.search)) return false;
  if (VOL_STEPS[state.volIdx] && !(c.volume >= VOL_STEPS[state.volIdx])) return false;
  if (state.filter === 'LONG') return c.pctDay >= MOVE_PCT;
  if (state.filter === 'SHORT') return c.pctDay <= -MOVE_PCT;
  return true;
}
function sortedCoins() {
  const a = [];
  for (const c of state.coins.values()) if (passesFilter(c)) a.push(c);
  const get = SORT_GET[state.sortKey] || SORT_GET.volume, dir = state.sortDir;
  a.sort((x, y) => {
    const av = get(x), bv = get(y);
    if (typeof av === 'string') return av < bv ? -dir : av > bv ? dir : 0;
    const an = av == null || av !== av, bn = bv == null || bv !== bv;   // missing values always last
    if (an || bn) return an === bn ? 0 : an ? 1 : -1;
    return dir * (av - bv);
  });
  return a;
}
const emptyMsg = () => {
  if (state.filter === 'LONG') return `No coins with a daily candle of +${MOVE_PCT}% or more.`;
  if (state.filter === 'SHORT') return `No coins with a daily candle of -${MOVE_PCT}% or less.`;
  if (state.search) return `No coins matching "${escapeHtml(state.search)}".`;
  if (VOL_STEPS[state.volIdx]) return `No coins with 24H volume above ${fmtVol(VOL_STEPS[state.volIdx])}.`;
  return 'No coins found.';
};

const rowEls = new Map(); let lastOrder = [], lastSort = 0, emptyShown = false;
function render() {
  lastSort = performance.now();
  const cs = sortedCoins();
  if (!cs.length) {
    rowsEl.innerHTML = `<tr><td colspan="8" class="loading">${emptyMsg()}</td></tr>`;
    rowEls.clear(); lastOrder = []; emptyShown = true;
  } else {
    let changed = emptyShown || cs.length !== lastOrder.length;
    for (const c of cs) {
      let tr = rowEls.get(c.symbol);
      if (!tr) { tr = makeRow(c); rowEls.set(c.symbol, tr); changed = true; }
      paintRow(tr, c);
    }
    if (!changed) for (let i = 0; i < cs.length; i++) if (cs[i] !== lastOrder[i]) { changed = true; break; }
    if (changed) {
      for (const s of rowEls.keys()) if (!state.coins.has(s)) rowEls.delete(s);
      const f = document.createDocumentFragment();
      for (const c of cs) f.appendChild(rowEls.get(c.symbol));
      rowsEl.replaceChildren(f);
    }
    lastOrder = cs; emptyShown = false;
  }
}
function paint() {                      // values only, no re-sorting
  for (const c of lastOrder) { const tr = rowEls.get(c.symbol); if (tr) paintRow(tr, c); }
}
let raf = 0;
function scheduleRender() {
  if (raf || document.hidden) return;
  raf = requestAnimationFrame(() => { raf = 0; if (performance.now() - lastSort >= RESORT_MS) render(); else paint(); });
}
function updateSortUI() {
  for (const th of ths) {
    th.classList.toggle('sorted-asc', th.dataset.sort === state.sortKey && state.sortDir === 1);
    th.classList.toggle('sorted-desc', th.dataset.sort === state.sortKey && state.sortDir === -1);
  }
}

ths.forEach(th => th.addEventListener('click', () => {
  const k = th.dataset.sort;
  if (state.sortKey === k) state.sortDir *= -1; else { state.sortKey = k; state.sortDir = k === 'symbol' ? 1 : -1; }
  updateSortUI(); render();
}));
function buildCopyText(c) {
  const name = c.symbol.replace('USDT', '/USDT');
  const stamp = new Date().toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
  return `${name} — Binance USDT Perp
Price: ${fmtPrice(c.price)}   |   24H Vol: ${fmtVol(c.volume)}

Daily Candle (vs prev close): ${pctText(c.pctDay)}
Weekly Candle (vs prev close): ${pctText(c.pctWeek)}
Monthly Candle (vs prev close): ${pctText(c.pctMonth)}

Snapshot: ${stamp}`;
}
async function copyCoinInfo(symbol, btn) {
  const c = state.coins.get(symbol);
  if (!c) return;
  const text = buildCopyText(c);
  try {
    await navigator.clipboard.writeText(text);
  } catch (e) {
    try {
      const ta = document.createElement('textarea');
      ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta); ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    } catch (e2) { console.warn('copy failed', e2); return; }
  }
  if (btn) {
    const orig = btn.textContent;
    btn.textContent = '✓'; btn.classList.add('copied');
    setTimeout(() => { btn.textContent = orig; btn.classList.remove('copied'); }, 1200);
  }
}
rowsEl.addEventListener('click', e => {
  const cpy = e.target.closest('[data-copy]');
  if (cpy) { copyCoinInfo(cpy.dataset.copy, cpy); return; }
  const b = e.target.closest('[data-flag]');
  if (!b) return;
  const s = b.dataset.flag, f = state.flags.get(s) || 0, n = f === 0 ? 1 : f === 1 ? 3 : 0;
  if (n) state.flags.set(s, n); else state.flags.delete(s);
  saveFlags();
  const tr = rowEls.get(s), c = state.coins.get(s);
  if (tr && c) paintRow(tr, c);
  if (state.sortKey === 'flag') render();
});
function updateFilterUI() {
  longBtn.classList.toggle('on', state.filter === 'LONG'); longBtn.setAttribute('aria-pressed', state.filter === 'LONG');
  shortBtn.classList.toggle('on', state.filter === 'SHORT'); shortBtn.setAttribute('aria-pressed', state.filter === 'SHORT');
}
function setFilter(f) {
  state.filter = state.filter === f ? null : f;
  updateFilterUI(); tableWrap.scrollTop = 0; render();
}
longBtn.addEventListener('click', () => setFilter('LONG'));
shortBtn.addEventListener('click', () => setFilter('SHORT'));
updateFilterUI();
function updateVolUI() {
  const v = VOL_STEPS[state.volIdx];
  volBtn.textContent = v ? `VOL ≥ ${fmtVol(v)}` : 'VOL';
  volBtn.classList.toggle('on', !!v);
  volBtn.setAttribute('aria-pressed', !!v);
}
volBtn.addEventListener('click', () => {
  state.volIdx = (state.volIdx + 1) % VOL_STEPS.length;
  updateVolUI(); tableWrap.scrollTop = 0; render();
});
updateVolUI();
searchInput.addEventListener('input', () => { state.search = searchInput.value.trim().toUpperCase(); render(); });

/* ================= candle-close countdowns (daily / weekly / monthly, all UTC like Binance) ================= */
const cdEls = { '1d': document.getElementById('cd1d'), '1w': document.getElementById('cd1w'), '1M': document.getElementById('cd1M') };
function fmtLeft(ms) {
  const t = Math.max(0, Math.floor(ms / 60000)), d = Math.floor(t / 1440), h = Math.floor(t % 1440 / 60), m = t % 60;
  return (d ? `${d}d ` : '') + (d || h ? `${h}h ` : '') + `${m}m`;
}
function updateCountdowns() {
  const now = nowMs();
  for (const tf of TFS) { const t = `closes in ${fmtLeft(nextClose(tf, now) - now)}`; if (cdEls[tf].textContent !== t) cdEls[tf].textContent = t; }
}

/* ================= Tehran clock (24h) — unchanged: device clock shown in Asia/Tehran ================= */
const tehranTime = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Tehran', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' });
const updateLast = () => { lastUpdate.textContent = tehranTime.format(Date.now()); };

/* ================= scheduling ================= */
let tickN = 0;
function tick() {
  setTimeout(tick, 1005 - Date.now() % 1000);   // aligned to the wall-clock second, so the clock flips exactly on time
  if (document.hidden) return;
  updateLast(); net.vis++;
  if (++tickN % 5 === 0) {                      // every 5 s: countdown text, candle rollover check, network estimate
    updateCountdowns(); checkCandles();
    if (net.vis > 60) statusText.title = `Network ≈ ${(net.bytes / net.vis * 3600 / 1e6).toFixed(1)} MB/h (uncompressed estimate)`;
  }
}
function onVisibility() {
  updateLast();
  if (document.hidden) { stopPolling(); saveCache(true); return; }
  updateCountdowns(); render(); checkCandles();
  if (!state.coins.size || Date.now() - state.lastOk > pollDelay()) refreshMarket(needFull()).then(startPolling);
  else startPolling();
}
async function boot() {
  await refreshMarket(needFull());      // fresh cache (< FULL_MS old) → only the light price request, no big snapshot
  startPolling();
}

function init() {
  if (cache.v === 2 && cache.sym && cache.m) {
    state.symbols = new Set(cache.sym); state.symT = cache.symT || 0; state.lastFull = Number(cache.t) || 0;
    for (const s of cache.sym) { const m = cache.m[s]; if (m) state.coins.set(s, newCoin(s, m[0], m[1])); }
    if (state.coins.size) { render(); setStatus('Showing saved data • updating…'); }
  } else setStatus('Loading Binance USDT perpetuals…');
  updateSortUI(); updateCountdowns(); updateLast();
  if (!document.hidden) boot();
  tick();
  document.addEventListener('visibilitychange', onVisibility);
  addEventListener('pagehide', () => saveCache(true));
  addEventListener('online', () => { if (document.hidden) return; stopPolling(); state.fails = 0; refreshMarket(needFull()).then(startPolling); });
}
init();
