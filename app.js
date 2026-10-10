/* ================= network layer (same resilient setup as the other scanner) ================= */
const DIRECT_API = 'https://fapi.binance.com';
const CORS_PROXIES = [
  full => `https://corsproxy.io/?url=${encodeURIComponent(full)}`,
  full => `https://api.allorigins.win/raw?url=${encodeURIComponent(full)}`
];
const MODES = ['DIRECT'].concat(CORS_PROXIES);
const TOTAL_MODES = MODES.length;
function buildUrl(mode, path) {
  const m = MODES[mode];
  if (m === 'DIRECT') return DIRECT_API + path;
  return m(DIRECT_API + path);
}
const readJSON = (k, d) => { try { return JSON.parse(localStorage.getItem(k)) ?? d; } catch (e) { return d; } };
let activeMode = Number(localStorage.getItem('signalApiMode') || 0);
if (!(Number.isInteger(activeMode) && activeMode >= 0 && activeMode < TOTAL_MODES)) activeMode = 0;
const modeLabel = m => MODES[m] === 'DIRECT' ? 'direct' : `proxy #${m}`;

let pauseUntil = 0;
async function fetchOnce(mode, path) {
  const w = pauseUntil - Date.now();
  if (w > 0) await sleep(w);
  const ctrl = new AbortController(), to = setTimeout(() => ctrl.abort(), 20000);
  try {
    const r = await fetch(buildUrl(mode, path), { cache: 'no-store', signal: ctrl.signal });
    if (!r.ok) {
      if (r.status === 429 || r.status === 418) pauseUntil = Date.now() + (r.status === 418 ? 60e3 : 10e3);
      const err = new Error(`${r.status} ${r.statusText}`); err.status = r.status; throw err;
    }
    return await r.json();
  } finally { clearTimeout(to); }
}
async function getJSON(path, retriesPerMode = 2) {
  let lastErr; const log = [];
  for (let step = 0; step < TOTAL_MODES; step++) {
    const mode = (activeMode + step) % TOTAL_MODES; let modeErr;
    for (let i = 0; i < retriesPerMode; i++) {
      try {
        const data = await fetchOnce(mode, path);
        if (mode !== activeMode) { activeMode = mode; localStorage.setItem('signalApiMode', String(mode)); }
        return data;
      } catch (x) {
        lastErr = modeErr = x;
        if (x.status && x.status !== 429 && x.status < 500) break;
        await sleep(600 * (i + 1));
      }
    }
    log.push(`${modeLabel(mode)}: ${modeErr?.message || 'network error (fetch blocked/failed)'}`);
  }
  const err = lastErr || new Error('Request failed on all connection modes');
  err.summary = log.join(' | '); throw err;
}
const concurrency = () => MODES[activeMode] === 'DIRECT' ? 16 : 6;
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ================= constants ================= */
const TFS = ['1d', '1w', '1M']; // candle timeframes: daily / weekly / monthly
const VOL_STEPS = [0, 10e6, 50e6, 100e6, 500e6, 1e9]; // min 24h USDT volume, cycled by the VOL button
const MOVE_PCT = 10; // LONG shows daily >= +10%, SHORT shows daily <= -10%
const MARKET_INTERVAL = 2 * 60e3; // market refresh every 2 minutes
const SYMBOLS_TTL = 3 * 36e5;
const CACHE_KEY = 'signalScannerCacheV2';
try { localStorage.removeItem('signalScannerCacheV1'); } catch (e) {} // old cache format (bigger, no longer used)

const cache = readJSON(CACHE_KEY, {});
const state = {
  coins: new Map(),
  flags: new Map(readJSON('signalFlags', []).filter(e => e[1] === 1 || e[1] === 3)), // red=1, green=3 (blue removed)
  symbols: null, symT: 0,
  sortKey: 'volume', sortDir: -1,
  filter: null,      // null | 'LONG' (daily >= +10%) | 'SHORT' (daily <= -10%)
  volIdx: 0,         // index into VOL_STEPS (0 = volume filter off)
  search: '',
  marketBusy: false, syncing: false, lastPrice: 0, lastFull: 0
};
const rowsEl = document.getElementById('rows');
const statusText = document.getElementById('statusText');
const statusDot = document.getElementById('statusDot');
const lastUpdate = document.getElementById('lastUpdate');
const volBtn = document.getElementById('volBtn');
const longBtn = document.getElementById('longBtn');
const shortBtn = document.getElementById('shortBtn');
const searchInput = document.getElementById('searchInput');

const escapeHtml = s => String(s).replace(/[&<>'"]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[ch]));
function setStatus(t, type = '') { statusText.textContent = t; statusDot.className = `status-dot ${type}`; }
const setOk = () => setStatus(`${state.coins.size} Binance USDT perpetuals${activeMode !== 0 ? ' • via ' + modeLabel(activeMode) : ''}`, 'ok');

/* ================= formatting ================= */
const fmtPrice = n => {
  if (!Number.isFinite(n)) return '--';
  const a = Math.abs(n), d = a >= 1 ? 2 : a >= .1 ? 4 : a >= .01 ? 6 : 8;
  return n.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
};
const calcPct = (o, p) => Number.isFinite(o) && o !== 0 && Number.isFinite(p) ? ((p - o) / o) * 100 : null;
const fmtVol = n => {
  if (!Number.isFinite(n)) return '--';
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toFixed(0);
};

/* ================= candle % (live price vs the previous CLOSED candle's close) ================= */
function recompute(c) {
  c.pctDay = calcPct(c['1d']?.p, c.price);
  c.pctWeek = calcPct(c['1w']?.p, c.price);
  c.pctMonth = calcPct(c['1M']?.p, c.price);
}

function newCoin(symbol, price, volume) {
  const c = { symbol, price, volume };
  for (const tf of TFS) { const cached = cache.k?.[tf]?.[symbol]; if (cached) c[tf] = cached; }
  recompute(c);
  return c;
}

/* ================= candle loading =================
   Only ONE value per timeframe is needed: the close of the last closed candle (the "previous close").
   It changes only when the current candle closes (st.nc = close time of the forming candle), so each coin
   is re-requested at most once per day / week / month. Binance weekly = Monday 00:00 UTC, monthly = 1st 00:00 UTC. */
function needsUpdate(c, tf, now) {
  const st = c[tf];
  return !st || now >= st.nc;
}
async function loadCandles(c, tf, now) {
  const ks = await getJSON(`/fapi/v1/klines?symbol=${encodeURIComponent(c.symbol)}&interval=${tf}&limit=3`);
  if (!Array.isArray(ks) || !ks.length) return;
  let prev = null, nc = null;
  for (const k of ks) {
    const closeTime = Number(k[6]);
    if (closeTime <= now) prev = Number(k[4]);   // last closed candle wins
    else if (nc === null) nc = closeTime;        // the candle that is still forming
  }
  c[tf] = { p: prev, nc: nc ?? now + 36e5 };     // p = null for a coin with no closed candle of this size yet
}
async function syncCandles() {
  if (state.syncing) return;
  state.syncing = true;
  try {
    const now = Date.now();
    const todo = [...state.coins.values()].filter(c => TFS.some(tf => needsUpdate(c, tf, now))).sort((a, b) => b.volume - a.volume);
    const total = todo.length; let p = 0, n = 0;
    async function worker() {
      while (p < total) {
        const c = todo[p++];
        try { await Promise.all(TFS.filter(tf => needsUpdate(c, tf, now)).map(tf => loadCandles(c, tf, now))); recompute(c); }
        catch (e) { console.warn('klines', c.symbol, e.message || e); }
        if (total > 20 && ++n % 10 === 0) { setStatus(`Loading candle data… ${n}/${total}`); scheduleRender(); }
      }
    }
    if (total) {
      if (total > 20) setStatus(`Loading candle data… 0/${total}`);
      await Promise.all(Array.from({ length: Math.min(concurrency(), total) }, worker));
    }
    saveCache();
    scheduleRender(); updateLast(); setOk();
  } finally { state.syncing = false; }
}

/* ================= persistent cache ================= */
let saveTimer = 0;
function saveCache() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    const k = { '1d': {}, '1w': {}, '1M': {} }, m = {};
    for (const c of state.coins.values()) {
      for (const tf of TFS) if (c[tf]) k[tf][c.symbol] = c[tf];
      m[c.symbol] = [c.price, c.volume];
    }
    try { localStorage.setItem(CACHE_KEY, JSON.stringify({ v: 2, sym: [...state.coins.keys()], symT: state.symT, k, m })); } catch (e) {}
  }, 1000);
}

/* ================= market data ================= */
async function ensureSymbols() {
  if (state.symbols && Date.now() - state.symT < SYMBOLS_TTL) return;
  try {
    const info = await getJSON('/fapi/v1/exchangeInfo');
    state.symbols = new Set(info.symbols.filter(s => s.contractType === 'PERPETUAL' && s.quoteAsset === 'USDT' && s.status === 'TRADING').map(s => s.symbol));
    state.symT = Date.now();
  } catch (e) { if (!state.symbols) throw e; }
}
function applyTicker(ts, full) {
  const allowed = state.symbols;
  if (full) for (const s of state.coins.keys()) if (!allowed.has(s)) state.coins.delete(s);
  for (const t of ts) {
    if (!allowed.has(t.symbol)) continue;
    const price = Number(t.lastPrice ?? t.price);
    let c = state.coins.get(t.symbol);
    if (!c) { if (!full) continue; c = newCoin(t.symbol, price, Number(t.quoteVolume)); state.coins.set(t.symbol, c); }
    else { c.price = price; if (full) c.volume = Number(t.quoteVolume); recompute(c); }
  }
}
async function refreshMarket(full) {
  if (state.marketBusy) return;
  state.marketBusy = true; let ok = false;
  try {
    if (full) await ensureSymbols();
    const ts = await getJSON(full ? '/fapi/v1/ticker/24hr' : '/fapi/v1/ticker/price');
    applyTicker(ts, full);
    state.lastPrice = Date.now(); if (full) state.lastFull = state.lastPrice;
    scheduleRender(); updateLast(); saveCache(); ok = true;
  } catch (e) {
    console.error(e);
    if (state.coins.size) setStatus('Update failed • retrying automatically', 'error');
    else {
      const detail = e.summary || e.message || '';
      const hint = /451/.test(detail) ? 'Binance blocks this location (e.g. USA) — turn on a VPN set to another country (Germany, Turkey, UAE…).'
        : 'Binance is not reachable from this connection — turn on a VPN (not USA) and reload.';
      setStatus(`Could not reach Binance • ${hint}`, 'error');
      rowsEl.innerHTML = `<tr><td colspan="8" class="loading">${escapeHtml(hint)}<br><small>${escapeHtml(detail)}</small></td></tr>`;
    }
  } finally { state.marketBusy = false; }
  if (ok && full) await syncCandles();
}

/* ================= rendering (keyed rows: only changed rows touch the DOM) ================= */
function candleCell(p) {
  if (!Number.isFinite(p)) return '<div class="cell-plain dim">--</div>';
  return `<div class="cell-plain">${p >= 0 ? '+' : ''}${p.toFixed(2)}%</div>`;
}
function flagCell(c) {
  const f = state.flags.get(c.symbol) || 0;
  const label = f === 1 ? 'Red flag' : f === 3 ? 'Green flag' : 'No flag';
  const cl = f === 1 ? 'red' : f === 3 ? 'green' : 'none';
  return `<div class="flag-wrap"><button class="flag-btn ${cl}" data-flag="${escapeHtml(c.symbol)}" aria-label="${label}" title="${label}">${f ? '⚑' : '⚐'}</button></div>`;
}
const rowHTML = c => `<td class="copy-cell"><button class="copy-btn" data-copy="${escapeHtml(c.symbol)}" title="Copy chart info">📋</button></td>
  <td class="flag-cell">${flagCell(c)}</td>
  <td class="pair"><span class="pair-name">${escapeHtml(c.symbol.replace('USDT', '/USDT'))}</span></td>
  <td class="price">${fmtPrice(c.price)}</td>
  <td class="volume">${fmtVol(c.volume)}</td>
  <td>${candleCell(c.pctDay)}</td>
  <td>${candleCell(c.pctWeek)}</td>
  <td>${candleCell(c.pctMonth)}</td>`;

function sortValue(c, k) {
  switch (k) {
    case 'flag': return state.flags.get(c.symbol) || 0;
    case 'symbol': return c.symbol;
    case 'price': return c.price;
    case 'volume': return c.volume;
    case 'pctDay': return Number.isFinite(c.pctDay) ? c.pctDay : -Infinity;
    case 'pctWeek': return Number.isFinite(c.pctWeek) ? c.pctWeek : -Infinity;
    case 'pctMonth': return Number.isFinite(c.pctMonth) ? c.pctMonth : -Infinity;
  }
  return 0;
}
function passesFilter(c) {
  if (state.search && !c.symbol.includes(state.search)) return false;
  if (VOL_STEPS[state.volIdx] && !(c.volume >= VOL_STEPS[state.volIdx])) return false;
  if (state.filter === 'LONG') return c.pctDay >= MOVE_PCT;
  if (state.filter === 'SHORT') return c.pctDay <= -MOVE_PCT;
  return true;
}
function sortedCoins() {
  const a = [...state.coins.values()].filter(passesFilter);
  a.sort((x, y) => {
    const av = sortValue(x, state.sortKey), bv = sortValue(y, state.sortKey);
    return typeof av === 'string' ? state.sortDir * av.localeCompare(bv) : state.sortDir * ((av ?? 0) - (bv ?? 0));
  });
  return a;
}
const emptyMsg = () => {
  if (state.filter === 'LONG') return `No coins with a daily candle of +${MOVE_PCT}% or more.`;
  if (state.filter === 'SHORT') return `No coins with a daily candle of -${MOVE_PCT}% or less.`;
  if (state.search) return `No coins matching "${state.search}".`;
  if (VOL_STEPS[state.volIdx]) return `No coins with 24H volume above ${fmtVol(VOL_STEPS[state.volIdx])}.`;
  return 'No coins found.';
};

const rowEls = new Map(); let lastOrder = [];
function render() {
  const cs = sortedCoins();
  if (!cs.length) {
    rowsEl.innerHTML = `<tr><td colspan="8" class="loading">${emptyMsg()}</td></tr>`;
    rowEls.clear(); lastOrder = [];
  } else {
    let changed = cs.length !== lastOrder.length;
    for (const c of cs) {
      let tr = rowEls.get(c.symbol);
      if (!tr) { tr = document.createElement('tr'); rowEls.set(c.symbol, tr); changed = true; }
      const h = rowHTML(c);
      if (tr._h !== h) { tr.innerHTML = h; tr._h = h; }
    }
    if (!changed) for (let i = 0; i < cs.length; i++) if (cs[i] !== lastOrder[i]) { changed = true; break; }
    if (changed) {
      for (const s of rowEls.keys()) if (!state.coins.has(s)) rowEls.delete(s);
      const f = document.createDocumentFragment();
      for (const c of cs) f.appendChild(rowEls.get(c.symbol));
      rowsEl.replaceChildren(f);
    }
    lastOrder = cs;
  }
  document.querySelectorAll('th[data-sort]').forEach(th => {
    th.classList.toggle('sorted-asc', th.dataset.sort === state.sortKey && state.sortDir === 1);
    th.classList.toggle('sorted-desc', th.dataset.sort === state.sortKey && state.sortDir === -1);
  });
}
let renderTimer = 0;
function scheduleRender() { if (renderTimer) return; renderTimer = setTimeout(() => { renderTimer = 0; render(); }, 250); }

document.querySelectorAll('th[data-sort]').forEach(th => th.addEventListener('click', () => {
  const k = th.dataset.sort;
  if (state.sortKey === k) state.sortDir *= -1; else { state.sortKey = k; state.sortDir = k === 'symbol' ? 1 : -1; }
  render();
}));
function buildCopyText(c) {
  const name = c.symbol.replace('USDT', '/USDT');
  const pct = p => Number.isFinite(p) ? `${p >= 0 ? '+' : ''}${p.toFixed(2)}%` : '--';
  const stamp = new Date().toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
  return `${name} — Binance USDT Perp
Price: ${fmtPrice(c.price)}   |   24H Vol: ${fmtVol(c.volume)}

Daily Candle (vs prev close): ${pct(c.pctDay)}
Weekly Candle (vs prev close): ${pct(c.pctWeek)}
Monthly Candle (vs prev close): ${pct(c.pctMonth)}

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
  localStorage.setItem('signalFlags', JSON.stringify([...state.flags.entries()]));
  render();
});
function updateFilterUI() {
  longBtn.classList.toggle('on', state.filter === 'LONG'); longBtn.setAttribute('aria-pressed', state.filter === 'LONG');
  shortBtn.classList.toggle('on', state.filter === 'SHORT'); shortBtn.setAttribute('aria-pressed', state.filter === 'SHORT');
}
function setFilter(f) {
  state.filter = state.filter === f ? null : f;
  updateFilterUI(); document.querySelector('.table-wrap').scrollTop = 0; render();
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
  updateVolUI(); document.querySelector('.table-wrap').scrollTop = 0; render();
});
updateVolUI();
searchInput.addEventListener('input', () => { state.search = searchInput.value.trim().toUpperCase(); render(); });

/* ================= candle-close countdowns (daily / weekly / monthly, all UTC like Binance) ================= */
const cdEls = { '1d': document.getElementById('cd1d'), '1w': document.getElementById('cd1w'), '1M': document.getElementById('cd1M') };
function nextClose(tf, now) {
  const d = new Date(now), y = d.getUTCFullYear(), m = d.getUTCMonth(), day = d.getUTCDate();
  if (tf === '1d') return Date.UTC(y, m, day + 1);
  if (tf === '1w') return Date.UTC(y, m, day + ((8 - d.getUTCDay()) % 7 || 7)); // next Monday 00:00 UTC
  return Date.UTC(y, m + 1, 1);                                                  // 1st of next month 00:00 UTC
}
function fmtLeft(ms) {
  const t = Math.max(0, Math.floor(ms / 60000)), d = Math.floor(t / 1440), h = Math.floor(t % 1440 / 60), m = t % 60;
  return (d ? `${d}d ` : '') + (d || h ? `${h}h ` : '') + `${m}m`;
}
function updateCountdowns() {
  const now = Date.now();
  for (const tf of TFS) { const t = `closes in ${fmtLeft(nextClose(tf, now) - now)}`; if (cdEls[tf].textContent !== t) cdEls[tf].textContent = t; }
}

/* ================= Tehran time (24h) ================= */
const tehranFmt = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Tehran', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' });
function tehranParts(t) {
  const o = {};
  for (const p of tehranFmt.formatToParts(new Date(t))) o[p.type] = p.value;
  if (o.hour === '24') o.hour = '00';
  return o;
}

/* ================= clock / scheduling ================= */
const updateLast = () => { const p = tehranParts(Date.now()); lastUpdate.textContent = `${p.hour}:${p.minute}:${p.second}`; };
function scheduleMarket() {
  const now = Date.now(), next = Math.ceil((now + 1) / MARKET_INTERVAL) * MARKET_INTERVAL;
  setTimeout(() => { if (!document.hidden) refreshMarket(true); scheduleMarket(); }, next - now + 1500);
}

function init() {
  if (cache.v === 2 && cache.sym && cache.m) {
    state.symbols = new Set(cache.sym); state.symT = cache.symT || 0;
    for (const s of cache.sym) { const m = cache.m[s]; if (m) state.coins.set(s, newCoin(s, m[0], m[1])); }
    if (state.coins.size) { render(); setStatus('Showing saved data • updating…'); }
  } else setStatus('Loading Binance USDT perpetuals…');
  updateCountdowns();
  refreshMarket(true);
  scheduleMarket();
  setInterval(() => { if (!document.hidden) updateCountdowns(); }, 1000);
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) return;
    if (Date.now() - state.lastFull >= MARKET_INTERVAL) refreshMarket(true);
  });
}
init();
