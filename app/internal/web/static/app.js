'use strict';
/* TrafficLight-UI – Frontend. Alle Auswertungen rechnet der Server; der Browser
   erhält fertige Aggregate per SSE (EventSource) und rendert nur sichtbare Tabellenzeilen. */

const $ = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
let storage;
try { storage = window.localStorage; } catch {}
let lang = I18n.detect(storage, navigator.languages?.length ? navigator.languages : [navigator.language]);
const t = (key, values) => I18n.translate(lang, key, values);
const ht = (key, values = {}) => t(key, Object.fromEntries(Object.entries(values).map(([k, v]) => [k, esc(v)])));
const fixed = (n, digits) => Number(n).toLocaleString(lang, { minimumFractionDigits: digits, maximumFractionDigits: digits, useGrouping: false });
const fmtN = n => Number(n || 0).toLocaleString(lang);
const fmtMs = ms => ms >= 60000 ? fixed(ms / 60000, 1) + ' min' : ms >= 1000 ? fixed(ms / 1000, 2) + ' s' : fixed(ms, ms < 10 ? 2 : 0) + ' ms';
const fmtB = b => b >= 2 ** 30 ? fixed(b / 2 ** 30, 2) + ' GB' : b >= 2 ** 20 ? fixed(b / 2 ** 20, 1) + ' MB' : b >= 1024 ? fixed(b / 1024, 1) + ' KB' : fmtN(b) + ' B';
const fmtT = time => time ? new Date(time).toLocaleString(lang) : '-';
const fmtTime = time => new Date(time).toLocaleTimeString(lang);
const ccName = cc => cc === 'LAN' ? t('private') : !cc || cc === '??' ? t('unknown') : (() => { try { return new Intl.DisplayNames([lang], { type: 'region' }).of(cc); } catch { return cc; } })();
const flag = cc => /^[A-Z]{2}$/.test(cc || '') ? String.fromCodePoint(...[...cc].map(c => 0x1F1A5 + c.charCodeAt(0))) : (cc === 'LAN' ? '🏠' : '🏳️');
const country = cc => `${flag(cc)} ${ccName(cc)}`;
const COLORS = ['#8492ad', '#3ecf8e', '#4f8cff', '#f5a524', '#f04f5f']; // sonstige, 2xx, 3xx, 4xx, 5xx
const CLASS_LABEL = ['1xx/–', '2xx', '3xx', '4xx', '5xx'];
const PALETTE = ['#4f8cff', '#3ecf8e', '#f5a524', '#f04f5f', '#a970ff', '#2ec5d3', '#ff7ab6', '#c3d14a', '#8492ad', '#ff9a3c'];
const ROW_H = 24, MAX_ROWS = 20000, PAGE = 300;

Chart.defaults.color = '#8492ad';
Chart.defaults.borderColor = '#26304a';
Chart.defaults.font.size = 11;
Chart.defaults.animation = false;

// ---------- Zustand (steht vollständig in der URL, damit Ansichten teilbar sind) ----------
const DEFAULTS = { range: 3600, cls: '2345', noint: true, nobots: false };
const S = { range: 3600, zoom: null, q: '', host: '', router: '', method: '', pod: '', cls: '2345', mindur: '', noint: true, nobots: false,
  extra: {}, x: [], xf: '', sort: 't', dir: 'desc' };
const EXTRA = { site: 'Host', client: 'IP', route: 'path', ua: 'UA', status: 'status', service: 'service', country: 'country' };
let SNAP = null, STATUS = null, paused = false, es = null, tl = null;
let liveState = { cls: '', key: 'connecting' }, statusError = null, modalEntry = null, refreshingLanguage = false;
const charts = {};

function loadHash() {
  const p = new URLSearchParams(location.hash.slice(1));
  if (p.has('range')) S.range = +p.get('range') || DEFAULTS.range;
  if (p.has('zfrom') && p.has('zto')) S.zoom = { from: +p.get('zfrom'), to: +p.get('zto') };
  for (const k of ['q', 'host', 'router', 'method', 'pod', 'mindur', 'xf', 'sort', 'dir']) if (p.has(k)) S[k] = p.get(k);
  if (p.has('cls')) S.cls = p.get('cls');
  if (p.has('noint')) S.noint = p.get('noint') === '1';
  if (p.has('nobots')) S.nobots = p.get('nobots') === '1';
  for (const k in EXTRA) if (p.has(k)) S.extra[k] = p.get(k);
  S.x = p.getAll('x').map(v => { const i = v.indexOf('='); return [v.slice(0, i), v.slice(i + 1)]; }).filter(x => x[0]);
}
// params baut die Abfrage für API und URL. forApi ergänzt Zeitzone und Standardwerte.
function params(forApi) {
  const p = new URLSearchParams();
  p.set('range', S.range);
  if (S.zoom) { p.set('zfrom', S.zoom.from); p.set('zto', S.zoom.to); }
  for (const k of ['q', 'host', 'router', 'method', 'pod', 'mindur', 'xf']) if (S[k]) p.set(k, S[k]);
  if (S.cls !== DEFAULTS.cls) p.set('cls', S.cls);
  if (forApi ? S.noint : !S.noint) p.set('noint', S.noint ? '1' : '0');
  if (S.nobots) p.set('nobots', '1');
  for (const k in EXTRA) if (S.extra[k] !== undefined && S.extra[k] !== '') p.set(k, S.extra[k]);
  for (const [k, v] of S.x) p.append('x', k + '=' + v);
  if (forApi) p.set('tz', new Date().getTimezoneOffset());
  else { if (S.sort !== 't') p.set('sort', S.sort); if (S.dir !== 'desc') p.set('dir', S.dir); }
  return p;
}
function syncInputs() {
  $('range').value = S.range;
  $('fText').value = S.q; $('fSite').value = S.host; $('fMinDur').value = S.mindur;
  setSelect($('fRouter'), S.router); setSelect($('fMethod'), S.method); setSelect($('fPod'), S.pod);
  document.querySelectorAll('.fSt').forEach(c => c.checked = S.cls.includes(c.value));
  $('fNoInt').checked = S.noint; $('fNoBots').checked = S.nobots;
}
function setSelect(sel, v) {
  if (v && ![...sel.options].some(o => o.value === v)) sel.add(new Option(v, v));
  sel.value = v;
}

// ---------- Verbindung (SSE) ----------
let applyTimer = null;
function apply(delay = 0) {
  clearTimeout(applyTimer);
  applyTimer = setTimeout(() => {
    history.replaceState(null, '', '#' + params(false));
    renderChips();
    $('btnExport').href = 'api/export.csv?' + params(true);
    connect();
    table.reload();
  }, delay);
}
function setLive(cls, key) { liveState = { cls, key }; $('live').querySelector('.dot').className = 'dot ' + cls; $('liveText').textContent = t(key); }
function setStatus(msg, err) { statusError = err ? msg : null; $('status').classList.toggle('st5', !!err); $('status').textContent = msg; }

function connect() {
  if (es) es.close();
  setLive('', 'connecting');
  es = new EventSource('api/stream?' + params(true));
  es.onopen = () => setLive(paused ? 'warn' : 'ok', paused ? 'paused' : 'live');
  es.onerror = () => setLive('err', 'reconnecting');
  es.addEventListener('snapshot', e => { if (!paused) { SNAP = JSON.parse(e.data); scheduleRender(); } });
  es.addEventListener('rows', e => { if (!paused) { const d = JSON.parse(e.data); table.live(d.rows, d.gap); } });
  es.addEventListener('sys', e => { sys.add(JSON.parse(e.data).logs); });
  es.addEventListener('status', e => { STATUS = JSON.parse(e.data); renderStatus(); });
  es.addEventListener('fail', e => setStatus(JSON.parse(e.data).error, true));
}
let renderQueued = false;
function scheduleRender() {
  if (renderQueued || document.hidden) return;
  renderQueued = true;
  requestAnimationFrame(() => { renderQueued = false; if (SNAP) render(SNAP); });
}
// Jeder offene Stream belegt eine der wenigen HTTP/1.1-Verbindungen des Browsers zum
// Server (max. 6 je Host, über alle Tabs). Tabs im Hintergrund geben ihre Verbindung
// deshalb nach kurzer Zeit frei und verbinden sich beim Zurückkehren neu.
let hiddenTimer = null;
function disconnect(text) { if (es) { es.close(); es = null; setLive('warn', text); } }
document.addEventListener('visibilitychange', () => {
  clearTimeout(hiddenTimer);
  if (document.hidden) hiddenTimer = setTimeout(() => disconnect('background'), 30000);
  else if (!es && !paused) apply();
  else if (SNAP) scheduleRender();
});
// Beim Verlassen der Seite sofort trennen (sonst hält der Zurück-Cache des Browsers die Verbindung offen).
window.addEventListener('pagehide', () => disconnect('disconnected'));
window.addEventListener('pageshow', e => { if (e.persisted && !paused) apply(); });

async function api(path) {
  const r = await fetch(path);
  const j = await r.json();
  if (!r.ok) throw new Error(j.error || r.statusText);
  return j;
}

// ---------- Filter ----------
function renderChips() {
  let h = Object.keys(EXTRA).filter(k => S.extra[k] !== undefined && S.extra[k] !== '')
    .map(k => `<span class="chip">${esc(['route', 'country', 'status', 'service'].includes(k) ? t(EXTRA[k]) : EXTRA[k])}: ${esc(k === 'country' ? country(S.extra[k]) : String(S.extra[k]).slice(0, 50))} <b data-k="${k}">✕</b></span>`).join('');
  S.x.forEach(([k, v], i) => h += `<span class="chip">${esc(k)}: ${esc(v.slice(0, 50))} <b data-x="${i}">✕</b></span>`);
  if (S.zoom) h += `<span class="chip">${esc(t('time'))}: ${esc(fmtT(S.zoom.from))} – ${esc(fmtTime(S.zoom.to))} <b data-k="zoom">✕</b></span>`;
  $('chips').innerHTML = h;
}
$('chips').addEventListener('click', e => {
  const b = e.target.closest('b'); if (!b) return;
  if (b.dataset.x !== undefined) S.x.splice(+b.dataset.x, 1);
  else if (b.dataset.k === 'zoom') S.zoom = null;
  else delete S.extra[b.dataset.k];
  apply();
});
function addFilter(k, v) { S.extra[k] = v; apply(); }
function readInputs() {
  S.q = $('fText').value.trim(); S.host = $('fSite').value.trim().toLowerCase(); S.mindur = $('fMinDur').value;
  S.router = $('fRouter').value; S.method = $('fMethod').value; S.pod = $('fPod').value;
  S.cls = [...document.querySelectorAll('.fSt:checked')].map(c => c.value).join('');
  S.noint = $('fNoInt').checked; S.nobots = $('fNoBots').checked;
}
document.querySelectorAll('#filters input, #filters select').forEach(el => {
  const typing = el.type === 'text' || el.type === 'number';
  el.addEventListener(typing ? 'input' : 'change', () => { readInputs(); apply(typing ? 350 : 0); });
});
$('range').addEventListener('change', () => { S.range = +$('range').value; S.zoom = null; apply(); });
$('btnReset').addEventListener('click', () => {
  Object.assign(S, { zoom: null, q: '', host: '', router: '', method: '', pod: '', cls: DEFAULTS.cls, mindur: '', noint: true, nobots: false, extra: {}, x: [] });
  syncInputs(); apply();
});
$('btnPause').addEventListener('click', () => {
  paused = !paused;
  $('btnPause').textContent = t(paused ? 'resume' : 'pause');
  $('btnPause').classList.toggle('on', paused);
  if (paused) setLive('warn', 'paused'); else apply();
});
function fillSelect(sel, values, label, cur) {
  const sig = lang + '\u0001' + values.join('\u0001');
  if (sel.dataset.sig === sig) return;
  sel.dataset.sig = sig;
  sel.innerHTML = `<option value="">${esc(label)}</option>` + values.map(v => `<option value="${esc(v)}">${esc(v)}</option>`).join('');
  setSelect(sel, cur);
}

// ---------- Zeitachsen ----------
function labels(se, span) {
  const out = new Array(se.n);
  for (let i = 0; i < se.n; i++) {
    const d = new Date(se.t0 + i * se.step);
    out[i] = se.step >= 86400000 ? d.toLocaleDateString(lang, { day: '2-digit', month: '2-digit' })
      : span > 86400000 ? d.toLocaleString(lang, { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' })
      : d.toLocaleTimeString(lang, se.step < 60000 ? undefined : { hour: '2-digit', minute: '2-digit' });
  }
  return out;
}
const XT = { maxRotation: 0, autoSkipPadding: 18 };
const stepLabel = s => s >= 86400000 ? fmtN(s / 86400000) + ' d' : s >= 3600000 ? fmtN(s / 3600000) + ' h' : s >= 60000 ? fmtN(s / 60000) + ' min' : fmtN(s / 1000) + ' s';

function drawChart(id, cfg) {
  const ch = charts[id];
  if (!ch) {
    cfg.options = Object.assign({ responsive: true, maintainAspectRatio: false }, cfg.options);
    charts[id] = new Chart($(id), cfg);
    return;
  }
  ch.data.labels = cfg.data.labels;
  ch.options.locale = lang;
  cfg.data.datasets.forEach((d, i) => { if (ch.data.datasets[i]) Object.assign(ch.data.datasets[i], d); else ch.data.datasets[i] = d; });
  ch.data.datasets.length = cfg.data.datasets.length;
  if (cfg.options?.scales?.y2 && ch.options.scales.y2) ch.options.scales.y2.display = cfg.options.scales.y2.display;
  ch.update('none');
}

// ---------- Zeitleiste (äußerer Zeitraum; Ziehen wählt einen Ausschnitt) ----------
const tlSelPlugin = {
  id: 'tlSel',
  beforeDatasetsDraw(chart) {
    if (!tl || !tl.n) return;
    const { ctx, chartArea: a, scales: { x } } = chart;
    let from = null, to = null;
    if (tl.drag) { from = Math.min(tl.drag.a, tl.drag.b); to = Math.max(tl.drag.a, tl.drag.b) + 1; }
    else if (S.zoom) { from = (S.zoom.from - tl.t0) / tl.step; to = (S.zoom.to - tl.t0) / tl.step; }
    if (from === null) return;
    const x0 = Math.max(a.left, x.getPixelForValue(from - 0.5)), x1 = Math.min(a.right, x.getPixelForValue(to - 0.5));
    ctx.save();
    ctx.fillStyle = 'rgba(0,0,0,.45)';
    ctx.fillRect(a.left, a.top, Math.max(0, x0 - a.left), a.height);
    ctx.fillRect(x1, a.top, Math.max(0, a.right - x1), a.height);
    ctx.fillStyle = 'rgba(79,140,255,.12)'; ctx.fillRect(x0, a.top, Math.max(1, x1 - x0), a.height);
    ctx.strokeStyle = '#4f8cff'; ctx.lineWidth = 1; ctx.strokeRect(x0 + .5, a.top + .5, Math.max(1, x1 - x0 - 1), a.height - 1);
    ctx.restore();
  }
};
function renderTimeline(s) {
  const t = s.timeline, drag = tl?.drag;
  tl = { t0: t.t0, step: t.step, n: t.n, drag };
  const sysN = (t.sys || []).reduce((a, b) => a + b, 0);
  const datasets = [1, 2, 3, 4, 0].map(c => ({ type: 'bar', label: CLASS_LABEL[c], data: t.c[c], backgroundColor: COLORS[c], stack: 's', barPercentage: 1, categoryPercentage: .95, yAxisID: 'y' }));
  datasets.push({ type: 'line', label: 'Traefik ERR/WRN', data: sysN ? t.sys : [], borderColor: '#ff7ab6', backgroundColor: '#ff7ab6', borderWidth: 1, pointRadius: 0, yAxisID: 'y2', order: -1 });
  $('tlInfo').textContent = I18n.translate(lang, 'timelineHint', { from: fmtT(s.from), to: fmtT(Math.min(s.to, s.now)), step: stepLabel(t.step) });
  drawChart('cTimeline', {
    type: 'bar',
    data: { labels: labels(t, s.to - s.from), datasets },
    options: {
      events: ['mousemove', 'mouseout'],
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'right', labels: { boxWidth: 10 } } },
      scales: {
        x: { stacked: true, ticks: { maxRotation: 0, autoSkipPadding: 20 }, grid: { display: false } },
        y: { stacked: true, beginAtZero: true, ticks: { maxTicksLimit: 4 } },
        y2: { display: sysN > 0, position: 'right', beginAtZero: true, grid: { display: false }, ticks: { maxTicksLimit: 4, color: '#ff7ab6', precision: 0 } }
      }
    },
    plugins: [tlSelPlugin]
  });
}
(() => {
  const cv = $('cTimeline');
  const clamp = i => Math.max(0, Math.min(tl.n - 1, Math.round(i)));
  const idx = ev => {
    const ch = charts.cTimeline; if (!ch || !tl?.n) return null;
    const r = cv.getBoundingClientRect(), x = ev.clientX - r.left, a = ch.chartArea;
    if (x < a.left - 5 || x > a.right + 5) return null;
    return clamp(ch.scales.x.getValueForPixel(x));
  };
  cv.addEventListener('mousedown', ev => { const i = idx(ev); if (i !== null) { tl.drag = { a: i, b: i }; ev.preventDefault(); } });
  window.addEventListener('mousemove', ev => {
    if (!tl?.drag) return;
    const ch = charts.cTimeline, r = cv.getBoundingClientRect(), x = Math.max(ch.chartArea.left, Math.min(ch.chartArea.right, ev.clientX - r.left));
    const i = clamp(ch.scales.x.getValueForPixel(x));
    if (i !== tl.drag.b) { tl.drag.b = i; ch.draw(); }
  });
  window.addEventListener('mouseup', () => {
    if (!tl?.drag) return;
    const lo = Math.min(tl.drag.a, tl.drag.b), hi = Math.max(tl.drag.a, tl.drag.b);
    tl.drag = null;
    S.zoom = { from: tl.t0 + lo * tl.step, to: tl.t0 + (hi + 1) * tl.step };
    apply();
  });
  cv.addEventListener('dblclick', () => { S.zoom = null; apply(); });
})();

// ---------- Dashboard ----------
function render(s) {
  const k = s.kpi, se = s.series;
  renderTimeline(s);
  renderNotice(s);
  fillSelect($('fRouter'), s.facets.routers, t('allRouters'), S.router);
  fillSelect($('fMethod'), s.facets.methods, t('allMethods'), S.method);
  fillSelect($('fPod'), s.facets.pods, t('allPods'), S.pod);
  const sig = s.facets.sites.join('\u0001');
  if ($('dlSite').dataset.sig !== sig) { $('dlSite').dataset.sig = sig; $('dlSite').innerHTML = s.facets.sites.map(h => `<option value="${esc(h)}">`).join(''); }

  const spanS = Math.max(1, (k.tMax - k.tMin) / 1000), pc = n => k.count ? fixed(n / k.count * 100, 1) : 0;
  const clip = s.clipFrom > s.zfrom ? '≥ ' : '';
  const kpis = [
    [t('requests'), fmtN(k.count) + (k.count !== k.total ? ` <small>/ ${fmtN(k.total)}</small>` : '')],
    ['Req/min', k.count ? fixed(k.count / spanS * 60, 1) : '0'],
    ['4xx', `<span class="st4">${fmtN(k.s4)}</span> <small>${pc(k.s4)}%</small>`],
    ['5xx', `<span class="st5">${fmtN(k.s5)}</span> <small>${pc(k.s5)}%</small>`],
    ['p50', fmtMs(k.p50)], ['p95', fmtMs(k.p95)], ['p99', fmtMs(k.p99)],
    [t('traffic'), fmtB(k.bytes)],
    [t('clients'), clip + fmtN(k.clients)],
    [t('countries'), STATUS && !STATUS.info.geo ? `<small>${esc(t('off'))}</small>` : fmtN(k.countries)],
    [t('range'), k.count ? `<small>${esc(fmtT(k.tMin))}<br>${esc(fmtT(k.tMax))}</small>` : '-'],
  ];
  $('kpis').innerHTML = kpis.map(([l, v]) => `<div class="kpi"><div class="l">${esc(l)}</div><div class="v">${v}</div></div>`).join('');

  const lab = labels(se, s.zto - s.zfrom);
  drawChart('cTime', {
    type: 'bar',
    data: { labels: lab, datasets: [1, 2, 3, 4, 0].map(c => ({ label: CLASS_LABEL[c], data: se.c[c], backgroundColor: COLORS[c], stack: 's' })) },
    options: { scales: { x: { stacked: true, ticks: XT, grid: { display: false } }, y: { stacked: true, beginAtZero: true } }, onClick: (ev, els) => {
      if (!els.length || !SNAP) return;
      const z = SNAP.series, from = z.t0 + els[0].index * z.step;
      S.zoom = { from, to: from + z.step }; apply();
    } }
  });
  const nul = a => (a || []).map(v => v < 0 ? null : v);
  drawChart('cLat', {
    type: 'line',
    data: { labels: lab, datasets: [
      { label: 'p50', data: nul(se.p50), borderColor: '#3ecf8e', pointRadius: 0, spanGaps: true, borderWidth: 1.5 },
      { label: 'p95', data: nul(se.p95), borderColor: '#f5a524', pointRadius: 0, spanGaps: true, borderWidth: 1.5 },
      { label: 'p99', data: nul(se.p99), borderColor: '#f04f5f', pointRadius: 0, spanGaps: true, borderWidth: 1.5 }] },
    options: { scales: { x: { ticks: XT, grid: { display: false } }, y: { type: 'logarithmic', ticks: { callback: v => fmtMs(+v), maxTicksLimit: 7 } } }, plugins: { tooltip: { callbacks: { label: c => `${c.dataset.label}: ${fmtMs(c.raw)}` } } } }
  });
  drawChart('cBytes', {
    type: 'line',
    data: { labels: lab, datasets: [{ label: 'Bytes', data: se.bytes, borderColor: '#a970ff', backgroundColor: 'rgba(169,112,255,.15)', fill: true, pointRadius: 0, borderWidth: 1.5 }] },
    options: { plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => fmtB(c.raw) } } }, scales: { x: { ticks: XT, grid: { display: false } }, y: { beginAtZero: true, ticks: { callback: v => fmtB(v) } } } }
  });
  const st = s.top.status || [];
  drawChart('cStatus', {
    type: 'doughnut',
    data: { labels: st.map(x => String(x.s || '–')), datasets: [{ data: st.map(x => x.n), backgroundColor: st.map(x => COLORS[x.s >= 200 && x.s < 600 ? Math.floor(x.s / 100) - 1 : 0]), borderColor: '#171e2e' }] },
    options: { plugins: { legend: { position: 'right' } }, onClick: (ev, els) => { if (els.length && SNAP) addFilter('status', SNAP.top.status[els[0].index].s); } }
  });
  doughnut('cMethod', s.top.method);
  doughnut('cProto', s.top.proto);
  drawChart('cHist', {
    type: 'bar',
    data: { labels: s.hist.edges.map(x => x === 0 ? '≥10s' : '<' + (x >= 1000 ? fmtN(x / 1000) + 's' : fmtN(x) + 'ms')), datasets: [{ data: s.hist.counts, backgroundColor: '#4f8cff' }] },
    options: { plugins: { legend: { display: false } } }
  });

  const errTotal = st.filter(x => x.s >= 400).reduce((a, x) => a + x.n, 0);
  topTable('tSite', s.top.site, t('hostNoWww'), k.count, { pick: r => ['site', r.k] });
  topTable('tService', s.top.service, t('serviceRouter'), k.count, { pick: r => ['service', r.k], fmt: r => `${esc(r.k)} <span class="mut">‹${esc(r.x)}›</span>` });
  topTable('tCountry', s.top.country, t('country'), k.count, { pick: r => ['country', r.k], fmt: r => esc(country(r.k)) });
  topTable('tPod', s.top.pod, 'Pod', k.count, { pick: r => ['pod', r.k] });
  topTable('tRoute', s.top.route, t('path'), s.scanned, { pick: r => ['route', r.k] });
  topTable('tClient', s.top.client, 'Client-IP', s.scanned, { pick: r => ['client', r.k], fmt: r => `${flag(r.x)} ${esc(r.k)}` });
  topTable('tUa', s.top.ua, 'User-Agent', s.scanned, { pick: r => ['ua', r.k] });
  topTable('tErr', s.top.err, t('statusPath'), s.clipFrom ? 0 : errTotal, { pick: r => ['err', r], fmt: r => `<span class="st${esc(String(r.s)[0])}">${esc(r.s)}</span> ${esc(r.k)}` });
  slowTable(s.top.slow || []);
  renderX(s);
  const clipText = s.clipFrom > s.zfrom ? t('since', { date: fmtT(s.clipFrom) }) : '';
  document.querySelectorAll('.clip').forEach(el => el.textContent = clipText);
  if (!refreshingLanguage || !statusError) setStatus(t('summary', { count: fmtN(k.count), mode: t(s.mode === 'detail' ? 'exact' : 'rollupMode'), ms: fixed(s.tookMs, 0), time: fmtTime(s.now) }));
}

function renderNotice(s) {
  const msgs = [];
  if (s.mode === 'rollup') msgs.push(t('rollupNotice', { date: `<b>${esc(fmtT(s.clipFrom))}</b>` }));
  if (s.mode === 'clipped') msgs.push(t('clippedNotice', { date: `<b>${esc(fmtT(s.clipFrom))}</b>` }));
  for (const p of STATUS?.pods || []) if (p.state === 'no-json') msgs.push(t('noJsonNotice', { pod: `<b>${esc(p.pod)}</b>`, code: '<code>--accesslog.format=json</code>' }));
  if (STATUS?.listErr) msgs.push(ht('podListError', { error: STATUS.listErr }));
  if (STATUS?.store.redisErr) msgs.push(`Redis: ${esc(STATUS.store.redisErr)}`);
  $('notice').hidden = !msgs.length;
  $('notice').innerHTML = msgs.map(m => `<div>⚠ ${m}</div>`).join('');
}

function doughnut(id, rows) {
  rows = (rows || []).slice(0, 10);
  drawChart(id, { type: 'doughnut', data: { labels: rows.map(r => r.k), datasets: [{ data: rows.map(r => r.n), backgroundColor: PALETTE, borderColor: '#171e2e' }] }, options: { plugins: { legend: { position: 'right' } } } });
}
const tops = {};
function topTable(id, rows, label, total, opt) {
  rows = rows || [];
  tops[id] = { rows, pick: opt.pick };
  const max = rows.length ? rows[0].n : 1, fmt = opt.fmt || (r => esc(r.k));
  $(id).innerHTML = rows.length ? `<table><tr><th>${esc(label)}</th><th class="n">${esc(t('count'))}</th>${total ? '<th class="n">%</th>' : ''}</tr>` +
    rows.map((r, i) => `<tr class="click" data-i="${i}"><td class="bar" title="${esc(r.k)}"><span class="b" style="width:${r.n / max * 100}%"></span><span class="t">${fmt(r)}</span></td><td class="n">${fmtN(r.n)}</td>${total ? `<td class="n">${fixed(r.n / total * 100, 1)}</td>` : ''}</tr>`).join('') + '</table>'
    : `<span class="mut">${esc(t('noData'))}</span>`;
}
function slowTable(rows) {
  tops.tSlow = { rows, pick: r => ['route', r.k] };
  $('tSlow').innerHTML = rows.length ? `<table><tr><th>${esc(t('path'))}</th><th class="n">Req</th><th class="n">Ø</th><th class="n">p95</th></tr>` +
    rows.map((r, i) => `<tr class="click" data-i="${i}"><td title="${esc(r.x + r.k)}"><span class="mut">${esc(r.x)}</span>${esc(r.k)}</td><td class="n">${fmtN(r.n)}</td><td class="n">${fmtMs(r.a || 0)}</td><td class="n">${fmtMs(r.p || 0)}</td></tr>`).join('') + '</table>'
    : `<span class="mut">${esc(t('noData'))}</span>`;
}
document.querySelector('main').addEventListener('click', e => {
  const tr = e.target.closest('tr.click'); if (!tr) return;
  const box = tr.closest('.scroll'), t = box && tops[box.id]; if (!t) return;
  const [k, v] = t.pick(t.rows[+tr.dataset.i]);
  if (k === 'noop') return;
  if (k === 'pod') { S.pod = v; syncInputs(); apply(); }
  else if (k === 'err') { S.extra.status = v.s; addFilter('route', v.k); }
  else if (k === 'x') { S.x = S.x.filter(x => x[0] !== v[0]); S.x.push(v); apply(); }
  else addFilter(k, v);
});

// ---------- Dynamische Felder ----------
function renderX(s) {
  const fields = (s.top.fields || []).map(f => f.k).sort();
  if (S.xf && !fields.includes(S.xf)) fields.push(S.xf);
  const sel = $('xf'), sig = lang + '\u0001' + fields.join('\u0001');
  if (sel.dataset.sig !== sig) {
    sel.dataset.sig = sig;
    sel.innerHTML = `<option value="">${esc(fields.length ? t('chooseField', { count: fmtN(fields.length) }) : t('noFields'))}</option>` + fields.map(f => `<option value="${esc(f)}">${esc(f)}</option>`).join('');
  }
  sel.value = S.xf;
  if (!S.xf) {
    const rows = s.top.fields || [];
    tops.tX = { rows, pick: () => ['noop'] };
    $('tX').innerHTML = rows.length ? `<table><tr><th>${esc(t('field'))}</th><th class="n">${esc(t('entriesWithValue'))}</th></tr>` + rows.map(r => `<tr class="click" data-f="${esc(r.k)}"><td>${esc(r.k)}</td><td class="n">${fmtN(r.n)}</td></tr>`).join('') + '</table>'
      : `<span class="mut">${t('fieldsEmpty', { code: '<code>--accesslog.fields.headers.names.Referer=keep</code>' })}</span>`;
    return;
  }
  topTable('tX', s.top.x, S.xf, s.scanned, { pick: r => ['x', [S.xf, r.k]] });
}
$('xf').addEventListener('change', () => { S.xf = $('xf').value; apply(); });
$('tX').addEventListener('click', e => { const tr = e.target.closest('tr[data-f]'); if (tr) { S.xf = tr.dataset.f; e.stopPropagation(); apply(); } });

// ---------- Request-Tabelle (virtuell: nur sichtbare Zeilen im DOM, Nachladen beim Scrollen) ----------
const table = (() => {
  const body = $('vtBody'), pad = $('vtPad'), rowsEl = $('vtRows');
  let rows = [], ids = new Set(), next = null, loading = false, gen = 0, clipFrom = 0, fresh = 0;
  const cols = () => [['t', t('time'), 0, 1], ['st', t('status'), 1, 1, 'status'], ['m', t('methodShort')], ['h', 'Host'], ['p', t('path')], ['d', t('duration'), 1, 1, 'dur'], ['s', t('size'), 1, 1, 'size'],
    ['c', t('client')], ['cc', t('country')], ['pr', 'Proto'], ['sv', t('service')], ['pod', 'Pod'], ...(S.xf ? [['x', S.xf]] : []), ['ua', 'User-Agent']];
  function head() {
    $('vt').classList.toggle('has-x', !!S.xf);
    $('vtHead').innerHTML = cols().map(([k, l, n, sortable, sk]) => {
      const key = sk || k, on = S.sort === key;
      return `<div class="${n ? 'n ' : ''}${sortable ? 'sort' : ''}" ${sortable ? `data-k="${key}"` : ''} title="${esc(l)}">${esc(l)}${on ? (S.dir === 'asc' ? ' ▲' : ' ▼') : ''}</div>`;
    }).join('');
  }
  const cell = (r, k) => k === 't' ? new Date(r.t).toLocaleTimeString(lang, { hour: 'numeric', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3 })
    : k === 'cc' ? `${flag(r.cc)} ${esc(r.cc || '')}` : k === 'd' ? fmtMs(r.d) : k === 's' ? fmtB(r.s) : k === 'pod' ? esc(r.pod.split('-').slice(-2).join('-')) : esc(r[k]);
  function draw() {
    const top = body.scrollTop, first = Math.max(0, Math.floor(top / ROW_H) - 8), last = Math.min(rows.length, first + Math.ceil(body.clientHeight / ROW_H) + 16);
    const c = cols();
    let h = '';
    for (let i = first; i < last; i++) {
      const r = rows[i];
      h += `<div class="vt-row${i < fresh ? ' new' : ''}" data-i="${i}">` + c.map(([k, , n]) =>
        `<div class="${n ? 'n' : ''}${k === 'st' ? ' st' + String(r.st)[0] : ''}" title="${esc(k === 't' ? fmtT(r.t) : k === 'cc' ? ccName(r.cc) : r[k])}">${cell(r, k)}</div>`).join('') + '</div>';
    }
    rowsEl.style.transform = `translateY(${first * ROW_H}px)`;
    rowsEl.innerHTML = h;
    fresh = 0;
    pad.style.height = rows.length * ROW_H + 'px';
    const more = next ? ' · ' + t('moreScroll') : '';
    $('reqInfo').textContent = t('loaded', { count: fmtN(rows.length) }) + more + (clipFrom ? ' · ' + t('rawSince', { date: fmtT(clipFrom) }) : '') + ' · ' + t('rowHint');
  }
  async function load(reset) {
    if (loading && !reset) return;
    const my = reset ? ++gen : gen;
    if (reset) { rows = []; ids = new Set(); next = null; body.scrollTop = 0; head(); }
    else if (!next || rows.length >= MAX_ROWS) return;
    loading = true;
    try {
      const p = params(true);
      p.set('sort', S.sort); p.set('dir', S.dir); p.set('limit', PAGE);
      if (next) p.set('cursor', next);
      const d = await api('api/rows?' + p);
      if (my !== gen) return;
      for (const r of d.rows) if (!ids.has(r.id)) { ids.add(r.id); rows.push(r); }
      next = d.next || null; clipFrom = d.clipFrom || 0;
      draw();
    } catch (e) { if (my === gen) setStatus(e.message, true); }
    finally { if (my === gen) loading = false; }
  }
  let raf = 0;
  body.addEventListener('scroll', () => {
    $('vtHead').scrollLeft = body.scrollLeft;
    if (!raf) raf = requestAnimationFrame(() => { raf = 0; draw(); });
    if (!refreshingLanguage && body.scrollTop + body.clientHeight > rows.length * ROW_H - 40 * ROW_H) load(false);
  });
  $('vtHead').addEventListener('click', e => {
    const k = e.target.closest('.sort')?.dataset.k; if (!k) return;
    if (S.sort === k) S.dir = S.dir === 'asc' ? 'desc' : 'asc'; else { S.sort = k; S.dir = 'desc'; }
    history.replaceState(null, '', '#' + params(false));
    load(true);
  });
  rowsEl.addEventListener('click', e => { const el = e.target.closest('.vt-row'); if (el) showEntry(rows[+el.dataset.i]); });
  return {
    reload: () => load(true),
    refresh: () => { head(); draw(); },
    // live: neue Zeilen (neueste zuerst) oben einfügen, ohne die Leseposition zu verschieben.
    live(add, gap) {
      if (S.sort !== 't' || S.dir !== 'desc' || loading) return;
      if (gap && body.scrollTop === 0) return load(true);
      add = add.filter(r => !ids.has(r.id));
      if (!add.length) return;
      for (const r of add) ids.add(r.id);
      // Zeilen kommen nicht streng zeitlich sortiert an (mehrere Pods) -> einsortieren.
      rows = add.concat(rows);
      const n = Math.min(rows.length, add.length + 200), headPart = rows.slice(0, n).sort((a, b) => b.t - a.t);
      for (let i = 0; i < n; i++) rows[i] = headPart[i];
      if (rows.length > MAX_ROWS) {
        for (const r of rows.splice(MAX_ROWS)) ids.delete(r.id);
        const l = rows[rows.length - 1], [hh, ii] = l.id.split('-');
        next = `${l.t}:${hh}:${ii}`;
      }
      if (body.scrollTop > 0) body.scrollTop += add.length * ROW_H; else fresh = add.length;
      draw();
    },
  };
})();

async function showEntry(r) {
  try {
    modalEntry = await api('api/entry?id=' + encodeURIComponent(r.id));
    renderEntry();
    $('modal').hidden = false;
  } catch (e) { setStatus(e.message, true); }
}
function renderEntry() {
  const d = modalEntry, dyn = d.dynamic.filter(k => !k.startsWith('_'));
  const val = v => typeof v === 'object' ? JSON.stringify(v) : String(v);
  $('modalBody').innerHTML = `<h3>${esc(t('dynamicCount', { count: fmtN(dyn.length) }))}</h3>` +
    (dyn.length ? '<table>' + dyn.map(k => `<tr><td>${esc(k)}</td><td>${esc(val(d.entry[k]))}</td></tr>`).join('') + '</table>' : `<p class="mut">${esc(t('none'))}</p>`) +
    `<h3>${ht('entryTitle', { pod: d.entry._pod, country: country(d.entry._cc), date: fmtT(d.entry._t) })}</h3><pre>${esc(JSON.stringify(d.entry, null, 2))}</pre>`;
}
const closeModal = () => { $('modal').hidden = true; };
$('modal').addEventListener('click', e => { if (e.target === $('modal')) closeModal(); });
$('modalClose').addEventListener('click', closeModal);
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

// ---------- Systemlogs ----------
const sys = (() => {
  let logs = [], level = '', seen = new Set();
  function draw() {
    const counts = new Map();
    for (const e of logs) counts.set(e.level || '?', (counts.get(e.level || '?') || 0) + 1);
    $('lvTabs').innerHTML = `<button data-l="" class="${level === '' ? 'on' : ''}">${esc(t('all'))} (${fmtN(logs.length)})</button>` +
      [...counts].sort((a, b) => b[1] - a[1]).map(([l, c]) => `<button data-l="${esc(l)}" class="lv${esc(l)} ${level === l ? 'on' : ''}">${esc(l)} (${fmtN(c)})</button>`).join('');
    const rows = logs.filter(e => !level || (e.level || '?') === level).slice(0, 500);
    $('tSys').innerHTML = rows.length ? `<table><tr><th>${esc(t('time'))}</th><th>Level</th><th>Pod</th><th>${esc(t('message'))}</th></tr>` +
      rows.map(e => `<tr><td>${esc(fmtT(e.t))}</td><td class="lv${esc(e.level)}">${esc(e.level)}</td><td class="mut">${esc(e.pod)}</td><td class="wrap">${esc(e.msg)}</td></tr>`).join('') + '</table>'
      : `<span class="mut">${t('sysEmpty', { code: '<code>--log.format=json</code>' })}</span>`;
  }
  $('lvTabs').addEventListener('click', e => { const b = e.target.closest('button'); if (b) { level = b.dataset.l; draw(); } });
  return {
    refresh: draw,
    add(list) { // neueste zuerst
      const fresh = list.filter(e => !seen.has(e.seq));
      fresh.forEach(e => seen.add(e.seq));
      logs = fresh.concat(logs).sort((a, b) => b.seq - a.seq).slice(0, 5000);
      draw();
    },
    async init() { try { this.add((await api('api/sys?limit=2000')).logs || []); } catch (e) { setStatus(e.message, true); } },
  };
})();

// ---------- Pods & Speicher ----------
const STATE = { streaming: 'stateStreaming', waiting: 'stateWaiting', 'no-json': 'stateNoJson', error: 'stateError', gone: 'stateGone' };
function renderStatus() {
  const s = STATUS, st = s.store, active = s.pods.filter(p => p.state !== 'gone'), ok = active.filter(p => p.state === 'streaming').length;
  const cls = s.listErr || st.redisErr ? 'err' : ok === active.length && ok > 0 ? 'ok' : ok > 0 ? 'warn' : 'err';
  $('pods').innerHTML = `<i class="dot ${cls}"></i>${fmtN(ok)}/${fmtN(active.length)} Pods`;
  $('podInfo').textContent = t('podInfo', { namespaces: s.info.namespaces.join(', '), selector: s.info.selector, count: fmtN(s.clients) });
  $('tPods').innerHTML = s.pods.length ? `<table><tr><th>Pod</th><th>Container</th><th>Node</th><th>${esc(t('state'))}</th><th>Format</th><th class="n">${esc(t('requests'))}</th><th class="n">${esc(t('systemLog'))}</th><th class="n">${esc(t('skipped'))}</th><th class="n">${esc(t('restarts'))}</th><th>${esc(t('lastLine'))}</th><th>${esc(t('note'))}</th></tr>` +
    s.pods.map(p => `<tr><td>${esc(p.pod)}</td><td>${esc(p.container)}</td><td>${esc(p.node)}</td><td class="state-${esc(p.state)}">● ${esc(STATE[p.state] ? t(STATE[p.state]) : p.state)}</td><td>${esc(p.format)}</td><td class="n">${fmtN(p.access)}</td><td class="n">${fmtN(p.system)}</td><td class="n">${fmtN(p.skipped)}</td><td class="n">${fmtN(p.restarts)}</td><td>${p.lastLine ? esc(fmtT(p.lastLine)) : '-'}</td><td class="wrap mut">${esc(p.error || '')}</td></tr>`).join('') + '</table>'
    : `<span class="mut">${ht('noPods', { namespaces: s.info.namespaces.join(', '), selector: s.info.selector })}</span>`;
  const pct = st.redisMax ? st.redisUsed / st.redisMax * 100 : 0;
  const items = [
    [t('storedRequests'), fmtN(st.stored)],
    [t('dataSince'), esc(st.oldest ? fmtT(st.oldest) : '-')],
    [t('rawDataSince'), esc(st.rawFrom && st.rawFrom < s.now ? fmtT(st.rawFrom) : '-')],
    [t('retention'), esc(t('days', { n: fmtN(st.retentionDays) }) + (st.rawRetentionDays !== st.retentionDays ? ` (${t('detailsDays', { days: t('days', { n: fmtN(st.rawRetentionDays) }) })})` : ''))],
    [t('redisMemory'), fmtB(st.redisUsed) + (st.redisMax ? ` / ${fmtB(st.redisMax)}<div class="meter"><i class="${pct > 80 ? 'hi' : ''}" style="width:${Math.min(100, pct)}%"></i></div>` : '')],
    [t('detailCache'), `${fmtN(st.cachedEntries)} / ${fmtN(st.detailMax)}`],
    [t('queue'), fmtN(st.queue)],
    [t('pruned'), esc(t('hoursDetails', { hours: fmtN(st.prunedHours) }))],
  ];
  $('storeInfo').innerHTML = items.map(([l, v]) => `<div><div class="l">${esc(l)}</div><div class="v">${v}</div></div>`).join('');
  $('ver').textContent = 'TrafficLight-UI ' + s.info.version;
  const max = st.retentionDays * 86400;
  [...$('range').options].forEach(o => { o.disabled = +o.value > max; });
  if (SNAP) renderNotice(SNAP);
}
$('pods').addEventListener('click', () => $('podCard').scrollIntoView({ behavior: 'smooth' }));

function translatePage() {
  document.documentElement.lang = lang;
  document.querySelectorAll('[data-i18n]').forEach(el => { el.textContent = t(el.dataset.i18n); });
  document.querySelectorAll('[data-i18n-title]').forEach(el => { el.title = t(el.dataset.i18nTitle); });
  document.querySelectorAll('[data-i18n-placeholder]').forEach(el => { el.placeholder = t(el.dataset.i18nPlaceholder); });
  document.querySelectorAll('[data-i18n-days]').forEach(el => { el.textContent = t('days', { n: fmtN(el.dataset.i18nDays) }); });
  $('language').value = lang;
  $('btnPause').textContent = t(paused ? 'resume' : 'pause');
  setLive(liveState.cls, liveState.key);
  Chart.defaults.locale = lang;
}
$('language').addEventListener('change', () => {
  const selected = I18n.supported($('language').value);
  if (!selected || selected === lang) return;
  const scroll = [...document.querySelectorAll('.scroll, .vt-body, #modalBox')].map(el => [el, el.scrollTop, el.scrollLeft]);
  const x = window.scrollX, y = window.scrollY;
  lang = selected;
  I18n.save(storage, lang);
  refreshingLanguage = true;
  translatePage();
  renderChips();
  if (SNAP) render(SNAP);
  if (STATUS) renderStatus();
  table.refresh();
  sys.refresh();
  if (!$('modal').hidden && modalEntry) renderEntry();
  scroll.forEach(([el, top, left]) => { el.scrollTop = top; el.scrollLeft = left; });
  window.scrollTo(x, y);
  requestAnimationFrame(() => { refreshingLanguage = false; });
});

// ---------- Start ----------
translatePage();
loadHash();
syncInputs();
sys.init();
apply();
