/* Container Market dashboards: live data loader (shared by v1 and v2, also run by node to refresh snapshot.json).
   Reads public.container_dashboard() with the project's PUBLIC publishable key (anon role; read-only RPC). No secrets here. */
(function (root) {
  const CFG = { url: 'https://zxitfgyojzhtlhuatxrj.supabase.co', key: 'sb_publishable_cd0HdF8gjK9wVdKO4CWd9A_EXqqygTJ', timeoutMs: 9000 };
  const norm = s => String(s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
  function parsePrice(raw) {
    raw = (raw || '').trim(); if (!raw) return [null, null, false];
    const m = raw.match(/^\s*\$?([\d,]+(?:\.\d+)?)\s*(.*)$/); if (!m) return [null, raw, false];
    const price = parseFloat(m[1].replace(/,/g, '')); let note = m[2].trim().replace(/^[() ]+|[() ]+$/g, '').trim() || null;
    const eod = !!(note && /\bEOD\b/i.test(note)); if (note && eod && norm(note) === 'eod') note = null;
    return [price, note, eod];
  }
  function mergeGroup(items, members) {
    const q = members.reduce((a, m) => a + items[m].q, 0), cap = members.some(m => items[m].cap);
    const st = members.filter(m => items[m].q > 0 && items[m].p != null).map(m => Object.assign({ tid: m }, items[m]));
    let best = null, wavg = null, prices = [];
    if (st.length) { best = st.reduce((a, b) => (b.p < a.p ? b : a)); const wq = st.reduce((a, x) => a + x.q, 0);
      wavg = st.reduce((a, x) => a + x.p * x.q, 0) / wq; prices = [...new Set(st.map(x => x.p))]; }
    return { q, cap, p: best ? best.p : null, p_from: best ? best.tid : null, eod: !!(best && best.eod),
      any_eod: members.some(m => items[m].q > 0 && items[m].eod), wavg: wavg != null ? Math.round(wavg * 100) / 100 : null,
      spread: prices.length > 1, n_stocked: st.length, n_members: members.length };
  }
  function buildSnap(L, base, source) {
    const types = base.types, groups = base.groups, issues = [];
    const dk = {}; L.depots.forEach(d => (dk[d.depot_key] = d));
    const meta = {}; (base.depots || []).forEach(d => (meta[norm(d.depot) + '|' + norm(d.sheet_city)] = d));
    const rowsBy = new Map();
    L.rows.forEach(([country, loc, dep, key, tid, qty, praw]) => {
      const k = country + '|' + loc + '|' + dep; if (!rowsBy.has(k)) rowsBy.set(k, { country, loc, dep, key, items: {} });
      rowsBy.get(k).items[tid] = [qty, praw];
    });
    const depots = [], used = new Set();
    for (const r of rowsBy.values()) {
      const items = {};
      types.forEach(t => {
        const [qq, praw] = r.items[t.id] || [0, null]; const q = qq ? Math.trunc(Number(qq)) : 0;
        const [p, note, eod] = parsePrice(praw);
        if (q > 0 && p == null) issues.push(`${r.dep} (${r.loc}) ${t.label}: qty ${q} but no price`);
        if (q === 0 && p != null) issues.push(`${r.dep} (${r.loc}) ${t.label}: price ${p.toFixed(0)} but qty 0`);
        if (q > 20) issues.push(`${r.dep} ${t.label}: qty ${q} > 20 (cap assumption broken)`);
        items[t.id] = { q, p, eod, note, raw: praw || null, cap: q === 20 };
      });
      const c = r.key && dk[r.key];
      if (!c) { issues.push(`No coordinates row for ${r.dep} (${r.loc})`); continue; }
      used.add(c.depot_key);
      const m = meta[norm(r.dep) + '|' + norm(r.loc)] || {};
      let st = c.state, sn = c.state_note || null;
      if (/\/|unknown/i.test(st || '')) { sn = sn || `State unconfirmed in coordinates file (${st}); pin is a city centroid, counted under MO for shading`; st = 'MO'; }
      const flag = m.flag !== undefined ? m.flag : (c.method === 'city centroid' ? 'centroid' : (c.confidence === 'low' || /unverified/.test(c.method || '') ? 'low' : null));
      const g = {}; groups.forEach(gr => (g[gr.id] = mergeGroup(items, gr.members)));
      depots.push({ id: depots.length, depot: r.dep, sheet_city: r.loc, country: c.country || r.country, state: st, state_note: sn,
        lat: c.lat, lng: c.lng, method: c.method, confidence: c.confidence, flag, address: m.address || null, coord_notes: m.coord_notes || null, items, g });
    }
    const H = L.history, by = {}, nat = {};
    H.by_state.forEach(([s, cat, mo, med, n]) => { ((by[s] = by[s] || {})[cat] = by[s][cat] || []).push([mo, med, n]); });
    H.national.forEach(([cat, mo, med, n]) => (nat[cat] = nat[cat] || []).push([mo, med, n]));
    const cap = L.capture || {}, sd = cap.sheet_date || H.last, [y, mo, d] = sd.split('-').map(Number);
    return { generated: new Date().toISOString(), data_source: source, fetched_at: new Date().toISOString(),
      report_date: `${mo}/${d}/${y}`, report_date_iso: sd, sheet_updated: cap.last_updated_text || '', source_csv: `supabase container capture #${cap.capture_id}`,
      pipeline: L.status, types, groups, hist_to_group: base.hist_to_group, depots,
      history: { tab: 'container.history (Supabase)', rows: H.rows, first: H.first, last: H.last, report_dates: H.report_dates,
        cats: base.history.cats, unmapped_labels: [], by_state: by, national: nat, dropped_low_prices: H.dropped_low_prices, state_remaps: {} },
      issues, coords_not_in_sheet: L.depots.filter(x => !used.has(x.depot_key)).map(x => `${x.depot} (${x.city})`) };
  }
  async function fetchLive(fetchImpl) {
    const f = fetchImpl || fetch, ac = typeof AbortController !== 'undefined' ? new AbortController() : null;
    const t = ac && setTimeout(() => ac.abort(), CFG.timeoutMs);
    try {
      const r = await f(CFG.url + '/rest/v1/rpc/container_dashboard', { method: 'POST', signal: ac && ac.signal,
        headers: { apikey: CFG.key, 'Content-Type': 'application/json' }, body: '{}' });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const j = await r.json(); if (!j || !j.rows || !j.rows.length) throw new Error('empty payload'); return j;
    } finally { t && clearTimeout(t); }
  }
  /* Browser: pick live or embedded snapshot, then run the app script (kept as text/plain #app) in global scope. */
  async function boot(embedded) {
    let snap, err = null;
    try { snap = buildSnap(await fetchLive(), embedded, 'live'); } catch (e) { err = e; snap = Object.assign({}, embedded, { data_source: 'cached' }); }
    root.__SNAP = snap; root.__liveError = err ? String(err && err.message || err) : null;
    const s = document.createElement('script'); s.textContent = 'const SNAP = window.__SNAP;\n' + document.getElementById('app').textContent;
    document.body.appendChild(s);
    const el = document.getElementById('stamp');
    if (el) { const live = snap.data_source === 'live', b = document.createElement('span');
      b.id = 'srcBadge'; b.className = 'src-badge ' + (live ? 'live' : 'cached');
      b.textContent = live ? 'LIVE' : 'CACHED';
      b.title = live ? 'Fetched from Supabase just now (' + new Date().toLocaleString() + ')' : 'Live fetch failed (' + root.__liveError + '); showing the snapshot saved in this file';
      el.insertAdjacentText('beforeend', ' '); el.appendChild(b); }
  }
  const api = { CFG, buildSnap, fetchLive, boot, parsePrice };
  if (typeof module !== 'undefined' && module.exports) module.exports = api; else root.ContainerLive = api;
})(typeof window !== 'undefined' ? window : globalThis);
