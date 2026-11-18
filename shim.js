/* Uniforma — capa de datos de la app.
   Reemplaza lo que en Claude hacían "db" (guardado), "mcp" (planillas de Google) y "downloads".
   Todo se guarda primero en el celular (funciona sin internet) y se manda a Google cuando hay señal. */
(function () {
  const K_CFG = "uniforma-conexion", K_DB = "uni-db", K_OUT = "uni-outbox", K_REPO = "uni-repo-pend", K_ESC = "uni-escribir-pend", K_FALLO = "uni-fallos";
  const ls = {
    get(k, d) { try { const v = localStorage.getItem(k); return v ? JSON.parse(v) : d; } catch (e) { return d; } },
    set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} }
  };
  const cfg = () => ls.get(K_CFG, null);
  window.uniformaConexion = {
    get: cfg,
    guardar(codigo) {
      const s = String(codigo || "").trim(); const i = s.lastIndexOf("#");
      if (i < 0 || !/^https:\/\/script\.google(usercontent)?\.com\//.test(s)) return false;
      ls.set(K_CFG, { url: s.slice(0, i), clave: s.slice(i + 1) }); return true;
    },
    borrar() { try { localStorage.removeItem(K_CFG); } catch (e) {} }
  };
  // Un link con #conectar=... deja la app conectada
  try {
    const m = location.hash.match(/conectar=([^&]+)/);
    if (m) { window.uniformaConexion.guardar(decodeURIComponent(m[1])); history.replaceState(null, "", location.pathname); }
  } catch (e) {}

  async function api(body) {
    const c = cfg(); if (!c) throw { code: "sin_conexion", message: "Falta conectar la app con Google." };
    if (!navigator.onLine) throw { code: "server_unavailable", message: "Sin señal" };
    let r;
    try { r = await fetch(c.url, { method: "POST", headers: { "Content-Type": "text/plain;charset=utf-8" }, body: JSON.stringify(Object.assign({}, body, { clave: c.clave })) }); }
    catch (e) { throw { code: "server_unavailable", message: "Sin señal" }; }
    let j; try { j = await r.json(); } catch (e) { throw { code: "upstream_error", message: "Google no respondió bien" }; }
    if (j.error) throw { code: j.codigo || j.error, message: j.mensaje || j.error };
    return j;
  }

  /* ---------------- base de datos local + sincronización ---------------- */
  let store = ls.get(K_DB, { cols: {}, desde: 0 });
  let outbox = ls.get(K_OUT, []);
  const subs = {}; // coleccion -> [cb]
  let primeraDescarga = false;
  const guardarLocal = () => { ls.set(K_DB, store); ls.set(K_OUT, outbox); };
  const col = c => (store.cols[c] ||= {});
  const rid = () => Array.from(crypto.getRandomValues(new Uint8Array(15)), b => "abcdefghijklmnopqrstuvwxyz0123456789"[b % 36]).join("").slice(0, 20);
  function snap(c, fromCache) {
    const docs = Object.entries(col(c)).map(([id, v]) => ({ id, data: () => JSON.parse(JSON.stringify(v)) }));
    return { docs, size: docs.length, empty: !docs.length, metadata: { fromCache } };
  }
  function avisar(c, fromCache) { (subs[c] || []).forEach(cb => { try { cb(snap(c, fromCache)); } catch (e) { console.warn(e); } }); }
  function escribirDoc(c, id, data) {
    if (data) col(c)[id] = JSON.parse(JSON.stringify(data)); else delete col(c)[id];
    outbox = outbox.filter(o => !(o.c === c && o.id === id)); outbox.push({ c, id, data: data || null });
    guardarLocal(); avisar(c, !primeraDescarga); programar(400);
  }
  function docRef(c, id) {
    id = id || rid();
    return {
      id,
      async set(data) { escribirDoc(c, id, data); },
      async update(p) { escribirDoc(c, id, Object.assign({}, col(c)[id] || {}, p)); },
      async delete() { escribirDoc(c, id, null); },
      async get() { const v = col(c)[id]; return { id, exists: !!v, data: () => v ? JSON.parse(JSON.stringify(v)) : undefined }; }
    };
  }
  const db = {
    collection(c) {
      return {
        doc: id => docRef(c, id),
        async add(data) { const r = docRef(c); await r.set(data); return r; },
        onSnapshot(cb) { (subs[c] ||= []).push(cb); setTimeout(() => cb(snap(c, !primeraDescarga)), 0); return () => { subs[c] = (subs[c] || []).filter(x => x !== cb); }; },
        async get() { return snap(c, !primeraDescarga); }
      };
    }
  };

  let sincronizando = false, timer = null;
  function programar(ms) { clearTimeout(timer); timer = setTimeout(sincronizar, ms); }
  async function sincronizar() {
    if (sincronizando || !cfg()) { estado(); return; }
    sincronizando = true; estado();
    try {
      // 1) mandar lo cargado
      if (outbox.length) {
        const envio = outbox.slice();
        await api({ op: "guardar", cambios: envio });
        outbox = outbox.filter(o => !envio.includes(o)); guardarLocal();
      }
      // 2) reposiciones a la librería pendientes
      let pend = ls.get(K_REPO, []);
      while (pend.length) {
        const p = pend[0];
        try { await api({ op: "sumarRepo", fecha: p.fecha, lineas: p.lineas, idMov: p.idMov }); }
        catch (e) { if (e.code === "server_unavailable") throw e; const f = ls.get(K_FALLO, []); f.push({ cuando: Date.now(), que: "Llevar a la librería " + p.fecha, error: e.message || e.code }); ls.set(K_FALLO, f); }
        pend.shift(); ls.set(K_REPO, pend);
      }
      // 3) hoja automática de Técnica (solo la última versión)
      const esc = ls.get(K_ESC, null);
      if (esc) { await api({ op: "escribir", planilla: esc.planilla, rango: esc.rango, valores: esc.valores }); if (JSON.stringify(ls.get(K_ESC, null)) === JSON.stringify(esc)) { try { localStorage.removeItem(K_ESC); } catch (e) {} } }
      // 4) traer lo nuevo (de Martina o de otro celular)
      const r = await api({ op: "traer", desde: store.desde || 0 });
      const cambiadas = new Set();
      const pendientes = new Set(outbox.map(o => o.c + "|" + o.id));
      (r.docs || []).forEach(d => {
        if (pendientes.has(d.c + "|" + d.id)) return;
        if (d.data) col(d.c)[d.id] = d.data; else delete col(d.c)[d.id];
        cambiadas.add(d.c);
      });
      store.desde = Math.max(store.desde || 0, ...(r.docs || []).map(d => d.t));
      const primera = !primeraDescarga; primeraDescarga = true; guardarLocal();
      Object.keys(subs).forEach(c => { if (primera || cambiadas.has(c)) avisar(c, false); });
      ultimoOk = Date.now();
    } catch (e) {
      ultimoError = e;
    } finally { sincronizando = false; estado(); }
  }
  window.addEventListener("online", () => programar(300));
  document.addEventListener("visibilitychange", () => { if (!document.hidden) programar(300); });
  setInterval(() => { if (!document.hidden) sincronizar(); }, 25000);

  /* ---------------- planillas de Google ---------------- */
  const cacheKey = (s, r) => "uni-cache:" + s + ":" + r;
  const mcp = {
    async callTool(server, tool, input) {
      if (tool === "get_values") {
        try {
          const r = await api({ op: "leer", planilla: input.spreadsheetId, rango: input.range });
          ls.set(cacheKey(input.spreadsheetId, input.range), { v: r.values, t: Date.now() });
          return { payload: { values: r.values } };
        } catch (e) {
          const c = ls.get(cacheKey(input.spreadsheetId, input.range), null);
          if (c && (e.code === "server_unavailable" || e.code === "upstream_error")) return { payload: { values: c.v }, cache: { storedAt: c.t, revalidating: false } };
          throw e;
        }
      }
      if (tool === "update_values") {
        // nunca reescribir la hoja de Técnica sin haber traído antes todos los cortes
        if (!primeraDescarga || !Object.keys(col("cortes")).length) return { payload: { ok: false } };
        ls.set(K_ESC, { planilla: input.spreadsheetId, rango: input.range, valores: input.values });
        programar(500); return { payload: { ok: true } };
      }
      if (tool === "sumar_repo") {
        const p = { fecha: input.fecha, lineas: input.lineas, idMov: input.idMov || rid() };
        try { const r = await api({ op: "sumarRepo", fecha: p.fecha, lineas: p.lineas, idMov: p.idMov }); return { payload: { letra: r.letra } }; }
        catch (e) {
          if (e.code !== "server_unavailable") throw e;
          const pend = ls.get(K_REPO, []); pend.push(p); ls.set(K_REPO, pend); estado();
          return { payload: { letra: "pendiente" } };
        }
      }
      throw { code: "not_in_manifest", message: "No permitido" };
    }
  };

  /* ---------------- descargas ---------------- */
  const downloads = {
    async save({ filename, data }) {
      const blob = data instanceof Blob ? data : new Blob([data]);
      try {
        const file = new File([blob], filename, { type: blob.type || "application/octet-stream" });
        if (navigator.canShare && navigator.canShare({ files: [file] })) { await navigator.share({ files: [file], title: filename }); return; }
      } catch (e) { if (e && e.name === "AbortError") throw { code: "declined" }; }
      const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = filename; document.body.appendChild(a); a.click();
      setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 2000);
    }
  };

  window.claude = {
    use: async n => n === "db" ? (cfg() ? db : db) : n === "mcp" ? (cfg() ? mcp : null) : n === "downloads" ? downloads : null
  };

  /* ---------------- cartelito de estado ---------------- */
  let ultimoOk = 0, ultimoError = null, pill = null;
  function estado() {
    if (!document.body) return;
    if (!pill) {
      pill = document.createElement("div"); pill.id = "uni-estado";
      pill.style.cssText = "position:fixed;left:12px;bottom:calc(12px + env(safe-area-inset-bottom,0px));z-index:50;font:600 12px system-ui,sans-serif;padding:6px 10px;border-radius:999px;box-shadow:0 2px 8px rgba(0,0,0,.15);pointer-events:none";
      document.body.appendChild(pill);
    }
    const n = outbox.length + ls.get(K_REPO, []).length + (ls.get(K_ESC, null) ? 1 : 0);
    const fallos = ls.get(K_FALLO, []);
    let t, bg = "#e3f4ea", fg = "#1d7a4a";
    if (!cfg()) { pill.hidden = true; return; }
    else if (fallos.length) { t = "⚠ " + fallos[fallos.length - 1].error; bg = "#fde8e6"; fg = "#b42318"; }
    else if (!navigator.onLine) { t = "Sin señal" + (n ? " · " + n + " por enviar" : " · todo guardado en el celular"); bg = "#fff1dc"; fg = "#a25a00"; }
    else if (sincronizando) t = "Sincronizando…";
    else if (n) { t = n + " por enviar"; bg = "#fff1dc"; fg = "#a25a00"; }
    else t = "✓ Al día";
    pill.hidden = false; pill.textContent = t; pill.style.background = bg; pill.style.color = fg;
  }
  window.uniformaEstado = { fallos: () => ls.get(K_FALLO, []), limpiarFallos: () => { try { localStorage.removeItem(K_FALLO); } catch (e) {} estado(); }, sincronizar };
  document.addEventListener("DOMContentLoaded", () => { estado(); sincronizar(); });
})();
