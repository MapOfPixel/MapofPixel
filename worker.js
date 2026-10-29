// Pixel Dünya sunucusu: tek bir ortak dünya, Cloudflare Durable Object üzerinde.
// Oda kodu yok. Siteye giren herkes aynı haritaya bağlanır.
import { DurableObject } from "cloudflare:workers";

// Bu değerler istemcideki (index.html) WORLD_W / WORLD_H ile aynı olmalı.
const W = 2334;
const H = 2583;
const N = W * H;
const CHUNK = 65536;      // kayıt parçası: 65536 hücre
// Dünyalar: max = sayaç sınırı (saniye), empty/filled = boş / dolu hücreye piksel koymanın bedeli.
// Sayaç saniyede 1 azalır. Dünya 1'in verisi eski "chunks" tablosunda durur, hiç dokunulmaz.
const WORLDS = {
  1: { max: 60,  empty: 1,   filled: 1, table: "chunks"  },
  2: { max: 120, empty: 0.5, filled: 2, table: "chunks2" },
  3: { max: 900, empty: 2,   filled: 4, table: "chunks3" },
};
const SLACK = 1;          // ağ gecikmesi payı
const FLUSH_MS = 3000;    // değişiklikler en geç 3 sn'de kaydedilir
const MAX_BATCH = 256;    // bir mesajda en fazla piksel

// Mesaj türleri (sunucu -> istemci, Uint32Array'in ilk elemanı)
const T_SNAPSHOT = 1, T_SNAPSHOT_END = 2, T_PIXELS = 3, T_REJECT = 4, T_ONLINE = 5;

// ---- Hesap yardımcıları ----
const enc = new TextEncoder();
const NAME_RE = /^[\p{L}\p{N}_. -]{3,16}$/u;
const hex = (u8) => [...u8].map((b) => b.toString(16).padStart(2, "0")).join("");
const unhex = (h) => Uint8Array.from(h.match(/../g) || [], (x) => parseInt(x, 16));
async function pbkdf2(pw, salt) {
  const key = await crypto.subtle.importKey("raw", enc.encode(pw), "PBKDF2", false, ["deriveBits"]);
  return new Uint8Array(await crypto.subtle.deriveBits(
    { name: "PBKDF2", hash: "SHA-256", salt, iterations: 100000 }, key, 256));
}
function sameBytes(a, b) {
  if (a.length !== b.length) return false;
  let d = 0;
  for (let i = 0; i < a.length; i++) d |= a[i] ^ b[i];
  return d === 0;
}
async function sha256hex(s) {
  return hex(new Uint8Array(await crypto.subtle.digest("SHA-256", enc.encode(s))));
}
class ApiError extends Error {
  constructor(msg, status = 400) { super(msg); this.status = status; }
}
function jsonRes(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: { "content-type": "application/json; charset=utf-8" } });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const allowedOrigin = (env.ALLOWED_ORIGIN || "").trim();
    if (url.pathname.startsWith("/api/")) {
      const cors = {
        "Access-Control-Allow-Origin": allowedOrigin || "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "content-type",
        "Access-Control-Max-Age": "86400",
        "Vary": "Origin",
      };
      if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });
      if (allowedOrigin && (request.headers.get("Origin") || "") !== allowedOrigin) {
        return new Response(JSON.stringify({ error: "İzin yok" }), { status: 403, headers: { ...cors, "content-type": "application/json" } });
      }
      const stub = env.WORLD.get(env.WORLD.idFromName("dunya"));
      const res = await stub.fetch(request);
      const headers = new Headers(res.headers);
      for (const [k, v] of Object.entries(cors)) headers.set(k, v);
      return new Response(res.body, { status: res.status, headers });
    }
    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("WebSocket gerekli", { status: 426 });
      }
      const allowed = (env.ALLOWED_ORIGIN || "").trim();
      if (allowed) {
        const origin = request.headers.get("Origin") || "";
        if (origin !== allowed) return new Response("İzin yok", { status: 403 });
      }
      const stub = env.WORLD.get(env.WORLD.idFromName("dunya"));
      return stub.fetch(request);
    }
    return new Response("Pixel Dünya sunucusu çalışıyor.", {
      headers: { "content-type": "text/plain; charset=utf-8" },
    });
  },
};

export class World extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // 0 = boş, 1..12 = renk numarası. Üç ayrı dünya.
    this.worlds = { 1: new Uint8Array(N), 2: new Uint8Array(N), 3: new Uint8Array(N) };
    this.dirty = { 1: new Set(), 2: new Set(), 3: new Set() };
    this.rates = new Map();         // oyuncu kimliği -> { used, ts }
    this.accCache = new Map();      // hesap no -> { total }
    this.accDirty = new Set();
    this.throttles = new Map();     // IP sınırları (bellekte)
    for (const w of [1, 2, 3]) {
      this.ctx.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS " + WORLDS[w].table + " (id INTEGER PRIMARY KEY, data BLOB)"
      );
    }
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS acc_rates (acc INTEGER NOT NULL, w INTEGER NOT NULL, " +
      "used REAL NOT NULL DEFAULT 0, ts INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (acc, w))"
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS accounts (" +
      "id INTEGER PRIMARY KEY, name TEXT NOT NULL, name_lc TEXT NOT NULL UNIQUE, " +
      "salt TEXT NOT NULL, hash TEXT NOT NULL, total INTEGER NOT NULL DEFAULT 0, " +
      "used REAL NOT NULL DEFAULT 0, ts INTEGER NOT NULL DEFAULT 0, " +
      "fails INTEGER NOT NULL DEFAULT 0, lock_until INTEGER NOT NULL DEFAULT 0)"
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS sessions (hash TEXT PRIMARY KEY, acc INTEGER NOT NULL)"
    );
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS admin_sessions (hash TEXT PRIMARY KEY)");
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    this.ctx.blockConcurrencyWhile(async () => {
      for (const w of [1, 2, 3]) {
        for (const row of this.ctx.storage.sql.exec("SELECT id, data FROM " + WORLDS[w].table)) {
          const start = row.id * CHUNK;
          if (start >= N) continue;
          const arr = new Uint8Array(row.data);
          this.worlds[w].set(arr.subarray(0, Math.min(arr.length, N - start)), start);
        }
      }
    });
  }

  // ---------- Hesap API ----------
  rows(q, ...args) { return [...this.ctx.storage.sql.exec(q, ...args)]; }

  tooMany(key, max) {
    const t = this.throttles.get(key);
    return !!t && Date.now() < t.until && t.n >= max;
  }
  hit(key, windowMs) {
    const now = Date.now();
    let t = this.throttles.get(key);
    if (!t || now >= t.until) t = { n: 0, until: now + windowMs };
    t.n++;
    this.throttles.set(key, t);
    if (this.throttles.size > 5000) {
      for (const [k, v] of this.throttles) if (now >= v.until) this.throttles.delete(k);
    }
  }

  async newSession(accId) {
    const raw = hex(crypto.getRandomValues(new Uint8Array(32)));
    this.ctx.storage.sql.exec("INSERT INTO sessions (hash, acc) VALUES (?, ?)", await sha256hex(raw), accId);
    return raw;
  }

  async accountByToken(token) {
    if (!token || token.length !== 64) return null;
    const r = this.rows(
      "SELECT a.* FROM sessions s JOIN accounts a ON a.id = s.acc WHERE s.hash = ?",
      await sha256hex(token)
    );
    return r[0] || null;
  }

  accountView(row) {
    return { id: row.id, name: row.name, total: row.total, used: row.used };
  }

  cleanName(v) {
    const name = String(v || "").trim().replace(/\s+/g, " ");
    if (!NAME_RE.test(name)) throw new ApiError("Ad 3-16 karakter olmalı (harf, rakam, boşluk, _ . -)");
    return name;
  }
  cleanPass(v) {
    const pw = String(v || "");
    if (pw.length < 6) throw new ApiError("Şifre en az 6 karakter olmalı");
    if (pw.length > 64) throw new ApiError("Şifre en fazla 64 karakter olabilir");
    return pw;
  }

  async register(body, ip) {
    if (this.tooMany("reg:" + ip, 5)) throw new ApiError("Çok fazla hesap açıldı, daha sonra tekrar dene", 429);
    const name = this.cleanName(body.name);
    const pw = this.cleanPass(body.password);
    if (this.rows("SELECT 1 FROM accounts WHERE name_lc = ?", name.toLowerCase()).length) {
      throw new ApiError("Bu ad alınmış", 409);
    }
    this.hit("reg:" + ip, 3600 * 1000);
    let id = 0;
    for (let i = 0; i < 50; i++) {
      const n = 100000 + (crypto.getRandomValues(new Uint32Array(1))[0] % 900000);
      if (!this.rows("SELECT 1 FROM accounts WHERE id = ?", n).length) { id = n; break; }
    }
    if (!id) throw new ApiError("ID üretilemedi, tekrar dene", 503);
    const salt = crypto.getRandomValues(new Uint8Array(16));
    const hash = await pbkdf2(pw, salt);
    this.ctx.storage.sql.exec(
      "INSERT INTO accounts (id, name, name_lc, salt, hash) VALUES (?, ?, ?, ?, ?)",
      id, name, name.toLowerCase(), hex(salt), hex(hash)
    );
    const token = await this.newSession(id);
    return { token, account: { id, name, total: 0, used: 0 } };
  }

  async login(body, ip) {
    if (this.tooMany("login:" + ip, 10)) throw new ApiError("Çok fazla hatalı deneme, biraz bekle", 429);
    const name = String(body.name || "").trim().replace(/\s+/g, " ");
    const pw = String(body.password || "").slice(0, 64);
    const row = this.rows("SELECT * FROM accounts WHERE name_lc = ?", name.toLowerCase())[0];
    const now = Date.now();
    if (row && row.lock_until > now) throw new ApiError("Bu hesap kısa süreliğine kilitlendi, birkaç dakika sonra dene", 429);
    const calc = await pbkdf2(pw, row ? unhex(row.salt) : new Uint8Array(16));
    if (!row || !sameBytes(calc, unhex(row.hash))) {
      this.hit("login:" + ip, 15 * 60 * 1000);
      if (row) {
        const fails = row.fails + 1;
        this.ctx.storage.sql.exec(
          "UPDATE accounts SET fails = ?, lock_until = ? WHERE id = ?",
          fails >= 5 ? 0 : fails, fails >= 5 ? now + 5 * 60 * 1000 : 0, row.id
        );
      }
      throw new ApiError("Ad veya şifre yanlış", 401);
    }
    this.ctx.storage.sql.exec("UPDATE accounts SET fails = 0, lock_until = 0 WHERE id = ?", row.id);
    const token = await this.newSession(row.id);
    return { token, account: this.accountView(this.accountFresh(row)) };
  }

  async recover(body, ip) {
    if (this.tooMany("rec:" + ip, 8)) throw new ApiError("Çok fazla hatalı deneme, daha sonra tekrar dene", 429);
    const idStr = String(body.id || "").trim();
    const pw = this.cleanPass(body.password);
    const row = /^\d{6}$/.test(idStr) ? this.rows("SELECT * FROM accounts WHERE id = ?", Number(idStr))[0] : null;
    if (!row) {
      this.hit("rec:" + ip, 3600 * 1000);
      throw new ApiError("Bu ID ile hesap bulunamadı", 404);
    }
    const salt = crypto.getRandomValues(new Uint8Array(16));
    const hash = await pbkdf2(pw, salt);
    this.ctx.storage.sql.exec(
      "UPDATE accounts SET salt = ?, hash = ?, fails = 0, lock_until = 0 WHERE id = ?",
      hex(salt), hex(hash), row.id
    );
    this.ctx.storage.sql.exec("DELETE FROM sessions WHERE acc = ?", row.id); // eski oturumlar kapanır
    const token = await this.newSession(row.id);
    return { token, account: this.accountView(this.accountFresh(row)) };
  }

  async logout(body) {
    const token = String(body.token || "");
    if (token.length === 64) this.ctx.storage.sql.exec("DELETE FROM sessions WHERE hash = ?", await sha256hex(token));
    return { ok: true };
  }

  // Hesabın bir dünyadaki sayacı. Dünya 1 eski "accounts" satırında, 2 ve 3 acc_rates içinde.
  loadAccRate(accId, w) {
    if (w === 1) {
      const r = this.rows("SELECT used, ts FROM accounts WHERE id = ?", accId)[0];
      return r ? { used: r.used, ts: r.ts } : { used: 0, ts: 0 };
    }
    const r = this.rows("SELECT used, ts FROM acc_rates WHERE acc = ? AND w = ?", accId, w)[0];
    return r ? { used: r.used, ts: r.ts } : { used: 0, ts: 0 };
  }

  // Bellekte daha yeni sayaç varsa onu kullan
  accountFresh(row) {
    const c = this.accCache.get(row.id);
    if (!c) return row;
    return { ...row, total: c.total };
  }

  // ---------- Yönetici ----------
  // Kod koda yazılmaz: Cloudflare'da ADMIN_CODE adlı gizli değer (secret) olarak tutulur.
  async adminLogin(body, ip) {
    const secret = String((this.env && this.env.ADMIN_CODE) || "");
    if (!secret) throw new ApiError("Yönetici özelliği kapalı", 503);
    if (this.tooMany("adm:" + ip, 5) || this.tooMany("adm:all", 20)) {
      throw new ApiError("Çok fazla hatalı deneme, daha sonra tekrar dene", 429);
    }
    const given = await sha256hex(String(body.code || "").slice(0, 200));
    const want = await sha256hex(secret);
    if (!sameBytes(unhex(given), unhex(want))) {
      this.hit("adm:" + ip, 3600 * 1000);
      this.hit("adm:all", 3600 * 1000);
      throw new ApiError("Yönetici kodu yanlış", 401);
    }
    const raw = hex(crypto.getRandomValues(new Uint8Array(32)));
    this.ctx.storage.sql.exec("INSERT INTO admin_sessions (hash) VALUES (?)", await sha256hex(raw));
    return { token: raw };
  }

  async adminLogout(body) {
    const t = String(body.token || "");
    if (t.length !== 64) return { ok: true };
    const h = await sha256hex(t);
    this.ctx.storage.sql.exec("DELETE FROM admin_sessions WHERE hash = ?", h);
    for (const s of this.ctx.getWebSockets()) {          // açık bağlantılarda da yetkiyi kaldır
      const a = s.deserializeAttachment() || {};
      if (a.ah === h) s.serializeAttachment({ ...a, admin: false, unl: false });
    }
    return { ok: true };
  }

  // Bir dünyanın haritasını tamamen boşaltır. Oyuncular bağlı kalır, ekranları temizlenir.
  resetWorld(w) {
    if (!WORLDS[w]) return;
    this.worlds[w].fill(0);
    this.dirty[w].clear();
    this.ctx.storage.sql.exec("DELETE FROM " + WORLDS[w].table);
    const msg = JSON.stringify({ type: "reset", world: w });
    for (const s of this.ctx.getWebSockets()) {
      const a = s.deserializeAttachment() || {};
      if ((WORLDS[a.w] ? a.w : 1) !== w) continue;
      try { s.send(msg); } catch (e) { /* kapanmış bağlantı */ }
    }
  }

  async api(request) {
    if (request.method !== "POST") return jsonRes({ error: "Yanlış istek" }, 405);
    let body;
    try { body = await request.json(); } catch (e) { return jsonRes({ error: "Geçersiz istek" }, 400); }
    if (!body || typeof body !== "object") return jsonRes({ error: "Geçersiz istek" }, 400);
    const ip = request.headers.get("CF-Connecting-IP") || "yerel";
    const path = new URL(request.url).pathname;
    try {
      if (path === "/api/register") return jsonRes(await this.register(body, ip));
      if (path === "/api/login") return jsonRes(await this.login(body, ip));
      if (path === "/api/recover") return jsonRes(await this.recover(body, ip));
      if (path === "/api/logout") return jsonRes(await this.logout(body));
      if (path === "/api/admin") return jsonRes(await this.adminLogin(body, ip));
      if (path === "/api/adminlogout") return jsonRes(await this.adminLogout(body));
      return jsonRes({ error: "Bulunamadı" }, 404);
    } catch (e) {
      if (e instanceof ApiError) return jsonRes({ error: e.message }, e.status);
      return jsonRes({ error: "Sunucu hatası" }, 500);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) return this.api(request);
    const token = url.searchParams.get("token") || "";
    const acc = token ? await this.accountByToken(token) : null;
    // Misafir kimliği "g" ile başlar, hesap kimliği "a" ile: ikisi karışmaz
    const id = acc ? "a" + acc.id : "g" + ((url.searchParams.get("id") || "").slice(0, 40) || crypto.randomUUID());
    const atok = url.searchParams.get("atok") || "";
    let admin = false, ah = "";
    if (atok.length === 64) {
      ah = await sha256hex(atok);
      admin = this.rows("SELECT 1 FROM admin_sessions WHERE hash = ?", ah).length > 0;
    }
    const wq = parseInt(url.searchParams.get("w") || "1", 10);
    const w = WORLDS[wq] ? wq : 1;
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    const att0 = acc ? { id, acc: acc.id, w } : { id, w };
    if (admin) { att0.admin = true; att0.ah = ah; }
    server.serializeAttachment(att0);
    if (admin) server.send(JSON.stringify({ type: "admin", ok: true }));
    else if (atok) server.send(JSON.stringify({ type: "badadmin" }));
    if (acc) {
      const now = Date.now();
      const rk = id + "|" + w;
      const saved = this.loadAccRate(acc.id, w);
      let used = Math.max(0, saved.used - (now - saved.ts) / 1000);
      const live = this.rates.get(rk);
      if (live) used = Math.max(0, live.used - (now - live.ts) / 1000);
      else this.rates.set(rk, { used, ts: now });
      if (!this.accCache.has(acc.id)) this.accCache.set(acc.id, { total: acc.total });
      const c = this.accCache.get(acc.id);
      server.send(JSON.stringify({ type: "account", account: { id: acc.id, name: acc.name, total: c.total, used, world: w } }));
    } else if (token) {
      server.send(JSON.stringify({ type: "badtoken" }));
    }
    this.sendSnapshot(server, w);
    this.broadcastOnline();
    return new Response(null, { status: 101, webSocket: client });
  }

  sendSnapshot(ws, w) {
    const SZ = 40000;
    let buf = new Uint32Array(SZ + 1);
    buf[0] = T_SNAPSHOT;
    let n = 0;
    const world = this.worlds[w];
    for (let i = 0; i < N; i++) {
      const c = world[i];
      if (c === 0) continue;
      buf[++n] = i * 16 + c;
      if (n === SZ) {
        ws.send(buf.buffer);
        buf = new Uint32Array(SZ + 1);
        buf[0] = T_SNAPSHOT;
        n = 0;
      }
    }
    if (n > 0) ws.send(buf.slice(0, n + 1).buffer);
    ws.send(new Uint32Array([T_SNAPSHOT_END]).buffer);
  }

  async webSocketMessage(ws, message) {
    if (typeof message === "string") {
      // "ping" otomatik yanıtlanıyor; buraya yalnızca yönetici komutları gelir
      if (message.charAt(0) !== "{" || message.length > 200) return;
      let m;
      try { m = JSON.parse(message); } catch (e) { return; }
      const a = ws.deserializeAttachment() || {};
      if (!a.admin || !m) return;                          // yönetici değilse hiçbir şey yapmaz
      if (m.type === "unlimited") ws.serializeAttachment({ ...a, unl: m.on === true });
      else if (m.type === "reset") this.resetWorld(Number(m.world));
      return;
    }
    if (message.byteLength === 0 || message.byteLength % 4 !== 0) return;
    if (message.byteLength > MAX_BATCH * 4) return;

    const arr = new Uint32Array(message);
    const att = ws.deserializeAttachment() || {};
    const id = att.id || "gbilinmiyor";
    const w = WORLDS[att.w] ? att.w : 1;      // eski bağlantılarda w yok: Dünya 1
    const unl = !!(att.admin && att.unl);     // yönetici sınırsız modu
    const cfg = WORLDS[w];
    const world = this.worlds[w];
    const rk = id + "|" + w;
    const now = Date.now();
    let r = this.rates.get(rk);
    if (!r && att.acc) {
      // Sunucu belleği sıfırlandıysa hesabın sayacını veritabanından geri yükle
      const saved = this.loadAccRate(att.acc, w);
      r = { used: Math.max(0, saved.used - (now - saved.ts) / 1000), ts: now };
    }
    if (!r) r = { used: 0, ts: now };
    r.used = Math.max(0, r.used - (now - r.ts) / 1000);
    r.ts = now;

    const accepted = [];
    const rejected = [];
    for (let k = 0; k < arr.length; k++) {
      const v = arr[k];
      const idx = v >>> 4;
      const c = v & 15;
      if (idx >= N || c < 1 || c > 12) continue;
      const cur = world[idx];
      if (cur === c) continue;
      const cost = cur ? cfg.filled : cfg.empty;   // dolu hücre daha pahalı olabilir
      if (!unl && r.used + cost > cfg.max + SLACK) {
        rejected.push(idx * 16 + cur);
        continue;
      }
      if (!unl) r.used += cost;
      world[idx] = c;
      this.dirty[w].add(idx >>> 16);
      accepted.push(v);
    }
    this.rates.set(rk, r);
    if (att.acc) {
      let c = this.accCache.get(att.acc);
      if (!c) {
        const row = this.rows("SELECT total FROM accounts WHERE id = ?", att.acc)[0];
        c = { total: row ? row.total : 0 };
        this.accCache.set(att.acc, c);
      }
      c.total += accepted.length;
      this.accDirty.add(att.acc + "|" + w);
    }

    if (rejected.length) ws.
