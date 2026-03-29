require("dotenv").config();
const fs = require("fs");
const path = require("path");
const https = require("https");
const http = require("http");
const { PassThrough } = require("stream");
let PImage = null;
try { PImage = require("pureimage"); } catch {}

const {
  Client,
  GatewayIntentBits,
  EmbedBuilder,
  AttachmentBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  StringSelectMenuBuilder,
  PermissionsBitField,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  SlashCommandBuilder,
} = require("discord.js");

// ====== НАСТРОЙКИ ======
const DISCORD_TOKEN = process.env.DISCORD_TOKEN;

const GUILD_ID = process.env.GUILD_ID;
const SUBMIT_CHANNEL_ID = process.env.SUBMIT_CHANNEL_ID;
const REVIEW_CHANNEL_ID = process.env.REVIEW_CHANNEL_ID;
const TIERLIST_CHANNEL_ID = process.env.TIERLIST_CHANNEL_ID;
const MOD_ROLE_ID = process.env.MOD_ROLE_ID;
const LOG_CHANNEL_ID = process.env.LOG_CHANNEL_ID || "";
const GRAPHIC_TIERLIST_CHANNEL_ID = process.env.GRAPHIC_TIERLIST_CHANNEL_ID || "";
const GRAPHIC_TIERLIST_TITLE = process.env.GRAPHIC_TIERLIST_TITLE || "ELO Tier List";

const SUBMIT_COOLDOWN_SECONDS = 120; // кулдаун на ВАЛИДНУЮ заявку
const PENDING_EXPIRE_HOURS = 48;     // протухание pending
const SUBMIT_SESSION_EXPIRE_MS = 10 * 60 * 1000;
const SUBMIT_UI_DELETE_MS = 25 * 1000;
const SUBMIT_PUBLIC_DELETE_MS = 10 * 1000;
const SUBMIT_PANEL_RESEND_INTERVAL_MS = 30 * 60 * 1000; // переотправка панели каждые 30 минут

// TODO: ВПИШИ СВОИ НАЗВАНИЯ ТИРОВ ТУТ (пока цифры)
// (можно менять через /elo labels тоже)
const DEFAULT_TIER_LABELS = { 1: "1", 2: "2", 3: "3", 4: "4", 5: "5" };
const DEFAULT_GRAPHIC_MESSAGE_TEXT = "Главное отображение ELO тир-листа. Текстовый tierlist-канал больше не используется.";
const DISABLE_TEXT_TIERLIST = true; // stage 2: текстовый tierlist-канал полностью выведен из работы

// ====== DB (файл) ======
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "db.json");

function loadDB() {
  if (!fs.existsSync(DB_PATH)) {
    return { config: {}, submissions: {}, ratings: {}, cooldowns: {}, miniCards: {} };
  }
  try {
    const data = JSON.parse(fs.readFileSync(DB_PATH, "utf8"));
    data.config ||= {};
    data.submissions ||= {};
    data.ratings ||= {};
    data.cooldowns ||= {};
    data.miniCards ||= {};
    return data;
  } catch {
    return { config: {}, submissions: {}, ratings: {}, cooldowns: {}, miniCards: {} };
  }
}

function saveDB(db) {
  fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2), "utf8");
}

const db = loadDB();
db.config.tierLabels ||= DEFAULT_TIER_LABELS;
db.miniCards ||= {};
db.config.submitPanel ||= { channelId: SUBMIT_CHANNEL_ID || "", messageId: "" };
db.config.graphicTierlist ||= {
  title: GRAPHIC_TIERLIST_TITLE,
  dashboardChannelId: GRAPHIC_TIERLIST_CHANNEL_ID || "",
  dashboardMessageId: "",
  lastUpdated: 0,
  image: { width: null, height: null, icon: null },
  tierColors: {
    5: "#ff6b6b",
    4: "#ff9f43",
    3: "#feca57",
    2: "#1dd1a1",
    1: "#54a0ff"
  },
  panel: {
    selectedTier: 5
  },
  messageText: DEFAULT_GRAPHIC_MESSAGE_TEXT
};
db.config.graphicTierlist.image ||= { width: null, height: null, icon: null };
db.config.graphicTierlist.tierColors ||= { 5: "#ff6b6b", 4: "#ff9f43", 3: "#feca57", 2: "#1dd1a1", 1: "#54a0ff" };
db.config.graphicTierlist.panel ||= { selectedTier: 5 };
if (!db.config.graphicTierlist.messageText) db.config.graphicTierlist.messageText = DEFAULT_GRAPHIC_MESSAGE_TEXT;
if (!db.config.graphicTierlist.title) db.config.graphicTierlist.title = GRAPHIC_TIERLIST_TITLE;
if (GRAPHIC_TIERLIST_CHANNEL_ID && db.config.graphicTierlist.dashboardChannelId !== GRAPHIC_TIERLIST_CHANNEL_ID) {
  db.config.graphicTierlist.dashboardChannelId = GRAPHIC_TIERLIST_CHANNEL_ID;
  db.config.graphicTierlist.dashboardMessageId = "";
}
saveDB(db);

// ====== HELPERS ======
function makeId() {
  return (Date.now().toString(36) + Math.random().toString(36).slice(2, 10)).toUpperCase();
}

function parseElo(text) {
  if (!text) return null;
  const m = text.match(/(\d{1,4})\+?/);
  if (!m) return null;
  const n = Number(m[1]);
  if (!Number.isFinite(n) || n <= 0) return null;
  return n;
}

function isImageAttachment(att) {
  if (!att) return false;
  const ct = att.contentType || "";
  if (ct.startsWith("image/")) return true;
  const url = (att.url || "").toLowerCase();
  return url.endsWith(".png") || url.endsWith(".jpg") || url.endsWith(".jpeg") || url.endsWith(".webp") || url.endsWith(".gif");
}

// Тиры "ОТ": 10 / 20 / 40 / 70 / 110 (ниже 10 — невалидно)
function tierFor(elo) {
  if (elo >= 110) return 5;
  if (elo >= 70) return 4;
  if (elo >= 40) return 3;
  if (elo >= 20) return 2;
  if (elo >= 10) return 1;
  return null;
}

function formatTierTitle(t) {
  const labels = db.config.tierLabels || DEFAULT_TIER_LABELS;
  // В тир-листе не добавляем префикс "Тир" перед кастомным названием.
  return `${labels[t] ?? t}`;
}

function sanitizeFileName(name, fallbackExt = "png") {
  const base = (name || "")
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80);

  if (!base) return `screenshot.${fallbackExt}`;
  // Если нет расширения — добавим.
  if (!/\.[a-z0-9]{2,5}$/i.test(base)) return `${base}.${fallbackExt}`;
  return base;
}

async function downloadToBuffer(url, timeoutMs = 15000) {
  const headers = {
    "User-Agent": "Mozilla/5.0 ChatGPTBot/1.0",
    "Accept": "image/avif,image/webp,image/apng,image/png,image/jpeg,*/*;q=0.8"
  };

  // 1) Node 18+: используем fetch
  if (typeof fetch === "function") {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: controller.signal, headers });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const ab = await res.arrayBuffer();
      return Buffer.from(ab);
    } finally {
      clearTimeout(t);
    }
  }

  // 2) Fallback (Node 16/17): качаем через http/https
  return await new Promise((resolve, reject) => {
    const lib = url.startsWith("https:") ? https : http;
    const req = lib.get(url, { headers }, (res) => {
      // редиректы
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        downloadToBuffer(res.headers.location, timeoutMs).then(resolve).catch(reject);
        return;
      }
      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }

      const chunks = [];
      res.on("data", (d) => chunks.push(d));
      res.on("end", () => resolve(Buffer.concat(chunks)));
    });
    req.on("error", reject);
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error("timeout"));
    });
  });
}

function isModerator(member) {
  if (!member) return false;
  if (member.permissions.has(PermissionsBitField.Flags.Administrator)) return true;
  if (MOD_ROLE_ID && member.roles?.cache?.has(MOD_ROLE_ID)) return true;
  return false;
}

// ====== ROLES: PER-TIER (RANK) ======
// Раньше выдавалась одна роль "участник тир-листа" (TIERLIST_ROLE_ID).
// Теперь: за КАЖДЫЙ тир/ранг выдаётся своя роль, и бот держит ровно одну из них.
// Настройка через .env (любые можно оставить пустыми — тогда роли не трогаем):
// TIER_ROLE_1_ID, TIER_ROLE_2_ID, TIER_ROLE_3_ID, TIER_ROLE_4_ID, TIER_ROLE_5_ID
const TIER_ROLE_IDS = {
  1: process.env.TIER_ROLE_1_ID || "",
  2: process.env.TIER_ROLE_2_ID || "",
  3: process.env.TIER_ROLE_3_ID || "",
  4: process.env.TIER_ROLE_4_ID || "",
  5: process.env.TIER_ROLE_5_ID || "",
};

let _guildCache = null;

async function getGuild(client) {
  if (_guildCache) return _guildCache;
  if (!GUILD_ID) return null;
  _guildCache = await client.guilds.fetch(GUILD_ID).catch(() => null);
  return _guildCache;
}

function allTierRoleIds() {
  return Object.values(TIER_ROLE_IDS).filter(Boolean);
}

async function ensureSingleTierRole(client, userId, targetTier, reason = "tier role sync") {
  const targetRoleId = TIER_ROLE_IDS[targetTier] || "";
  const all = allTierRoleIds();

  // если роли не настроены — ничего не делаем
  if (!all.length) return;

  const guild = await getGuild(client);
  if (!guild) return;

  const member = await guild.members.fetch(userId).catch(() => null);
  if (!member) return;

  // 1) снять все "не те" тир-роли
  const toRemove = all.filter(rid => rid !== targetRoleId && member.roles.cache.has(rid));
  for (const rid of toRemove) {
    await member.roles.remove(rid, reason).catch(() => {});
  }

  // 2) надеть нужную (если она задана)
  if (targetRoleId && !member.roles.cache.has(targetRoleId)) {
    await member.roles.add(targetRoleId, reason).catch(() => {});
  }
}

async function clearAllTierRoles(client, userId, reason = "tier role clear") {
  const all = allTierRoleIds();
  if (!all.length) return;

  const guild = await getGuild(client);
  if (!guild) return;

  const member = await guild.members.fetch(userId).catch(() => null);
  if (!member) return;

  for (const rid of all) {
    if (member.roles.cache.has(rid)) {
      await member.roles.remove(rid, reason).catch(() => {});
    }
  }
}

async function syncTierRolesOnStart(client) {
  const ids = Object.keys(db.ratings || {});
  if (!ids.length) return;

  for (const uid of ids) {
    const r = db.ratings[uid];
    if (!r?.tier) continue;
    await ensureSingleTierRole(client, uid, Number(r.tier), "sync from db");
  }
}

// ====== GRAPHIC TIERLIST (PNG DASHBOARD) ======
const GRAPHIC_TIER_ORDER = [5, 4, 3, 2, 1];
const DEFAULT_GRAPHIC_TIER_COLORS = {
  5: "#ff6b6b",
  4: "#ff9f43",
  3: "#feca57",
  2: "#1dd1a1",
  1: "#54a0ff"
};
let graphicFontsReady = false;
let GRAPHIC_FONT_REG = "GraphicFontRegular";
let GRAPHIC_FONT_BOLD = "GraphicFontBold";
let GRAPHIC_FONT_INFO = { regularFile: null, boldFile: null, usedFallback: false, source: "none", loadError: null };
const graphicAvatarCache = new Map();
const GRAPHIC_AVATAR_DISK_DIR = process.env.GRAPHIC_AVATAR_CACHE_DIR || path.join(__dirname, 'graphic_avatar_cache');

function ensureGraphicAvatarDiskDir() {
  try { fs.mkdirSync(GRAPHIC_AVATAR_DISK_DIR, { recursive: true }); } catch {}
}

function getGraphicAvatarDiskPath(userId) {
  ensureGraphicAvatarDiskDir();
  return path.join(GRAPHIC_AVATAR_DISK_DIR, `${String(userId || 'unknown')}.png`);
}

async function loadGraphicAvatarFromDisk(userId) {
  if (!userId) return null;
  const fp = getGraphicAvatarDiskPath(userId);
  if (!fs.existsSync(fp)) return null;
  try {
    const buf = fs.readFileSync(fp);
    const img = await decodeImageFromBuffer(buf);
    if (!img) return null;
    graphicAvatarCache.set(`disk:${userId}`, img);
    return img;
  } catch {
    return null;
  }
}

function saveGraphicAvatarBufferToDisk(userId, buf) {
  if (!userId || !buf?.length) return false;
  try {
    fs.writeFileSync(getGraphicAvatarDiskPath(userId), buf);
    return true;
  } catch {
    return false;
  }
}

function getGraphicTierlistState() {
  db.config.graphicTierlist ||= {
    title: GRAPHIC_TIERLIST_TITLE,
    dashboardChannelId: GRAPHIC_TIERLIST_CHANNEL_ID || "",
    dashboardMessageId: "",
    lastUpdated: 0,
    image: { width: null, height: null, icon: null },
    tierColors: { ...DEFAULT_GRAPHIC_TIER_COLORS },
    panel: {
      selectedTier: 5
    },
    messageText: DEFAULT_GRAPHIC_MESSAGE_TEXT
  };
  db.config.graphicTierlist.image ||= { width: null, height: null, icon: null };
  db.config.graphicTierlist.tierColors ||= { ...DEFAULT_GRAPHIC_TIER_COLORS };
  db.config.graphicTierlist.panel ||= { selectedTier: 5 };
  if (!db.config.graphicTierlist.messageText) db.config.graphicTierlist.messageText = DEFAULT_GRAPHIC_MESSAGE_TEXT;
  if (!db.config.graphicTierlist.title) db.config.graphicTierlist.title = GRAPHIC_TIERLIST_TITLE;
  if (GRAPHIC_TIERLIST_CHANNEL_ID && db.config.graphicTierlist.dashboardChannelId !== GRAPHIC_TIERLIST_CHANNEL_ID) {
    db.config.graphicTierlist.dashboardChannelId = GRAPHIC_TIERLIST_CHANNEL_ID;
    db.config.graphicTierlist.dashboardMessageId = "";
  }
  for (const t of GRAPHIC_TIER_ORDER) {
    if (!db.config.graphicTierlist.tierColors[t]) db.config.graphicTierlist.tierColors[t] = DEFAULT_GRAPHIC_TIER_COLORS[t];
  }
  return db.config.graphicTierlist;
}

function getGraphicMessageText() {
  const state = getGraphicTierlistState();
  const raw = String(state.messageText ?? DEFAULT_GRAPHIC_MESSAGE_TEXT).trim();
  return raw || DEFAULT_GRAPHIC_MESSAGE_TEXT;
}

function previewGraphicMessageText(max = 220) {
  const text = getGraphicMessageText().replace(/\s+/g, ' ').trim();
  if (text.length <= max) return text;
  return `${text.slice(0, Math.max(1, max - 1)).trimEnd()}…`;
}

function getGraphicDashboardEmbedDescription() {
  return getGraphicMessageText();
}

function getGraphicMessageTextModalValue() {
  const text = getGraphicMessageText();
  return text.length <= 4000 ? text : text.slice(0, 4000);
}

function getGraphicImageConfig() {
  const state = getGraphicTierlistState();
  const cfg = state.image || {};
  const w = Number(cfg.width) || 2000;
  const h = Number(cfg.height) || 1200;
  const icon = Number(cfg.icon) || 112;
  return {
    W: Math.max(1200, w),
    H: Math.max(700, h),
    ICON: Math.max(64, icon)
  };
}

function applyGraphicImageDelta(kind, delta) {
  const state = getGraphicTierlistState();
  state.image ||= { width: null, height: null, icon: null };
  const cfg = getGraphicImageConfig();

  if (kind === "icon") {
    state.image.icon = Math.max(64, Math.min(256, cfg.ICON + delta));
  } else if (kind === "width") {
    state.image.width = Math.max(1200, Math.min(4096, cfg.W + delta));
  } else if (kind === "height") {
    state.image.height = Math.max(700, Math.min(2160, cfg.H + delta));
  }
}

function resetGraphicImageOverrides() {
  const state = getGraphicTierlistState();
  state.image ||= { width: null, height: null, icon: null };
  state.image.width = null;
  state.image.height = null;
  state.image.icon = null;
}

function isDiscordCdnUrl(url) {
  if (!url) return false;
  try {
    const host = new URL(url).hostname.toLowerCase();
    return host === "cdn.discordapp.com" || host === "media.discordapp.net";
  } catch {
    return false;
  }
}

function normalizeDiscordAvatarUrl(url) {
  if (!url) return "";
  try {
    const u = new URL(url);
    if (!isDiscordCdnUrl(u.toString())) return u.toString();
    const file = u.pathname || "";
    u.pathname = file.replace(/\.(webp|gif|jpg|jpeg)$/i, ".png");
    u.searchParams.set("size", "256");
    u.searchParams.delete("width");
    u.searchParams.delete("height");
    return u.toString();
  } catch {
    return String(url);
  }
}

function normalizeHexColor(input) {
  const raw = String(input || "").trim();
  if (!raw) return null;
  const m = raw.match(/^#?([0-9a-fA-F]{6})$/);
  if (!m) return null;
  return `#${m[1].toLowerCase()}`;
}

function setGraphicTierColor(tier, color) {
  const state = getGraphicTierlistState();
  const hex = normalizeHexColor(color);
  if (!hex) return false;
  state.tierColors ||= { ...DEFAULT_GRAPHIC_TIER_COLORS };
  state.tierColors[tier] = hex;
  return true;
}

function resetGraphicTierColor(tier) {
  const state = getGraphicTierlistState();
  state.tierColors ||= { ...DEFAULT_GRAPHIC_TIER_COLORS };
  state.tierColors[tier] = DEFAULT_GRAPHIC_TIER_COLORS[tier] || "#cccccc";
}

function resetAllGraphicTierColors() {
  const state = getGraphicTierlistState();
  state.tierColors = { ...DEFAULT_GRAPHIC_TIER_COLORS };
}

function clearGraphicAvatarCache() {
  graphicAvatarCache.clear();
  try {
    if (fs.existsSync(GRAPHIC_AVATAR_DISK_DIR)) {
      for (const f of fs.readdirSync(GRAPHIC_AVATAR_DISK_DIR)) {
        try { fs.unlinkSync(path.join(GRAPHIC_AVATAR_DISK_DIR, f)); } catch {}
      }
    }
  } catch {}
}

function buildGraphicBucketsFromRatings() {
  const buckets = { 1: [], 2: [], 3: [], 4: [], 5: [] };
  const entries = Object.values(db.ratings || {});

  for (const raw of entries) {
    const tier = Number(raw?.tier);
    if (!buckets[tier]) continue;
    buckets[tier].push({
      userId: raw.userId,
      name: raw.name || raw.userId,
      username: String(raw.username || "").trim() || raw.name || raw.userId,
      elo: Number(raw.elo) || 0,
      tier,
      avatarUrl: normalizeDiscordAvatarUrl(raw.avatarUrl || "")
    });
  }

  for (const t of Object.keys(buckets)) {
    buckets[t].sort((a, b) => {
      if ((b.elo || 0) !== (a.elo || 0)) return (b.elo || 0) - (a.elo || 0);
      return String(a.name || "").localeCompare(String(b.name || ""), "ru");
    });
  }

  return buckets;
}

function listGraphicFontFiles() {
  const candidates = [
    path.join(__dirname, "assets", "fonts"),
    "/usr/share/fonts/truetype/dejavu",
    "/usr/share/fonts/truetype/liberation2",
    "/usr/share/fonts/truetype/freefont"
  ];

  const out = [];
  for (const dir of candidates) {
    try {
      if (!fs.existsSync(dir)) continue;
      for (const f of fs.readdirSync(dir)) {
        if (f.toLowerCase().endsWith(".ttf")) out.push(path.join(dir, f));
      }
    } catch {}
  }
  return out;
}

function pickGraphicFontFiles() {
  const preferredPairs = [
    [
      "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
      "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
      "system-dejavu"
    ],
    [
      "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
      "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
      "system-liberation"
    ],
    [
      path.join(__dirname, "assets", "fonts", "NotoSans-Regular.ttf"),
      path.join(__dirname, "assets", "fonts", "NotoSans-Bold.ttf"),
      "repo-assets"
    ]
  ];

  for (const [regularFile, boldFile, source] of preferredPairs) {
    if (fs.existsSync(regularFile) && fs.existsSync(boldFile)) {
      return { regularFile, boldFile, usedFallback: false, source, loadError: null };
    }
  }

  const any = listGraphicFontFiles();
  if (any.length) {
    return { regularFile: any[0], boldFile: any[0], usedFallback: true, source: "any-ttf", loadError: null };
  }

  return { regularFile: null, boldFile: null, usedFallback: true, source: "none", loadError: "No TTF fonts found" };
}

function ensureGraphicFonts() {
  if (!PImage) return false;
  if (graphicFontsReady) return true;

  const picked = pickGraphicFontFiles();
  GRAPHIC_FONT_INFO = picked;

  if (!picked.regularFile || !picked.boldFile) {
    graphicFontsReady = false;
    return false;
  }

  try {
    PImage.registerFont(picked.regularFile, GRAPHIC_FONT_REG).loadSync();
    PImage.registerFont(picked.boldFile, GRAPHIC_FONT_BOLD).loadSync();
    GRAPHIC_FONT_INFO.loadError = null;
    graphicFontsReady = true;
    return true;
  } catch (err) {
    GRAPHIC_FONT_INFO.loadError = String(err?.message || err || "font load failed");
    graphicFontsReady = false;
    return false;
  }
}

function setGraphicFont(ctx, px, kind = "regular") {
  const family = kind === "bold" ? GRAPHIC_FONT_BOLD : GRAPHIC_FONT_REG;
  ctx.font = `${Math.max(1, Math.floor(px))}px ${family}`;
}

function measureGraphicTextWidth(ctx, text) {
  try {
    return Number(ctx.measureText(String(text || "")).width) || 0;
  } catch {
    return String(text || "").length * 12;
  }
}

function centerGraphicTextX(ctx, text, left, width) {
  const tw = measureGraphicTextWidth(ctx, text);
  return Math.floor(left + Math.max(0, (width - tw) / 2));
}

function wrapGraphicTextLines(ctx, text, maxWidth, maxLines = 3) {
  const source = String(text || "").trim();
  if (!source) return [""];

  const out = [];
  const words = source.split(/\s+/).filter(Boolean);

  function pushWordSmart(word) {
    if (measureGraphicTextWidth(ctx, word) <= maxWidth) {
      out.push(word);
      return;
    }

    let chunk = "";
    for (const ch of word) {
      const candidate = chunk + ch;
      if (!chunk || measureGraphicTextWidth(ctx, candidate) <= maxWidth) {
        chunk = candidate;
      } else {
        out.push(chunk);
        chunk = ch;
      }
    }
    if (chunk) out.push(chunk);
  }

  const pieces = [];
  for (const word of words) {
    if (measureGraphicTextWidth(ctx, word) <= maxWidth) pieces.push(word);
    else {
      let chunk = "";
      for (const ch of word) {
        const candidate = chunk + ch;
        if (!chunk || measureGraphicTextWidth(ctx, candidate) <= maxWidth) chunk = candidate;
        else {
          pieces.push(chunk);
          chunk = ch;
        }
      }
      if (chunk) pieces.push(chunk);
    }
  }

  let line = "";
  for (const part of pieces) {
    const candidate = line ? `${line} ${part}` : part;
    if (!line || measureGraphicTextWidth(ctx, candidate) <= maxWidth) {
      line = candidate;
      continue;
    }
    out.push(line);
    line = part;
  }
  if (line) out.push(line);

  if (out.length <= maxLines) return out;

  const trimmed = out.slice(0, maxLines);
  let last = trimmed[maxLines - 1];
  while (last.length > 1 && measureGraphicTextWidth(ctx, `${last}…`) > maxWidth) {
    last = last.slice(0, -1).trimEnd();
  }
  trimmed[maxLines - 1] = `${last}…`;
  return trimmed;
}

function fitGraphicWrappedText(ctx, text, kind, maxWidth, maxHeight, startPx, minPx = 22, maxLines = 3) {
  for (let px = startPx; px >= minPx; px -= 2) {
    setGraphicFont(ctx, px, kind);
    const lines = wrapGraphicTextLines(ctx, text, maxWidth, maxLines);
    const lineH = Math.max(px + 4, Math.floor(px * 1.15));
    const totalH = lines.length * lineH;
    const widest = Math.max(...lines.map(line => measureGraphicTextWidth(ctx, line)), 0);
    if (widest <= maxWidth && totalH <= maxHeight) {
      return { px, lines, lineH, totalH };
    }
  }

  setGraphicFont(ctx, minPx, kind);
  const lines = wrapGraphicTextLines(ctx, text, maxWidth, maxLines);
  const lineH = Math.max(minPx + 4, Math.floor(minPx * 1.15));
  return { px: minPx, lines, lineH, totalH: lines.length * lineH };
}

function trimGraphicTextToWidth(ctx, text, maxWidth) {
  let out = String(text || "").trim();
  if (!out) return "";
  if (measureGraphicTextWidth(ctx, out) <= maxWidth) return out;
  while (out.length > 1 && measureGraphicTextWidth(ctx, `${out}…`) > maxWidth) {
    out = out.slice(0, -1).trimEnd();
  }
  return out.length ? `${out}…` : "";
}

function fitGraphicSingleLineText(ctx, text, kind, maxWidth, startPx, minPx = 10) {
  const source = String(text || "").trim();
  if (!source) return { px: minPx, text: "" };

  for (let px = startPx; px >= minPx; px -= 1) {
    setGraphicFont(ctx, px, kind);
    if (measureGraphicTextWidth(ctx, source) <= maxWidth) return { px, text: source };
  }

  setGraphicFont(ctx, minPx, kind);
  return { px: minPx, text: trimGraphicTextToWidth(ctx, source, maxWidth) };
}

function drawGraphicOutlinedText(ctx, text, x, y, fill = "#ffffff", outline = "#000000") {
  const offsets = [
    [-2, 0], [2, 0], [0, -2], [0, 2],
    [-1, -1], [1, -1], [-1, 1], [1, 1]
  ];
  ctx.fillStyle = outline;
  for (const [dx, dy] of offsets) ctx.fillText(text, x + dx, y + dy);
  ctx.fillStyle = fill;
  ctx.fillText(text, x, y);
}

function drawGraphicTierTitle(ctx, text, boxX, boxY, boxW, boxH) {
  const fit = fitGraphicWrappedText(ctx, text, "bold", boxW, boxH, 56, 22, 3);
  fillColor(ctx, '#111111');
  setGraphicFont(ctx, fit.px, 'bold');

  let y = Math.floor(boxY + Math.max(0, (boxH - fit.totalH) / 2)) + fit.px;
  for (const line of fit.lines) {
    ctx.fillText(line, boxX, y);
    y += fit.lineH;
  }
}

function hexToRgb(hex) {
  const h = String(hex || "#cccccc").replace("#", "");
  const n = parseInt(h, 16);
  return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255 };
}

function fillColor(ctx, hex) {
  const { r, g, b } = hexToRgb(hex);
  ctx.fillStyle = `rgb(${r},${g},${b})`;
}

function bufferToPassThrough(buf) {
  const s = new PassThrough();
  s.end(buf);
  return s;
}

async function decodeImageFromBuffer(buf) {
  if (!PImage || !buf) return null;
  try {
    return await PImage.decodePNGFromStream(bufferToPassThrough(buf));
  } catch {}
  try {
    return await PImage.decodeJPEGFromStream(bufferToPassThrough(buf));
  } catch {}
  return null;
}

async function fetchGraphicAvatarFromUrl(url) {
  const normalized = normalizeDiscordAvatarUrl(url || "");
  if (!normalized) return { img: null, buf: null, url: "" };
  const cacheHit = graphicAvatarCache.get(normalized);
  if (cacheHit) return { img: cacheHit, buf: null, url: normalized };

  try {
    const buf = await downloadToBuffer(normalized, 15000);
    const img = await decodeImageFromBuffer(buf);
    if (img) {
      graphicAvatarCache.set(normalized, img);
      return { img, buf, url: normalized };
    }
  } catch {}

  return { img: null, buf: null, url: normalized };
}

async function getFreshDiscordAvatarUrls(client, userId) {
  const urls = [];
  if (!client || !userId) return urls;

  try {
    const guild = await getGuild(client);
    const member = guild ? await guild.members.fetch(userId).catch(() => null) : null;
    if (member) {
      const memberUrl = normalizeDiscordAvatarUrl(member.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 }));
      if (memberUrl) urls.push(memberUrl);
      const user = member.user || null;
      if (user) {
        const userUrl = normalizeDiscordAvatarUrl(user.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 }));
        const defaultUrl = normalizeDiscordAvatarUrl(user.defaultAvatarURL || "");
        if (userUrl) urls.push(userUrl);
        if (defaultUrl) urls.push(defaultUrl);
      }
    }
  } catch {}

  try {
    const user = await client.users.fetch(userId).catch(() => null);
    if (user) {
      const userUrl = normalizeDiscordAvatarUrl(user.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 }));
      const defaultUrl = normalizeDiscordAvatarUrl(user.defaultAvatarURL || "");
      if (userUrl) urls.push(userUrl);
      if (defaultUrl) urls.push(defaultUrl);
    }
  } catch {}

  return [...new Set(urls.filter(Boolean))];
}

async function loadGraphicAvatarForPlayer(client, player) {
  const userId = player?.userId || "";
  const rating = db.ratings?.[userId];

  if (userId && graphicAvatarCache.has(`disk:${userId}`)) {
    return graphicAvatarCache.get(`disk:${userId}`);
  }

  const diskImg = await loadGraphicAvatarFromDisk(userId);
  if (diskImg) return diskImg;

  const candidates = [];
  const push = (url) => {
    const normalized = normalizeDiscordAvatarUrl(url || "");
    if (normalized) candidates.push(normalized);
  };

  push(player?.avatarUrl);
  push(rating?.avatarUrl);
  for (const freshUrl of await getFreshDiscordAvatarUrls(client, userId)) push(freshUrl);

  for (const url of [...new Set(candidates)]) {
    const res = await fetchGraphicAvatarFromUrl(url);
    if (!res.img) continue;

    if (userId && res.buf) {
      saveGraphicAvatarBufferToDisk(userId, res.buf);
      graphicAvatarCache.set(`disk:${userId}`, res.img);
    }

    if (player) player.avatarUrl = res.url;
    if (rating && rating.avatarUrl !== res.url) {
      rating.avatarUrl = res.url;
      rating.updatedAt = new Date().toISOString();
      saveDB(db);
    }
    return res.img;
  }

  return null;
}

async function hydrateGraphicAvatarUrls(client) {
  if (!client) return 0;
  let changed = 0;

  for (const [userId, rating] of Object.entries(db.ratings || {})) {
    const current = normalizeDiscordAvatarUrl(rating?.avatarUrl || "");
    const freshList = await getFreshDiscordAvatarUrls(client, userId);
    const best = freshList[0] || current || "";
    if (!best) continue;
    if (best !== rating.avatarUrl) {
      rating.avatarUrl = best;
      changed++;
    }
  }

  if (changed) saveDB(db);
  return changed;
}

async function hydrateGraphicUsernames(client) {
  if (!client) return 0;
  let changed = 0;

  for (const [userId, rating] of Object.entries(db.ratings || {})) {
    let nextUsername = String(rating?.username || "").trim();

    try {
      const user = await client.users.fetch(userId).catch(() => null);
      if (user?.username) nextUsername = String(user.username).trim();
    } catch {}

    if (!nextUsername) continue;
    if (nextUsername !== rating.username) {
      rating.username = nextUsername;
      changed++;
    }
  }

  if (changed) saveDB(db);
  return changed;
}

async function renderGraphicTierlistPng(client = null) {
  if (!PImage) throw new Error('Не найден модуль pureimage. Установи: npm i pureimage');
  if (!ensureGraphicFonts()) throw new Error(`Не удалось загрузить системный шрифт для PNG. source=${GRAPHIC_FONT_INFO.source || "none"}. ${GRAPHIC_FONT_INFO.loadError || ""}`.trim());

  const state = getGraphicTierlistState();
  const buckets = buildGraphicBucketsFromRatings();
  const entries = Object.values(db.ratings || {});
  const { W, H: H_CFG, ICON } = getGraphicImageConfig();

  const topY = 120;
  const leftW = Math.floor(W * 0.24);
  const rightPadding = 36;
  const gap = Math.max(10, Math.floor(ICON * 0.16));
  const overlayH = Math.max(24, Math.floor(ICON * 0.24));
  const rightW = W - leftW - rightPadding - 24;
  const cols = Math.max(1, Math.floor((rightW + gap) / (ICON + gap)));

  const rowHeights = GRAPHIC_TIER_ORDER.map((tierKey) => {
    const n = (buckets[tierKey] || []).length;
    const rowsNeeded = Math.max(1, Math.ceil(n / cols));
    const iconsH = rowsNeeded * (ICON + gap) - gap;
    const needed = 18 + iconsH + 22 + 12;
    return Math.max(needed, 160);
  });

  const footerH = 44;
  const neededH = topY + rowHeights.reduce((a, b) => a + b, 0) + footerH;
  const H = Math.max(H_CFG, neededH);

  const img = PImage.make(W, H);
  const ctx = img.getContext('2d');

  fillColor(ctx, '#242424');
  ctx.fillRect(0, 0, W, H);

  fillColor(ctx, '#ffffff');
  setGraphicFont(ctx, 64, "bold");
  ctx.fillText(state.title || GRAPHIC_TIERLIST_TITLE, 40, 82);

  fillColor(ctx, '#cfcfcf');
  setGraphicFont(ctx, 22, "regular");
  ctx.fillText(`players: ${entries.length}. updated: ${new Date().toLocaleString('ru-RU')}`, 40, H - 18);

  let yCursor = topY;

  for (let i = 0; i < GRAPHIC_TIER_ORDER.length; i++) {
    const tierKey = GRAPHIC_TIER_ORDER[i];
    const y = yCursor;
    const rowH = rowHeights[i];
    yCursor += rowH;

    fillColor(ctx, '#2f2f2f');
    ctx.fillRect(leftW, y, W - leftW - rightPadding, rowH - 12);

    fillColor(ctx, state.tierColors?.[tierKey] || '#cccccc');
    ctx.fillRect(40, y, leftW - 40, rowH - 12);

    const blockH = rowH - 12;
    const labelX = 40 + 56;
    const labelW = (leftW - 40) - 56 - 18;
    const bottomLabelY = y + blockH - 18;
    const titleBoxY = y + 16;
    const titleBoxH = Math.max(44, bottomLabelY - titleBoxY - 18);

    drawGraphicTierTitle(ctx, formatTierTitle(tierKey), labelX, titleBoxY, labelW, titleBoxH);

    fillColor(ctx, '#111111');
    setGraphicFont(ctx, 24, "regular");
    ctx.fillText(`TIER ${tierKey}`, labelX, bottomLabelY);

    const list = buckets[tierKey] || [];
    const rightX = leftW + 24;
    const rightY = y + 18;

    for (let idx = 0; idx < list.length; idx++) {
      const player = list[idx];
      const col = idx % cols;
      const row = Math.floor(idx / cols);
      const x = rightX + col * (ICON + gap);
      const yy = rightY + row * (ICON + gap);

      const avatar = await loadGraphicAvatarForPlayer(client, player);

      fillColor(ctx, '#171717');
      ctx.fillRect(x - 3, yy - 3, ICON + 6, ICON + 6);

      if (avatar) {
        ctx.drawImage(avatar, x, yy, ICON, ICON);
      } else {
        fillColor(ctx, '#555555');
        ctx.fillRect(x, yy, ICON, ICON);
        fillColor(ctx, '#f3f3f3');
        setGraphicFont(ctx, Math.max(18, Math.floor(ICON * 0.28)), "bold");
        const initials = String(player.name || "?").trim().split(/\s+/).slice(0, 2).map(s => s[0] || "").join("").toUpperCase() || "?";
        const ix = x + Math.max(10, Math.floor((ICON - (initials.length * Math.max(14, Math.floor(ICON * 0.16)))) / 2));
        const iy = yy + Math.floor(ICON / 2) + Math.max(8, Math.floor(ICON * 0.08));
        ctx.fillText(initials, ix, iy);
      }

      const usernameBarH = Math.max(22, Math.floor(ICON * 0.24));
      ctx.fillStyle = 'rgba(0,0,0,0.78)';
      ctx.fillRect(x, yy + ICON - usernameBarH, ICON, usernameBarH);

      const usernameFit = fitGraphicSingleLineText(
        ctx,
        String(player.username || player.name || player.userId || "").trim(),
        "bold",
        Math.max(10, ICON - 10),
        Math.max(11, Math.floor(ICON * 0.18)),
        10
      );
      setGraphicFont(ctx, usernameFit.px, "bold");
      ctx.fillStyle = 'rgba(255,255,255,0.98)';
      const usernameY = yy + ICON - Math.max(6, Math.floor((usernameBarH - usernameFit.px) / 2)) - 1;
      ctx.fillText(usernameFit.text, centerGraphicTextX(ctx, usernameFit.text, x, ICON), usernameY);

      const eloText = String(player.elo || 0);
      const eloPx = Math.max(18, Math.floor(ICON * 0.22));
      setGraphicFont(ctx, eloPx, "bold");
      const eloW = measureGraphicTextWidth(ctx, eloText);
      const eloX = x + ICON - eloW - 8;
      const eloY = yy + eloPx + 8;
      drawGraphicOutlinedText(ctx, eloText, eloX, eloY, "#ffffff", "#000000");
    }
  }

  const chunks = [];
  const stream = new PassThrough();
  stream.on('data', c => chunks.push(c));
  await PImage.encodePNGToStream(img, stream);
  stream.end();
  return Buffer.concat(chunks);
}

function buildGraphicDashboardComponents() {
  return [new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('graphic_refresh').setLabel('Обновить PNG').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel').setLabel('PNG панель').setStyle(ButtonStyle.Primary)
  )];
}

async function ensureGraphicTierlistMessage(client, forcedChannelId = null) {
  const state = getGraphicTierlistState();
  const channelId = forcedChannelId || state.dashboardChannelId || GRAPHIC_TIERLIST_CHANNEL_ID;
  if (!channelId) return null;

  const channel = await client.channels.fetch(channelId).catch(() => null);
  if (!channel?.isTextBased()) throw new Error('GRAPHIC_TIERLIST_CHANNEL_ID: не текстовый канал');

  let msg = null;
  if (state.dashboardMessageId) {
    try { msg = await channel.messages.fetch(state.dashboardMessageId); } catch {}
  }

  await hydrateGraphicAvatarUrls(client).catch(() => 0);
  await hydrateGraphicUsernames(client).catch(() => 0);
  const png = await renderGraphicTierlistPng(client);
  const attachment = new AttachmentBuilder(png, { name: 'elo-tierlist.png' });
  const embed = new EmbedBuilder()
    .setTitle(state.title || GRAPHIC_TIERLIST_TITLE)
    .setDescription(getGraphicDashboardEmbedDescription())
    .setImage('attachment://elo-tierlist.png');

  if (!msg) {
    msg = await channel.send({ embeds: [embed], files: [attachment], components: buildGraphicDashboardComponents() });
    try { await msg.pin(); } catch {}
    state.dashboardMessageId = msg.id;
  } else {
    await msg.edit({ embeds: [embed], files: [attachment], components: buildGraphicDashboardComponents(), attachments: [] });
  }

  state.dashboardChannelId = channelId;
  state.lastUpdated = Date.now();
  saveDB(db);
  return msg;
}

async function refreshGraphicTierlist(client) {
  const state = getGraphicTierlistState();
  if (!state.dashboardChannelId || !state.dashboardMessageId) {
    if (GRAPHIC_TIERLIST_CHANNEL_ID) {
      await ensureGraphicTierlistMessage(client, GRAPHIC_TIERLIST_CHANNEL_ID);
      return true;
    }
    return false;
  }

  const channel = await client.channels.fetch(state.dashboardChannelId).catch(() => null);
  if (!channel?.isTextBased()) return false;

  let msg = null;
  try { msg = await channel.messages.fetch(state.dashboardMessageId); } catch {}
  if (!msg) {
    await ensureGraphicTierlistMessage(client, state.dashboardChannelId);
    return true;
  }

  await hydrateGraphicAvatarUrls(client).catch(() => 0);
  await hydrateGraphicUsernames(client).catch(() => 0);
  const png = await renderGraphicTierlistPng(client);
  const attachment = new AttachmentBuilder(png, { name: 'elo-tierlist.png' });
  const embed = new EmbedBuilder()
    .setTitle(state.title || GRAPHIC_TIERLIST_TITLE)
    .setDescription(getGraphicDashboardEmbedDescription())
    .setImage('attachment://elo-tierlist.png');

  await msg.edit({ embeds: [embed], files: [attachment], components: buildGraphicDashboardComponents(), attachments: [] });
  state.lastUpdated = Date.now();
  saveDB(db);
  return true;
}


async function bumpGraphicTierlist(client) {
  const state = getGraphicTierlistState();
  const channelId = state.dashboardChannelId || GRAPHIC_TIERLIST_CHANNEL_ID;
  if (!channelId) return false;

  const channel = await client.channels.fetch(channelId).catch(() => null);
  if (!channel?.isTextBased()) return false;

  await hydrateGraphicUsernames(client).catch(() => 0);
  const png = await renderGraphicTierlistPng();
  const attachment = new AttachmentBuilder(png, { name: 'elo-tierlist.png' });
  const embed = new EmbedBuilder()
    .setTitle(state.title || GRAPHIC_TIERLIST_TITLE)
    .setDescription(getGraphicDashboardEmbedDescription())
    .setImage('attachment://elo-tierlist.png');

  const oldMessageId = state.dashboardMessageId || "";
  const msg = await channel.send({ embeds: [embed], files: [attachment], components: buildGraphicDashboardComponents() });

  state.dashboardChannelId = channel.id;
  state.dashboardMessageId = msg.id;
  state.lastUpdated = Date.now();
  saveDB(db);

  if (oldMessageId && oldMessageId !== msg.id) {
    const oldMsg = await channel.messages.fetch(oldMessageId).catch(() => null);
    if (oldMsg) await oldMsg.delete().catch(() => {});
  }
  return true;
}
function buildGraphicPanelTierSelect() {
  const graphic = getGraphicTierlistState();
  const selected = Number(graphic.panel?.selectedTier) || 5;
  const menu = new StringSelectMenuBuilder()
    .setCustomId('graphic_panel_select_tier')
    .setPlaceholder('Выбери тир для будущей настройки')
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(
      GRAPHIC_TIER_ORDER.map((t) => ({ label: `Tier ${t}`, value: String(t), default: selected === t }))
    );
  return new ActionRowBuilder().addComponents(menu);
}

function buildGraphicPanelPayload() {
  const graphic = getGraphicTierlistState();
  const cfg = getGraphicImageConfig();
  const selectedTier = Number(graphic.panel?.selectedTier) || 5;
  const tierLabel = formatTierTitle(selectedTier);
  const tierColor = graphic.tierColors?.[selectedTier] || DEFAULT_GRAPHIC_TIER_COLORS[selectedTier] || "#cccccc";

  const e = new EmbedBuilder()
    .setTitle('PNG Panel')
    .setDescription([
      `**Title:** ${graphic.title || GRAPHIC_TIERLIST_TITLE}`,
      `**Канал:** ${graphic.dashboardChannelId ? `<#${graphic.dashboardChannelId}>` : 'не задан'}`,
      `**Message ID:** ${graphic.dashboardMessageId || '—'}`,
      `**Картинка:** ${cfg.W}×${cfg.H}`,
      `**Иконки:** ${cfg.ICON}px`,
      `**Выбранный тир:** ${selectedTier} → **${tierLabel}**`,
      `**Цвет тира:** ${tierColor}`,
      `**Текст сообщения:** ${previewGraphicMessageText(170)}`,
      '',
      'Панель меняет только PNG-контур и связанные подписи и цвета. Текстовый tierlist-канал отключён и больше не используется.'
    ].join('\n'));

  const row1 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('graphic_panel_refresh').setLabel('Пересобрать').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel_title').setLabel('Название PNG').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('graphic_panel_message_text').setLabel('Текст сообщения').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('graphic_panel_rename').setLabel('Переименовать тир').setStyle(ButtonStyle.Primary)
  );

  const row2 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('graphic_panel_icon_minus').setLabel('Иконки -').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel_icon_plus').setLabel('Иконки +').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel_w_minus').setLabel('Ширина -').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel_w_plus').setLabel('Ширина +').setStyle(ButtonStyle.Secondary)
  );

  const row3 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('graphic_panel_h_minus').setLabel('Высота -').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel_h_plus').setLabel('Высота +').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel_set_color').setLabel('Цвет тира').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('graphic_panel_reset_color').setLabel('Сброс цвета тира').setStyle(ButtonStyle.Secondary)
  );

  const row4 = buildGraphicPanelTierSelect();

  const row5 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('graphic_panel_reset_img').setLabel('Сбросить размеры').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel_reset_colors').setLabel('Сбросить все цвета').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel_clear_cache').setLabel('Сбросить кэш ав').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel_fonts').setLabel('Шрифты').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('graphic_panel_close').setLabel('Закрыть').setStyle(ButtonStyle.Danger)
  );

  return { embeds: [e], components: [row1, row2, row3, row4, row5] };
}

function hoursSince(iso) {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return 999999;
  return (Date.now() - t) / 36e5;
}

async function logLine(client, text) {
  if (!LOG_CHANNEL_ID) return;
  const ch = await client.channels.fetch(LOG_CHANNEL_ID).catch(() => null);
  if (ch?.isTextBased()) await ch.send(text).catch(() => {});
}

async function dmUser(client, userId, text) {
  try {
    const user = await client.users.fetch(userId);
    await user.send(text);
  } catch {}
}

const submitSessions = new Map();

function getSubmitPanelState() {
  db.config.submitPanel ||= { channelId: SUBMIT_CHANNEL_ID || "", messageId: "" };
  if (!db.config.submitPanel.channelId && SUBMIT_CHANNEL_ID) db.config.submitPanel.channelId = SUBMIT_CHANNEL_ID;
  return db.config.submitPanel;
}

function scheduleDeleteMessage(msg, ms = SUBMIT_PUBLIC_DELETE_MS) {
  if (!msg) return;
  setTimeout(() => {
    msg.delete().catch(() => {});
  }, ms);
}

function scheduleDeleteInteractionReply(interaction, ms = SUBMIT_UI_DELETE_MS) {
  setTimeout(() => {
    interaction.deleteReply().catch(() => {});
  }, ms);
}

function getSubmitCooldownLeftSeconds(userId) {
  const last = db.cooldowns[userId] || 0;
  return Math.max(0, SUBMIT_COOLDOWN_SECONDS - Math.floor((Date.now() - last) / 1000));
}

function getPendingSubmissionForUser(userId) {
  return Object.values(db.submissions || {}).find((s) => s.userId === userId && s.status === "pending") || null;
}

function getActiveSubmitSession(userId) {
  const session = submitSessions.get(userId);
  if (!session) return null;
  if ((Date.now() - Number(session.createdAt || 0)) > SUBMIT_SESSION_EXPIRE_MS) {
    submitSessions.delete(userId);
    return null;
  }
  return session;
}

function setSubmitSession(userId, data) {
  submitSessions.set(userId, { ...data, createdAt: Date.now() });
}

function clearSubmitSession(userId) {
  submitSessions.delete(userId);
}

function isLikelyImageUrl(url) {
  if (!url) return false;
  try {
    const u = new URL(url);
    if (!/^https?:$/i.test(u.protocol)) return false;
    const full = `${u.pathname}${u.search}`;
    return isDiscordCdnUrl(u.toString()) || /\.(png|jpe?g|webp|gif)(\?|$)/i.test(full);
  } catch {
    return false;
  }
}

function getSubmitEligibilityError(userId, rawText = null) {
  const pending = getPendingSubmissionForUser(userId);
  if (pending) return "У тебя уже есть заявка на проверке. Дождись решения модера.";

  const cooldownLeft = getSubmitCooldownLeftSeconds(userId);
  if (cooldownLeft > 0) return `Кулдаун. Подожди ${cooldownLeft} сек и попробуй снова.`;

  if (rawText !== null) {
    const elo = parseElo(rawText);
    const tier = elo ? tierFor(elo) : null;
    if (!elo || !tier) return "Нужен текст с числом ELO минимум 10. Пример: `73`";

    const current = db.ratings[userId];
    if (current && Number(current.elo) === Number(elo)) {
      return "У тебя уже стоит такой же ELO в тир-листе. Если изменится — присылай новый скрин.";
    }
  }

  return null;
}

function buildSubmitHubEmbed() {
  return new EmbedBuilder()
    .setTitle("ELO заявки")
    .setDescription([
      "Жми **Отправить заявку ELO** и сразу вводи текст с числом ELO.",
      "После этого просто отправь **следующим сообщением** скрин с подтверждением(экран в ранкед) в этот канал.",
    ].join("\n"));
}

function buildSubmitHubComponents() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId("elo_submit_open").setLabel("Отправить заявку ELO").setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId("elo_submit_card").setLabel("Моя карточка").setStyle(ButtonStyle.Secondary)
    )
  ];
}

function formatRuDateTime(value) {
  if (!value) return null;
  const ts = Date.parse(value);
  if (!Number.isFinite(ts)) return null;
  return new Date(ts).toLocaleString("ru-RU");
}

function getLatestSubmissionForUser(userId, allowedStatuses = null) {
  const list = Object.values(db.submissions || {}).filter((sub) => {
    if (!sub || sub.userId !== userId) return false;
    if (!allowedStatuses || !allowedStatuses.length) return true;
    return allowedStatuses.includes(sub.status);
  });

  list.sort((a, b) => {
    const ta = Date.parse(b.reviewedAt || b.createdAt || 0) || 0;
    const tb = Date.parse(a.reviewedAt || a.createdAt || 0) || 0;
    return ta - tb;
  });

  return list[0] || null;
}

async function fetchSubmissionProofUrl(client, sub, fallbackUrl = "") {
  if (!sub) return fallbackUrl || "";
  if (sub.reviewAttachmentUrl) return sub.reviewAttachmentUrl;

  const msg = await fetchReviewMessage(client, sub).catch(() => null);
  const attUrl = msg?.attachments?.first?.()?.url || "";
  if (attUrl) {
    sub.reviewAttachmentUrl = attUrl;
    if (!sub.reviewImage || String(sub.reviewImage).startsWith("attachment://")) {
      sub.reviewImage = attUrl;
    }
    saveDB(db);
    return attUrl;
  }

  return fallbackUrl || sub.screenshotUrl || "";
}

async function buildMyCardPayload(client, userId) {
  const rating = db.ratings[userId];
  const pending = getPendingSubmissionForUser(userId);
  const session = getActiveSubmitSession(userId);

  if (rating) {
    const approvedSub = getLatestSubmissionForUser(userId, ["approved"]);
    const proofUrl = await fetchSubmissionProofUrl(client, approvedSub, rating.proofUrl || "");
    const embed = new EmbedBuilder()
      .setTitle("Моя ELO карточка")
      .setDescription([
        "Статус: **в тир-листе**",
        `ELO: **${rating.elo}**`,
        `Тир: **${rating.tier}** (${formatTierTitle(rating.tier)})`,
        rating.updatedAt ? `Обновлено: **${formatRuDateTime(rating.updatedAt)}**` : null,
        proofUrl ? `[Открыть скрин](${proofUrl})` : null
      ].filter(Boolean).join("\n"));

    if (rating.avatarUrl) embed.setThumbnail(rating.avatarUrl);
    if (proofUrl) embed.setImage(proofUrl);
    return { embeds: [embed], ephemeral: true };
  }

  if (pending) {
    const proofUrl = await fetchSubmissionProofUrl(client, pending, pending.screenshotUrl || "");
    const embed = new EmbedBuilder()
      .setTitle("Моя ELO карточка")
      .setDescription([
        "Статус: **заявка на проверке**",
        `ELO: **${pending.elo}**`,
        `Тир по числу: **${pending.tier}** (${formatTierTitle(pending.tier)})`,
        `ID: \`${pending.id}\``,
        pending.createdAt ? `Создано: **${formatRuDateTime(pending.createdAt)}**` : null,
        proofUrl ? `[Открыть скрин](${proofUrl})` : null
      ].filter(Boolean).join("\n"));

    if (proofUrl) embed.setImage(proofUrl);
    return { embeds: [embed], ephemeral: true };
  }

  if (session) {
    return {
      content: "Текст уже принят. Теперь просто отправь **одним следующим сообщением** скрин в этот канал. Можно вставить картинку из буфера через **Ctrl+V**.",
      ephemeral: true
    };
  }

  return {
    content: "Тебя пока нет в тир-листе и активной заявки тоже нет.",
    ephemeral: true
  };
}

async function ensureSubmitHubMessage(client, forcedChannelId = null) {
  const state = getSubmitPanelState();
  const channelId = forcedChannelId || state.channelId || SUBMIT_CHANNEL_ID;
  if (!channelId) return null;

  const channel = await client.channels.fetch(channelId).catch(() => null);
  if (!channel?.isTextBased()) throw new Error("SUBMIT_CHANNEL_ID: не текстовый канал");

  let msg = null;
  if (state.messageId) {
    try { msg = await channel.messages.fetch(state.messageId); } catch {}
  }

  const payload = {
    embeds: [buildSubmitHubEmbed()],
    components: buildSubmitHubComponents()
  };

  if (!msg) {
    msg = await channel.send(payload);
    try { await msg.pin(); } catch {}
    state.messageId = msg.id;
  } else {
    await msg.edit(payload).catch(() => {});
  }

  state.channelId = channelId;
  saveDB(db);
  return msg;
}

async function repostSubmitHubMessage(client) {
  const state = getSubmitPanelState();
  const channelId = state.channelId || SUBMIT_CHANNEL_ID;
  if (!channelId) return null;

  const channel = await client.channels.fetch(channelId).catch(() => null);
  if (!channel?.isTextBased()) return null;

  // Удаляем старое сообщение, если оно есть
  if (state.messageId) {
    const oldMsg = await channel.messages.fetch(state.messageId).catch(() => null);
    if (oldMsg) await oldMsg.delete().catch(() => {});
    state.messageId = "";
  }

  // Отправляем новое сообщение (оно окажется внизу канала)
  const payload = {
    embeds: [buildSubmitHubEmbed()],
    components: buildSubmitHubComponents()
  };
  const msg = await channel.send(payload).catch(() => null);
  if (!msg) return null;

  state.messageId = msg.id;
  state.channelId = channelId;
  saveDB(db);
  return msg;
}

async function createPendingSubmissionFromUrl(client, { user, member, rawText, screenshotUrl, messageUrl }) {
  const elo = parseElo(rawText);
  const tier = elo ? tierFor(elo) : null;
  if (!screenshotUrl || !elo || !tier) {
    throw new Error("Нужен скрин и число ELO минимум 10.");
  }

  const submissionId = makeId();
  let reviewFile = null;
  let reviewImage = screenshotUrl;
  let reviewFileName = null;

  try {
    const buf = await downloadToBuffer(screenshotUrl);
    reviewFileName = sanitizeFileName(`${submissionId}_screenshot`);
    reviewFile = new AttachmentBuilder(buf, { name: reviewFileName });
    reviewImage = `attachment://${reviewFileName}`;
  } catch {
    reviewFile = null;
    reviewImage = screenshotUrl;
    reviewFileName = null;
  }

  const prevCooldown = db.cooldowns[user.id] || 0;
  db.submissions[submissionId] = {
    id: submissionId,
    userId: user.id,
    name: member?.displayName || user.username,
    elo,
    tier,
    screenshotUrl,
    reviewImage,
    reviewFileName,
    messageUrl: messageUrl || screenshotUrl,
    status: "pending",
    createdAt: new Date().toISOString(),
    reviewChannelId: null,
    reviewMessageId: null,
  };

  db.cooldowns[user.id] = Date.now();
  saveDB(db);

  const sub = db.submissions[submissionId];
  const sent = await postReviewRecord(client, sub, reviewFile, "pending", [], [buildReviewButtons(submissionId)]);
  if (sent?.attachments?.first?.()?.url) {
    sub.reviewAttachmentUrl = sent.attachments.first().url;
    if (!sub.reviewImage || String(sub.reviewImage).startsWith("attachment://")) {
      sub.reviewImage = sub.reviewAttachmentUrl;
    }
  }
  if (!sent) {
    delete db.submissions[submissionId];
    if (prevCooldown) db.cooldowns[user.id] = prevCooldown;
    else delete db.cooldowns[user.id];
    saveDB(db);
    throw new Error("Не удалось отправить заявку в review-канал.");
  }

  saveDB(db);
  return sub;
}

async function fetchReviewMessage(client, sub) {
  if (!sub.reviewChannelId || !sub.reviewMessageId) return null;
  const ch = await client.channels.fetch(sub.reviewChannelId).catch(() => null);
  if (!ch?.isTextBased()) return null;
  const msg = await ch.messages.fetch(sub.reviewMessageId).catch(() => null);
  return msg;
}

async function supersedePendingSubmissionsForUser(client, userId, moderatorTag) {
  let changed = 0;
  for (const sub of Object.values(db.submissions || {})) {
    if (!sub || sub.userId !== userId || sub.status !== "pending") continue;
    sub.status = "superseded";
    sub.reviewedBy = moderatorTag;
    sub.reviewedAt = new Date().toISOString();
    sub.rejectReason = "Добавлено/обновлено модератором напрямую";
    changed++;
    const msg = await fetchReviewMessage(client, sub);
    if (msg) {
      await msg.edit({
        embeds: [buildReviewEmbed(sub, "superseded", [{ name: "Причина", value: sub.rejectReason, inline: false }])],
        components: [],
      }).catch(() => {});
    }
  }
  if (changed) saveDB(db);
  return changed;
}

async function upsertRatingDirect(client, targetUser, screenshotAttachment, rawText, moderatorTag) {
  if (!targetUser) throw new Error("target user is required");
  if (!screenshotAttachment || !isImageAttachment(screenshotAttachment)) {
    throw new Error("Нужен скрин-картинка.");
  }

  const elo = parseElo(rawText);
  const tier = elo ? tierFor(elo) : null;
  if (!elo || !tier) {
    throw new Error("Нужно число ELO минимум 10.");
  }

  const guild = await getGuild(client).catch(() => null);
  const member = guild ? await guild.members.fetch(targetUser.id).catch(() => null) : null;
  const rating = db.ratings[targetUser.id] || { userId: targetUser.id };

  rating.userId = targetUser.id;
  rating.name = member?.displayName || targetUser.username;
  rating.username = targetUser.username;
  rating.elo = elo;
  rating.tier = tier;
  rating.proofUrl = screenshotAttachment.url;
  rating.avatarUrl = normalizeDiscordAvatarUrl(
    (member?.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 })) ||
    targetUser.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 }) ||
    targetUser.defaultAvatarURL ||
    ""
  );
  rating.updatedAt = new Date().toISOString();

  db.ratings[targetUser.id] = rating;
  saveDB(db);

  await loadGraphicAvatarForPlayer(client, rating).catch(() => null);
  await refreshGraphicTierlist(client).catch(() => false);
  await ensureSingleTierRole(client, targetUser.id, tier, "Manual tier set by moderator");
  await supersedePendingSubmissionsForUser(client, targetUser.id, moderatorTag);
  const manualSub = await createManualApprovedReviewRecord(client, targetUser, screenshotAttachment, elo, tier, moderatorTag).catch(() => null);
  if (manualSub?.reviewAttachmentUrl) {
    rating.proofUrl = manualSub.reviewAttachmentUrl;
    db.ratings[targetUser.id] = rating;
  }
  saveDB(db);

  return rating;
}


// ====== MINI CARDS (SUBMIT CHANNEL) ======
// Отключено: в канале подачи больше не создаём карточки.
function buildMiniCardEmbed() {
  return null;
}

async function upsertMiniCardMessage(client, rating) {
  void client;
  void rating;
  return { changed: false };
}

async function deleteMiniCardMessage(client, userId) {
  const msgId = (db.miniCards || {})[userId];
  if (!msgId) {
    if (db.miniCards && (userId in db.miniCards)) {
      delete db.miniCards[userId];
      saveDB(db);
    }
    return false;
  }

  const ch = await client.channels.fetch(SUBMIT_CHANNEL_ID).catch(() => null);
  if (ch?.isTextBased()) {
    const msg = await ch.messages.fetch(msgId).catch(() => null);
    if (msg) await msg.delete().catch(() => {});
  }

  delete db.miniCards[userId];
  saveDB(db);
  return true;
}

async function syncMiniCards(client) {
  db.miniCards ||= {};
  let removed = 0;

  for (const uid of Object.keys(db.miniCards)) {
    const ok = await deleteMiniCardMessage(client, uid);
    if (ok) removed++;
  }

  db.miniCards = {};
  saveDB(db);
  return { created: 0, removed, total: Object.keys(db.ratings || {}).length };
}


async function postReviewRecord(client, sub, fileAttachment = null, statusLabel = null, extraFields = [], components = []) {
  const reviewChannel = await client.channels.fetch(REVIEW_CHANNEL_ID).catch(() => null);
  if (!reviewChannel || !reviewChannel.isTextBased()) return null;

  const payload = {
    embeds: [buildReviewEmbed(sub, statusLabel || sub.status || "pending", extraFields)],
    components,
  };
  if (fileAttachment) payload.files = [fileAttachment];

  const sent = await reviewChannel.send(payload);
  sub.reviewChannelId = sent.channel.id;
  sub.reviewMessageId = sent.id;
  return sent;
}

async function createManualApprovedReviewRecord(client, targetUser, screenshotAttachment, elo, tier, moderatorTag) {
  const submissionId = makeId();
  const guild = await getGuild(client).catch(() => null);
  const member = guild ? await guild.members.fetch(targetUser.id).catch(() => null) : null;

  let reviewFile = null;
  let reviewImage = screenshotAttachment.url;
  let reviewFileName = null;
  try {
    const buf = await downloadToBuffer(screenshotAttachment.url);
    reviewFileName = sanitizeFileName(`${submissionId}_${screenshotAttachment.name || "screenshot"}`);
    reviewFile = new AttachmentBuilder(buf, { name: reviewFileName });
    reviewImage = `attachment://${reviewFileName}`;
  } catch {}

  const sub = {
    id: submissionId,
    userId: targetUser.id,
    name: member?.displayName || targetUser.username,
    elo,
    tier,
    screenshotUrl: screenshotAttachment.url,
    reviewImage,
    reviewFileName,
    messageUrl: screenshotAttachment.url,
    status: "approved",
    createdAt: new Date().toISOString(),
    reviewedBy: moderatorTag,
    reviewedAt: new Date().toISOString(),
    reviewChannelId: null,
    reviewMessageId: null,
    manual: true,
  };

  db.submissions[submissionId] = sub;
  saveDB(db);
  const sent = await postReviewRecord(
    client,
    sub,
    reviewFile,
    "approved",
    [{ name: "Источник", value: `Ручное добавление модератором: ${moderatorTag}`, inline: false }],
    []
  );
  if (sent?.attachments?.first?.()?.url) {
    sub.reviewAttachmentUrl = sent.attachments.first().url;
    if (!sub.reviewImage || String(sub.reviewImage).startsWith("attachment://")) {
      sub.reviewImage = sub.reviewAttachmentUrl;
    }
  }
  saveDB(db);
  return sub;
}

// ====== LEGACY TEXT TIERLIST CLEANUP ======
function cleanupLegacyTextTierlistState() {
  let clearedCards = 0;
  let clearedIndexLink = false;

  if (db.config.indexMessageId) {
    db.config.indexMessageId = "";
    clearedIndexLink = true;
  }

  for (const rating of Object.values(db.ratings || {})) {
    if (!rating || !Object.prototype.hasOwnProperty.call(rating, "cardMessageId")) continue;
    delete rating.cardMessageId;
    clearedCards++;
  }

  if (clearedIndexLink || clearedCards) saveDB(db);
  return { clearedCards, clearedIndexLink };
}

async function rebuildEloTierlist(client) {
  const ids = Object.keys(db.ratings || {});
  let total = 0;
  let retiered = 0;
  let hidden = 0;
  let rolesSynced = 0;

  const cleanup = cleanupLegacyTextTierlistState();

  for (const uid of ids) {
    const rating = db.ratings[uid];
    if (!rating) continue;
    total++;

    const elo = Number(rating.elo) || 0;
    const prevTier = Number.isFinite(Number(rating.tier)) ? Number(rating.tier) : null;
    const nextTier = tierFor(elo);

    if (prevTier !== nextTier) retiered++;
    rating.tier = nextTier;
    rating.updatedAt = new Date().toISOString();

    if (!nextTier) {
      hidden++;
      await clearAllTierRoles(client, uid, "Rebuild invalid rating");
      rolesSynced++;
      continue;
    }

    await ensureSingleTierRole(client, uid, nextTier, "Rebuild retier");
    rolesSynced++;
  }

  saveDB(db);
  const pngUpdated = await refreshGraphicTierlist(client).catch(() => false);
  saveDB(db);

  return { total, retiered, hidden, rolesSynced, pngUpdated, cleanup };
}

// ====== REVIEW UI ======
function buildReviewEmbed(sub, statusLabel, extraFields = []) {
  const e = new EmbedBuilder()
    .setTitle(`ELO заявка (${statusLabel})`)
    .setDescription(
      `Игрок: <@${sub.userId}> (${sub.name})\n` +
      `ELO: **${sub.elo}**\n` +
      `Тир (по числу): **${sub.tier}**\n` +
      `Сообщение: [link](${sub.messageUrl})\n` +
      `ID: \`${sub.id}\``
    )
    // Главное: в review показываем через attachment://..., если мы перезалили файл.
    .setImage(sub.reviewImage || sub.screenshotUrl);

  if (extraFields.length) e.addFields(...extraFields);
  return e;
}

function buildReviewButtons(subId) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`approve:${subId}`).setLabel("Approve").setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId(`edit:${subId}`).setLabel("Edit ELO").setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId(`reject:${subId}`).setLabel("Reject").setStyle(ButtonStyle.Danger)
  );
}

// ====== SLASH COMMANDS ======
function buildCommands() {
  return [
    new SlashCommandBuilder()
      .setName("elo")
      .setDescription("ELO tierlist commands")
      .addSubcommand(s => s.setName("me").setDescription("Показать мой рейтинг"))
      .addSubcommand(s => s.setName("user").setDescription("Показать рейтинг игрока")
        .addUserOption(o => o.setName("target").setDescription("Игрок").setRequired(true)))
      .addSubcommand(s => s.setName("pending").setDescription("Показать pending заявки (модеры)"))
      .addSubcommand(s => s.setName("rebuild").setDescription("Пересчитать тиры, роли и PNG тир-лист (модеры)"))
      .addSubcommand(s => s.setName("graphicsetup").setDescription("Создать/пересоздать PNG тир-лист в отдельном канале (модеры)")
        .addChannelOption(o => o.setName("channel").setDescription("Канал для PNG тир-листа").setRequired(true)))
      .addSubcommand(s => s.setName("graphicrebuild").setDescription("Пересобрать PNG тир-лист (модеры)"))
      .addSubcommand(s => s.setName("graphicbump").setDescription("Отправить PNG тир-лист заново вниз канала и удалить старое сообщение (модеры)"))
      .addSubcommand(s => s.setName("graphicstatus").setDescription("Статус PNG тир-листа (модеры)"))
      .addSubcommand(s => s.setName("graphicpanel").setDescription("Панель PNG тир-листа (модеры)"))
      .addSubcommand(s => s.setName("remove").setDescription("Удалить игрока из тир-листа (модеры)")
        .addUserOption(o => o.setName("target").setDescription("Игрок").setRequired(true)))
      .addSubcommand(s => s.setName("modset").setDescription("Добавить или обновить игрока напрямую (модеры)")
        .addUserOption(o => o.setName("target").setDescription("Игрок").setRequired(true))
        .addAttachmentOption(o => o.setName("screenshot").setDescription("Скрин-пруф").setRequired(true))
        .addStringOption(o => o.setName("text").setDescription("Текст после юзернейма, из него берётся ELO").setRequired(true)))
      .addSubcommand(s => s.setName("wipe").setDescription("Очистить рейтинг полностью (модеры)")
        .addStringOption(o => o.setName("mode").setDescription("Режим очистки. После stage 2 оба режима чистят рейтинг без review-карточек").setRequired(true)
          .addChoices(
            { name: "soft", value: "soft" },
            { name: "hard", value: "hard" }
          ))
        .addStringOption(o => o.setName("confirm").setDescription('Напиши WIPE чтобы подтвердить').setRequired(true)))
      .addSubcommand(s => s.setName("labels").setDescription("Поменять названия тиров (модеры)")
        .addStringOption(o => o.setName("t1").setDescription("Название тира 1").setRequired(true))
        .addStringOption(o => o.setName("t2").setDescription("Название тира 2").setRequired(true))
        .addStringOption(o => o.setName("t3").setDescription("Название тира 3").setRequired(true))
        .addStringOption(o => o.setName("t4").setDescription("Название тира 4").setRequired(true))
        .addStringOption(o => o.setName("t5").setDescription("Название тира 5").setRequired(true)))
  ].map(c => c.toJSON());
}

async function registerGuildCommands(client) {
  if (!GUILD_ID) throw new Error("Нет GUILD_ID в .env");
  const guild = await client.guilds.fetch(GUILD_ID);
  await guild.commands.set(buildCommands());
}

// ====== DISCORD CLIENT ======
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
  ],
});

client.once("ready", async () => {
  console.log(`Logged in as ${client.user.tag}`);

  // рег слэш-команд (guild, применяются быстро)
  await registerGuildCommands(client);

  cleanupLegacyTextTierlistState();
  await syncTierRolesOnStart(client);
  await syncMiniCards(client);
  try {
    const graphic = getGraphicTierlistState();
    if (graphic.dashboardChannelId || GRAPHIC_TIERLIST_CHANNEL_ID) {
      await ensureGraphicTierlistMessage(client, graphic.dashboardChannelId || GRAPHIC_TIERLIST_CHANNEL_ID);
    }
  } catch (e) {
    console.error("Graphic tierlist setup failed:", e?.message || e);
  }

  try {
    await ensureSubmitHubMessage(client, SUBMIT_CHANNEL_ID);
  } catch (e) {
    console.error("Submit panel setup failed:", e?.message || e);
  }

  // Переотправляем панель каждые 30 минут, чтобы она всегда была внизу канала
  setInterval(async () => {
    try {
      await repostSubmitHubMessage(client);
    } catch (e) {
      console.error("Submit panel resend failed:", e?.message || e);
    }
  }, SUBMIT_PANEL_RESEND_INTERVAL_MS);

  console.log("Ready");
});

// ====== SUBMIT CHANNEL ONLY ======
client.on("messageCreate", async (message) => {
  if (message.author.bot) return;
  if (message.channelId !== SUBMIT_CHANNEL_ID) return;

  const session = getActiveSubmitSession(message.author.id);
  if (!session) {
    const warn = await message.reply("Заявки тут теперь подаются только через кнопку **Отправить заявку ELO** ниже.").catch(() => null);
    if (warn) scheduleDeleteMessage(warn);
    await message.delete().catch(() => {});
    return;
  }

  const pending = getPendingSubmissionForUser(message.author.id);
  if (pending) {
    clearSubmitSession(message.author.id);
    const warn = await message.reply("У тебя уже есть заявка на проверке. Дождись решения модера.").catch(() => null);
    if (warn) scheduleDeleteMessage(warn);
    await message.delete().catch(() => {});
    return;
  }

  const attachment = message.attachments.first();
  if (!attachment || !isImageAttachment(attachment)) {
    const warn = await message.reply("Сейчас нужен **один следующий месседж именно с картинкой**. Можно просто вставить скрин из буфера через **Ctrl+V**. Текст уже сохранён.").catch(() => null);
    if (warn) scheduleDeleteMessage(warn);
    await message.delete().catch(() => {});
    return;
  }

  try {
    await createPendingSubmissionFromUrl(client, {
      user: message.author,
      member: message.member,
      rawText: session.rawText,
      screenshotUrl: attachment.url,
      messageUrl: message.url,
    });

    clearSubmitSession(message.author.id);
    const ok = await message.reply("Заявка отправлена на проверку модерам.").catch(() => null);
    if (ok) scheduleDeleteMessage(ok);
  } catch (err) {
    clearSubmitSession(message.author.id);
    const warn = await message.reply(String(err?.message || err || "Не удалось отправить заявку.")).catch(() => null);
    if (warn) scheduleDeleteMessage(warn, 12000);
  }

  await message.delete().catch(() => {});
});

// ====== INTERACTIONS: slash + buttons + modals ======
client.on("interactionCreate", async (interaction) => {
  // ---- SLASH ----
  if (interaction.isChatInputCommand()) {
    if (interaction.commandName !== "elo") return;

    const sub = interaction.options.getSubcommand();

    // /elo me
    if (sub === "me") {
      const r = db.ratings[interaction.user.id];
      if (!r) {
        await interaction.reply({ content: "Тебя нет в тир-листе.", ephemeral: true });
        return;
      }
      await interaction.reply({
        content: `Ты: <@${r.userId}>\nELO: **${r.elo}**\nТир: **${r.tier}** (${formatTierTitle(r.tier)})`,
        ephemeral: true,
      });
      return;
    }

    // /elo user
    if (sub === "user") {
      const target = interaction.options.getUser("target", true);
      const r = db.ratings[target.id];
      if (!r) {
        await interaction.reply({ content: "Этого игрока нет в тир-листе.", ephemeral: true });
        return;
      }
      await interaction.reply({
        content: `Игрок: <@${r.userId}> (${r.name})\nELO: **${r.elo}**\nТир: **${r.tier}** (${formatTierTitle(r.tier)})`,
        ephemeral: true,
      });
      return;
    }

    // mod-only from here
    if (!isModerator(interaction.member)) {
      await interaction.reply({ content: "Нет прав.", ephemeral: true });
      return;
    }

    // /elo pending
    if (sub === "pending") {
      const pend = Object.values(db.submissions)
        .filter(s => s.status === "pending")
        .sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt))
        .slice(0, 15);

      if (!pend.length) {
        await interaction.reply({ content: "Pending заявок нет.", ephemeral: true });
        return;
      }

      const lines = pend.map(s =>
        `• <@${s.userId}> ELO **${s.elo}** (id \`${s.id}\`)`
      );

      await interaction.reply({
        content: `Pending (${pend.length} из ${Object.values(db.submissions).filter(s=>s.status==="pending").length}):\n${lines.join("\n")}`,
        ephemeral: true,
      });
      return;
    }

    // /elo rebuild
    if (sub === "rebuild") {
      await interaction.deferReply({ ephemeral: true });
      const res = await rebuildEloTierlist(client);
      const lines = [
        `Готово. Проверено: ${res.total}`,
        `Сменили тир: ${res.retiered}`,
        `Скрыто как невалидные: ${res.hidden}`,
        `Роли синкнуты: ${res.rolesSynced}`,
        `Review-карточки не тронуты: да`,
        `Очищено legacy card links: ${res.cleanup?.clearedCards || 0}`,
        `Сброшен legacy index link: ${res.cleanup?.clearedIndexLink ? "да" : "нет"}`,
        `PNG: ${res.pngUpdated ? "обновлён" : "не настроен или пропущен"}`
      ];
      await interaction.editReply({ content: lines.join("\n") });
      return;
    }

    // /elo graphicsetup
    if (sub === "graphicsetup") {
      await interaction.deferReply({ ephemeral: true });
      const channel = interaction.options.getChannel("channel", true);
      const graphic = getGraphicTierlistState();
      graphic.dashboardChannelId = channel.id;
      saveDB(db);
      await ensureGraphicTierlistMessage(client, channel.id);
      await interaction.editReply({ content: `PNG тир-лист создан/обновлён в <#${channel.id}>.` });
      return;
    }

    // /elo graphicrebuild
    if (sub === "graphicrebuild") {
      await interaction.deferReply({ ephemeral: true });
      const ok = await refreshGraphicTierlist(client);
      await interaction.editReply({ content: ok ? "PNG тир-лист обновлён." : "PNG тир-лист ещё не настроен. Сначала /elo graphicsetup." });
      return;
    }

    // /elo graphicbump
    if (sub === "graphicbump") {
      await interaction.deferReply({ ephemeral: true });
      const ok = await bumpGraphicTierlist(client);
      await interaction.editReply({ content: ok ? "PNG тир-лист отправлен заново вниз канала. Старое сообщение удалено." : "PNG тир-лист ещё не настроен. Сначала /elo graphicsetup." });
      return;
    }

    // /elo graphicstatus
    if (sub === "graphicstatus") {
      const graphic = getGraphicTierlistState();
      const cfg = getGraphicImageConfig();
      const lines = [
        `title: ${graphic.title || GRAPHIC_TIERLIST_TITLE}`,
        `messageText: ${previewGraphicMessageText(120)}`,
        `channelId: ${graphic.dashboardChannelId || "—"}`,
        `messageId: ${graphic.dashboardMessageId || "—"}`,
        `img: ${cfg.W}x${cfg.H}, icon=${cfg.ICON}`,
        `selectedTier: ${graphic.panel?.selectedTier || 5} -> ${formatTierTitle(graphic.panel?.selectedTier || 5)}`,
        `tierColors: ${GRAPHIC_TIER_ORDER.map(t => `${t}=${graphic.tierColors?.[t] || DEFAULT_GRAPHIC_TIER_COLORS[t]}`).join(', ')}`,
        `lastUpdated: ${graphic.lastUpdated ? new Date(graphic.lastUpdated).toLocaleString("ru-RU") : "—"}`,
        `font regular: ${GRAPHIC_FONT_INFO.regularFile ? path.basename(GRAPHIC_FONT_INFO.regularFile) : "(none)"}`,
        `font bold: ${GRAPHIC_FONT_INFO.boldFile ? path.basename(GRAPHIC_FONT_INFO.boldFile) : "(none)"}`,
        `font source: ${GRAPHIC_FONT_INFO.source || "(none)"}`,
        `font error: ${GRAPHIC_FONT_INFO.loadError || "(none)"}`
      ];
      await interaction.reply({ content: lines.join("\n"), ephemeral: true });
      return;
    }

    // /elo graphicpanel
    if (sub === "graphicpanel") {
      await interaction.reply({ ...buildGraphicPanelPayload(), ephemeral: true });
      return;
    }

    // /elo modset
    if (sub === "modset") {
      await interaction.deferReply({ ephemeral: true });
      const target = interaction.options.getUser("target", true);
      const screenshot = interaction.options.getAttachment("screenshot", true);
      const rawText = interaction.options.getString("text", true);

      try {
        const rating = await upsertRatingDirect(client, target, screenshot, rawText, interaction.user.tag);
        await interaction.editReply({
          content: `Ок. <@${target.id}> теперь в тир-листе. ELO **${rating.elo}**, тир **${rating.tier}**.`
        });
        await dmUser(client, target.id, `Модератор обновил твой рейтинг.
ELO: ${rating.elo}
Тир: ${rating.tier}
Пруф: ${rating.proofUrl}`);
        await logLine(client, `MODSET: <@${target.id}> ELO ${rating.elo} -> Tier ${rating.tier} by ${interaction.user.tag}`);
      } catch (err) {
        await interaction.editReply({ content: String(err?.message || err || "Не удалось добавить игрока.") });
      }
      return;
    }

    // /elo labels
    if (sub === "labels") {
      await interaction.deferReply({ ephemeral: true });
      db.config.tierLabels = {
        1: interaction.options.getString("t1", true),
        2: interaction.options.getString("t2", true),
        3: interaction.options.getString("t3", true),
        4: interaction.options.getString("t4", true),
        5: interaction.options.getString("t5", true),
      };
      saveDB(db);
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply({ content: "Названия тиров обновлены. PNG тоже обновлён, если был настроен." });
      return;
    }

    // /elo remove
    if (sub === "remove") {
      await interaction.deferReply({ ephemeral: true });
      const target = interaction.options.getUser("target", true);
      const rating = db.ratings[target.id];

      if (!rating) {
        await interaction.editReply({ content: "Этого игрока нет в тир-листе." });
        return;
      }

      delete db.ratings[target.id];
      saveDB(db);
      await deleteMiniCardMessage(client, target.id);
      await refreshGraphicTierlist(client).catch(() => false);
      await clearAllTierRoles(client, target.id, "Removed from tierlist");

      await interaction.editReply({ content: `Удалил <@${target.id}> из тир-листа. PNG тоже обновлён, если был настроен.` });
      return;
    }

    // /elo wipe
    if (sub === "wipe") {
      await interaction.deferReply({ ephemeral: true });
      const mode = interaction.options.getString("mode", true);
      const confirm = interaction.options.getString("confirm", true);

      if (confirm !== "WIPE") {
        await interaction.editReply({ content: 'Не подтверждено. В confirm надо написать ровно: WIPE' });
        return;
      }

      const _wipeIds = Object.keys(db.ratings || {});
      for (const uid of _wipeIds) {
        await clearAllTierRoles(client, uid, "Wipe ratings");
      }

      const _miniIds = Object.keys(db.miniCards || {});
      for (const uid of _miniIds) {
        await deleteMiniCardMessage(client, uid);
      }
      db.miniCards = {};

      db.ratings = {};
      saveDB(db);
      await refreshGraphicTierlist(client).catch(() => false);

      await logLine(client, `WIPE_RATINGS (${mode}) by ${interaction.user.tag}`);
      await interaction.editReply({ content: `Рейтинг очищен. mode=${mode}` });
      return;
    }

    return;
  }

  // ---- BUTTONS ----
  if (interaction.isButton()) {
    if (interaction.customId === "elo_submit_open" || interaction.customId === "elo_submit_start") {
      const session = getActiveSubmitSession(interaction.user.id);
      if (session) {
        await interaction.reply({
          content: "У тебя уже начат шаг отправки. Просто пришли одним следующим сообщением картинку в этот канал. Можно вставить её из буфера через **Ctrl+V**.",
          ephemeral: true
        });
        scheduleDeleteInteractionReply(interaction);
        return;
      }

      const blockReason = getSubmitEligibilityError(interaction.user.id);
      if (blockReason) {
        await interaction.reply({ content: blockReason, ephemeral: true });
        scheduleDeleteInteractionReply(interaction);
        return;
      }

      const modal = new ModalBuilder().setCustomId("elo_submit_modal").setTitle("ELO заявка");
      const textInput = new TextInputBuilder()
        .setCustomId("elo_submit_text")
        .setLabel("Текст с числом ELO")
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(true)
        .setMaxLength(1000)
        .setPlaceholder("Например 73 или мой elo 73");

      modal.addComponents(new ActionRowBuilder().addComponents(textInput));
      await interaction.showModal(modal);
      return;
    }

    if (interaction.customId === "elo_submit_card") {
      await interaction.reply(await buildMyCardPayload(client, interaction.user.id));
      scheduleDeleteInteractionReply(interaction);
      return;
    }

    if (interaction.customId === "elo_submit_cancel") {
      clearSubmitSession(interaction.user.id);
      await interaction.reply({ content: "Ок. Отменено.", ephemeral: true });
      scheduleDeleteInteractionReply(interaction);
      return;
    }

    if (interaction.customId === "graphic_refresh") {
      if (!isModerator(interaction.member)) {
        await interaction.reply({ content: "Нет прав.", ephemeral: true });
        return;
      }
      await interaction.deferReply({ ephemeral: true });
      const ok = await refreshGraphicTierlist(client);
      await interaction.editReply(ok ? "PNG тир-лист обновлён." : "PNG тир-лист ещё не настроен. Сначала /elo graphicsetup.");
      return;
    }

    if (interaction.customId === "graphic_panel") {
      if (!isModerator(interaction.member)) {
        await interaction.reply({ content: "Нет прав.", ephemeral: true });
        return;
      }
      await interaction.reply({ ...buildGraphicPanelPayload(), ephemeral: true });
      return;
    }

    if (interaction.customId.startsWith("graphic_panel_")) {
      if (!isModerator(interaction.member)) {
        await interaction.reply({ content: "Нет прав.", ephemeral: true });
        return;
      }

      const graphic = getGraphicTierlistState();

      if (interaction.customId === "graphic_panel_close") {
        await interaction.update({ content: "Ок.", embeds: [], components: [] });
        return;
      }

      if (interaction.customId === "graphic_panel_fonts") {
        if (!ensureGraphicFonts()) throw new Error(`Не удалось загрузить системный шрифт для PNG. source=${GRAPHIC_FONT_INFO.source || "none"}. ${GRAPHIC_FONT_INFO.loadError || ""}`.trim());
        const files = listGraphicFontFiles();
        const lines = [
          `ttf: ${files.length ? files.map(f => path.basename(f)).join(", ") : "(none)"}`,
          `picked regular: ${GRAPHIC_FONT_INFO.regularFile ? path.basename(GRAPHIC_FONT_INFO.regularFile) : "(null)"}`,
          `picked bold: ${GRAPHIC_FONT_INFO.boldFile ? path.basename(GRAPHIC_FONT_INFO.boldFile) : "(null)"}`,
          `fallback: ${GRAPHIC_FONT_INFO.usedFallback}`,
          `source: ${GRAPHIC_FONT_INFO.source || "(none)"}`,
          `error: ${GRAPHIC_FONT_INFO.loadError || "(none)"}`
        ];
        await interaction.reply({ content: lines.join("\n"), ephemeral: true });
        return;
      }

      if (interaction.customId === "graphic_panel_title") {
        const graphic = getGraphicTierlistState();
        const modal = new ModalBuilder()
          .setCustomId("graphic_panel_title_modal")
          .setTitle("Название PNG тир-листа");

        const input = new TextInputBuilder()
          .setCustomId("graphic_title")
          .setLabel("Название наверху картинки")
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setMaxLength(80)
          .setValue(String(graphic.title || GRAPHIC_TIERLIST_TITLE).slice(0, 80));

        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === "graphic_panel_message_text") {
        const modal = new ModalBuilder()
          .setCustomId("graphic_panel_message_text_modal")
          .setTitle("Текст сообщения PNG тир-листа");

        const input = new TextInputBuilder()
          .setCustomId("graphic_message_text")
          .setLabel("Текст под заголовком сообщения")
          .setStyle(TextInputStyle.Paragraph)
          .setRequired(true)
          .setMaxLength(4000)
          .setValue(getGraphicMessageTextModalValue());

        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === "graphic_panel_rename") {
        const graphic = getGraphicTierlistState();
        const tierKey = Number(graphic.panel?.selectedTier) || 5;
        const currentName = formatTierTitle(tierKey);

        const modal = new ModalBuilder()
          .setCustomId(`graphic_panel_rename_modal:${tierKey}`)
          .setTitle(`Переименовать тир ${tierKey}`);

        const input = new TextInputBuilder()
          .setCustomId("tier_name")
          .setLabel("Новое название")
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setMaxLength(32)
          .setValue(String(currentName).slice(0, 32));

        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === "graphic_panel_set_color") {
        const graphic = getGraphicTierlistState();
        const tierKey = Number(graphic.panel?.selectedTier) || 5;
        const currentColor = graphic.tierColors?.[tierKey] || DEFAULT_GRAPHIC_TIER_COLORS[tierKey] || "#cccccc";

        const modal = new ModalBuilder()
          .setCustomId(`graphic_panel_color_modal:${tierKey}`)
          .setTitle(`Цвет тира ${tierKey}`);

        const input = new TextInputBuilder()
          .setCustomId("tier_color")
          .setLabel("HEX цвет. пример #ff6b6b")
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setMaxLength(7)
          .setValue(String(currentColor).slice(0, 7));

        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === "graphic_panel_refresh") {
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_icon_minus" || interaction.customId === "graphic_panel_icon_plus") {
        applyGraphicImageDelta("icon", interaction.customId.endsWith("plus") ? 12 : -12);
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_w_minus" || interaction.customId === "graphic_panel_w_plus") {
        applyGraphicImageDelta("width", interaction.customId.endsWith("plus") ? 200 : -200);
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_h_minus" || interaction.customId === "graphic_panel_h_plus") {
        applyGraphicImageDelta("height", interaction.customId.endsWith("plus") ? 120 : -120);
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_reset_img") {
        resetGraphicImageOverrides();
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_reset_color") {
        const graphic = getGraphicTierlistState();
        resetGraphicTierColor(Number(graphic.panel?.selectedTier) || 5);
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_reset_colors") {
        resetAllGraphicTierColors();
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_clear_cache") {
        clearGraphicAvatarCache();
        await interaction.reply({ content: "Кэш аватарок очищен. Следующая пересборка заново подтянет картинки.", ephemeral: true });
        return;
      }
    }

    const [action, submissionId] = interaction.customId.split(":");
    const sub = db.submissions[submissionId];

    if (!isModerator(interaction.member)) {
      await interaction.reply({ content: "Нет прав.", ephemeral: true });
      return;
    }
    if (!sub) {
      await interaction.reply({ content: "Заявка не найдена.", ephemeral: true });
      return;
    }
    if (sub.status !== "pending") {
      await interaction.reply({ content: `Уже обработано: ${sub.status}`, ephemeral: true });
      return;
    }
    if (hoursSince(sub.createdAt) > PENDING_EXPIRE_HOURS) {
      sub.status = "expired";
      saveDB(db);
      const msg = await fetchReviewMessage(client, sub);
      if (msg) await msg.edit({ embeds: [buildReviewEmbed(sub, "expired")], components: [] }).catch(() => {});
      await interaction.reply({ content: "Заявка протухла (expired).", ephemeral: true });
      return;
    }

    // Approve
    if (action === "approve") {
      const tier = tierFor(sub.elo);
      if (!tier) {
        sub.status = "rejected";
        sub.reviewedBy = interaction.user.tag;
        sub.reviewedAt = new Date().toISOString();
        sub.rejectReason = "ELO ниже 10";
        saveDB(db);

        await interaction.message.edit({
          embeds: [buildReviewEmbed(sub, "rejected", [{ name: "Причина", value: sub.rejectReason, inline: false }])],
          components: [],
        }).catch(() => {});
        await interaction.reply({ content: "ELO ниже 10. Отклонено.", ephemeral: true });
        return;
      }

      sub.tier = tier;
      sub.status = "approved";
      sub.reviewedBy = interaction.user.tag;
      sub.reviewedAt = new Date().toISOString();

      const user = await client.users.fetch(sub.userId);
      const guild = await getGuild(client).catch(() => null);
      const member = guild ? await guild.members.fetch(sub.userId).catch(() => null) : null;
      const rating = db.ratings[sub.userId] || { userId: sub.userId };

      rating.userId = sub.userId;
      rating.name = sub.name;
      rating.username = user.username;
      rating.elo = sub.elo;
      rating.tier = tier;
      rating.proofUrl = sub.reviewAttachmentUrl || sub.screenshotUrl;
      rating.avatarUrl = normalizeDiscordAvatarUrl((member?.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 })) || user.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 }) || user.defaultAvatarURL || "");
      rating.updatedAt = new Date().toISOString();

      db.ratings[sub.userId] = rating;
      saveDB(db);
      await loadGraphicAvatarForPlayer(client, rating).catch(() => null);

      saveDB(db);
      await refreshGraphicTierlist(client).catch(() => false);
      await ensureSingleTierRole(client, sub.userId, tier, "Approved tier role");

      await interaction.message.edit({ embeds: [buildReviewEmbed(sub, "approved")], components: [] }).catch(() => {});
      await interaction.reply({ content: "Одобрено. Тир-лист обновлён. PNG тоже обновлён, если был настроен.", ephemeral: true });

      await dmUser(client, sub.userId, `Одобрено.\nELO: ${sub.elo}\nТир: ${sub.tier}\nПруф: ${sub.reviewAttachmentUrl || sub.screenshotUrl}`);
      await logLine(client, `APPROVE: <@${sub.userId}> ELO ${sub.elo} -> Tier ${sub.tier} (id ${submissionId}) by ${interaction.user.tag}`);
      saveDB(db);
      return;
    }

    // Edit ELO modal
    if (action === "edit") {
      const modal = new ModalBuilder().setCustomId(`edit_elo:${submissionId}`).setTitle("Edit ELO");
      const input = new TextInputBuilder()
        .setCustomId("elo")
        .setLabel("Новое ELO (минимум 10)")
        .setStyle(TextInputStyle.Short)
        .setRequired(true)
        .setValue(String(sub.elo));
      modal.addComponents(new ActionRowBuilder().addComponents(input));
      await interaction.showModal(modal);
      return;
    }

    // Reject reason modal
    if (action === "reject") {
      const modal = new ModalBuilder().setCustomId(`reject_reason:${submissionId}`).setTitle("Reject reason");
      const input = new TextInputBuilder()
        .setCustomId("reason")
        .setLabel("Причина отказа (коротко)")
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(true);
      modal.addComponents(new ActionRowBuilder().addComponents(input));
      await interaction.showModal(modal);
      return;
    }
  }

  if (interaction.isStringSelectMenu()) {
    if (!isModerator(interaction.member)) {
      await interaction.reply({ content: "Нет прав.", ephemeral: true });
      return;
    }

    if (interaction.customId === "graphic_panel_select_tier") {
      const graphic = getGraphicTierlistState();
      graphic.panel.selectedTier = Number(interaction.values?.[0] || 5) || 5;
      saveDB(db);
      await interaction.update(buildGraphicPanelPayload());
      return;
    }
  }

  // ---- MODAL SUBMITS ----
  if (interaction.isModalSubmit()) {
    if (interaction.customId === "elo_submit_modal") {
      const rawText = (interaction.fields.getTextInputValue("elo_submit_text") || "").trim();

      const blockReason = getSubmitEligibilityError(interaction.user.id, rawText);
      if (blockReason) {
        await interaction.reply({ content: blockReason, ephemeral: true });
        scheduleDeleteInteractionReply(interaction);
        return;
      }

      setSubmitSession(interaction.user.id, { rawText });
      await interaction.reply({
        content: "Текст принят. Теперь отправь **одним следующим сообщением** скрин в этот канал. Можно обычным вложением или просто вставить картинку из буфера через **Ctrl+V**. Бот сам удалит её после обработки.",
        ephemeral: true
      });
      scheduleDeleteInteractionReply(interaction);
      return;
    }

    if (!isModerator(interaction.member)) {
      await interaction.reply({ content: "Нет прав.", ephemeral: true });
      return;
    }

    if (interaction.customId === "graphic_panel_title_modal") {
      const graphic = getGraphicTierlistState();
      const title = (interaction.fields.getTextInputValue("graphic_title") || "").trim().slice(0, 80);
      if (!title) {
        await interaction.reply({ content: "Пустое название.", ephemeral: true });
        return;
      }
      graphic.title = title;
      saveDB(db);
      await interaction.deferReply({ ephemeral: true });
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply(`Ок. Теперь PNG называется: **${title}**.`);
      return;
    }

    if (interaction.customId === "graphic_panel_message_text_modal") {
      const graphic = getGraphicTierlistState();
      const text = (interaction.fields.getTextInputValue("graphic_message_text") || "").trim();
      if (!text) {
        await interaction.reply({ content: "Пустой текст.", ephemeral: true });
        return;
      }
      graphic.messageText = text.slice(0, 4000);
      saveDB(db);
      await interaction.deferReply({ ephemeral: true });
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply("Ок. Текст сообщения PNG обновлён.");
      return;
    }

    if (interaction.customId.startsWith("graphic_panel_rename_modal:")) {
      const tierKey = Number(interaction.customId.split(":")[1] || 5) || 5;
      const name = (interaction.fields.getTextInputValue("tier_name") || "").trim().slice(0, 32);
      if (!name) {
        await interaction.reply({ content: "Пустое имя.", ephemeral: true });
        return;
      }
      db.config.tierLabels ||= { ...DEFAULT_TIER_LABELS };
      db.config.tierLabels[tierKey] = name;
      saveDB(db);
      await interaction.deferReply({ ephemeral: true });
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply(`Ок. Теперь **${tierKey}** называется: **${name}**.`);
      return;
    }

    if (interaction.customId.startsWith("graphic_panel_color_modal:")) {
      const tierKey = Number(interaction.customId.split(":")[1] || 5) || 5;
      const raw = interaction.fields.getTextInputValue("tier_color");
      const hex = normalizeHexColor(raw);
      if (!hex) {
        await interaction.reply({ content: "Нужен HEX цвет вида #ff6b6b", ephemeral: true });
        return;
      }
      setGraphicTierColor(tierKey, hex);
      saveDB(db);
      await interaction.deferReply({ ephemeral: true });
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply(`Ок. Цвет тира **${tierKey}** теперь **${hex}**.`);
      return;
    }

    const [kind, submissionId] = interaction.customId.split(":");
    const sub = db.submissions[submissionId];

    if (!sub || sub.status !== "pending") {
      await interaction.reply({ content: "Заявка не найдена или уже обработана.", ephemeral: true });
      return;
    }

    if (hoursSince(sub.createdAt) > PENDING_EXPIRE_HOURS) {
      sub.status = "expired";
      saveDB(db);
      const msg = await fetchReviewMessage(client, sub);
      if (msg) await msg.edit({ embeds: [buildReviewEmbed(sub, "expired")], components: [] }).catch(() => {});
      await interaction.reply({ content: "Заявка протухла (expired).", ephemeral: true });
      return;
    }

    // edit_elo
    if (kind === "edit_elo") {
      const val = interaction.fields.getTextInputValue("elo");
      const newElo = parseElo(val);
      const newTier = newElo ? tierFor(newElo) : null;

      if (!newElo || !newTier) {
        await interaction.reply({ content: "Нужно число ELO минимум 10.", ephemeral: true });
        return;
      }

      sub.elo = newElo;
      sub.tier = newTier;
      saveDB(db);

      const msg = await fetchReviewMessage(client, sub);
      if (msg) {
        await msg.edit({
          embeds: [buildReviewEmbed(sub, "pending", [{ name: "Изменено", value: `ELO исправил: ${interaction.user.tag}`, inline: false }])],
          components: [buildReviewButtons(submissionId)],
        }).catch(() => {});
      }

      await interaction.reply({ content: `ELO обновлено: ${newElo} (тир ${newTier}).`, ephemeral: true });
      return;
    }

    // reject_reason
    if (kind === "reject_reason") {
      const reason = interaction.fields.getTextInputValue("reason").slice(0, 800);

      sub.status = "rejected";
      sub.reviewedBy = interaction.user.tag;
      sub.reviewedAt = new Date().toISOString();
      sub.rejectReason = reason;
      saveDB(db);

      const msg = await fetchReviewMessage(client, sub);
      if (msg) {
        await msg.edit({
          embeds: [buildReviewEmbed(sub, "rejected", [{ name: "Причина", value: reason, inline: false }])],
          components: [],
        }).catch(() => {});
      }

      await interaction.reply({ content: "Отклонено.", ephemeral: true });
      await dmUser(client, sub.userId, `Отклонено.\nПричина: ${reason}\nПруф: ${sub.screenshotUrl}`);
      await logLine(client, `REJECT: <@${sub.userId}> ELO ${sub.elo} (id ${submissionId}) by ${interaction.user.tag} | reason: ${reason}`);
      return;
    }
  }
});

client.login(DISCORD_TOKEN);
