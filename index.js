require("dotenv").config();
const fs = require("fs");
const path = require("path");
const https = require("https");
const http = require("http");

const {
  Client,
  GatewayIntentBits,
  EmbedBuilder,
  AttachmentBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
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
const TIERLIST_ROLE_ID = process.env.TIERLIST_ROLE_ID || "";

const SUBMIT_COOLDOWN_SECONDS = 120; // кулдаун на ВАЛИДНУЮ заявку
const PENDING_EXPIRE_HOURS = 48;     // протухание pending

// TODO: ВПИШИ СВОИ НАЗВАНИЯ ТИРОВ ТУТ (пока цифры)
// (можно менять через /elo labels тоже)
const DEFAULT_TIER_LABELS = { 1: "1", 2: "2", 3: "3", 4: "4", 5: "5" };

// ====== DB (файл) ======
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "db.json");

function loadDB() {
  if (!fs.existsSync(DB_PATH)) {
    return { config: {}, submissions: {}, ratings: {}, cooldowns: {} };
  }
  try {
    const data = JSON.parse(fs.readFileSync(DB_PATH, "utf8"));
    data.config ||= {};
    data.submissions ||= {};
    data.ratings ||= {};
    data.cooldowns ||= {};
    return data;
  } catch {
    return { config: {}, submissions: {}, ratings: {}, cooldowns: {} };
  }
}

function saveDB(db) {
  fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2), "utf8");
}

const db = loadDB();
db.config.tierLabels ||= DEFAULT_TIER_LABELS;
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

// Тиры "ОТ": 15 / 35 / 60 / 90 / 120 (ниже 15 — невалидно)
function tierFor(elo) {
  if (elo >= 120) return 5;
  if (elo >= 90) return 4;
  if (elo >= 60) return 3;
  if (elo >= 35) return 2;
  if (elo >= 15) return 1;
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
  // 1) Node 18+: используем fetch
  if (typeof fetch === "function") {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: controller.signal });
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
    const req = lib.get(url, (res) => {
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

// ====== ROLE: TIERLIST MEMBER ======
let _tierlistGuildCache = null;

async function getTierlistGuild(client) {
  if (_tierlistGuildCache) return _tierlistGuildCache;
  if (!GUILD_ID) return null;
  _tierlistGuildCache = await client.guilds.fetch(GUILD_ID).catch(() => null);
  return _tierlistGuildCache;
}

async function setTierlistRole(client, userId, shouldHave, reason = "tierlist") {
  if (!TIERLIST_ROLE_ID) return;
  const guild = await getTierlistGuild(client);
  if (!guild) return;

  const member = await guild.members.fetch(userId).catch(() => null);
  if (!member) return;

  const has = member.roles.cache.has(TIERLIST_ROLE_ID);
  if (shouldHave && !has) {
    await member.roles.add(TIERLIST_ROLE_ID, reason).catch(() => {});
  } else if (!shouldHave && has) {
    await member.roles.remove(TIERLIST_ROLE_ID, reason).catch(() => {});
  }
}

async function syncTierlistRolesOnStart(client) {
  if (!TIERLIST_ROLE_ID) return;
  const ids = Object.keys(db.ratings || {});
  if (!ids.length) return;

  for (const uid of ids) {
    await setTierlistRole(client, uid, true, "sync from db");
  }
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

async function fetchReviewMessage(client, sub) {
  if (!sub.reviewChannelId || !sub.reviewMessageId) return null;
  const ch = await client.channels.fetch(sub.reviewChannelId).catch(() => null);
  if (!ch?.isTextBased()) return null;
  const msg = await ch.messages.fetch(sub.reviewMessageId).catch(() => null);
  return msg;
}

// ====== TIERLIST INDEX ======
async function ensureIndexMessage(client) {
  const channel = await client.channels.fetch(TIERLIST_CHANNEL_ID);
  if (!channel || !channel.isTextBased()) throw new Error("TIERLIST_CHANNEL_ID: не текстовый канал");

  if (db.config.indexMessageId) {
    try {
      const msg = await channel.messages.fetch(db.config.indexMessageId);
      if (msg) return msg;
    } catch {}
  }

  const embed = new EmbedBuilder()
    .setTitle("ТИР-СПИСОК (авто)")
    .setDescription("Пока пусто.");

  const msg = await channel.send({ embeds: [embed] });
  try { await msg.pin(); } catch {}
  db.config.indexMessageId = msg.id;
  saveDB(db);
  return msg;
}

function buildIndexEmbed() {
  const entries = Object.values(db.ratings);
  const tiers = { 1: [], 2: [], 3: [], 4: [], 5: [] };

  for (const r of entries) {
    const t = Number(r.tier);
    if (tiers[t]) tiers[t].push(r);
  }

  for (const t of Object.keys(tiers)) {
    tiers[t].sort((a, b) => (b.elo || 0) - (a.elo || 0));
  }

  const embed = new EmbedBuilder()
    .setTitle("ТИР-СПИСОК (авто)")
    .setFooter({ text: "Подача: #elo-submit • Проверка: #elo-review" });

  for (const t of [5, 4, 3, 2, 1]) {
    const list = tiers[t];
    if (!list.length) {
      embed.addFields({ name: formatTierTitle(t), value: "—", inline: false });
      continue;
    }
    const lines = list.slice(0, 50).map((r, i) => `${i + 1}. <@${r.userId}> (${r.name}) — **${r.elo}**`);
    embed.addFields({ name: formatTierTitle(t), value: lines.join("\n"), inline: false });
  }

  return embed;
}

async function updateIndex(client) {
  const indexMsg = await ensureIndexMessage(client);
  await indexMsg.edit({ embeds: [buildIndexEmbed()] });
}

async function upsertCardMessage(client, rating, approvedByTag) {
  const channel = await client.channels.fetch(TIERLIST_CHANNEL_ID);
  if (!channel || !channel.isTextBased()) return;

  const embed = new EmbedBuilder()
    .setAuthor({
      name: `${rating.name} • ${formatTierTitle(rating.tier)}`,
      iconURL: rating.avatarUrl || undefined,
    })
    .setTitle(`ELO: ${rating.elo}`)
    .addFields(
      { name: "Тир", value: `**${rating.tier}**`, inline: true },
      { name: "ELO", value: `**${rating.elo}**`, inline: true },
      { name: "Пруф", value: rating.proofUrl ? `[скрин](${rating.proofUrl})` : "—", inline: true }
    )
    .setFooter({ text: `Approved by ${approvedByTag}` });

  if (rating.proofUrl) embed.setImage(rating.proofUrl);

  if (rating.cardMessageId) {
    try {
      const msg = await channel.messages.fetch(rating.cardMessageId);
      await msg.edit({ embeds: [embed] });
      return;
    } catch {
      rating.cardMessageId = "";
    }
  }

  const msg = await channel.send({ embeds: [embed] });
  rating.cardMessageId = msg.id;
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
      .addSubcommand(s => s.setName("rebuild").setDescription("Пересобрать закреп (модеры)"))
      .addSubcommand(s => s.setName("remove").setDescription("Удалить игрока из тир-листа (модеры)")
        .addUserOption(o => o.setName("target").setDescription("Игрок").setRequired(true)))
      .addSubcommand(s => s.setName("wipe").setDescription("Очистить рейтинг полностью (модеры)")
        .addStringOption(o => o.setName("mode").setDescription("soft=только база, hard=база+удалить карточки").setRequired(true)
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

  await ensureIndexMessage(client);
  await updateIndex(client);
  await syncTierlistRolesOnStart(client);

  console.log("Ready");
});

// ====== SUBMIT CHANNEL ONLY ======
client.on("messageCreate", async (message) => {
  if (message.author.bot) return;
  if (message.channelId !== SUBMIT_CHANNEL_ID) return;

  const elo = parseElo(message.content);
  const attachment = message.attachments.first();
  const tier = elo ? tierFor(elo) : null;

  // невалидно -> удалить и не отправлять
  if (!attachment || !isImageAttachment(attachment) || !elo || !tier) {
    const warn = await message.reply("Невалидно. Нужен **скрин (картинка)** и **ELO числом от 15**. Пример: `73`");
    setTimeout(() => warn.delete().catch(() => {}), 8000);
    message.delete().catch(() => {});
    return;
  }

  // дубликат ELO -> не принимать
  const current = db.ratings[message.author.id];
  if (current && Number(current.elo) === Number(elo)) {
    const warn = await message.reply("У тебя уже стоит **такой же ELO** в тир-листе. Если изменится — присылай новый скрин.");
    setTimeout(() => warn.delete().catch(() => {}), 8000);
    message.delete().catch(() => {});
    return;
  }

  // pending уже есть
  const hasPending = Object.values(db.submissions).some(
    (s) => s.userId === message.author.id && s.status === "pending"
  );
  if (hasPending) {
    const warn = await message.reply("У тебя уже есть заявка на проверке. Дождись решения модера.");
    setTimeout(() => warn.delete().catch(() => {}), 8000);
    message.delete().catch(() => {});
    return;
  }

  // кулдаун только на валидные
  const now = Date.now();
  const last = db.cooldowns[message.author.id] || 0;
  const left = SUBMIT_COOLDOWN_SECONDS - Math.floor((now - last) / 1000);
  if (left > 0) {
    const warn = await message.reply(`Кулдаун. Подожди ${left} сек и попробуй снова.`);
    setTimeout(() => warn.delete().catch(() => {}), 8000);
    message.delete().catch(() => {});
    return;
  }

  const submissionId = makeId();

  // Перезаливаем скрин в review, чтобы картинка стабильно открывалась
  // (не зависит от временных ссылок и удаления исходного сообщения).
  let reviewFile = null;
  let reviewImage = attachment.url;
  let reviewFileName = null;
  try {
    const buf = await downloadToBuffer(attachment.url);
    reviewFileName = sanitizeFileName(`${submissionId}_${attachment.name || "screenshot"}`);
    reviewFile = new AttachmentBuilder(buf, { name: reviewFileName });
    reviewImage = `attachment://${reviewFileName}`;
  } catch {
    // fallback: оставляем URL как есть (лучше чем ничего)
    reviewFile = null;
    reviewImage = attachment.url;
    reviewFileName = null;
  }
  db.submissions[submissionId] = {
    id: submissionId,
    userId: message.author.id,
    name: message.member?.displayName || message.author.username,
    elo,
    tier,
    screenshotUrl: attachment.url,
    reviewImage,
    reviewFileName,
    messageUrl: message.url,
    status: "pending",
    createdAt: new Date().toISOString(),
    reviewChannelId: null,
    reviewMessageId: null,
  };

  // кулдаун ставим после валидной заявки
  db.cooldowns[message.author.id] = Date.now();
  saveDB(db);

  const reviewChannel = await client.channels.fetch(REVIEW_CHANNEL_ID).catch(() => null);
  if (!reviewChannel || !reviewChannel.isTextBased()) return;

  const sub = db.submissions[submissionId];
  const payload = {
    embeds: [buildReviewEmbed(sub, "pending")],
    components: [buildReviewButtons(submissionId)],
  };
  if (reviewFile) payload.files = [reviewFile];
  const sent = await reviewChannel.send(payload);

  // сохраняем, чтобы модалки могли редактировать сообщение
  sub.reviewChannelId = sent.channel.id;
  sub.reviewMessageId = sent.id;
  saveDB(db);

  const ok = await message.reply("Заявка отправлена на проверку модерам.");
  setTimeout(() => ok.delete().catch(() => {}), 8000);

  message.delete().catch(() => {});
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
      await updateIndex(client);
      await interaction.reply({ content: "Закреп пересобран.", ephemeral: true });
      return;
    }

    // /elo labels
    if (sub === "labels") {
      db.config.tierLabels = {
        1: interaction.options.getString("t1", true),
        2: interaction.options.getString("t2", true),
        3: interaction.options.getString("t3", true),
        4: interaction.options.getString("t4", true),
        5: interaction.options.getString("t5", true),
      };
      saveDB(db);
      await updateIndex(client);
      await interaction.reply({ content: "Названия тиров обновлены.", ephemeral: true });
      return;
    }

    // /elo remove
    if (sub === "remove") {
      const target = interaction.options.getUser("target", true);
      const rating = db.ratings[target.id];

      if (!rating) {
        await interaction.reply({ content: "Этого игрока нет в тир-листе.", ephemeral: true });
        return;
      }

      if (rating.cardMessageId) {
        const ch = await client.channels.fetch(TIERLIST_CHANNEL_ID).catch(() => null);
        if (ch?.isTextBased()) {
          const msg = await ch.messages.fetch(rating.cardMessageId).catch(() => null);
          if (msg) await msg.delete().catch(() => {});
        }
      }

      delete db.ratings[target.id];
      saveDB(db);
      await updateIndex(client);
      await setTierlistRole(client, target.id, false, "Removed from tierlist");

      await interaction.reply({ content: `Удалил <@${target.id}> из тир-листа.`, ephemeral: true });
      return;
    }

    // /elo wipe
    if (sub === "wipe") {
      const mode = interaction.options.getString("mode", true);
      const confirm = interaction.options.getString("confirm", true);

      if (confirm !== "WIPE") {
        await interaction.reply({ content: 'Не подтверждено. В confirm надо написать ровно: WIPE', ephemeral: true });
        return;
      }

      if (mode === "hard") {
        const ch = await client.channels.fetch(TIERLIST_CHANNEL_ID).catch(() => null);
        if (ch?.isTextBased()) {
          for (const r of Object.values(db.ratings)) {
            if (!r.cardMessageId) continue;
            const msg = await ch.messages.fetch(r.cardMessageId).catch(() => null);
            if (msg) await msg.delete().catch(() => {});
          }
        }
      }

      const _wipeIds = Object.keys(db.ratings || {});
      for (const uid of _wipeIds) {
        await setTierlistRole(client, uid, false, "Wipe ratings");
      }

      db.ratings = {};
      saveDB(db);
      await updateIndex(client);

      await logLine(client, `WIPE_RATINGS (${mode}) by ${interaction.user.tag}`);
      await interaction.reply({ content: `Рейтинг очищен. mode=${mode}`, ephemeral: true });
      return;
    }

    return;
  }

  // ---- BUTTONS ----
  if (interaction.isButton()) {
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
        sub.rejectReason = "ELO ниже 15";
        saveDB(db);

        await interaction.message.edit({
          embeds: [buildReviewEmbed(sub, "rejected", [{ name: "Причина", value: sub.rejectReason, inline: false }])],
          components: [],
        }).catch(() => {});
        await interaction.reply({ content: "ELO ниже 15. Отклонено.", ephemeral: true });
        return;
      }

      sub.tier = tier;
      sub.status = "approved";
      sub.reviewedBy = interaction.user.tag;
      sub.reviewedAt = new Date().toISOString();

      const user = await client.users.fetch(sub.userId);
      const rating = db.ratings[sub.userId] || { userId: sub.userId };

      rating.userId = sub.userId;
      rating.name = sub.name;
      rating.elo = sub.elo;
      rating.tier = tier;
      rating.proofUrl = sub.screenshotUrl;
      rating.avatarUrl = user.displayAvatarURL({ size: 128 });
      rating.updatedAt = new Date().toISOString();

      db.ratings[sub.userId] = rating;
      saveDB(db);

      await upsertCardMessage(client, rating, interaction.user.tag);
      saveDB(db);
      await updateIndex(client);
      await setTierlistRole(client, sub.userId, true, "Approved to tierlist");

      await interaction.message.edit({ embeds: [buildReviewEmbed(sub, "approved")], components: [] }).catch(() => {});
      await interaction.reply({ content: "Одобрено. Тир-лист обновлён.", ephemeral: true });

      await dmUser(client, sub.userId, `Одобрено.\nELO: ${sub.elo}\nТир: ${sub.tier}\nПруф: ${sub.screenshotUrl}`);
      await logLine(client, `APPROVE: <@${sub.userId}> ELO ${sub.elo} -> Tier ${sub.tier} (id ${submissionId}) by ${interaction.user.tag}`);
      saveDB(db);
      return;
    }

    // Edit ELO modal
    if (action === "edit") {
      const modal = new ModalBuilder().setCustomId(`edit_elo:${submissionId}`).setTitle("Edit ELO");
      const input = new TextInputBuilder()
        .setCustomId("elo")
        .setLabel("Новое ELO (минимум 15)")
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

  // ---- MODAL SUBMITS ----
  if (interaction.isModalSubmit()) {
    const [kind, submissionId] = interaction.customId.split(":");
    const sub = db.submissions[submissionId];

    if (!isModerator(interaction.member)) {
      await interaction.reply({ content: "Нет прав.", ephemeral: true });
      return;
    }
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
        await interaction.reply({ content: "Нужно число ELO минимум 15.", ephemeral: true });
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