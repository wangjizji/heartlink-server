"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");

const HOST = process.env.HEARTLINK_HOST || "0.0.0.0";
const PORT = Number(process.env.PORT || process.env.HEARTLINK_PORT || 5310);
const DATA_DIR = path.join(__dirname, "data");
const DATA_FILE = path.join(DATA_DIR, "messages.json");
const UPLOAD_DIR = path.join(__dirname, "uploads");
const MAX_MESSAGES_PER_ROOM = 2000;
const CALL_SIGNAL_TTL_MS = 10 * 60 * 1000;
const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || "").trim();
const HEARTLINK_AI_MODEL = String(process.env.HEARTLINK_AI_MODEL || "gpt-5").trim();
const emptyStore = () => ({ version: 1, rooms: {}, pairings: {} });

const loadStore = () => {
  try {
    const parsed = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    return parsed && typeof parsed === "object" && parsed.rooms ? parsed : emptyStore();
  } catch (error) {
    if (error && error.code !== "ENOENT") console.error("读取消息数据失败：", error.message);
    return emptyStore();
  }
};

let store = loadStore();
let saveQueue = Promise.resolve();
const databasePool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } })
  : null;

const initializePersistence = async () => {
  if (!databasePool) return;
  await databasePool.query(`
    CREATE TABLE IF NOT EXISTS heartlink_store (
      id INTEGER PRIMARY KEY,
      payload JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  const result = await databasePool.query("SELECT payload FROM heartlink_store WHERE id = 1");
  if (result.rows[0]?.payload && typeof result.rows[0].payload === "object") {
    store = result.rows[0].payload;
    return;
  }
  await databasePool.query(
    "INSERT INTO heartlink_store (id, payload) VALUES (1, $1::jsonb) ON CONFLICT (id) DO NOTHING",
    [JSON.stringify(store)]
  );
};

const saveStore = () => {
  const snapshot = JSON.stringify(store, null, 2);
  saveQueue = saveQueue.then(async () => {
    await fs.promises.mkdir(DATA_DIR, { recursive: true });
    const tempFile = `${DATA_FILE}.tmp`;
    await fs.promises.writeFile(tempFile, snapshot, "utf8");
    await fs.promises.rename(tempFile, DATA_FILE);
    if (databasePool) {
      await databasePool.query(
        "INSERT INTO heartlink_store (id, payload, updated_at) VALUES (1, $1::jsonb, NOW()) ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()",
        [snapshot]
      );
    }
  }).catch((error) => console.error("保存消息数据失败：", error.message));
  return saveQueue;
};

const sendJson = (response, statusCode, payload) => {
  const body = Buffer.from(JSON.stringify(payload), "utf8");
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": body.length,
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Cache-Control": "no-store"
  });
  response.end(body);
};

const readJsonBody = (request) => new Promise((resolve, reject) => {
  const chunks = [];
  let size = 0;
  request.on("data", (chunk) => {
    size += chunk.length;
    if (size > 8 * 1024 * 1024) {
      reject(new Error("请求内容过大"));
      request.destroy();
      return;
    }
    chunks.push(chunk);
  });
  request.on("end", () => {
    try {
      const text = Buffer.concat(chunks).toString("utf8");
      resolve(text ? JSON.parse(text) : {});
    } catch {
      reject(new Error("JSON 格式不正确"));
    }
  });
  request.on("error", reject);
});

const readBinaryBody = (request) => new Promise((resolve, reject) => {
  const chunks = [];
  let size = 0;
  request.on("data", (chunk) => {
    size += chunk.length;
    if (size > 100 * 1024 * 1024) {
      reject(new Error("文件超过 100 MB 限制"));
      request.destroy();
      return;
    }
    chunks.push(chunk);
  });
  request.on("end", () => resolve(Buffer.concat(chunks)));
  request.on("error", reject);
});

const handleAiWrite = async (request, response) => {
  if (!OPENAI_API_KEY) {
    sendJson(response, 503, { ok: false, code: "AI_NOT_CONFIGURED", message: "AI 服务尚未配置。" });
    return;
  }
  let payload;
  try { payload = await readJsonBody(request); }
  catch { sendJson(response, 400, { ok: false, code: "INVALID_JSON", message: "请求内容格式错误。" }); return; }
  const prompt = String(payload?.prompt || "").trim();
  if (!prompt) { sendJson(response, 400, { ok: false, code: "PROMPT_REQUIRED", message: "请先写下想表达的内容。" }); return; }
  if (prompt.length > 1200) { sendJson(response, 400, { ok: false, code: "PROMPT_TOO_LONG", message: "内容太长，请缩短后重试。" }); return; }
  const scenes = { "love-letter": "想念或情书", apology: "认真道歉", anniversary: "纪念日心意" };
  const tones = { natural: "自然真诚", gentle: "温柔克制", serious: "认真稳重" };
  const scene = scenes[String(payload.context || "")] || "日常心意";
  const tone = tones[String(payload.tone || "")] || "自然真诚";
  const partnerName = String(payload.partnerName || "TA").trim().slice(0, 40) || "TA";
  const extraContext = String(payload.extraContext || "").trim().slice(0, 600);
  const developerPrompt = "你是情侣聊天 App HeartLink 的中文表达助手。只整理用户已经提供的真实想法，不捏造经历，不替用户承诺，不使用道德绑架或催促原谅。只返回 JSON：{\"title\":\"20个汉字以内标题\",\"text\":\"60至180个汉字正文\"}。";
  const userPrompt = `场景：${scene}\n语气：${tone}\n对方称呼：${partnerName}\n补充背景：${extraContext}\n用户真实想法：${prompt}`;
  try {
    const apiResponse = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({ model: HEARTLINK_AI_MODEL, input: [
        { role: "developer", content: [{ type: "input_text", text: developerPrompt }] },
        { role: "user", content: [{ type: "input_text", text: userPrompt }] }
      ], max_output_tokens: 500 })
    });
    if (!apiResponse.ok) throw new Error(`OpenAI ${apiResponse.status}`);
    const result = await apiResponse.json();
    const outputText = (result.output || []).flatMap((item) => item.content || [])
      .find((item) => item.type === "output_text")?.text?.trim() || "";
    const draft = JSON.parse(outputText.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, ""));
    const title = String(draft.title || "").trim().slice(0, 20);
    const text = String(draft.text || "").trim().slice(0, 180);
    if (!title || !text) throw new Error("AI 返回内容不完整");
    sendJson(response, 200, { ok: true, title, text, model: HEARTLINK_AI_MODEL });
  } catch (error) {
    console.error("AI 写作失败：", error.message);
    sendJson(response, 502, { ok: false, code: "AI_GENERATION_FAILED", message: "AI 整理失败，本次不会扣除权益，请稍后重试。" });
  }
};

const getRoom = (roomId) => {
  if (!store.rooms[roomId]) store.rooms[roomId] = { updatedAt: Date.now(), messages: [] };
  return store.rooms[roomId];
};

const getPairingsStore = () => {
  if (!store.pairings || typeof store.pairings !== "object") {
    store.pairings = {};
  }
  return store.pairings;
};

const getProfilesStore = () => {
  if (!store.profiles || typeof store.profiles !== "object") {
    store.profiles = {};
  }
  return store.profiles;
};

const getCallSignalsStore = () => {
  if (!store.callSignals || typeof store.callSignals !== "object") {
    store.callSignals = {};
  }
  return store.callSignals;
};

const cleanCallSignals = (signals) => {
  const cutoff = Date.now() - CALL_SIGNAL_TTL_MS;
  const validSignals = signals.filter((item) => Number(item?.createdAt || 0) >= cutoff);
  if (validSignals.length > 300) validSignals.splice(0, validSignals.length - 300);
  return validSignals;
};

const isPairedDeviceInRoom = (roomId, deviceId) => {
  const normalizedDeviceId = String(deviceId || "").trim();
  if (!normalizedDeviceId) return false;
  return Object.values(getPairingsStore()).some((pairing) => {
    return pairing && pairing.status === "paired"
      && String(pairing.roomId || "").trim().toLowerCase() === roomId
      && (String(pairing.ownerId || "") === normalizedDeviceId || String(pairing.guestId || "") === normalizedDeviceId);
  });
};

const normalizeAvatarData = (value) => {
  const avatarData = String(value || "").trim();
  if (!avatarData) return "";
  if (!/^data:image\/(?:jpeg|jpg|png|webp);base64,[a-z0-9+/=]+$/i.test(avatarData)) {
    throw new Error("头像格式不支持");
  }
  if (avatarData.length > 700 * 1024) throw new Error("头像不能超过 512 KB");
  return avatarData;
};

const normalizePairCode = (value) => String(value || "").trim().toUpperCase();

const getPairing = (code) => {
  const normalizedCode = normalizePairCode(code);
  if (!normalizedCode) return null;
  return getPairingsStore()[normalizedCode] || null;
};

const savePairing = async (pairing) => {
  const normalizedCode = normalizePairCode(pairing && pairing.code);
  if (!normalizedCode) return null;
  const nextPairing = {
    code: normalizedCode,
    ownerId: String(pairing.ownerId || "").trim(),
    ownerName: String(pairing.ownerName || "邀请人").trim() || "邀请人",
    guestId: String(pairing.guestId || "").trim(),
    guestName: String(pairing.guestName || "对方").trim() || "对方",
    ownerPublicKey: String(pairing.ownerPublicKey || "").trim(),
    guestPublicKey: String(pairing.guestPublicKey || "").trim(),
    roomId: String(pairing.roomId || normalizedCode).trim().toLowerCase(),
    status: String(pairing.status || "pending"),
    updatedAt: Date.now()
  };
  getPairingsStore()[normalizedCode] = nextPairing;
  await saveStore();
  return nextPairing;
};

const nextPairCode = () => {
  const seed = Math.random().toString(36).slice(2, 8).toUpperCase();
  return `HL-${seed}`;
};

const safeFileName = (value) => String(value || "文件").replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 80) || "file";
const fileId = () => `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
const normalizeMessage = (payload, roomId) => {
  const id = String(payload && payload.id || "").trim();
  const senderId = String(payload && payload.senderId || "").trim();
  const text = String(payload && (payload.text || payload.content) || "").trim();
  const type = String(payload && payload.type || "text");
  const audioDataUrl = String(payload && payload.audioDataUrl || "").trim();
  if (audioDataUrl.startsWith("data:") && audioDataUrl.length > 200000) {
    throw new Error("语音必须先上传为文件，不能直接写入消息");
  }
  const encryptedText = type === "text"
    && payload && payload.encryption
    && String(payload.encryption.ciphertext || "").trim()
    && String(payload.encryption.iv || "").trim();
  if (!id || !senderId || (!text && !encryptedText && !(type === "voice" && audioDataUrl))) return null;
  return {
    ...payload,
    id,
    roomId,
    senderId,
    text,
    type,
    sentAt: Number(payload.sentAt || Date.now()),
    serverUpdatedAt: Date.now()
  };
};

const server = http.createServer(async (request, response) => {
  const method = String(request.method || "GET").toUpperCase();
  if (method === "OPTIONS") {
    sendJson(response, 204, {});
    return;
  }
  const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);
  if (url.pathname === "/api/ai/write") {
    if (method !== "POST") { sendJson(response, 405, { ok: false, message: "当前请求方法不支持" }); return; }
    await handleAiWrite(request, response);
    return;
  }
  const uploadMatch = url.pathname.match(/^\/api\/chat\/files(?:\/([^/]+))?$/);
  if (uploadMatch) {
    const storedId = uploadMatch[1] ? decodeURIComponent(uploadMatch[1]).trim() : "";
    if (method === "GET" && storedId) {
      const files = await fs.promises.readdir(UPLOAD_DIR).catch(() => []);
      const fileName = files.find((name) => name.startsWith(`${storedId}__`));
      if (!fileName) {
        sendJson(response, 404, { error: "文件不存在" });
        return;
      }
      const filePath = path.join(UPLOAD_DIR, fileName);
      const mimeType = decodeURIComponent(fileName.split("__")[1] || "application/octet-stream");
      try {
        const stat = await fs.promises.stat(filePath);
        const rangeHeader = String(request.headers.range || "").trim();
        const commonHeaders = {
          "Content-Type": mimeType,
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "public, max-age=86400",
          "Accept-Ranges": "bytes"
        };
        if (!rangeHeader) {
          response.writeHead(200, { ...commonHeaders, "Content-Length": stat.size });
          fs.createReadStream(filePath).pipe(response);
          return;
        }
        const match = rangeHeader.match(/^bytes=(\d*)-(\d*)$/i);
        if (!match || (!match[1] && !match[2])) {
          response.writeHead(416, { ...commonHeaders, "Content-Range": `bytes */${stat.size}` });
          response.end();
          return;
        }
        let start = match[1] ? Number(match[1]) : Math.max(0, stat.size - Number(match[2]));
        let end = match[2] ? Number(match[2]) : stat.size - 1;
        if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || start >= stat.size || start > end) {
          response.writeHead(416, { ...commonHeaders, "Content-Range": `bytes */${stat.size}` });
          response.end();
          return;
        }
        end = Math.min(end, stat.size - 1);
        response.writeHead(206, {
          ...commonHeaders,
          "Content-Length": end - start + 1,
          "Content-Range": `bytes ${start}-${end}/${stat.size}`
        });
        fs.createReadStream(filePath, { start, end }).pipe(response);
      } catch {
        sendJson(response, 404, { error: "文件不存在" });
      }
      return;
    }
    if (method === "POST" && !storedId) {
      try {
        const contentType = String(request.headers["content-type"] || "").split(";", 1)[0].trim().toLowerCase();
        if (contentType && contentType !== "application/json") {
          const buffer = await readBinaryBody(request);
          if (!buffer.length) {
            sendJson(response, 400, { error: "文件内容为空" });
            return;
          }
          const id = fileId();
          const mimeType = contentType;
          const originalName = decodeURIComponent(String(request.headers["x-file-name"] || "文件")).trim() || "文件";
          const fileName = `${id}__${encodeURIComponent(mimeType)}__${safeFileName(originalName)}`;
          await fs.promises.mkdir(UPLOAD_DIR, { recursive: true });
          await fs.promises.writeFile(path.join(UPLOAD_DIR, fileName), buffer);
          sendJson(response, 201, { id, url: `/api/chat/files/${encodeURIComponent(id)}`, name: originalName, mimeType, size: buffer.length });
          return;
        }
        const payload = await readJsonBody(request);
        const data = String(payload && payload.data || "").trim();
        if (!data) {
          sendJson(response, 400, { error: "文件内容为空" });
          return;
        }
        const buffer = Buffer.from(data, "base64");
        if (!buffer.length || buffer.length > 100 * 1024 * 1024) {
          sendJson(response, 413, { error: "文件超过 100 MB 限制" });
          return;
        }
        const id = fileId();
        const mimeType = String(payload.mimeType || "application/octet-stream").split(";")[0].trim();
        const fileName = `${id}__${encodeURIComponent(mimeType)}__${safeFileName(payload.name || "file")}`;
        await fs.promises.mkdir(UPLOAD_DIR, { recursive: true });
        await fs.promises.writeFile(path.join(UPLOAD_DIR, fileName), buffer);
        sendJson(response, 201, { id, url: `/api/chat/files/${encodeURIComponent(id)}`, name: String(payload.name || "文件"), mimeType, size: buffer.length });
      } catch (error) {
        sendJson(response, 400, { error: error.message || "文件上传失败" });
      }
      return;
    }
    sendJson(response, 405, { error: "当前请求方法不支持" });
    return;
  }
  if (method === "POST" && url.pathname === "/api/pairings/recover") {
    try {
      const payload = await readJsonBody(request);
      const deviceId = String(payload && payload.deviceId || "").trim();
      if (!deviceId) {
        sendJson(response, 400, { error: "缺少设备编号" });
        return;
      }
      const pairing = Object.values(getPairingsStore()).find((item) => {
        return item && item.status === "paired" && (String(item.ownerId || "") === deviceId || String(item.guestId || "") === deviceId);
      }) || null;
      if (!pairing) {
        sendJson(response, 404, { error: "没有找到当前设备的已配对关系" });
        return;
      }
      const isOwner = String(pairing.ownerId || "") === deviceId;
      sendJson(response, 200, {
        roomId: pairing.roomId,
        status: pairing.status,
        role: isOwner ? "owner" : "guest",
        partnerName: isOwner ? pairing.guestName : pairing.ownerName
      });
    } catch (error) {
      sendJson(response, 400, { error: error.message || "恢复绑定失败" });
    }
    return;
  }
  const profileRoomMatch = url.pathname.match(/^\/api\/profiles\/rooms\/([^/]+)$/);
  if (profileRoomMatch) {
    const roomId = decodeURIComponent(profileRoomMatch[1]).trim().toLowerCase();
    if (!roomId) {
      sendJson(response, 400, { error: "缺少房间号" });
      return;
    }
    const deviceId = String(request.headers["x-device-id"] || "").trim();
    if (!isPairedDeviceInRoom(roomId, deviceId)) {
      sendJson(response, 403, { error: "无权访问该房间资料" });
      return;
    }
    const profilesStore = getProfilesStore();
    if (!profilesStore[roomId] || typeof profilesStore[roomId] !== "object") {
      profilesStore[roomId] = { updatedAt: Date.now(), profiles: {} };
    }
    const roomProfiles = profilesStore[roomId];
    if (method === "GET") {
      sendJson(response, 200, { profiles: Object.values(roomProfiles.profiles || {}) });
      return;
    }
    if (method === "POST") {
      try {
        const payload = await readJsonBody(request);
        const id = String(payload.id || "").trim();
        if (!id) {
          sendJson(response, 400, { error: "缺少资料设备标识" });
          return;
        }
        roomProfiles.profiles[id] = {
          id,
          nickname: String(payload.nickname || "小鹿").trim().slice(0, 40) || "小鹿",
          signature: String(payload.signature || "").trim().slice(0, 120),
          avatarIndex: Number.isFinite(Number(payload.avatarIndex)) ? Number(payload.avatarIndex) : 0,
          avatarData: normalizeAvatarData(payload.avatarData),
          updatedAt: Date.now()
        };
        roomProfiles.updatedAt = Date.now();
        await saveStore();
        sendJson(response, 200, { ok: true, profile: roomProfiles.profiles[id] });
      } catch (error) {
        sendJson(response, 400, { error: error.message || "资料保存失败" });
      }
      return;
    }
    sendJson(response, 405, { error: "不支持的请求方法" });
    return;
  }
  const callSignalMatch = url.pathname.match(/^\/api\/call\/rooms\/([^/]+)\/signals$/);
  if (callSignalMatch) {
    const roomId = decodeURIComponent(callSignalMatch[1]).trim().toLowerCase();
    if (!roomId) {
      sendJson(response, 400, { error: "缺少通话房间号" });
      return;
    }
    const callStore = getCallSignalsStore();
    if (!Array.isArray(callStore[roomId])) callStore[roomId] = [];
    const originalSignals = callStore[roomId];
    const signals = cleanCallSignals(originalSignals);
    if (signals.length !== originalSignals.length) {
      callStore[roomId] = signals;
      await saveStore();
    }
    if (method === "GET") {
      const after = Math.max(0, Number(url.searchParams.get("after") || 0));
      sendJson(response, 200, { signals: signals.filter((item) => Number(item.id || 0) > after) });
      return;
    }
    if (method === "POST") {
      try {
        const payload = await readJsonBody(request);
        const senderId = String(payload && payload.senderId || "").trim();
        const type = String(payload && payload.type || "").trim();
        if (!senderId || !type) {
          sendJson(response, 400, { error: "通话信令不完整" });
          return;
        }
        const signal = {
          ...payload,
          id: Date.now() * 1000 + Math.floor(Math.random() * 1000),
          senderId,
          type,
          createdAt: Date.now()
        };
        signals.push(signal);
        callStore[roomId] = cleanCallSignals(signals);
        await saveStore();
        sendJson(response, 200, { ok: true, signal });
      } catch (error) {
        sendJson(response, 400, { error: error.message || "通话信令发送失败" });
      }
      return;
    }
    sendJson(response, 405, { error: "当前请求方法不支持" });
    return;
  }
  const pairRoomMatch = url.pathname.match(/^\/api\/pairings\/rooms\/([^/]+)(?:\/unbind)?$/);
  if (pairRoomMatch) {
    const roomId = decodeURIComponent(pairRoomMatch[1]).trim().toLowerCase();
    if (!roomId) {
      sendJson(response, 400, { error: "缺少房间号" });
      return;
    }
    const pairing = Object.values(getPairingsStore()).find((item) => String(item.roomId || "").toLowerCase() === roomId) || null;
    const legacyRoomExists = Boolean(store.rooms[roomId]);
    if (!pairing && !legacyRoomExists) {
      sendJson(response, 404, { error: "接口不存在" });
      return;
    }
    if (method === "GET") {
      sendJson(response, 200, pairing || { roomId, status: "unbound", legacyRoom: true });
      return;
    }
    if (method === "POST") {
      try {
        const payload = await readJsonBody(request);
        const actorId = String(payload && payload.actorId || "").trim();
        if (!actorId) {
          sendJson(response, 400, { error: "缺少设备编号" });
          return;
        }
        if (pairing && pairing.ownerId && actorId !== pairing.ownerId && actorId !== pairing.guestId) {
          sendJson(response, 403, { error: "无权操作这条绑定关系" });
          return;
        }
        if (pairing) {
          pairing.status = "unbound";
          pairing.guestId = "";
          pairing.guestName = "";
          pairing.updatedAt = Date.now();
          await saveStore();
          sendJson(response, 200, pairing);
          return;
        }
        sendJson(response, 200, { roomId, status: "unbound", legacyRoom: true });
      } catch (error) {
        sendJson(response, 400, { error: error.message || "解除绑定失败" });
      }
      return;
    }
    sendJson(response, 405, { error: "当前请求方法不支持" });
    return;
  }
  const pairKeyMatch = url.pathname.match(/^\/api\/pairings\/rooms\/([^/]+)\/keys$/);
  if (pairKeyMatch && method === "POST") {
    const roomId = decodeURIComponent(pairKeyMatch[1]).trim().toLowerCase();
    const pairing = Object.values(getPairingsStore()).find((item) => String(item.roomId || "").toLowerCase() === roomId) || null;
    if (!pairing || pairing.status !== "paired") {
      sendJson(response, 404, { error: "没有找到有效绑定关系" });
      return;
    }
    try {
      const payload = await readJsonBody(request);
      const actorId = String(payload && payload.actorId || "").trim();
      const publicKey = String(payload && payload.publicKey || "").trim();
      if (!actorId || !publicKey || publicKey.length > 1024) {
        sendJson(response, 400, { error: "设备公钥不完整" });
        return;
      }
      if (actorId === pairing.ownerId) {
        if (pairing.ownerPublicKey && pairing.ownerPublicKey !== publicKey) {
          sendJson(response, 409, { error: "邀请方设备密钥已经固定" });
          return;
        }
        pairing.ownerPublicKey = publicKey;
      } else if (actorId === pairing.guestId) {
        if (pairing.guestPublicKey && pairing.guestPublicKey !== publicKey) {
          sendJson(response, 409, { error: "接收方设备密钥已经固定" });
          return;
        }
        pairing.guestPublicKey = publicKey;
      } else {
        sendJson(response, 403, { error: "设备不属于这条绑定关系" });
        return;
      }
      pairing.updatedAt = Date.now();
      await saveStore();
      sendJson(response, 200, pairing);
    } catch (error) {
      sendJson(response, 400, { error: error.message || "设备公钥保存失败" });
    }
    return;
  }
  if (method === "GET" && url.pathname === "/api/pairings") {
    sendJson(response, 200, { pairings: Object.values(getPairingsStore()) });
    return;
  }
  if (method === "POST" && url.pathname === "/api/pairings") {
    try {
      const payload = await readJsonBody(request);
      const ownerId = String(payload && payload.ownerId || "").trim();
      const ownerName = String(payload && payload.ownerName || "邀请人").trim() || "邀请人";
      if (!ownerId) {
        sendJson(response, 400, { error: "缺少设备编号" });
        return;
      }
      const code = nextPairCode();
      const ownerPublicKey = String(payload && payload.ownerPublicKey || "").trim();
      const pairing = await savePairing({ code, ownerId, ownerName, ownerPublicKey, roomId: code.toLowerCase(), status: "pending" });
      sendJson(response, 200, pairing);
    } catch (error) {
      sendJson(response, 400, { error: error.message || "创建二维码失败" });
    }
    return;
  }
  const pairCodeMatch = url.pathname.match(/^\/api\/pairings\/([^/]+)$/);
  if (pairCodeMatch) {
    const code = normalizePairCode(decodeURIComponent(pairCodeMatch[1]));
    const pairing = getPairing(code);
    if (!pairing) {
      sendJson(response, 404, { error: "接口不存在" });
      return;
    }
    if (method === "GET") {
      sendJson(response, 200, pairing);
      return;
    }
    if (method === "POST") {
      try {
        const payload = await readJsonBody(request);
        const guestId = String(payload && payload.guestId || "").trim();
        const guestName = String(payload && payload.guestName || "对方").trim() || "对方";
        if (!guestId) {
          sendJson(response, 400, { error: "缺少设备编号" });
          return;
        }
        if (pairing.status === "unbound") {
          sendJson(response, 409, { error: "这条绑定关系已解除" });
          return;
        }
        pairing.guestId = guestId;
        pairing.guestName = guestName;
        pairing.status = pairing.ownerId ? "pending" : "paired";
        pairing.updatedAt = Date.now();
        await saveStore();
        sendJson(response, 200, pairing);
      } catch (error) {
        sendJson(response, 400, { error: error.message || "加入申请失败" });
      }
      return;
    }
    sendJson(response, 405, { error: "当前请求方法不支持" });
    return;
  }
  const pairJoinMatch = url.pathname.match(/^\/api\/pairings\/([^/]+)\/join$/);
  if (pairJoinMatch && method === "POST") {
    const code = normalizePairCode(decodeURIComponent(pairJoinMatch[1]));
    const pairing = getPairing(code);
    if (!pairing) {
      sendJson(response, 404, { error: "接口不存在" });
      return;
    }
    try {
      const payload = await readJsonBody(request);
      const guestId = String(payload && payload.guestId || "").trim();
      const guestName = String(payload && payload.guestName || "对方").trim() || "对方";
      const guestPublicKey = String(payload && payload.guestPublicKey || "").trim();
      if (!guestId) {
        sendJson(response, 400, { error: "缺少设备编号" });
        return;
      }
      if (pairing.status === "unbound") {
        sendJson(response, 409, { error: "这条绑定关系已解除" });
        return;
      }
      pairing.guestId = guestId;
      pairing.guestName = guestName;
      if (guestPublicKey) pairing.guestPublicKey = guestPublicKey;
      pairing.status = pairing.ownerId ? "pending" : "paired";
      pairing.updatedAt = Date.now();
      await saveStore();
      sendJson(response, 200, pairing);
    } catch (error) {
      sendJson(response, 400, { error: error.message || "加入申请失败" });
    }
    return;
  }
  const pairConfirmMatch = url.pathname.match(/^\/api\/pairings\/([^/]+)\/confirm$/);
  if (pairConfirmMatch && method === "POST") {
    const code = normalizePairCode(decodeURIComponent(pairConfirmMatch[1]));
    const pairing = getPairing(code);
    if (!pairing) {
      sendJson(response, 404, { error: "接口不存在" });
      return;
    }
    try {
      const payload = await readJsonBody(request);
      const ownerId = String(payload && payload.ownerId || "").trim();
      if (pairing.ownerId && ownerId && ownerId !== pairing.ownerId) {
        sendJson(response, 403, { error: "无权确认这条绑定关系" });
        return;
      }
      pairing.status = "paired";
      pairing.updatedAt = Date.now();
      await saveStore();
      sendJson(response, 200, pairing);
    } catch (error) {
      sendJson(response, 400, { error: error.message || "确认失败" });
    }
    return;
  }
  if (method === "GET" && url.pathname === "/api/health") {
    sendJson(response, 200, { ok: true, service: "heartlink-server", time: Date.now() });
    return;
  }
  const typingMatch = url.pathname.match(/^\/api\/chat\/rooms\/([^/]+)\/typing$/);
  if (typingMatch) {
    const typingRoomId = decodeURIComponent(typingMatch[1]).trim();
    if (!typingRoomId) {
      sendJson(response, 400, { error: "缺少聊天室编号" });
      return;
    }
    const typingRoom = getRoom(typingRoomId);
    if (method === "GET") {
      const typing = typingRoom.typing && Date.now() - Number(typingRoom.typing.updatedAt || 0) < 12000
        ? typingRoom.typing
        : null;
      if (!typing && typingRoom.typing) typingRoom.typing = null;
      sendJson(response, 200, { roomId: typingRoomId, typing, updatedAt: typingRoom.updatedAt });
      return;
    }
    if (method === "POST") {
      try {
        const payload = await readJsonBody(request);
        const senderId = String(payload && payload.senderId || "").trim();
        if (!senderId) {
          sendJson(response, 400, { error: "缺少设备编号" });
          return;
        }
        const active = payload.active === true;
        typingRoom.typing = active ? {
          roomId: typingRoomId,
          senderId,
          active: true,
          updatedAt: Date.now()
        } : null;
        typingRoom.updatedAt = Date.now();
        sendJson(response, 200, { ok: true, roomId: typingRoomId, typing: typingRoom.typing });
      } catch (error) {
        sendJson(response, 400, { error: error.message || "输入状态保存失败" });
      }
      return;
    }
    sendJson(response, 405, { error: "当前请求方法不支持" });
    return;
  }
  const presenceMatch = url.pathname.match(/^\/api\/chat\/rooms\/([^/]+)\/presence$/);
  if (presenceMatch) {
    const presenceRoomId = decodeURIComponent(presenceMatch[1]).trim();
    if (!presenceRoomId) {
      sendJson(response, 400, { error: "缺少聊天室编号" });
      return;
    }
    const presenceRoom = getRoom(presenceRoomId);
    if (!presenceRoom.presence || typeof presenceRoom.presence !== "object") presenceRoom.presence = {};
    if (method === "GET") {
      const viewerId = String(url.searchParams.get("viewerId") || "").trim();
      const now = Date.now();
      const peers = Object.values(presenceRoom.presence).filter((item) => {
        return item && item.peerId !== viewerId && now - Number(item.lastSeenAt || 0) < 15000;
      });
      sendJson(response, 200, {
        roomId: presenceRoomId,
        online: peers.length > 0,
        lastSeenAt: peers.reduce((latest, item) => Math.max(latest, Number(item.lastSeenAt || 0)), 0)
      });
      return;
    }
    if (method === "POST") {
      try {
        const payload = await readJsonBody(request);
        const peerId = String(payload && payload.peerId || "").trim();
        if (!peerId) {
          sendJson(response, 400, { error: "缺少设备编号" });
          return;
        }
        if (payload.online === false) delete presenceRoom.presence[peerId];
        else presenceRoom.presence[peerId] = { peerId, lastSeenAt: Date.now() };
        presenceRoom.updatedAt = Date.now();
        sendJson(response, 200, { ok: true, peerId, online: payload.online !== false });
      } catch (error) {
        sendJson(response, 400, { error: error.message || "在线状态保存失败" });
      }
      return;
    }
    sendJson(response, 405, { error: "当前请求方法不支持" });
    return;
  }
  const match = url.pathname.match(/^\/api\/chat\/rooms\/([^/]+)\/messages(?:\/([^/]+))?$/);
  if (!match) {
    sendJson(response, 404, { error: "接口不存在" });
    return;
  }
  const roomId = decodeURIComponent(match[1]).trim();
  const messageId = match[2] ? decodeURIComponent(match[2]).trim() : "";
  if (!roomId) {
    sendJson(response, 400, { error: "缺少聊天室编号" });
    return;
  }
  const room = getRoom(roomId);
  if (method === "GET" && !messageId) {
    const viewerId = String(url.searchParams.get("viewerId") || "").trim();
    const shouldMarkRead = url.searchParams.get("markRead") === "1";
    let statusChanged = false;
    if (viewerId) {
      const now = Date.now();
      room.messages.forEach((message) => {
        if (message.recalled || !message.senderId || String(message.senderId) === viewerId) return;
        if (!Number(message.deliveredAt || 0)) {
          message.deliveredAt = now;
          message.delivered = true;
          statusChanged = true;
        }
        if (shouldMarkRead && !Number(message.readAt || 0)) {
          message.readAt = now;
          message.read = true;
          statusChanged = true;
        }
        if (statusChanged) message.serverUpdatedAt = now;
      });
    }
    if (statusChanged) {
      room.updatedAt = Date.now();
    }
    sendJson(response, 200, { roomId, messages: room.messages, updatedAt: room.updatedAt });
    if (statusChanged) {
      void saveStore().catch((error) => console.error("保存消息送达状态失败：", error.message));
    }
    return;
  }
  if (method === "POST" && messageId) {
    try {
      const payload = await readJsonBody(request);
      payload.id = messageId;
      const existingIndex = room.messages.findIndex((item) => String(item.id) === messageId);
      const existingMessage = existingIndex >= 0 ? room.messages[existingIndex] : null;
      if (existingMessage && existingMessage.recalled) {
        sendJson(response, 409, existingMessage);
        return;
      }
      const message = normalizeMessage(payload, roomId);
      if (!message) {
        sendJson(response, 400, { error: "消息编号、发送人和文字内容不能为空" });
        return;
      }
      message.sending = false;
      message.sendFailed = false;
      message.deliveredAt = Number(existingMessage?.deliveredAt || 0);
      message.readAt = Number(existingMessage?.readAt || 0);
      message.delivered = message.deliveredAt > 0;
      message.read = message.readAt > 0;
      const index = existingIndex;
      if (index >= 0) room.messages[index] = message;
      else room.messages.push(message);
      room.messages = room.messages.slice(-MAX_MESSAGES_PER_ROOM);
      room.updatedAt = Date.now();
      await saveStore();
      sendJson(response, 200, message);
    } catch (error) {
      sendJson(response, 400, { error: error.message || "消息保存失败" });
    }
    return;
  }
  if (method === "DELETE" && messageId) {
    const index = room.messages.findIndex((item) => String(item.id) === messageId);
    if (index < 0) {
      sendJson(response, 404, { error: "娌℃湁鎵惧埌杩欐潯娑堟伅" });
      return;
    }
    const previous = room.messages[index];
    const recalledAt = Date.now();
    room.messages[index] = {
      id: messageId,
      roomId,
      senderId: String(previous.senderId || ""),
      type: String(previous.type || "text"),
      recalled: true,
      recalledAt,
      serverUpdatedAt: recalledAt
    };
    room.updatedAt = Date.now();
    await saveStore();
    sendJson(response, 200, { ok: true, messageId });
    return;
  }
  sendJson(response, 405, { error: "当前请求方法不支持" });
});

initializePersistence()
  .then(() => {
    server.listen(PORT, HOST, () => {
      console.log(`HeartLink server started: http://${HOST}:${PORT}`);
      console.log(`Health check: http://127.0.0.1:${PORT}/api/health`);
    });
  })
  .catch((error) => {
    console.error("Database initialization failed:", error.message);
    process.exit(1);
  });


