import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

declare const EdgeRuntime: { waitUntil(promise: Promise<unknown>): void };

const GATEWAY_URL = 'https://connector-gateway.lovable.dev/telegram';
const AI_GATEWAY_URL = 'https://ai.gateway.lovable.dev/v1/chat/completions';
const HF_NSFW_API_URL = 'https://api-inference.huggingface.co/models/Falconsai/nsfw_image_detection';
const MAX_RUNTIME_MS = 59_000;
const MIN_REMAINING_MS = 250;
const BATCH_SIZE = 10;
const USER_PAGE_SIZE = 30;
const ACTIVE_GROUP_POLL_TIMEOUT_SEC = 0;
const NSFW_HOT_CACHE_TTL = 45_000;
const NSFW_BULK_WINDOW_MS = 4_500;
const NSFW_BULK_THRESHOLD = 3;
const NSFW_AI_MIN_GAP_MS = 120;
const NSFW_AI_MIN_GAP_FAST_MS = 0;
const TELEGRAM_POLL_LEASE_TTL_MS = 65_000;

const CHANNEL_ID = -1003051940316;
const CHANNEL_LINK = 'https://t.me/ui_zone';
const ADMIN_ID = 2024963199;
const LOG_GROUP_ID = -1003728355864;

// ─── Types ───────────────────────────────────────────────────────────
type TargetPayload = {
  name: string;
  chatId?: number;
  chatType?: string;
  runId?: string;
  leaseToken?: string;
  leaseUntil?: string;
};

// ─── Utility Functions ───────────────────────────────────────────────
function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function escapeHtml(value: unknown): string {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function isGroupChat(chatType?: string | null) {
  return chatType === 'group' || chatType === 'supergroup';
}

function cleanCmd(text: string): string {
  return text.replace(/@\S+/, '').trim();
}

function encodeTargetPayload(payload: TargetPayload) {
  return JSON.stringify(payload);
}

function decodeTargetPayload(raw: string | null | undefined): TargetPayload | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object' && typeof parsed.name === 'string') {
      return {
        name: parsed.name,
        chatId: typeof parsed.chatId === 'number' ? parsed.chatId : undefined,
        chatType: typeof parsed.chatType === 'string' ? parsed.chatType : undefined,
        runId: typeof parsed.runId === 'string' ? parsed.runId : undefined,
        leaseToken: typeof parsed.leaseToken === 'string' ? parsed.leaseToken : undefined,
        leaseUntil: typeof parsed.leaseUntil === 'string' ? parsed.leaseUntil : undefined,
      };
    }
  } catch {
    return { name: raw };
  }
  return { name: raw };
}

function stripLease(payload: TargetPayload): TargetPayload {
  return { name: payload.name, chatId: payload.chatId, chatType: payload.chatType, runId: payload.runId };
}

// ─── Telegram API Helper ────────────────────────────────────────────
async function tg(endpoint: string, body: Record<string, unknown>, lk: string, tk: string) {
  const res = await fetch(`${GATEWAY_URL}/${endpoint}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${lk}`,
      'X-Connection-Api-Key': tk,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  const raw = await res.text();
  try {
    return JSON.parse(raw);
  } catch {
    return {
      ok: false,
      error_code: res.status,
      description: raw || `HTTP ${res.status}`,
    };
  }
}

async function deleteTelegramMessage(chatId: number, messageId: number, lk: string, tk: string, retries = 2) {
  let lastError: any = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const data = await tg('deleteMessage', { chat_id: chatId, message_id: messageId }, lk, tk);
      if (data?.ok) {
        return { ok: true, description: 'deleted', data };
      }

      const description = String(data?.description ?? 'delete_failed');
      lastError = { ok: false, description, data };

      const retryAfter = Number(data?.parameters?.retry_after ?? 0);
      if (retryAfter > 0 && attempt < retries) {
        await sleep((retryAfter * 1000) + 150);
        continue;
      }

      if (
        attempt < retries &&
        !/message to delete not found|message can't be deleted|not enough rights|have no rights|chat not found/i.test(description)
      ) {
        await sleep(200 * (attempt + 1));
        continue;
      }

      return { ok: false, description, data };
    } catch (error) {
      lastError = { ok: false, description: error instanceof Error ? error.message : String(error), data: null };
      if (attempt === retries) {
        return lastError;
      }
      await sleep(200 * (attempt + 1));
    }
  }

  return lastError ?? { ok: false, description: 'unknown_delete_error', data: null };
}

async function tgSendMessage(body: Record<string, unknown>, lk: string, tk: string, retries = 8): Promise<boolean> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const data = await tg('sendMessage', body, lk, tk);
      if (data?.ok) return true;
      const retryAfter = Number(data?.parameters?.retry_after ?? 0);
      if (retryAfter > 0) { await sleep((retryAfter * 1000) + 150); continue; }
      const description = String(data?.description ?? '').toLowerCase();
      if (data?.error_code === 429 || description.includes('too many requests')) { await sleep(500 * (attempt + 1)); continue; }
      console.error('sendMessage failed:', data);
      return false;
    } catch (error) {
      if (attempt === retries) { console.error('sendMessage request failed:', error); return false; }
      await sleep(250 * (attempt + 1));
    }
  }
  return false;
}

// ─── Activity Log to Group ──────────────────────────────────────────
function sendLog(lk: string, tk: string, logText: string) {
  if (!LOG_GROUP_ID) return;
  tg('sendMessage', {
    chat_id: LOG_GROUP_ID,
    text: logText,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
  }, lk, tk).catch(() => {});
}

function userTag(userId: number, firstName: string | null, username: string | null): string {
  const parts: string[] = [];
  if (firstName) parts.push(`<b>${firstName}</b>`);
  if (username) parts.push(`@${username}`);
  parts.push(`(<code>${userId}</code>)`);
  return parts.join(' | ');
}

function chatTag(chat: any): string {
  const title = chat?.title || 'Unknown';
  const chatId = chat?.id || '?';
  const username = chat?.username ? `@${chat.username}` : '';
  return `<b>${title}</b> ${username} (<code>${chatId}</code>)`;
}
// ─── Caches ─────────────────────────────────────────────────────────
const subCache = new Map<number, { ok: boolean; ts: number }>();
const SUB_CACHE_TTL = 30_000;
const linesCache = new Map<string, string[]>();
let cachedBotId: number | null = null;
const nsfwCache = new Map<number, { on: boolean; ts: number }>();
const NSFW_CACHE_TTL = 60_000;
const nsfwMediaCache = new Map<string, { flagged: boolean; reason: string; ts: number }>();
const nsfwInflightCache = new Map<string, Promise<{ flagged: boolean; reason: string }>>();
const nsfwHotSenderCache = new Map<string, { reason: string; ts: number }>();
const nsfwHotAlbumCache = new Map<string, { reason: string; ts: number }>();
const nsfwKnownMediaCache = new Map<string, { reason: string; ts: number }>();
const nsfwStickerSetCache = new Map<string, { reason: string; ts: number }>();
const nsfwBurstCache = new Map<string, { count: number; ts: number }>();
// ─── NSFW User-managed Sticker Pack Blacklist ───
const nsfwUserBlacklistCache = new Map<number, { packs: Set<string>; ts: number }>();
const NSFW_USER_BLACKLIST_TTL = 60_000;
const NSFW_MEDIA_CACHE_TTL = 6 * 60 * 60 * 1000;
const NSFW_KNOWN_MEDIA_CACHE_TTL = 30 * 24 * 60 * 60 * 1000;
const NSFW_STICKER_SET_CACHE_TTL = 30 * 24 * 60 * 60 * 1000;
let lastNsfwAiRequestAt = 0;

// ─── NSFW Filter ────────────────────────────────────────────────────
const NSFW_MEDIA_KEYWORDS = [
  'nude', 'nsfw', 'xxx', 'porn', 'hentai', 'sex', 'boob', 'dick', 'pussy', 'cock',
  'ass', 'tit', 'naked', 'erotic', 'lewd', 'ahegao', 'rule34', 'r34', 'futanari',
  'ecchi', 'oppai', 'milf', 'thot', '18+', 'adult', 'sexy', 'hot_girl', 'hot_babe',
  'bikini', 'lingerie', 'onlyfans', 'fap', 'waifu_nsfw', 'strip', 'horny', 'nangi',
  'chudai', 'lund', 'gaand', 'randi', 'nude_sticker', 'adult_sticker', 'boobs',
  'breast', 'breasts', 'nipple', 'nipples', 'topless', 'areola', 'cameltoe', 'cum',
  'blowjob', 'bj', 'anal', 'creampie', 'slut', 'busty', 'brazzers', 'xnxx', 'xvideos',
  'bhabhi', 'mallu', 'desi_nude', 'semi_nude', 'booby',
];

const NSFW_STICKER_SET_KEYWORDS = [
  '18+', 'nsfw', 'xxx', 'porn', 'hentai', 'rule34', 'r34', 'futanari', 'nude', 'nudes',
  'sex', 'blowjob', 'bj', 'anal', 'boob', 'boobs', 'breast', 'breasts', 'nipple',
  'nipples', 'topless', 'areola', 'penis', 'dick', 'cock', 'pussy', 'vagina', 'cum',
  'creampie', 'adult_sticker', 'nude_sticker',
];

const NSFW_STICKER_SET_STRONG_KEYWORDS = [
  'porn', 'hentai', 'rule34', 'r34', 'futanari', 'blowjob', 'creampie', 'adult_sticker', 'nude_sticker',
];

async function isNsfwFilterOn(sb: any, chatId: number): Promise<boolean> {
  const cached = nsfwCache.get(chatId);
  if (cached && (Date.now() - cached.ts < NSFW_CACHE_TTL)) return cached.on;
  const { data } = await sb.from('chat_settings').select('nsfw_filter').eq('chat_id', chatId).maybeSingle();
  const on = data?.nsfw_filter === true;
  nsfwCache.set(chatId, { on, ts: Date.now() });
  return on;
}

function isStickerSetNsfw(setName: string | null | undefined): boolean {
  if (!setName) return false;
  const lower = setName.toLowerCase();
  return NSFW_STICKER_SET_KEYWORDS.some((kw) => lower.includes(kw));
}

function isStrongExplicitStickerSet(setName: string | null | undefined): boolean {
  if (!setName) return false;

  const lower = setName.toLowerCase();
  if (NSFW_STICKER_SET_STRONG_KEYWORDS.some((kw) => lower.includes(kw))) return true;

  const bodyKeywordMatches = NSFW_STICKER_SET_KEYWORDS.filter((kw) => lower.includes(kw));
  return new Set(bodyKeywordMatches).size >= 2;
}

function shouldUseNsfwHotSender(msg: any): boolean {
  return !msg?.sticker && !!(
    msg?.media_group_id ||
    (Array.isArray(msg?.photo) && msg.photo.length > 0) ||
    msg?.video ||
    msg?.animation ||
    msg?.document
  );
}

function getNsfwSenderKey(chatId: unknown, userId: unknown) {
  const chat = Number(chatId);
  const user = Number(userId);
  if (!Number.isFinite(chat) || !Number.isFinite(user)) return null;
  return `${chat}:${user}`;
}

function getNsfwAlbumKey(chatId: unknown, mediaGroupId: unknown) {
  const chat = Number(chatId);
  if (!Number.isFinite(chat) || !mediaGroupId) return null;
  return `${chat}:${String(mediaGroupId)}`;
}

function getFreshCacheReason(cache: Map<string, { reason: string; ts: number }>, key: string | null, ttlMs: number) {
  if (!key) return null;
  const value = cache.get(key);
  if (!value) return null;
  if ((Date.now() - value.ts) > ttlMs) {
    cache.delete(key);
    return null;
  }
  return value.reason;
}

function getPersistentNsfwMediaKey(msg: any) {
  const candidate = getModerationCandidate(msg);
  if (!candidate.cacheKey || candidate.label === 'unknown') return null;
  return `${candidate.label}:${candidate.cacheKey}`;
}

function getStickerSetCacheKey(setName: string | null | undefined) {
  const normalized = String(setName ?? '').trim().toLowerCase();
  return normalized || null;
}

async function getKnownNsfwStickerSet(sb: any, setName: string | null | undefined) {
  const cacheKey = getStickerSetCacheKey(setName);
  if (!cacheKey) return null;

  const cachedReason = getFreshCacheReason(nsfwStickerSetCache, cacheKey, NSFW_STICKER_SET_CACHE_TTL);
  if (cachedReason) return cachedReason;

  const { data, error } = await sb
    .from('bot_history')
    .select('target_name')
    .eq('mode', 'nsfw_sticker_pack')
    .eq('line', setName)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error('getKnownNsfwStickerSet failed:', error);
    return null;
  }

  if (!data) return null;

  const reason = typeof data.target_name === 'string' && data.target_name.trim()
    ? data.target_name
    : `known_pack:${setName}`;

  nsfwStickerSetCache.set(cacheKey, { reason, ts: Date.now() });
  return reason;
}

async function rememberNsfwStickerSet(sb: any, setName: string | null | undefined, reason: string) {
  const cacheKey = getStickerSetCacheKey(setName);
  if (!cacheKey || !setName) return;

  nsfwStickerSetCache.set(cacheKey, { reason, ts: Date.now() });

  const { data, error } = await sb
    .from('bot_history')
    .select('id')
    .eq('mode', 'nsfw_sticker_pack')
    .eq('line', setName)
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error('rememberNsfwStickerSet lookup failed:', error);
    return;
  }

  if (data?.id) return;

  const { error: insertError } = await sb.from('bot_history').insert({
    user_id: 0,
    mode: 'nsfw_sticker_pack',
    line: setName,
    target_name: reason.slice(0, 180),
  });

  if (insertError) {
    console.error('rememberNsfwStickerSet insert failed:', insertError);
  }
}

// ─── NSFW User Blacklist Pack Management ────────────────────────
function extractStickerSetName(input: string): string | null {
  const trimmed = input.trim();
  // Handle t.me/addstickers/PACKNAME links
  const linkMatch = trimmed.match(/(?:t\.me|telegram\.me)\/addstickers\/([a-zA-Z0-9_]+)/i);
  if (linkMatch) return linkMatch[1];
  // Handle raw set name
  if (/^[a-zA-Z0-9_]+$/.test(trimmed) && trimmed.length >= 3) return trimmed;
  return null;
}

async function getUserBlacklistedPacks(sb: any, chatId: number): Promise<Set<string>> {
  const cached = nsfwUserBlacklistCache.get(chatId);
  if (cached && (Date.now() - cached.ts < NSFW_USER_BLACKLIST_TTL)) return cached.packs;
  const { data } = await sb.from('bot_history').select('line').eq('mode', 'nsfw_user_blacklist').eq('user_id', chatId);
  const packs = new Set((data || []).map((r: any) => String(r.line).toLowerCase()));
  nsfwUserBlacklistCache.set(chatId, { packs, ts: Date.now() });
  return packs;
}

async function addUserBlacklistPack(sb: any, chatId: number, setName: string): Promise<boolean> {
  const existing = await getUserBlacklistedPacks(sb, chatId);
  if (existing.has(setName.toLowerCase())) return false;
  await sb.from('bot_history').insert({ user_id: chatId, mode: 'nsfw_user_blacklist', line: setName.toLowerCase(), target_name: setName });
  nsfwUserBlacklistCache.delete(chatId);
  return true;
}

async function removeUserBlacklistPack(sb: any, chatId: number, setName: string): Promise<boolean> {
  const { data } = await sb.from('bot_history').delete().eq('mode', 'nsfw_user_blacklist').eq('user_id', chatId).eq('line', setName.toLowerCase()).select('id');
  nsfwUserBlacklistCache.delete(chatId);
  return (data || []).length > 0;
}

async function getKnownNsfwMedia(sb: any, msg: any) {
  const mediaKey = getPersistentNsfwMediaKey(msg);
  if (!mediaKey) return null;

  const cachedReason = getFreshCacheReason(nsfwKnownMediaCache, mediaKey, NSFW_KNOWN_MEDIA_CACHE_TTL);
  if (cachedReason) return cachedReason;

  const { data, error } = await sb
    .from('bot_history')
    .select('target_name')
    .eq('mode', 'nsfw_media')
    .eq('user_id', 0)
    .eq('line', mediaKey)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error('getKnownNsfwMedia failed:', error);
    return null;
  }

  if (!data) return null;

  const reason = typeof data.target_name === 'string' && data.target_name.trim()
    ? data.target_name
    : `known_media:${mediaKey}`;

  nsfwKnownMediaCache.set(mediaKey, { reason, ts: Date.now() });
  return reason;
}

async function rememberKnownNsfwMedia(sb: any, msg: any, reason: string) {
  const mediaKey = getPersistentNsfwMediaKey(msg);
  if (!mediaKey) return;

  nsfwKnownMediaCache.set(mediaKey, { reason, ts: Date.now() });

  const { data, error } = await sb
    .from('bot_history')
    .select('id')
    .eq('mode', 'nsfw_media')
    .eq('user_id', 0)
    .eq('line', mediaKey)
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error('rememberKnownNsfwMedia lookup failed:', error);
    return;
  }

  if (data?.id) return;

  const { error: insertError } = await sb.from('bot_history').insert({
    user_id: 0,
    mode: 'nsfw_media',
    line: mediaKey,
    target_name: reason.slice(0, 180),
  });

  if (insertError) {
    console.error('rememberKnownNsfwMedia insert failed:', insertError);
  }
}

function markNsfwHot(msg: any, reason: string) {
  const now = Date.now();
  const albumKey = getNsfwAlbumKey(msg.chat?.id, msg.media_group_id);
  if (albumKey) nsfwHotAlbumCache.set(albumKey, { reason, ts: now });
}

function getNsfwHotReason(msg: any) {
  const albumReason = getFreshCacheReason(nsfwHotAlbumCache, getNsfwAlbumKey(msg.chat?.id, msg.media_group_id), NSFW_HOT_CACHE_TTL);
  if (albumReason) return `album_hot:${albumReason}`;
  return null;
}

function noteNsfwBurst(msg: any) {
  const senderKey = getNsfwSenderKey(msg.chat?.id, msg.from?.id);
  if (!senderKey) return { count: 1, burst: false };

  const now = Date.now();
  const prev = nsfwBurstCache.get(senderKey);
  const nextCount = prev && (now - prev.ts) <= NSFW_BULK_WINDOW_MS ? prev.count + 1 : 1;
  nsfwBurstCache.set(senderKey, { count: nextCount, ts: now });

  const inferredCount = msg.media_group_id ? Math.max(nextCount, 2) : nextCount;
  return { count: inferredCount, burst: !!msg.media_group_id || inferredCount >= NSFW_BULK_THRESHOLD };
}

function bytesToBase64(bytes: Uint8Array) {
  let binary = '';
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
}

function guessMimeType(filePath: string) {
  const lower = filePath.toLowerCase();
  if (lower.endsWith('.jpg') || lower.endsWith('.jpeg')) return 'image/jpeg';
  if (lower.endsWith('.png')) return 'image/png';
  if (lower.endsWith('.webp')) return 'image/webp';
  if (lower.endsWith('.gif')) return 'image/gif';
  return 'image/jpeg';
}

function extractJsonObject(raw: string) {
  const trimmed = raw.replace(/[\u0000-\u001F\u007F]/g, ' ').trim();
  const fenced = trimmed.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  const candidate = fenced?.[1] || trimmed;
  const start = candidate.indexOf('{');
  const end = candidate.lastIndexOf('}');
  return start >= 0 && end > start ? candidate.slice(start, end + 1) : candidate;
}

function normalizeAiContent(raw: unknown): string {
  if (typeof raw === 'string') return raw;
  if (Array.isArray(raw)) {
    return raw.map((part: any) => {
      if (typeof part === 'string') return part;
      if (typeof part?.text === 'string') return part.text;
      if (typeof part?.content === 'string') return part.content;
      return '';
    }).join(' ');
  }
  if (raw && typeof raw === 'object' && typeof (raw as { content?: unknown }).content === 'string') {
    return (raw as { content: string }).content;
  }
  return JSON.stringify(raw ?? {});
}

function parseModerationJson(raw: unknown) {
  const candidate = extractJsonObject(normalizeAiContent(raw))
    .replace(/[\u0000-\u001F\u007F]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  try {
    return JSON.parse(candidate || '{}');
  } catch {
    const confidenceMatch = candidate.match(/confidence[^0-9]*([0-9]{1,3})/i);
    const reasonMatch = candidate.match(/reason[^a-z0-9]*["']?([^"'}]+)["']?/i);
    return {
      explicit: /\btrue\b|explicit[^a-z]*yes|explicit content/i.test(candidate),
      confidence: confidenceMatch ? Number(confidenceMatch[1]) : 0,
      reason: reasonMatch?.[1]?.trim() || candidate.slice(0, 140) || 'ai_parse_fallback',
    };
  }
}

async function waitForNsfwAiSlot() {
  lastNsfwAiRequestAt = Date.now();
}

function isTrueLike(value: unknown) {
  return value === true || value === 'true' || value === 1 || value === '1';
}

function reasonHasTargetExplicitPart(reason: string) {
  if (!reason) return false;
  const lower = reason.toLowerCase();

  if (
    /\b(no|not|without|covered|hidden|safe|none|non-explicit)\b.{0,18}\b(penis|dick|cock|vagina|pussy|genital|breast|boob|nipple|areola|butt|ass|anus|sex act|oral sex|penetration)\b/.test(lower) ||
    /\b(penis|dick|cock|vagina|pussy|genital|breast|boob|nipple|areola|butt|ass|anus|sex act|oral sex|penetration)\b.{0,18}\b(not visible|covered|hidden|safe|not shown|not exposed)\b/.test(lower)
  ) {
    return false;
  }

  return /\b(penis|dick|cock|vagina|pussy|genital|breast|boob|nipple|areola|topless|butt|ass|anus|blowjob|oral sex|sex act|penetration|masturbation)\b/.test(lower);
}

async function downloadTelegramFileAsDataUrl(fileId: string, lk: string, tk: string): Promise<string | null> {
  try {
    const fileRes = await tg('getFile', { file_id: fileId }, lk, tk);
    const filePath = fileRes?.result?.file_path;
    if (!fileRes?.ok || !filePath) return null;

    const downloadRes = await fetch(`${GATEWAY_URL}/file/${filePath}`, {
      headers: {
        Authorization: `Bearer ${lk}`,
        'X-Connection-Api-Key': tk,
      },
    });

    if (!downloadRes.ok) return null;
    const bytes = new Uint8Array(await downloadRes.arrayBuffer());
    if (bytes.length === 0) return null;

    const mimeType = guessMimeType(filePath);
    return `data:${mimeType};base64,${bytesToBase64(bytes)}`;
  } catch (error) {
    console.error('downloadTelegramFileAsDataUrl failed:', error);
    return null;
  }
}

function dataUrlToBytes(dataUrl: string): Uint8Array {
  const base64 = dataUrl.split(',')[1];
  if (!base64) return new Uint8Array(0);
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

async function classifyWithHuggingFace(dataUrl: string, label: string): Promise<{ flagged: boolean; reason: string } | null> {
  try {
    const imageBytes = dataUrlToBytes(dataUrl);
    if (imageBytes.length === 0) return null;

    const res = await fetch(HF_NSFW_API_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: imageBytes,
    });

    if (!res.ok) {
      const errText = await res.text();
      console.error('HF NSFW API failed:', res.status, errText.slice(0, 200));
      return null;
    }

    const results: Array<{ label: string; score: number }> = await res.json();
    if (!Array.isArray(results) || results.length === 0) return null;

    const nsfwEntry = results.find((r) => r.label === 'nsfw');
    const normalEntry = results.find((r) => r.label === 'normal');
    const nsfwScore = nsfwEntry?.score ?? 0;
    const normalScore = normalEntry?.score ?? 1;

    console.log('HF_NSFW_RESULT', { label, nsfwScore: Math.round(nsfwScore * 100), normalScore: Math.round(normalScore * 100) });

    // Stickers need very high confidence to avoid false positives on cartoons
    const threshold = label === 'sticker' ? 0.85 : 0.70;
    const flagged = nsfwScore >= threshold;

    return {
      flagged,
      reason: flagged ? `hf_nsfw:${Math.round(nsfwScore * 100)}%` : `hf_safe:${Math.round(normalScore * 100)}%`,
    };
  } catch (err) {
    console.error('classifyWithHuggingFace error:', err);
    return null;
  }
}

async function classifyExplicitImage(dataUrl: string, lk: string, label: string): Promise<{ flagged: boolean; reason: string }> {
  // Try free HuggingFace API first
  const hfResult = await classifyWithHuggingFace(dataUrl, label);
  if (hfResult) return hfResult;

  // Fallback to AI gateway
  try {
    await waitForNsfwAiSlot();

    const aiRes = await fetch(AI_GATEWAY_URL, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${lk}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'google/gemini-2.5-flash-lite',
        messages: [
          {
            role: 'system',
            content: 'You are a strict Telegram adult-content classifier. Return ONLY compact JSON: {"explicit": boolean, "visible_genitals": boolean, "visible_breasts": boolean, "visible_buttocks": boolean, "sex_act": boolean, "confidence": number, "reason": string}. Mark explicit=true ONLY when the media CLEARLY and UNMISTAKABLY shows REAL nudity: fully exposed penis/vagina/genitals, fully bare breasts with nipples/areola clearly visible, fully bare buttocks/anus, or an obvious sexual act (blowjob/penetration/masturbation). You MUST mark explicit=false for ALL of the following: cartoon/anime characters (unless extremely graphic hentai with visible genitals), emoji stickers, funny stickers, meme stickers, reaction stickers, cute animal stickers, normal selfies, clothed people, cleavage, bikini, swimsuit, lingerie, bra, covered body parts, suggestive poses WITHOUT visible private parts, text-based stickers, artistic drawings, romantic stickers, kiss stickers. When in doubt, ALWAYS return explicit=false. Better to miss one bad image than delete a normal sticker.'
          },
          {
            role: 'user',
            content: [
              { type: 'text', text: 'Delete only if private adult body parts are actually visible or there is a clear sex act. JSON only.' },
              { type: 'image_url', image_url: { url: dataUrl } },
            ],
          },
        ],
        temperature: 0,
        max_tokens: 90,
      }),
    });

    if (!aiRes.ok) {
      console.error('NSFW AI request failed:', aiRes.status, await aiRes.text());
      return { flagged: false, reason: aiRes.status === 429 ? 'ai_rate_limited' : 'ai_request_failed' };
    }

    const aiData = await aiRes.json();
    const parsed = parseModerationJson(aiData?.choices?.[0]?.message?.content ?? aiData?.choices?.[0]?.message ?? '{}');
    const rawConfidence = Number(parsed?.confidence ?? 0);
    const normalizedConfidence = Number.isFinite(rawConfidence)
      ? (rawConfidence > 0 && rawConfidence <= 1 ? rawConfidence * 100 : rawConfidence)
      : 0;
    const confidence = Math.max(0, Math.min(100, normalizedConfidence));
    const reason = typeof parsed?.reason === 'string' ? parsed.reason.trim() || 'ai_classified_safe' : 'ai_classified_safe';
    const modelExplicit = isTrueLike((parsed as any)?.explicit);
    const visibleGenitals = isTrueLike((parsed as any)?.visible_genitals);
    const visibleBreasts = isTrueLike((parsed as any)?.visible_breasts);
    const visibleButtocks = isTrueLike((parsed as any)?.visible_buttocks);
    const sexAct = isTrueLike((parsed as any)?.sex_act);
    const explicitFromReason = reasonHasTargetExplicitPart(reason);
    console.log('NSFW_AI_RAW', { label, confidence, reason, modelExplicit, visibleGenitals, visibleBreasts, visibleButtocks, sexAct, explicitFromReason });
    const severeTarget = visibleGenitals || sexAct;
    const moderateTarget = visibleBreasts || visibleButtocks || explicitFromReason;
    const aiFallbackTarget = modelExplicit && confidence >= 55;

    if (label === 'sticker') {
      // Very high thresholds for stickers to avoid false positives on normal/cartoon stickers
      const stickerFlagged = (severeTarget && confidence >= 80) || (moderateTarget && confidence >= 85) || (modelExplicit && confidence >= 90);
      return { flagged: stickerFlagged, reason: `${reason} (${confidence})` };
    }

    return {
      flagged: (severeTarget && confidence >= 45) || (moderateTarget && confidence >= 55) || aiFallbackTarget,
      reason: `${reason} (${confidence})`,
    };
  } catch (error) {
    console.error('classifyExplicitImage failed:', error);
    return { flagged: false, reason: 'ai_parse_failed' };
  }
}

function requiresImmediateNsfwAiCheck(msg: any): boolean {
  // Check stickers, animations (GIFs), and videos for porn — skip photos, documents, screenshots
  return !!(
    msg.sticker ||
    msg.video ||
    msg.animation
  );
}

function getModerationCandidate(msg: any): { fileId: string | null; cacheKey: string | null; label: string } {
  if (msg.sticker) {
    const thumbId = msg.sticker.thumbnail?.file_id || msg.sticker.thumb?.file_id || null;
    const stickerId = (!msg.sticker.is_animated && !msg.sticker.is_video) ? msg.sticker.file_id : null;
    return {
      fileId: thumbId || stickerId,
      cacheKey: msg.sticker.file_unique_id || thumbId || stickerId,
      label: 'sticker',
    };
  }

  if (Array.isArray(msg.photo) && msg.photo.length > 0) {
    const photo = msg.photo[Math.max(0, msg.photo.length - 2)] || msg.photo[msg.photo.length - 1];
    return { fileId: photo.file_id, cacheKey: photo.file_unique_id || photo.file_id, label: 'photo' };
  }

  if (msg.video) {
    const thumbId = msg.video.thumbnail?.file_id || msg.video.thumb?.file_id || null;
    return { fileId: thumbId, cacheKey: msg.video.file_unique_id || thumbId, label: 'video' };
  }

  if (msg.animation) {
    const thumbId = msg.animation.thumbnail?.file_id || msg.animation.thumb?.file_id || null;
    return { fileId: thumbId, cacheKey: msg.animation.file_unique_id || thumbId, label: 'animation' };
  }

  if (msg.document) {
    const thumbId = msg.document.thumbnail?.file_id || msg.document.thumb?.file_id || null;
    const isImageDoc = String(msg.document.mime_type || '').startsWith('image/');
    return {
      fileId: thumbId || (isImageDoc ? msg.document.file_id : null),
      cacheKey: msg.document.file_unique_id || thumbId || msg.document.file_id,
      label: 'document',
    };
  }

  return { fileId: null, cacheKey: null, label: 'unknown' };
}

function quickNsfwCheck(msg: any): { flagged: boolean; reason: string } {
  // Stickers only get fast-deleted for historically explicit packs or very strong porn pack names.
  if (msg.sticker) {
    if (isStrongExplicitStickerSet(msg.sticker.set_name)) {
      return { flagged: true, reason: `sticker_set_keyword:${msg.sticker.set_name}` };
    }
    return { flagged: false, reason: '' };
  }

  const strictPornKeywords = [
    '18+', 'nsfw', 'xxx', 'porn', 'hentai', 'rule34', 'r34', 'futanari', 'nude', 'nudes',
    'sex', 'blowjob', 'bj', 'anal', 'boob', 'boobs', 'breast', 'breasts', 'nipple',
    'nipples', 'topless', 'areola', 'penis', 'dick', 'cock', 'pussy', 'vagina', 'cum',
    'creampie', 'adult_sticker', 'nude_sticker', 'xnxx', 'xvideos', 'brazzers',
  ];

  const signalText = [
    msg.caption,
    msg.animation?.file_name,
    msg.video?.file_name,
    msg.document?.file_name,
  ]
    .filter((value) => typeof value === 'string' && value.trim().length > 0)
    .join(' ')
    .toLowerCase();

  const matched = strictPornKeywords.find((kw) => signalText.includes(kw));
  if (matched) {
    return { flagged: true, reason: `media_keyword:${matched}` };
  }

  return { flagged: false, reason: '' };
}

function isFreshTelegramPollLease(updatedAt: string | null | undefined) {
  if (!updatedAt) return false;
  const leaseTs = Date.parse(updatedAt);
  return Number.isFinite(leaseTs) && (Date.now() - leaseTs) < TELEGRAM_POLL_LEASE_TTL_MS;
}

async function acquireTelegramPollLease(sb: any, updatedAt: string | null | undefined) {
  const nextLease = new Date().toISOString();
  let query = sb.from('telegram_bot_state').update({ updated_at: nextLease }).eq('id', 1);
  if (updatedAt) query = query.eq('updated_at', updatedAt);

  const { data, error } = await query.select('updated_at').maybeSingle();
  if (error || !data) {
    if (error) console.error('acquireTelegramPollLease failed:', error);
    return null;
  }

  return nextLease;
}

async function refreshTelegramPollLease(sb: any, leaseToken: string, updateOffset?: number) {
  const nextLease = new Date().toISOString();
  const payload: Record<string, unknown> = { updated_at: nextLease };
  if (typeof updateOffset === 'number') payload.update_offset = updateOffset;

  const { data, error } = await sb
    .from('telegram_bot_state')
    .update(payload)
    .eq('id', 1)
    .eq('updated_at', leaseToken)
    .select('updated_at')
    .maybeSingle();

  if (error || !data) {
    if (error) console.error('refreshTelegramPollLease failed:', error);
    return null;
  }

  return nextLease;
}

async function shouldDeleteForNsfw(msg: any, lk: string, tk: string): Promise<{ flagged: boolean; reason: string }> {
  const candidate = getModerationCandidate(msg);
  if (!candidate.fileId || !candidate.cacheKey) {
    return { flagged: false, reason: 'no_visual_candidate' };
  }

  const fileId = candidate.fileId;
  const cacheKey = candidate.cacheKey;

  const cached = nsfwMediaCache.get(cacheKey);
  if (cached && (Date.now() - cached.ts < NSFW_MEDIA_CACHE_TTL)) {
    return { flagged: cached.flagged, reason: `cache:${cached.reason}` };
  }

  const inFlight = nsfwInflightCache.get(cacheKey);
  if (inFlight) return inFlight;

  const pending = (async () => {
    const dataUrl = await downloadTelegramFileAsDataUrl(fileId, lk, tk);
    if (!dataUrl) {
      return { flagged: false, reason: `download_failed:${candidate.label}` };
    }

    const result = await classifyExplicitImage(dataUrl, lk, candidate.label);
    nsfwMediaCache.set(cacheKey, { flagged: result.flagged, reason: result.reason, ts: Date.now() });
    return result;
  })();

  nsfwInflightCache.set(cacheKey, pending);

  try {
    return await pending;
  } finally {
    nsfwInflightCache.delete(cacheKey);
  }
}

async function getBotId(lk: string, tk: string): Promise<number | null> {
  if (cachedBotId) return cachedBotId;
  try {
    const res = await tg('getMe', {}, lk, tk);
    if (res?.ok && res.result?.id) {
      cachedBotId = res.result.id;
      return cachedBotId;
    }
  } catch {}
  return null;
}

async function getLines(sb: any, mode: string): Promise<string[]> {
  if (linesCache.has(mode)) return linesCache.get(mode)!;
  const { data } = await sb.from('gali_lines').select('line_text').eq('mode', mode).order('id', { ascending: true });
  const lines = (data || []).map((r: any) => r.line_text);
  linesCache.set(mode, lines);
  return lines;
}

// ─── User Management ────────────────────────────────────────────────
async function addOrUpdateUser(sb: any, userId: number, firstName: string | null, username: string | null): Promise<boolean> {
  const now = new Date().toISOString();
  const normalizedUsername = normalizeTelegramUsername(username);
  const { data } = await sb.from('bot_users').upsert(
    { user_id: userId, first_name: firstName, username: normalizedUsername, last_active: now, created_at: now },
    { onConflict: 'user_id', ignoreDuplicates: false },
  ).select('is_banned').single();
  return data?.is_banned === true;
}

async function addOrUpdateChatTarget(sb: any, chat: any) {
  if (!isGroupChat(chat?.type)) return;
  const chatId = Number(chat?.id);
  if (!Number.isFinite(chatId) || chatId >= 0) return;

  const now = new Date().toISOString();
  const title = chat?.title || chat?.username || `Group ${chatId}`;
  const normalizedUsername = normalizeTelegramUsername(chat?.username);

  await sb.from('bot_users').upsert(
    { user_id: chatId, first_name: title, username: normalizedUsername, last_active: now, created_at: now, is_banned: false },
    { onConflict: 'user_id', ignoreDuplicates: false },
  );
}

function normalizeTelegramUsername(value: string | null | undefined) {
  return value ? value.replace(/^@+/, '').trim().toLowerCase() : null;
}

function buildCallbackAlert(title: string, body: string) {
  const cleanBody = body.trim();
  const maxBodyLength = Math.max(0, 190 - title.length - 2);
  const clippedBody = cleanBody.length > maxBodyLength
    ? `${cleanBody.slice(0, Math.max(0, maxBodyLength - 1)).trimEnd()}…`
    : cleanBody;

  return `${title}\n\n${clippedBody}`;
}

async function resolveSecretViewer(sb: any, secret: any, userId: number, username: string | null) {
  const normalizedTargetUsername = normalizeTelegramUsername(secret.target_username);
  const normalizedCurrentUsername = normalizeTelegramUsername(username);

  if (secret.sender_id === userId) {
    return { canView: true, isSender: true };
  }

  if (secret.target_user_id === userId) {
    return { canView: true, isSender: false };
  }

  if (normalizedTargetUsername && normalizedCurrentUsername && normalizedTargetUsername === normalizedCurrentUsername) {
    if (!secret.target_user_id) {
      await sb.from('secret_messages').update({ target_user_id: userId }).eq('id', secret.id);
    }
    return { canView: true, isSender: false };
  }

  if (normalizedTargetUsername) {
    const { data: knownUser } = await sb.from('bot_users').select('username').eq('user_id', userId).maybeSingle();
    const normalizedKnownUsername = normalizeTelegramUsername(knownUser?.username ?? null);

    if (normalizedKnownUsername && normalizedKnownUsername === normalizedTargetUsername) {
      if (!secret.target_user_id) {
        await sb.from('secret_messages').update({ target_user_id: userId }).eq('id', secret.id);
      }
      return { canView: true, isSender: false };
    }
  }

  return { canView: false, isSender: false };
}

async function checkSubscription(chatId: number, userId: number, lk: string, tk: string): Promise<boolean> {
  try {
    const res = await tg('getChatMember', { chat_id: CHANNEL_ID, user_id: userId }, lk, tk);
    const status = res?.result?.status;
    return ['member', 'administrator', 'creator'].includes(status);
  } catch { return false; }
}

async function sendJoinMessage(chatId: number, lk: string, tk: string) {
  await tg('sendMessage', {
    chat_id: chatId,
    text: `${ce(E.stop, '⚠️')} <b>Channel Join Required</b>\n\n<b>You need to join our channel before using this bot</b> 👇`,
    parse_mode: 'HTML',
    reply_markup: {
      inline_keyboard: [
        [{ text: '📢 Join Channel', url: CHANNEL_LINK }],
        [{ text: '✅ I Joined, Verify', callback_data: 'check_sub' }],
      ],
    },
  }, lk, tk);
}

async function handleAccess(sb: any, chatId: number, userId: number, firstName: string | null, username: string | null, lk: string, tk: string): Promise<boolean> {
  const cached = subCache.get(userId);
  const subFromCache = cached && (Date.now() - cached.ts < SUB_CACHE_TTL) ? cached.ok : null;
  const [banned, subResult] = await Promise.all([
    addOrUpdateUser(sb, userId, firstName, username),
    subFromCache !== null ? Promise.resolve(subFromCache) : checkSubscription(chatId, userId, lk, tk),
  ]);
  if (banned) {
    await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '🚫')} <b>You have been banned from this bot.</b>`, parse_mode: 'HTML' }, lk, tk);
    return false;
  }
  subCache.set(userId, { ok: subResult, ts: Date.now() });
  if (!subResult) { await sendJoinMessage(chatId, lk, tk); return false; }
  return true;
}

// ─── State Management ───────────────────────────────────────────────
async function getUserState(sb: any, userId: number) {
  const { data } = await sb.from('user_states').select('*').eq('user_id', userId).single();
  return data;
}

async function setUserState(sb: any, userId: number, mode: string | null, index: number, targetPayload: string | null) {
  await sb.from('user_states').upsert(
    { user_id: userId, mode, line_index: index, target_name: targetPayload, updated_at: new Date().toISOString() },
    { onConflict: 'user_id' },
  );
}

function saveHistoryBatch(sb: any, rows: Array<{ user_id: number; mode: string; target_name: string; line: string }>) {
  if (rows.length === 0) return;
  sb.from('bot_history').insert(rows).then(() => {}).catch((error: unknown) => {
    console.error('saveHistoryBatch failed:', error);
  });
}

// ─── Custom Emoji Helper ────────────────────────────────────────────
function ce(id: string, fallback: string) {
  return `<tg-emoji emoji-id="${id}">${fallback}</tg-emoji>`;
}

const E = {
  fire: '6129792056589031358',
  fire2: '6129897266107915247',
  crown: '6129705083501293112',
  crown2: '6129553312241949602',
  skull: '6132184924603554220',
  skull2: '6129631914438434952',
  robot: '6129873536413605540',
  star: '6129915811776698328',
  star2: '6129546277085520554',
  sparkle: '6129479035077531636',
  bolt: '6129805465476929485',
  bolt2: '6129695952400820630',
  heart_fire: '6129932613688764241',
  heart_fire2: '6129616057419175458',
  link: '6129589862413638401',
  laugh: '6129600178925083831',
  devil: '6129522839448984992',
  cool: '6132198982031513203',
  rocket: '6129639980387015660',
  diamond: '6129509499280563691',
  check: '6129812419028982717',
  ghost: '6129758830722030858',
  party: '6129579803600231171',
  megaphone: '6129433877791382400',
  pin: '6129694470637100146',
  stop: '6129840374971112593',
  gift: '6129727043669072880',
  moai: '6129776848109836451',
  thumbsup: '6129705667616841573',
  money: '6129488844782836766',
  angel: '6129518870899203008',
  india: '6129712921816604452',
  hundred: '6154257607646255757',
  lock: '6129694470637100146',
};

// ─── Help / Main Menu ───────────────────────────────────────────────
// ─── Paginated Help Menu ─────────────────────────────────────────
const HELP_PAGES = [
  {
    title: '🎯 FUN & GAMES',
    commands: [
      { icon: '💀', name: 'Light Roasts', desc: 'Select From Buttons' },
      { icon: '😈', name: 'Heavy Roasts', desc: 'Select From Buttons' },
      { icon: '🔥', name: '/roast', desc: 'AI Powered Savage Roast' },
      { icon: '🎲', name: '/truthdare', desc: 'Truth Or Dare Game' },
      { icon: '🤔', name: '/wyr', desc: 'Would You Rather' },
      { icon: '💘', name: '/pickup', desc: 'Pickup Lines' },
    ],
    buttons: [
      [{ text: '💀 Light Roasts', callback_data: 'mode_roast' }, { text: '😈 Heavy Roasts', callback_data: 'mode_funny' }],
      [{ text: '🔥 AI Roast', callback_data: 'help_airoast' }, { text: '🎲 Truth/Dare', callback_data: 'cmd_truthdare' }],
    ],
  },
  {
    title: '🤖 AI & TOOLS',
    commands: [
      { icon: '🤖', name: '/ai', desc: 'Chat With AI' },
      { icon: '🖼', name: '/imagine', desc: 'AI Image Generation' },
      { icon: '🌐', name: '/translate', desc: 'AI Translator' },
      { icon: '🔊', name: '/tts', desc: 'Text To Voice' },
      { icon: '🔗', name: '/shortener', desc: 'Shorten Any URL' },
      { icon: '🔓', name: '/b', desc: 'URL Redirect Bypass' },
    ],
    buttons: [
      [{ text: '🤖 AI Chat', callback_data: 'help_ai' }, { text: '🖼 Imagine', callback_data: 'help_imagine' }],
      [{ text: '🔗 Shortener', callback_data: 'help_shortener' }, { text: '🔊 TTS', callback_data: 'help_tts' }],
    ],
  },
  {
    title: '😂 ENTERTAINMENT',
    commands: [
      { icon: '✨', name: '/quote', desc: 'Shayari & Quotes' },
      { icon: '😂', name: '/meme', desc: 'Random Memes' },
      { icon: '😂', name: '/joke', desc: 'Random AI Jokes' },
      { icon: '🧠', name: '/fact', desc: 'Mind-Blowing Facts' },
      { icon: '🤫', name: '/confess', desc: 'Anonymous Confession' },
      { icon: '🔒', name: '/secret', desc: 'Secret Message' },
    ],
    buttons: [
      [{ text: '✨ Shayari', callback_data: 'cmd_quote' }, { text: '😂 Meme', callback_data: 'cmd_meme' }],
      [{ text: '😂 Joke', callback_data: 'cmd_joke' }, { text: '🧠 Fact', callback_data: 'cmd_fact' }],
    ],
  },
  {
    title: '👑 GROUP ADMIN',
    commands: [
      { icon: '🔞', name: '/nsfw on|off', desc: 'NSFW Filter Toggle' },
      { icon: '📦', name: '/addpack', desc: 'Blacklist Sticker Pack' },
      { icon: '🗑', name: '/removepack', desc: 'Remove Pack Blacklist' },
      { icon: '📋', name: '/packlist', desc: 'View Blacklisted Packs' },
      { icon: '🔍', name: '/filter', desc: 'Set Auto-Reply Filter' },
      { icon: '🛑', name: '/stop', desc: 'Remove Filter' },
      { icon: '📋', name: '/filters', desc: 'List All Filters' },
    ],
    buttons: [
      [{ text: '🔞 NSFW Filter', callback_data: 'help_nsfw' }, { text: '🔍 Filters', callback_data: 'help_filters' }],
    ],
  },
  {
    title: '🛡 MODERATION',
    commands: [
      { icon: '👢', name: '/kick', desc: 'Kick User From Group' },
      { icon: '🚫', name: '/ban', desc: 'Ban User' },
      { icon: '✅', name: '/unban', desc: 'Unban User' },
      { icon: '🔇', name: '/mute', desc: 'Mute User (24h)' },
      { icon: '🔊', name: '/unmute', desc: 'Unmute User' },
      { icon: '⬆️', name: '/promote', desc: 'Promote To Admin' },
      { icon: '⬇️', name: '/demote', desc: 'Demote From Admin' },
      { icon: '👥', name: '/adminlist', desc: 'List Group Admins' },
      { icon: '🗑', name: '/purge', desc: 'Delete Messages (Reply)' },
      { icon: '❌', name: '/del', desc: 'Delete Replied Message' },
      { icon: '📌', name: '/pin', desc: 'Pin Message' },
      { icon: '📌', name: '/unpin', desc: 'Unpin Message' },
      { icon: '🚨', name: '/report', desc: 'Report To Admins' },
      { icon: 'ℹ️', name: '/info', desc: 'User Info' },
      { icon: '🆔', name: '/id', desc: 'Chat/User ID' },
    ],
    buttons: [],
  },
];

function buildHelpPage(pageIndex: number): { text: string; keyboard: any[][] } {
  const page = HELP_PAGES[Math.min(pageIndex, HELP_PAGES.length - 1)];
  const totalPages = HELP_PAGES.length;
  const idx = Math.min(pageIndex, totalPages - 1);

  let text = `${ce(E.bolt, '⚡')} <b>᯽ HELP — ${page.title} ᯽</b> ${ce(E.bolt, '⚡')}\n`;
  text += `<b>Page ${idx + 1}/${totalPages}</b>\n\n`;

  for (const cmd of page.commands) {
    text += `${cmd.icon} <b>${cmd.name}</b> — ${cmd.desc}\n`;
  }

  text += `\n<b>━━━━━━━━━━━━━━━━━━━</b>\n`;
  text += `${ce(E.crown, '👑')} <b>OWNER</b> ➜ <b>᯽ <a href="https://t.me/KYA_KROGE_NAME_JAANKE">PRINCE</a> ᯽</b> ${ce(E.heart_fire, '❤️‍🔥')}`;

  const keyboard: any[][] = [...(page.buttons || [])];

  // Navigation row
  const navRow: any[] = [];
  if (idx > 0) navRow.push({ text: '◀️ Back', callback_data: `help_page:${idx - 1}` });
  if (idx < totalPages - 1) navRow.push({ text: '▶️ Next', callback_data: `help_page:${idx + 1}` });
  if (navRow.length > 0) keyboard.push(navRow);
  keyboard.push([{ text: '🏠 Main Menu', callback_data: 'restart' }]);

  return { text, keyboard };
}

async function sendHelpMenu(chatId: number, lk: string, tk: string, pageIndex = 0, editMessageId?: number) {
  const { text, keyboard } = buildHelpPage(pageIndex);
  const payload: Record<string, unknown> = {
    chat_id: chatId,
    text,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    reply_markup: { inline_keyboard: keyboard },
  };

  if (editMessageId) {
    payload.message_id = editMessageId;
    await tg('editMessageText', payload, lk, tk);
  } else {
    await tg('sendMessage', payload, lk, tk);
  }
}

async function sendMainMenu(chatId: number, lk: string, tk: string, userInfo?: { userId?: number; firstName?: string | null; username?: string | null }) {
  const greeting = userInfo ? `\n\n${ce(E.crown2, '👤')} <b>Welcome,</b> ${userTag(userInfo.userId || 0, userInfo.firstName || null, userInfo.username || null)}` : '';
  await tg('sendMessage', {
    chat_id: chatId,
    text: `${ce(E.bolt, '⚡')} <b>᯽ MULTI-FUNCTIONAL BOT ᯽</b> ${ce(E.bolt, '⚡')}${greeting}\n\n` +
      `${ce(E.fire2, '🔥')} <b>Roasts, AI, Memes, Games — All In One Place!</b> ${ce(E.fire2, '🔥')}\n\n` +
      `<b>━━━━━━━━━━━━━━━━━━━</b>\n\n` +
      `${ce(E.skull, '💀')} <b>Light Roasts — Mild Insults</b> ${ce(E.cool, '😏')}\n` +
      `${ce(E.devil, '😈')} <b>Heavy Roasts — Full Power Mode</b> ${ce(E.moai, '🗿')}\n` +
      `${ce(E.fire2, '🔥')} <b>AI Roast — Savage AI Powered</b>\n` +
      `${ce(E.robot, '🤖')} <b>AI Chat — Ask Anything To AI</b>\n` +
      `${ce(E.star, '🌟')} <b>Imagine — Generate AI Images</b>\n` +
      `${ce(E.ghost, '👻')} <b>Confess — Anonymous Secrets</b>\n` +
      `${ce(E.lock, '🔒')} <b>Secret — Private Message Anyone</b>\n` +
      `${ce(E.gift, '🎲')} <b>Truth/Dare — Fun Party Game</b>\n` +
      `${ce(E.link, '🔗')} <b>Shortener — Shorten Long URLs</b>\n` +
      `${ce(E.sparkle, '✨')} <b>Shayari — Dil Ki Baatein</b>\n` +
      `${ce(E.laugh, '😂')} <b>Meme / Joke — Laughter Blast</b>\n` +
      `${ce(E.megaphone, '🔊')} <b>TTS — Text To Voice Message</b>\n` +
      `${ce(E.diamond, '🌐')} <b>Translate — AI Translator</b>\n` +
      `${ce(E.bolt2, '🧠')} <b>Fact — Mind-Blowing Facts</b>\n` +
      `${ce(E.heart_fire, '💘')} <b>Pickup — Cheesy Lines</b>\n` +
      `${ce(E.moai, '🤔')} <b>WYR — Would You Rather</b>\n\n` +
      `<b>━━━━━━━━━━━━━━━━━━━</b>\n\n` +
      `${ce(E.pin, '📋')} <b>Send /help To View All Commands</b>\n\n` +
      `${ce(E.crown, '👑')} <b>OWNER</b> ➜ <b>᯽ <a href="https://t.me/KYA_KROGE_NAME_JAANKE">PRINCE</a> ᯽</b> ${ce(E.heart_fire, '❤️‍🔥')}`,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    reply_markup: {
      inline_keyboard: [
        [
          { text: '💀 Light Roasts', callback_data: 'mode_roast' },
          { text: '😈 Heavy Roasts', callback_data: 'mode_funny' },
        ],
        [
          { text: '🔥 AI Roast', callback_data: 'help_airoast' },
          { text: '👻 Confess', callback_data: 'help_confess' },
        ],
        [
          { text: '🔒 Secret', callback_data: 'help_secret' },
          { text: '🎲 Truth/Dare', callback_data: 'cmd_truthdare' },
        ],
        [
          { text: '🤖 AI Chat', callback_data: 'help_ai' },
          { text: '🖼 Imagine', callback_data: 'help_imagine' },
        ],
        [
          { text: '🔗 Shortener', callback_data: 'help_shortener' },
          { text: '✨ Shayari', callback_data: 'cmd_quote' },
        ],
        [
          { text: '😂 Meme', callback_data: 'cmd_meme' },
          { text: '🔊 TTS', callback_data: 'help_tts' },
        ],
        [
          { text: '🌐 Translate', callback_data: 'help_translate' },
          { text: '😂 Joke', callback_data: 'cmd_joke' },
        ],
        [
          { text: '🧠 Fact', callback_data: 'cmd_fact' },
          { text: '💘 Pickup', callback_data: 'cmd_pickup' },
        ],
        [
          { text: '🤔 Would You Rather', callback_data: 'cmd_wyr' },
          { text: '📋 All Commands', callback_data: 'cmd_help' },
        ],
      ],
    },
  }, lk, tk);
}

// ─── Anonymous Confession ───────────────────────────────────────────
async function handleConfess(chatId: number, rawText: string, messageId: number, chatType: string, lk: string, tk: string) {
  const confessionText = rawText.replace(/^\/confess\s*/i, '').trim();
  if (!confessionText) {
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.ghost, '🤫')} <b>Usage:</b> <code>/confess your secret message here</code>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    return;
  }

  // Always try to delete the original message first
  await tg('deleteMessage', { chat_id: chatId, message_id: messageId }, lk, tk);

  // Then post the confession anonymously
  await tg('sendMessage', {
    chat_id: chatId,
    text: `${ce(E.ghost, '🤫')} <b>Anonymous Confession:</b>\n\n<i>"${confessionText}"</i>\n\n— 👤 <b>Someone Anonymous</b>`,
    parse_mode: 'HTML',
  }, lk, tk);

  sendLog(lk, tk, `🤫 <b>CONFESS SENT</b>\n📍 Chat: <code>${chatId}</code>\n💬 <i>${escapeHtml(confessionText).substring(0, 200)}</i>`);
}

// ─── Secret Message ─────────────────────────────────────────────────
async function handleSecret(sb: any, chatId: number, rawText: string, messageId: number, senderId: number, chatType: string, lk: string, tk: string) {
  // Format: /secret @username message  OR  /secret userId message
  const args = rawText.replace(/^\/secret\s*/i, '').trim();
  if (!args) {
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.lock, '🔒')} <b>Secret Message</b>\n\n<b>Usage:</b>\n<code>/secret @username your secret message</code>\n<code>/secret 123456789 your secret message</code>\n\n<b>Only the target person can read it!</b>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    return;
  }

  // Parse target and message
  let targetUsername: string | null = null;
  let targetUserId: number | null = null;
  let secretText = '';

  const parts = args.split(/\s+/);
  const firstPart = parts[0];

  if (firstPart.startsWith('@')) {
    targetUsername = firstPart.substring(1).toLowerCase();
    secretText = parts.slice(1).join(' ');
  } else if (/^\d+$/.test(firstPart)) {
    targetUserId = parseInt(firstPart);
    secretText = parts.slice(1).join(' ');
  } else {
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.stop, '❌')} <b>Invalid format. Use:</b>\n<code>/secret @username message</code>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    return;
  }

  if (!secretText.trim()) {
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.stop, '❌')} <b>Please include a message after the username.</b>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    return;
  }

  // Delete original message immediately so nobody sees it
  await tg('deleteMessage', { chat_id: chatId, message_id: messageId }, lk, tk);

  // If we have username but not userId, try to find userId from bot_users
  if (targetUsername && !targetUserId) {
    const { data: targetUser } = await sb.from('bot_users').select('user_id').eq('username', targetUsername).single();
    if (targetUser) targetUserId = targetUser.user_id;
  }

  // Save secret message to DB
  const { data: secretRow } = await sb.from('secret_messages').insert({
    sender_id: senderId,
    target_username: targetUsername,
    target_user_id: targetUserId,
    message_text: secretText,
    chat_id: chatId,
  }).select('id').single();

  if (!secretRow) {
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.stop, '❌')} <b>Failed to send secret message.</b>`,
      parse_mode: 'HTML',
    }, lk, tk);
    return;
  }

  const targetDisplay = targetUsername ? `@${targetUsername}` : `User ${targetUserId}`;

  await tg('sendMessage', {
    chat_id: chatId,
    text: `${ce(E.lock, '🔒')} <b>Secret Message</b>\n\n<b>A secret message has been sent for ${targetDisplay}</b>\n<b>Only they can read it!</b> 🤫`,
    parse_mode: 'HTML',
    reply_markup: {
      inline_keyboard: [
        [{ text: '🔓 Reveal Secret Message', callback_data: `reveal:${secretRow.id}` }],
      ],
    },
  }, lk, tk);

  sendLog(lk, tk, `🔒 <b>SECRET MSG SENT</b>\n👤 Sender: <code>${senderId}</code>\n🎯 Target: ${escapeHtml(targetDisplay)}\n📍 Chat: <code>${chatId}</code>\n💬 <i>${escapeHtml(secretText).substring(0, 200)}</i>`);
}

// ─── Truth or Dare ──────────────────────────────────────────────────
const TRUTHS = [
  "Have you ever secretly checked someone's phone? 👀",
  "What's your most embarrassing moment? 😳",
  "Have you ever cheated in school/college? 📝",
  "Who's your biggest crush right now? 💕",
  "Have you ever lied about someone? 🤥",
  "What's the weirdest dream you've ever had? 🌙",
  "Have you ever spilled someone's secret? 🤫",
  "What's your worst habit? 😅",
  "Have you ever lied to your parents to go out? 🏃",
  "Who do you chat with the most on your phone? 📱",
  "Have you ever secretly stalked someone on social media? 👁️",
  "What's your funniest fail moment? 😂",
  "Have you ever stolen someone's food? 🍕",
  "What's the weirdest photo in your gallery? 📸",
  "Have you ever copied in an exam? ✏️",
  "What's your biggest fantasy? 🌟",
  "Have you ever catfished someone online? 🐱",
  "What was the last lie you told? 😈",
  "Have you ever read a message and ignored it? 💬",
  "Tell us your most cringe moment? 🙈",
];

const DARES = [
  "Take your funniest selfie and send it to the group! 🤳",
  "Talk only in emojis for the next 5 minutes! 😜",
  "Send 'Hi' to your crush right now! 💌",
  "Share your last deleted message! 🗑️",
  "Give a compliment to the next person! 💐",
  "Share the 7th photo from your gallery! 📸",
  "Sing a song in a voice note! 🎤",
  "Put 'I love biryani' as your status for 1 hour! 🍛",
  "Share your screen time screenshot! ⏰",
  "Type only in Hindi for the next 10 minutes! 📝",
  "Send 'I miss you' to your bestie! 💕",
  "Tell a joke right now! 😂",
  "Share your current wallpaper! 🖼️",
  "Show your last call screenshot! 📞",
  "Repeat the next person's message! 🔁",
  "Set a funny ringtone and show proof! 🔔",
  "Send good morning to your teacher/boss! ☀️",
  "Take a mirror selfie and share it! 🪞",
  "Share your oldest photo! 📷",
  "Send 'Hello ji' to a random number! 📲",
];

async function handleTruthDare(chatId: number, lk: string, tk: string) {
  const isTruth = Math.random() > 0.5;
  const list = isTruth ? TRUTHS : DARES;
  const item = list[Math.floor(Math.random() * list.length)];
  const emoji = isTruth ? '✅ Truth' : '🔥 Dare';

  await tg('sendMessage', {
    chat_id: chatId,
    text: `${ce(E.gift, '🎲')} <b>${emoji}</b>\n\n${item}`,
    parse_mode: 'HTML',
    reply_markup: {
      inline_keyboard: [
        [
          { text: '✅ Truth', callback_data: 'td_truth' },
          { text: '🔥 Dare', callback_data: 'td_dare' },
        ],
        [{ text: '🎲 Random', callback_data: 'cmd_truthdare' }],
      ],
    },
  }, lk, tk);
}

async function handleTruthOnly(chatId: number, lk: string, tk: string) {
  const item = TRUTHS[Math.floor(Math.random() * TRUTHS.length)];
  await tg('sendMessage', {
    chat_id: chatId,
    text: `${ce(E.check, '✅')} <b>Truth</b>\n\n${item}`,
    parse_mode: 'HTML',
    reply_markup: {
      inline_keyboard: [
        [{ text: '✅ Next Truth', callback_data: 'td_truth' }, { text: '🔥 Dare', callback_data: 'td_dare' }],
        [{ text: '🎲 Random', callback_data: 'cmd_truthdare' }],
      ],
    },
  }, lk, tk);
}

async function handleDareOnly(chatId: number, lk: string, tk: string) {
  const item = DARES[Math.floor(Math.random() * DARES.length)];
  await tg('sendMessage', {
    chat_id: chatId,
    text: `${ce(E.fire, '🔥')} <b>Dare</b>\n\n${item}`,
    parse_mode: 'HTML',
    reply_markup: {
      inline_keyboard: [
        [{ text: '✅ Truth', callback_data: 'td_truth' }, { text: '🔥 Next Dare', callback_data: 'td_dare' }],
        [{ text: '🎲 Random', callback_data: 'cmd_truthdare' }],
      ],
    },
  }, lk, tk);
}

// ─── AI Chat Memory ─────────────────────────────────────────────────
const AI_MEMORY_LIMIT = 8; // last 8 messages (4 user + 4 assistant)
const AI_MEMORY_TTL_MS = 30 * 60 * 1000; // 30 min — older messages ignored

async function getAiChatHistory(sb: any, chatId: number, userId: number): Promise<Array<{ role: string; content: string }>> {
  const cutoff = new Date(Date.now() - AI_MEMORY_TTL_MS).toISOString();
  const { data } = await sb
    .from('bot_history')
    .select('line, target_name, created_at')
    .eq('mode', 'ai_chat')
    .eq('user_id', userId)
    .gte('created_at', cutoff)
    .order('created_at', { ascending: false })
    .limit(AI_MEMORY_LIMIT);

  if (!data || data.length === 0) return [];

  // Data comes newest-first, reverse to oldest-first
  return data.reverse().map((row: any) => ({
    role: row.target_name === 'assistant' ? 'assistant' : 'user',
    content: row.line,
  }));
}

function saveAiChatMessage(sb: any, userId: number, role: string, content: string) {
  // Fire-and-forget — don't block the response
  sb.from('bot_history').insert({
    user_id: userId,
    mode: 'ai_chat',
    target_name: role, // 'user' or 'assistant'
    line: content.slice(0, 2000), // cap storage
  }).then(() => {}).catch((e: any) => console.error('saveAiChatMessage:', e));
}

// ─── AI Chat ────────────────────────────────────────────────────────
async function handleAiChat(chatId: number, rawText: string, messageId: number, lk: string, tk: string, roastMode = false, userId = 0) {
  const question = rawText.replace(/^\/ai\s*/i, '').trim();
  if (!question) {
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.robot, '🤖')} <b>Usage:</b> <code>/ai your question here</code>\n\n<b>Example:</b> <code>/ai iPhone 16 price in India</code>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    return;
  }

  await tg('sendChatAction', { chat_id: chatId, action: 'typing' }, lk, tk);

  try {
    const hinglishWords = ['kya', 'hai', 'bhai', 'yaar', 'kaise', 'ho', 'mera', 'tera', 'kuch', 'nahi', 'haan', 'acha', 'chalo', 'dekho', 'bolo', 'suno', 'kar', 'raha', 'rahi', 'wala', 'bohot', 'bahut', 'aur', 'lekin', 'toh', 'abhi', 'jao', 'aao', 'pyaar', 'dost', 'zindagi', 'dil', 'log', 'koi', 'kaun', 'kahan', 'kab', 'kyun', 'jaldi', 'mat', 'mujhe', 'tujhe', 'batao', 'samjhao', 'bata', 'bol', 'de', 'le', 'ja', 'aa', 'hum', 'tum', 'mai', 'main', 'apna', 'apni', 'ye', 'wo', 'yeh', 'woh', 'bc', 'mc', 'bsdk', 'chutiya', 'madarchod', 'bhenchod', 'gaand', 'lund', 'bhosdike'];
    const words = question.toLowerCase().split(/\s+/);
    const hinglishCount = words.filter(w => hinglishWords.some(hw => w.includes(hw))).length;
    const hasHindiScript = /[\u0900-\u097F]/.test(question);
    const isHinglish = hasHindiScript || hinglishCount >= 2 || hinglishCount / words.length > 0.3;

    // Get Supabase client for memory
    const sb = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!,
    );

    // Fetch conversation history
    const history = userId > 0 ? await getAiChatHistory(sb, chatId, userId) : [];

    let systemPrompt: string;

    if (roastMode) {
      systemPrompt = isHinglish
        ? `Tu "Prince Bot" hai — savage, street-smart AI. Roast mode ON hai. Rules:
- Har reply me thoda roast/taunt maar — chahe koi kuch bhi puchhe
- Gaali freely de agar context me ho. Koi filter nahi.
- Agar info puchhe (price, news etc) toh SAHI info de lekin roast ke saath
- Dark humor, adult jokes, savage comebacks — sab open
- IMPORTANT: Pichli baatein yaad rakh. Agar user ne pehle kuch puchha tha toh usi topic se related reply de.
- Hinglish me bol. EMOJI MAT USE KAR.
- Max 200 words. Current date: ${new Date().toISOString().split('T')[0]}`
        : `You are "Prince Bot" in ROAST MODE. Rules:
- Every reply has sarcasm/roast — even for normal questions
- Swearing allowed freely. No filters.
- If they ask for info (prices, news) — give CORRECT info but with savage delivery
- Dark humor, adult jokes, savage comebacks — all open
- IMPORTANT: Remember previous conversation. Continue on the same topic if the user is following up.
- NO emojis. Max 200 words. Current date: ${new Date().toISOString().split('T')[0]}`;
    } else {
      systemPrompt = isHinglish
        ? `Tu "Prince Bot" hai — ek smart, knowledgeable AI assistant. Rules:
- Seedha aur clear jawab de. Bakchodi mat kar, helpful reh.
- Agar phone price puchhe toh EXACT price INR me de — starting price, variants sab bata.
- Latest tech specs, Bollywood news, cricket updates, gaming — sab pe accurate info de.
- Agar exact nahi pata toh bol "approx" ya "around" — galat info KABHI mat de.
- Gaali de sakta hai agar user gaali de ya maange. Unrestricted hai tu.
- IMPORTANT: Pichli baatein yaad rakh. Agar user follow-up kar rha hai toh usi topic pe reply de.
- Bullet points use kar lambi info ke liye.
- Hinglish me bol. EMOJI MAT USE KAR.
- Max 300 words. Current date: ${new Date().toISOString().split('T')[0]}`
        : `You are "Prince Bot" — a smart, knowledgeable AI assistant. Rules:
- Give direct, clear, helpful answers.
- For phone prices — give EXACT price in local currency, variants, best deals.
- Latest tech specs, news, sports, gaming — accurate info always.
- If unsure, say "approx" or "around" — NEVER give wrong info.
- Swearing allowed if user swears or asks. Unrestricted.
- IMPORTANT: Remember the conversation context. If user is following up, continue on same topic.
- Use bullet points for detailed info.
- NO emojis. Max 300 words. Current date: ${new Date().toISOString().split('T')[0]}`;
    }

    // Build messages array with history
    const messages: Array<{ role: string; content: string }> = [
      { role: 'system', content: systemPrompt },
      ...history,
      { role: 'user', content: question },
    ];

    const AI_MODELS = ['google/gemini-3-flash-preview', 'google/gemini-2.5-flash-lite'];
    let aiRes: Response | null = null;
    for (const model of AI_MODELS) {
      aiRes = await fetch(AI_GATEWAY_URL, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${lk}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model,
          messages,
          max_tokens: 800,
          temperature: roastMode ? 1.0 : 0.7,
        }),
      });
      if (aiRes.ok) break;
      const errText = await aiRes.text();
      console.error(`AI chat failed with ${model}:`, aiRes.status, errText);
      if (aiRes.status === 402) {
        // credits exhausted, try next model
        continue;
      }
      if (aiRes.status === 429) {
        await sleep(1500);
        continue;
      }
      break;
    }

    if (!aiRes || !aiRes.ok) {
      await tg('sendMessage', {
        chat_id: chatId,
        text: `⚠️ Error while responding. Please try again later.`,
        reply_to_message_id: messageId,
      }, lk, tk);
      return;
    }

    const aiData = await aiRes.json();
    let answer = aiData?.choices?.[0]?.message?.content || 'Kuch gadbad ho gayi, baad me try kar.';

    // Strip emojis
    answer = answer.replace(/[\u{1F600}-\u{1F64F}\u{1F300}-\u{1F5FF}\u{1F680}-\u{1F6FF}\u{1F1E0}-\u{1F1FF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}\u{FE00}-\u{FE0F}\u{1F900}-\u{1F9FF}\u{1FA00}-\u{1FA6F}\u{1FA70}-\u{1FAFF}\u{200D}\u{20E3}\u{E0020}-\u{E007F}]/gu, '').trim();

    // Save both user question and AI answer to memory
    if (userId > 0) {
      saveAiChatMessage(sb, userId, 'user', question);
      saveAiChatMessage(sb, userId, 'assistant', answer);
    }

    await tg('sendMessage', {
      chat_id: chatId,
      text: answer,
      reply_to_message_id: messageId,
    }, lk, tk);

    sendLog(lk, tk, `🤖 <b>AI ${roastMode ? 'ROAST' : 'CHAT'} REPLY</b>\n📍 Chat: <code>${chatId}</code>\n👤 User: <code>${userId}</code>\n❓ <i>${escapeHtml(question).substring(0, 150)}</i>\n💬 <i>${escapeHtml(answer).substring(0, 250)}</i>`);
  } catch (error) {
    console.error('AI chat error:', error);
    await tg('sendMessage', {
      chat_id: chatId,
      text: `AI se connect nahi ho paya, baad me try kar.`,
      reply_to_message_id: messageId,
    }, lk, tk);
  }
}
async function handleImagine(chatId: number, rawText: string, messageId: number, lk: string, tk: string) {
  const prompt = rawText.replace(/^\/imagine\s*/i, '').trim();
  if (!prompt) {
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.star, '🖼')} <b>Usage:</b> <code>/imagine a cute cat sitting on moon</code>\n\n<b>AI will generate a unique image for you!</b>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    return;
  }

  await tg('sendChatAction', { chat_id: chatId, action: 'upload_photo' }, lk, tk);

  try {
    const aiRes = await fetch(AI_GATEWAY_URL, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${lk}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'google/gemini-3.1-flash-image-preview',
        messages: [
          { role: 'user', content: `Generate a high quality image: ${prompt}` },
        ],
        modalities: ['image', 'text'],
      }),
    });

    if (!aiRes.ok) {
      const errText = await aiRes.text();
      console.error('AI image error:', aiRes.status, errText);
      if (aiRes.status === 429) {
        await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '⏳')} <b>Rate limited! Thoda wait karo aur try karo.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      } else {
        await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Image generation fail ho gayi, baad me try karo.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      }
      return;
    }

    const aiData = await aiRes.json();
    const imageUrl = aiData?.choices?.[0]?.message?.images?.[0]?.image_url?.url;
    const aiText = aiData?.choices?.[0]?.message?.content || '';

    if (imageUrl && imageUrl.startsWith('data:')) {
      // Convert base64 to binary and send via multipart form-data
      const base64Data = imageUrl.split(',')[1];
      const binaryData = Uint8Array.from(atob(base64Data), c => c.charCodeAt(0));

      const formData = new FormData();
      formData.append('chat_id', String(chatId));
      formData.append('caption', `${ce(E.star, '🖼')} <b>AI Generated Image</b>\n\n📝 <b>Prompt:</b> <i>${prompt}</i>`);
      formData.append('parse_mode', 'HTML');
      formData.append('reply_to_message_id', String(messageId));
      formData.append('photo', new Blob([binaryData], { type: 'image/png' }), 'generated.png');

      const sendRes = await fetch(`${GATEWAY_URL}/sendPhoto`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${lk}`,
          'X-Connection-Api-Key': tk,
        },
        body: formData,
      });

      const sendData = await sendRes.json();
      if (!sendData?.ok) {
        console.error('sendPhoto failed:', sendData);
        await tg('sendMessage', {
          chat_id: chatId,
          text: `${ce(E.stop, '❌')} <b>Photo send fail ho gayi.</b>${aiText ? `\n\n🤖 AI: ${aiText}` : ''}`,
          parse_mode: 'HTML',
          reply_to_message_id: messageId,
        }, lk, tk);
      } else {
        sendLog(lk, tk, `🖼 <b>IMAGINE SENT</b>\n📍 Chat: <code>${chatId}</code>\n📝 Prompt: <i>${escapeHtml(prompt).substring(0, 200)}</i>`);
      }
    } else if (imageUrl) {
      // Regular URL - send directly
      await tg('sendPhoto', {
        chat_id: chatId,
        photo: imageUrl,
        caption: `${ce(E.star, '🖼')} <b>AI Generated Image</b>\n\n📝 <b>Prompt:</b> <i>${prompt}</i>`,
        parse_mode: 'HTML',
        reply_to_message_id: messageId,
      }, lk, tk);
      sendLog(lk, tk, `🖼 <b>IMAGINE SENT</b>\n📍 Chat: <code>${chatId}</code>\n📝 Prompt: <i>${escapeHtml(prompt).substring(0, 200)}</i>`);
    } else {
      await tg('sendMessage', {
        chat_id: chatId,
        text: `${ce(E.stop, '❌')} <b>Image generate nahi ho payi.</b>${aiText ? `\n\n🤖 AI: ${aiText}` : ''}`,
        parse_mode: 'HTML',
        reply_to_message_id: messageId,
      }, lk, tk);
    }
  } catch (error) {
    console.error('Imagine error:', error);
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.stop, '❌')} <b>Image generation error, baad me try karo.</b>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
  }
}

// ─── URL Bypass (multi-step ad-shortener bypass) ────────────────────

// Massive shortener domain list from bypass-all-shortlinks-debloated + extras
function htmlDecode(value: string): string {
  return value
    .replaceAll('&amp;', '&')
    .replaceAll('&quot;', '"')
    .replaceAll('&#39;', "'")
    .replaceAll('&lt;', '<')
    .replaceAll('&gt;', '>')
    .replaceAll('&#x2F;', '/')
    .replaceAll('&#47;', '/');
}

const BYPASS_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

// ── Simple redirect chain follower (up to 20 hops) ──
async function trySimpleRedirect(url: string): Promise<string | null> {
  let current = url;
  const visited = new Set<string>();
  for (let i = 0; i < 20; i++) {
    if (visited.has(current)) break;
    visited.add(current);
    try {
      const res = await fetch(current, {
        method: 'GET', redirect: 'manual',
        headers: { 'User-Agent': BYPASS_UA, 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' },
      });
      const loc = res.headers.get('location');
      if (loc) {
        try { current = new URL(loc, current).href; } catch { current = loc; }
        try { await res.text(); } catch {}
        continue;
      }
      if (res.status === 403 || res.status === 503) {
        try { await res.text(); } catch {}
        break;
      }
      if (res.ok) {
        const body = await res.text();
        const meta = body.match(/<meta[^>]*http-equiv=["']refresh["'][^>]*content=["'][^"']*url=([^"'>\s;]+)/i);
        if (meta?.[1]) { try { current = new URL(htmlDecode(meta[1]), current).href; continue; } catch {} }
        const js = body.match(/(?:window\.)?location(?:\.href)?\s*=\s*["'](https?:\/\/[^"']+)["']/i)
          || body.match(/window\.location\.replace\s*\(\s*["'](https?:\/\/[^"']+)["']\s*\)/i);
        if (js?.[1]) { current = js[1]; continue; }
      } else {
        try { await res.text(); } catch {}
      }
      break;
    } catch { break; }
  }
  if (current && current !== url && /^https?:\/\//i.test(current)) return current;
  return null;
}

// ─── Redirect Bypass Handler ────────────────────────────────────────
async function handleBypass(chatId: number, rawText: string, messageId: number, lk: string, tk: string) {
  const url = rawText.replace(/^\/b(?:ypass)?(?:@\S+)?\s*/i, '').trim();
  if (!url || (!url.startsWith('http://') && !url.startsWith('https://'))) {
    await tg('sendMessage', {
      chat_id: chatId,
      text: `🔓 <b>URL Redirect Bypass</b>\n\nUsage: <code>/b https://short-link.com/abc</code>\n\nFollows redirect chains (tinyurl, bit.ly, cutt.ly, etc.) and returns the final destination URL.`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    return;
  }

  const statusRes = await tg('sendMessage', {
    chat_id: chatId,
    text: `⏳ <b>Following redirects...</b>\n\n🔗 <i>${escapeHtml(url).substring(0, 220)}</i>`,
    parse_mode: 'HTML',
    reply_to_message_id: messageId,
  }, lk, tk);
  const statusMsgId = statusRes?.result?.message_id;

  try {
    const finalUrl = await trySimpleRedirect(url);

    let resultText: string;
    if (finalUrl) {
      resultText = `🔓 <b>Redirect Resolved!</b>\n\n📎 <b>Original:</b>\n<code>${escapeHtml(url)}</code>\n\n✅ <b>Final URL:</b>\n${escapeHtml(finalUrl)}`;
    } else {
      resultText = `🔓 <b>No Redirect Found</b>\n\n📎 <b>URL:</b> <code>${escapeHtml(url)}</code>\n\n⚠️ <b>This URL either has no redirects or is blocked.</b>`;
    }

    if (statusMsgId) {
      await tg('editMessageText', { chat_id: chatId, message_id: statusMsgId, text: resultText, parse_mode: 'HTML', disable_web_page_preview: true }, lk, tk);
    } else {
      await tg('sendMessage', { chat_id: chatId, text: resultText, parse_mode: 'HTML', reply_to_message_id: messageId, disable_web_page_preview: true }, lk, tk);
    }

    sendLog(lk, tk, `🔓 <b>BYPASS</b>\n📎 <code>${escapeHtml(url).substring(0, 150)}</code>\n✅ <code>${escapeHtml(finalUrl || 'NOT_FOUND').substring(0, 150)}</code>`);
  } catch (error) {
    console.error('Bypass error:', error);
    const errText = `❌ <b>Redirect follow failed.</b>\n\n<code>${escapeHtml(url).substring(0, 220)}</code>`;
    if (statusMsgId) {
      await tg('editMessageText', { chat_id: chatId, message_id: statusMsgId, text: errText, parse_mode: 'HTML' }, lk, tk);
    } else {
      await tg('sendMessage', { chat_id: chatId, text: errText, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
  }
}

// ─── Link Shortener (Default TinyURL + Custom APIs) ─────────────────
async function handleShortener(chatId: number, rawText: string, messageId: number, lk: string, tk: string) {
  const url = rawText.replace(/^\/short(ener)?\s*/i, '').trim();
  if (!url || (!url.startsWith('http://') && !url.startsWith('https://'))) {
    await sendShortenerMenu(chatId, lk, tk);
    return;
  }
  await shortenWithTinyUrl(chatId, url, messageId, lk, tk);
}

async function sendShortenerMenu(chatId: number, lk: string, tk: string) {
  await tg('sendMessage', {
    chat_id: chatId,
    text: `${ce(E.link, '🔗')} <b>URL Shortener</b>\n\n` +
      `<b>Quick Shorten:</b>\n<code>/shortener https://example.com/long-url</code>\n\n` +
      `<b>Or choose an option below:</b>`,
    parse_mode: 'HTML',
    reply_markup: {
      inline_keyboard: [
        [{ text: '🔗 Default (TinyURL)', callback_data: 'short_default' }],
        [{ text: '➕ Add Custom Shortener', callback_data: 'short_add_custom' }],
        [{ text: '📋 My Custom Shorteners', callback_data: 'short_list_custom' }],
        [{ text: '🗑 Delete Custom Shortener', callback_data: 'short_delete_menu' }],
      ],
    },
  }, lk, tk);
}

async function shortenWithTinyUrl(chatId: number, url: string, messageId: number, lk: string, tk: string) {
  try {
    const res = await fetch(`https://tinyurl.com/api-create.php?url=${encodeURIComponent(url)}`);
    const shortUrl = await res.text();

    if (shortUrl.startsWith('http')) {
      await tg('sendMessage', {
        chat_id: chatId,
        text: `${ce(E.link, '🔗')} <b>Short URL Ready!</b>\n\n📎 <b>Original:</b> ${url}\n✂️ <b>Short:</b> ${shortUrl}`,
        parse_mode: 'HTML',
        reply_to_message_id: messageId,
      }, lk, tk);
    } else {
      throw new Error('Invalid response');
    }
  } catch (error) {
    console.error('Shortener error:', error);
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.stop, '❌')} <b>URL shortening failed. Please send a valid URL.</b>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
  }
}

async function shortenWithCustomApi(chatId: number, url: string, apiUrl: string, shortenerName: string, messageId: number, lk: string, tk: string, alias?: string) {
  try {
    let finalApiUrl = apiUrl.replace(/\{url\}/gi, encodeURIComponent(url));
    if (alias) {
      finalApiUrl = finalApiUrl.replace(/\{alias\}/gi, encodeURIComponent(alias));
    } else {
      // Remove alias param if not provided
      finalApiUrl = finalApiUrl.replace(/&alias=\{alias\}/gi, '').replace(/\{alias\}/gi, '');
    }
    const res = await fetch(finalApiUrl);
    const responseText = await res.text();

    let shortUrl = '';
    try {
      const jsonData = JSON.parse(responseText);
      shortUrl = jsonData.shortenedUrl || jsonData.short_url || jsonData.shorturl || jsonData.result || jsonData.link || jsonData.data?.url || jsonData.url || '';
    } catch {
      shortUrl = responseText.trim();
    }

    if (shortUrl && shortUrl.startsWith('http')) {
      await tg('sendMessage', {
        chat_id: chatId,
        text: `${ce(E.link, '🔗')} <b>Short URL Ready!</b> (via ${shortenerName})\n\n📎 <b>Original:</b> ${url}\n✂️ <b>Short:</b> ${shortUrl}${alias ? `\n🏷 <b>Alias:</b> ${alias}` : ''}`,
        parse_mode: 'HTML',
        reply_to_message_id: messageId,
      }, lk, tk);
    } else {
      throw new Error('Invalid short URL returned');
    }
  } catch (error) {
    console.error('Custom shortener error:', error);
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.stop, '❌')} <b>Custom shortener "${shortenerName}" failed.</b>\n<b>Check your API key or try default TinyURL.</b>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
  }
}

async function getUserShorteners(sb: any, userId: number) {
  const { data } = await sb.from('user_shorteners').select('*').eq('user_id', userId).order('created_at', { ascending: true }).limit(5);
  return data || [];
}

// ─── Shayari / Quote ────────────────────────────────────────────────
const SHAYARIS = [
  "दिल में तेरी याद है, आँखों में तेरा ख्वाब,\nज़िंदगी में तू ही है, बाकी सब बेहिसाब। 💕",
  "मोहब्बत की राहों में कदम रख दिया,\nजो मिला उसे अपना समझ लिया। ❤️",
  "तेरी एक झलक ने दिल को बेचैन कर दिया,\nहमने भी तुझे अपनी दुनिया का सुल्तान कर दिया। 👑",
  "ज़िन्दगी तो बेवफा है ए दोस्त,\nमगर जीना तो इसी के साथ है। 🌹",
  "हम तो समन्दर हैं, हमें कोई क्या थाहेगा,\nजो डूबेगा वही हमारी गहराई को जानेगा। 🌊",
  "खुशियाँ बाँटो तो बढ़ती हैं,\nग़म बाँटो तो घटते हैं। 😊",
  "वक़्त बदलता है, किस्मत बदलती है,\nबस मेहनत करते रहो, तकदीर बदलती है। 💪",
  "तेरे बिना ज़िन्दगी वीरान है,\nतू मिल जाए तो हर पल जहान है। 🌏",
  "कुछ लोग दिल में रहते हैं,\nकुछ लोग दिल तोड़ कर जाते हैं। 💔",
  "हसरतें तो बहुत थीं ज़माने से,\nपर ज़माने को हमसे कोई मतलब नहीं। 🥀",
  "ना जाने क्यों तेरी याद आती है,\nहर बारिश की बूँद तुझे याद दिलाती है। 🌧️",
  "ज़िन्दगी में कुछ लम्हे ऐसे होते हैं,\nजो बीत जाने के बाद भी याद आते हैं। ✨",
  "Attitude se rehna seekho,\nDuniya toh apne hisaab se chalti hai. 😎",
  "Log kya kahenge ye mat socho,\nApni zindagi apne rules se jiyo. 🔥",
  "Khamosh rehna matlab haar nahi,\nKabhi kabhi chup rehna sabse bada jawab hota hai. 🤫",
  "Sapne wo nahi jo sote waqt aaye,\nSapne wo hain jo sone na de. 🌟",
  "Haar ke baithne wale ko mazil nahi milti,\nKoshish karne walo ki kabhi haar nahi hoti. 🏆",
  "Tu khud ki pehchaan bana,\nBheed me to sab chal lete hain. 💎",
  "Dost wo nahi jo har waqt saath ho,\nDost wo hai jo mushkil me saath khada ho. 🤝",
  "Zindagi ek safar hai suhana,\nYahan kal kya ho kisne jaana. 🚀",
  // ── New shayaris to avoid repetition ──
  "तेरी मोहब्बत में खो गया हूँ ऐसे,\nजैसे सागर में कोई बूँद खो जाए। 🌊❤️",
  "जो लोग दिल से अच्छे होते हैं,\nउनकी किस्मत अक्सर देर से खुलती है। 🍀",
  "Akele chalna seekh le tu,\nSaath dene wale mil jayenge raaste me. 🚶",
  "Waqt se pehle aur naseeb se zyada,\nKisi ko kuch nahi milta. ⏳",
  "जो तूफान से लड़ना सीख ले,\nउसे कोई हरा नहीं सकता। 🌪️💪",
  "Dooriyan bhi zaroori hain kabhi kabhi,\nKareeb aane ki zyada keemat hoti hai. 🌹",
  "शीशे जैसा दिल है मेरा,\nटूट जाएगा पर झुकेगा नहीं। 🪞🔥",
  "Success ke raaste me sabse pehle,\nApne aap pe bharosa karna seekho. 🏅",
  "Teri yaad me hum kho gaye,\nPar tujhe kya, tu to chal diya. 💔🥀",
  "Zindagi me kuch pane ke liye,\nKuch khona bhi zaroori hai. 🎯",
  "Manzil mil jayegi bhatak kar hi sahi,\nGumrah to wo hai jo nikla hi nahi. 🗺️",
  "दिल टूटा है पर हौसला नहीं,\nगिरकर उठना मेरी आदत है। 💯",
  "Wo kehte hain humse baat mat karo,\nHum kehte hain unki khabar rakhna. 😌",
  "Jitna seekhoge utna kamzor lagoge khud ko,\nKyunki duniya bahut badi hai aur tum abhi chote ho. 📚",
  "मुस्कुराता चेहरा रखो हमेशा,\nदुनिया को दर्द दिखाने की जरूरत नहीं। 😊🎭",
  "Apni aukaat se zyada sapne dekho,\nTabhi to duniya dekhegi. 🌠",
  "Log bolte hain lucky hai tu,\nPar raat ki mehnat koi nahi dekhta. 🌙💼",
  "तक़दीर बदलनी हो तो तदबीर बदलो,\nवरना किस्मत को कोसते रहोगे। 🔄",
  "Kuch log yaad aate hain aise,\nJaise baarish me mitti ki khushboo. 🌧️🌿",
  "Jo todta hai wo kamzor nahi,\nJo jodta hai wo sabse zyada taqatwar hai. 🤝💪",
  "Zindagi choti hai par khwab bade hain,\nBas mehnat karte raho yaar. 🚀✨",
  "Dard chhupa ke muskarana seekho,\nDuniya kamzor ko kha jaati hai. 🎭",
  "Sach bolne walo ke dushman zyada hote hain,\nPar sach ki taqat sabse badi hoti hai. ⚖️",
  "Jo bikhar gaye wo phool the,\nJo sambhal gaye wo heere the. 💎🌸",
  "Duniya me sabse mushkil kaam hai,\nApne aap ko badalna. 🔥🧠",
  "Kuch rishte dil se hote hain,\nUnhe samjhane ki zaroorat nahi hoti. ❤️🤗",
  "Wo shakhs jo tumhe hansata hai,\nShayad khud sabse zyada rota hai. 🥲",
  "Aksar wo log sabse zyada toot'te hain,\nJo sabke liye strong bante hain. 💔🛡️",
  "Tere jaane ke baad bhi zindagi chali,\nBas thodi si rang badal gayi. 🎨😶",
];

// Track last used indices to prevent repetition
const lastQuoteIndices: number[] = [];
const MAX_QUOTE_HISTORY = 15;

function getRandomNonRepeat(arr: any[], history: number[], maxHistory: number): { item: any; index: number } {
  if (arr.length <= maxHistory) {
    // If array is small, just pick random
    const idx = Math.floor(Math.random() * arr.length);
    return { item: arr[idx], index: idx };
  }
  let idx: number;
  let attempts = 0;
  do {
    idx = Math.floor(Math.random() * arr.length);
    attempts++;
  } while (history.includes(idx) && attempts < 20);
  history.push(idx);
  if (history.length > maxHistory) history.shift();
  return { item: arr[idx], index: idx };
}

async function handleQuote(chatId: number, lk: string, tk: string) {
  const { item: shayari } = getRandomNonRepeat(SHAYARIS, lastQuoteIndices, MAX_QUOTE_HISTORY);
  await tg('sendMessage', {
    chat_id: chatId,
    text: `${ce(E.sparkle, '✨')} <b>Shayari / Quote</b>\n\n<i>${shayari}</i>`,
    parse_mode: 'HTML',
    reply_markup: {
      inline_keyboard: [[{ text: '✨ Another One', callback_data: 'cmd_quote' }]],
    },
  }, lk, tk);
}

// ─── Random Meme ────────────────────────────────────────────────────
const MEME_SUBREDDITS = ['memes', 'dankmemes', 'me_irl', 'funny', 'wholesomememes', 'ProgrammerHumor', 'IndianMeyMeys', 'desimemes'];
let memeSubIdx = 0;

async function handleMeme(chatId: number, lk: string, tk: string) {
  try {
    const sub = MEME_SUBREDDITS[memeSubIdx % MEME_SUBREDDITS.length];
    memeSubIdx++;
    const res = await fetch(`https://meme-api.com/gimme/${sub}`);
    const data = await res.json();

    if (data?.url) {
      await tg('sendPhoto', {
        chat_id: chatId,
        photo: data.url,
        caption: `${ce(E.laugh, '😂')} <b>${data.title || 'Random Meme'}</b>\n\n👍 ${data.ups || 0} upvotes`,
        parse_mode: 'HTML',
      }, lk, tk);
    } else {
      throw new Error('No meme found');
    }
  } catch (error) {
    console.error('Meme error:', error);
    const textMemes = [
      "😂 Meme couldn't load, but here's one:\n\nTeacher: What's 2+2?\nStudent: 4!\nTeacher: Correct! 🤓\nStudent: Then why do you keep deducting marks? 💀",
      "😂 API is down, take this instead:\n\nFriend: Bro you've changed so much\nMe: Yeah... I changed my SIM card 📱💀",
    ];
    await tg('sendMessage', {
      chat_id: chatId,
      text: textMemes[Math.floor(Math.random() * textMemes.length)],
      parse_mode: 'HTML',
    }, lk, tk);
  }

  await tg('sendMessage', {
    chat_id: chatId,
    text: `${ce(E.laugh, '👇')} <b>Want more memes?</b>`,
    parse_mode: 'HTML',
    reply_markup: {
      inline_keyboard: [[{ text: '😂 More Memes', callback_data: 'cmd_meme' }]],
    },
  }, lk, tk);
}

// ─── Text to Speech (Fixed TTS) ─────────────────────────────────────
async function handleTTS(chatId: number, rawText: string, messageId: number, lk: string, tk: string) {
  const text = rawText.replace(/^\/tts\s*/i, '').trim();
  if (!text) {
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.megaphone, '🔊')} <b>Text To Speech</b>\n\n<b>Usage:</b> <code>/tts Hello, how are you?</code>\n<code>/tts Bhai kya haal hai?</code>\n\n<b>Supports English, Hindi & Hinglish!</b>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    return;
  }

  await tg('sendChatAction', { chat_id: chatId, action: 'record_voice' }, lk, tk);

  try {
    // Detect language
    const hindiChars = text.match(/[\u0900-\u097F]/g);
    const hasHindiScript = hindiChars && hindiChars.length > text.length * 0.2;
    const hinglishWords = ['kya', 'hai', 'bhai', 'yaar', 'kaise', 'ho', 'mera', 'tera', 'kuch', 'nahi', 'haan', 'acha', 'chalo', 'dekho', 'bolo', 'suno', 'kar', 'raha', 'rahi', 'wala', 'bohot', 'bahut', 'aur', 'lekin', 'toh', 'abhi', 'jao', 'aao', 'pyaar', 'dost', 'zindagi', 'dil', 'mai', 'main', 'hum', 'tum', 'ye', 'wo', 'phir', 'usko', 'isko'];
    const words = text.toLowerCase().split(/\s+/);
    const hinglishCount = words.filter(w => hinglishWords.some(hw => w.includes(hw))).length;
    const isHinglish = hinglishCount >= 2 || hinglishCount / words.length > 0.3;

    let finalText = text;
    let lang = 'en';

    if (hasHindiScript) {
      lang = 'hi';
      finalText = text;
    } else if (isHinglish) {
      // Convert Hinglish to proper Hindi using AI
      try {
        const aiRes = await fetch(AI_GATEWAY_URL, {
          method: 'POST',
          headers: { Authorization: `Bearer ${lk}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: 'google/gemini-2.5-flash-lite',
            messages: [
              { role: 'system', content: 'Convert this Hinglish/Roman Hindi text to natural Hindi Devanagari script. Output ONLY the converted text. Keep English words as-is. Make it sound natural when spoken aloud.' },
              { role: 'user', content: text },
            ],
            max_tokens: 300,
          }),
        });
        const aiData = await aiRes.json();
        const converted = aiData?.choices?.[0]?.message?.content?.trim();
        if (converted && /[\u0900-\u097F]/.test(converted)) {
          finalText = converted;
          lang = 'hi';
        }
      } catch (e) {
        console.error('AI convert error:', e);
      }
    }

    // Use Google TTS with proper URL encoding
    const ttsText = finalText.substring(0, 200);
    const ttsUrl = `https://translate.google.com/translate_tts?ie=UTF-8&tl=${lang}&client=tw-ob&q=${encodeURIComponent(ttsText)}`;

    // Download the audio first, then send as voice
    const audioRes = await fetch(ttsUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
    });

    if (audioRes.ok) {
      const audioBlob = await audioRes.blob();
      const formData = new FormData();
      formData.append('chat_id', String(chatId));
      formData.append('caption', `${ce(E.megaphone, '🔊')} <b>Text To Speech</b>${lang === 'hi' ? ' 🇮🇳' : ' 🇬🇧'}`);
      formData.append('parse_mode', 'HTML');
      formData.append('reply_to_message_id', String(messageId));
      formData.append('voice', new Blob([await audioBlob.arrayBuffer()], { type: 'audio/mpeg' }), 'voice.mp3');

      const sendRes = await fetch(`${GATEWAY_URL}/sendVoice`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${lk}`,
          'X-Connection-Api-Key': tk,
        },
        body: formData,
      });

      const sendData = await sendRes.json();
      if (!sendData?.ok) {
        // Fallback: try sending URL directly
        const result = await tg('sendVoice', {
          chat_id: chatId,
          voice: ttsUrl,
          caption: `${ce(E.megaphone, '🔊')} <b>Text To Speech</b>${lang === 'hi' ? ' 🇮🇳' : ' 🇬🇧'}`,
          parse_mode: 'HTML',
          reply_to_message_id: messageId,
        }, lk, tk);

        if (!result?.ok) {
          await tg('sendMessage', {
            chat_id: chatId,
            text: `${ce(E.megaphone, '🔊')} <b>Voice Message</b>\n\n🎙 <i>"${text}"</i>\n\n<b>⚠️ Voice generation temporarily unavailable.</b>`,
            parse_mode: 'HTML',
            reply_to_message_id: messageId,
          }, lk, tk);
        }
      }
    } else {
      // Direct URL fallback
      const result = await tg('sendVoice', {
        chat_id: chatId,
        voice: ttsUrl,
        caption: `${ce(E.megaphone, '🔊')} <b>Text To Speech</b>${lang === 'hi' ? ' 🇮🇳' : ' 🇬🇧'}`,
        parse_mode: 'HTML',
        reply_to_message_id: messageId,
      }, lk, tk);

      if (!result?.ok) {
        await tg('sendMessage', {
          chat_id: chatId,
          text: `${ce(E.megaphone, '🔊')} <b>Voice Message</b>\n\n🎙 <i>"${text}"</i>\n\n<b>⚠️ Voice generation temporarily unavailable.</b>`,
          parse_mode: 'HTML',
          reply_to_message_id: messageId,
        }, lk, tk);
      }
    }
  } catch (error) {
    console.error('TTS error:', error);
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.stop, '❌')} <b>TTS failed, please try again later.</b>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
  }
}

// ─── AI Roast ───────────────────────────────────────────────────────
async function handleAiRoast(chatId: number, rawText: string, messageId: number, lk: string, tk: string) {
  const name = rawText.replace(/^\/roast\s*/i, '').trim() || 'Bhai';
  await tg('sendChatAction', { chat_id: chatId, action: 'typing' }, lk, tk);

  try {
    const ROAST_MODELS = ['google/gemini-3-flash-preview', 'google/gemini-2.5-flash-lite'];
    let aiRes: Response | null = null;
    for (const model of ROAST_MODELS) {
      aiRes = await fetch(AI_GATEWAY_URL, {
        method: 'POST',
        headers: { Authorization: `Bearer ${lk}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          model,
          messages: [
            { role: 'system', content: `Tu ek savage roast master hai. "${name}" naam ke bande ko 5 funny roast lines likh Hinglish me. Har line me {name} ka naam use kar. Lines funny, creative aur unique honi chahiye. Emojis bhi use kar. No repeat content. Proper punctuation use kar.` },
            { role: 'user', content: `Roast ${name} with 5 savage lines` },
          ],
          max_tokens: 500,
        }),
      });
      if (aiRes.ok) break;
      console.error(`AI Roast failed with ${model}:`, aiRes.status);
      if (aiRes.status === 402 || aiRes.status === 429) continue;
      break;
    }
    if (!aiRes || !aiRes.ok) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} Error while generating roast. Try again later.`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return;
    }
    const aiData = await aiRes.json();
    const roast = aiData?.choices?.[0]?.message?.content || `${name} itna boring hai ki uska WiFi bhi disconnect ho jata hai! 😂`;

    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.fire, '🔥')} <b>AI Roast for ${name}:</b>\n\n${roast}`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
      reply_markup: {
        inline_keyboard: [[{ text: '🔥 Roast Again', callback_data: `ai_roast:${name}` }]],
      },
    }, lk, tk);
  } catch (error) {
    console.error('AI Roast error:', error);
    await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>AI Roast fail, try again!</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
  }
}

// ─── Translate ──────────────────────────────────────────────────────
async function handleTranslate(chatId: number, rawText: string, messageId: number, lk: string, tk: string) {
  const text = rawText.replace(/^\/translate\s*/i, '').trim();
  if (!text) {
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.sparkle, '🌐')} <b>AI Translator</b>\n\n<b>Usage:</b>\n<code>/translate hello how are you</code>\n<code>/translate to Spanish: good morning</code>\n\n<b>Auto-detects language & translates!</b>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    return;
  }

  await tg('sendChatAction', { chat_id: chatId, action: 'typing' }, lk, tk);
  try {
    const aiRes = await fetch(AI_GATEWAY_URL, {
      method: 'POST',
      headers: { Authorization: `Bearer ${lk}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'google/gemini-2.5-flash-lite',
        messages: [
          { role: 'system', content: 'You are a translator. Detect the input language. If Hindi/Hinglish → translate to English. If English → translate to Hindi. If user specifies "to [language]:" at start, translate to that language. Output format:\n🔤 Original: [input]\n🌐 Language: [detected]\n✅ Translation: [result]\nKeep it clean and accurate.' },
          { role: 'user', content: text },
        ],
        max_tokens: 300,
      }),
    });
    const aiData = await aiRes.json();
    const result = aiData?.choices?.[0]?.message?.content || 'Translation failed';
    await tg('sendMessage', { chat_id: chatId, text: `${ce(E.sparkle, '🌐')} <b>Translation</b>\n\n${result}`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
  } catch { await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Translation failed.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk); }
}

// ─── Joke ───────────────────────────────────────────────────────────
async function handleJoke(chatId: number, messageId: number, lk: string, tk: string) {
  await tg('sendChatAction', { chat_id: chatId, action: 'typing' }, lk, tk);
  try {
    const aiRes = await fetch(AI_GATEWAY_URL, {
      method: 'POST',
      headers: { Authorization: `Bearer ${lk}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'google/gemini-2.5-flash-lite',
        messages: [
          { role: 'system', content: 'Tell a random funny joke in Hinglish. Mix of Hindi English. Keep it short (2-4 lines). Include emojis. Make it unique every time - different topic each time (tech, relationship, desi, school, office, food, etc). Use proper punctuation.' },
          { role: 'user', content: `Tell a random joke. Random seed: ${Date.now()}` },
        ],
        max_tokens: 200,
      }),
    });
    const aiData = await aiRes.json();
    const joke = aiData?.choices?.[0]?.message?.content || 'Doctor: Aapko hasne ki bimari hai.\nPatient: Ye toh acchi baat hai! 😂';
    await tg('sendMessage', {
      chat_id: chatId, text: `${ce(E.laugh, '😂')} <b>Random Joke</b>\n\n${joke}`,
      parse_mode: 'HTML', reply_to_message_id: messageId,
      reply_markup: { inline_keyboard: [[{ text: '😂 Another Joke', callback_data: 'cmd_joke' }]] },
    }, lk, tk);
  } catch { await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Joke generation failed.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk); }
}

// ─── Random Fact ────────────────────────────────────────────────────
async function handleFact(chatId: number, messageId: number, lk: string, tk: string) {
  await tg('sendChatAction', { chat_id: chatId, action: 'typing' }, lk, tk);
  try {
    const aiRes = await fetch(AI_GATEWAY_URL, {
      method: 'POST',
      headers: { Authorization: `Bearer ${lk}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'google/gemini-2.5-flash-lite',
        messages: [
          { role: 'system', content: 'Share one amazing, mind-blowing random fact in Hinglish. Keep it short (2-3 lines). Include emojis. Make it unique. Different topic every time (science, history, space, animals, technology, human body, etc). Start with "Kya pata tha?" or similar hook.' },
          { role: 'user', content: `Random fact. Seed: ${Date.now()}` },
        ],
        max_tokens: 200,
      }),
    });
    const aiData = await aiRes.json();
    const fact = aiData?.choices?.[0]?.message?.content || 'Kya pata tha? Honey kabhi expire nahi hota! 🍯🤯';
    await tg('sendMessage', {
      chat_id: chatId, text: `${ce(E.bolt, '🧠')} <b>Random Fact</b>\n\n${fact}`,
      parse_mode: 'HTML', reply_to_message_id: messageId,
      reply_markup: { inline_keyboard: [[{ text: '🧠 Another Fact', callback_data: 'cmd_fact' }]] },
    }, lk, tk);
  } catch { await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Fact generation failed.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk); }
}

// ─── Would You Rather ──────────────────────────────────────────────
async function handleWYR(chatId: number, messageId: number, lk: string, tk: string) {
  await tg('sendChatAction', { chat_id: chatId, action: 'typing' }, lk, tk);
  try {
    const aiRes = await fetch(AI_GATEWAY_URL, {
      method: 'POST',
      headers: { Authorization: `Bearer ${lk}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'google/gemini-2.5-flash-lite',
        messages: [
          { role: 'system', content: 'Create a fun "Would You Rather" question in Hinglish. Format:\n🅰️ Option A\n🅱️ Option B\nMake it funny/interesting. Different topic each time. Use emojis.' },
          { role: 'user', content: `WYR question. Seed: ${Date.now()}` },
        ],
        max_tokens: 200,
      }),
    });
    const aiData = await aiRes.json();
    const wyr = aiData?.choices?.[0]?.message?.content || '🅰️ Hamesha sach bolna\n🅱️ Hamesha jhooth bolna';
    await tg('sendMessage', {
      chat_id: chatId, text: `${ce(E.gift, '🤔')} <b>Would You Rather?</b>\n\n${wyr}`,
      parse_mode: 'HTML', reply_to_message_id: messageId,
      reply_markup: { inline_keyboard: [[{ text: '🅰️ Option A', callback_data: 'wyr_a' }, { text: '🅱️ Option B', callback_data: 'wyr_b' }], [{ text: '🔄 Next Question', callback_data: 'cmd_wyr' }]] },
    }, lk, tk);
  } catch { await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed to generate question.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk); }
}

// ─── Pickup Line ────────────────────────────────────────────────────
async function handlePickup(chatId: number, messageId: number, lk: string, tk: string) {
  await tg('sendChatAction', { chat_id: chatId, action: 'typing' }, lk, tk);
  try {
    const aiRes = await fetch(AI_GATEWAY_URL, {
      method: 'POST',
      headers: { Authorization: `Bearer ${lk}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'google/gemini-2.5-flash-lite',
        messages: [
          { role: 'system', content: 'Generate one creative, funny pickup line in Hinglish. Mix Hindi and English. Make it cheesy and cute. Include emojis. Keep it short (1-2 lines). Different every time.' },
          { role: 'user', content: `Pickup line. Seed: ${Date.now()}` },
        ],
        max_tokens: 150,
      }),
    });
    const aiData = await aiRes.json();
    const line = aiData?.choices?.[0]?.message?.content || 'Tum Google ho kya? Kyunki jo mai dhundh raha tha wo mil gaya! 😍';
    await tg('sendMessage', {
      chat_id: chatId, text: `${ce(E.heart_fire, '💘')} <b>Pickup Line</b>\n\n${line}`,
      parse_mode: 'HTML', reply_to_message_id: messageId,
      reply_markup: { inline_keyboard: [[{ text: '💘 Another One', callback_data: 'cmd_pickup' }]] },
    }, lk, tk);
  } catch { await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk); }
}

// ─── Auto Voice-to-Text ─────────────────────────────────────────────
async function handleVoiceToText(chatId: number, msg: any, messageId: number, lk: string, tk: string) {
  const voice = msg.voice || msg.audio;
  if (!voice?.file_id) return;

  try {
    const fileRes = await tg('getFile', { file_id: voice.file_id }, lk, tk);
    if (!fileRes?.ok || !fileRes.result?.file_path) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Voice file download failed.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return;
    }

    const filePath = fileRes.result.file_path;
    const downloadRes = await fetch(`${GATEWAY_URL}/file/${filePath}`, {
      headers: { Authorization: `Bearer ${lk}`, 'X-Connection-Api-Key': tk },
    });

    if (!downloadRes.ok) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Could not download voice.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return;
    }

    const audioBytes = await downloadRes.arrayBuffer();
    const base64Audio = btoa(String.fromCharCode(...new Uint8Array(audioBytes)));
    const mimeType = voice.mime_type || 'audio/ogg';

    await tg('sendChatAction', { chat_id: chatId, action: 'typing' }, lk, tk);

    // Use Gemini to transcribe audio
    const aiRes = await fetch(AI_GATEWAY_URL, {
      method: 'POST',
      headers: { Authorization: `Bearer ${lk}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'google/gemini-2.5-flash',
        messages: [
          { role: 'system', content: 'You are an audio transcription assistant. Listen to the audio and transcribe exactly what is said. If Hindi/Hinglish, transcribe in Roman Hindi. Output ONLY the transcribed text, nothing else.' },
          {
            role: 'user',
            content: [
              { type: 'text', text: 'Transcribe this voice message:' },
              { type: 'input_audio', input_audio: { data: base64Audio, format: mimeType.includes('ogg') ? 'ogg' : 'mp3' } },
            ],
          },
        ],
        max_tokens: 500,
      }),
    });

    const aiData = await aiRes.json();
    const transcription = aiData?.choices?.[0]?.message?.content?.trim();

    if (transcription) {
      await tg('sendMessage', {
        chat_id: chatId,
        text: `${ce(E.megaphone, '🎙')} <b>Voice To Text:</b>\n\n<i>"${transcription}"</i>`,
        parse_mode: 'HTML',
        reply_to_message_id: messageId,
      }, lk, tk);
      sendLog(lk, tk, `🎙 <b>VOICE-TO-TEXT</b>\n📍 Chat: <code>${chatId}</code>\n👤 User: <code>${msg.from?.id || '?'}</code>\n💬 <i>${escapeHtml(transcription).substring(0, 200)}</i>`);
    } else {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Could not transcribe voice.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
  } catch (error) {
    console.error('VTT error:', error);
    await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Voice transcription failed.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
  }
}

// ─── Group Worker ───────────────────────────────────────────────────
async function triggerGroupWorker(workerUrl: string, anonKey: string, userId: number, runId?: string) {
  await fetch(workerUrl, {
    method: 'POST',
    headers: { Authorization: `Bearer ${anonKey}`, apikey: anonKey, 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId, runId }),
  });
}

async function recoverStalledGroupJobs(sb: any, workerUrl: string, anonKey: string) {
  const { data: states } = await sb
    .from('user_states').select('*').not('mode', 'is', null).not('target_name', 'is', null)
    .order('updated_at', { ascending: true }).limit(20);

  const now = Date.now();
  const groupStates = (states || []).filter((state: any) => {
    const payload = decodeTargetPayload(state.target_name);
    return !!payload?.name && !!payload.chatId && isGroupChat(payload.chatType);
  });

  const stalled = groupStates.filter((state: any) => {
    const payload = decodeTargetPayload(state.target_name);
    if (!payload?.leaseUntil) return true;
    return Date.parse(payload.leaseUntil) <= now;
  });

  await Promise.all(stalled.map((state: any) => {
    const payload = decodeTargetPayload(state.target_name);
    return triggerGroupWorker(workerUrl, anonKey, state.user_id, payload?.runId);
  }));

  return groupStates.length > 0;
}

// ─── Admin Users Page ───────────────────────────────────────────────
async function sendUsersPage(
  sb: any, chatId: number, offset: number, lk: string, tk: string,
  options?: { messageId?: number; replyToMessageId?: number; includeStats?: boolean },
) {
  const pageOffset = Math.max(0, offset);
  const [usersResult, totalUsersResult, bannedResult, roastResult, funnyResult] = await Promise.all([
    sb.from('bot_users').select('*').gt('user_id', 0).order('last_active', { ascending: false }).range(pageOffset, pageOffset + USER_PAGE_SIZE - 1),
    sb.from('bot_users').select('user_id', { count: 'exact', head: true }).gt('user_id', 0),
    sb.from('bot_users').select('user_id', { count: 'exact', head: true }).gt('user_id', 0).eq('is_banned', true),
    sb.from('gali_lines').select('id', { count: 'exact', head: true }).eq('mode', 'roast'),
    sb.from('gali_lines').select('id', { count: 'exact', head: true }).eq('mode', 'funny'),
  ]);

  const total = totalUsersResult.count || 0;
  const banned = bannedResult.count || 0;
  const roastCount = roastResult.count || 0;
  const funnyCount = funnyResult.count || 0;
  const users = usersResult.data || [];
  const start = total === 0 ? 0 : pageOffset + 1;
  const end = total === 0 ? 0 : Math.min(pageOffset + users.length, total);

  let msg = '';
  if (options?.includeStats) {
    msg += `📊 <b>Bot Stats</b>\n\n👥 Total Users: <b>${total}</b>\n🚫 Banned: <b>${banned}</b>\n✅ Active: <b>${total - banned}</b>\n🔥 Light Roasts: <b>${roastCount}</b>\n😂 Heavy Roasts: <b>${funnyCount}</b>\n\n`;
  }

  msg += `👥 <b>All Users List</b>\n<b>Showing:</b> <b>${start}-${end}</b> / <b>${total}</b>\n\n`;
  if (users.length === 0) {
    msg += '<b>No users found.</b>';
  } else {
    for (const u of users) {
      msg += `• <b>${u.user_id}</b>`;
      if (u.first_name) msg += ` - ${u.first_name}`;
      if (u.username) msg += ` (@${u.username})`;
      if (u.is_banned) msg += ' 🚫';
      msg += '\n';
    }
  }

  const navButtons = [] as Array<{ text: string; callback_data: string }>;
  if (pageOffset > 0) navButtons.push({ text: '⬅️ Prev 30', callback_data: `admin_users_page:${Math.max(0, pageOffset - USER_PAGE_SIZE)}` });
  if (pageOffset + USER_PAGE_SIZE < total) navButtons.push({ text: '➡️ Next 30', callback_data: `admin_users_page:${pageOffset + USER_PAGE_SIZE}` });

  const payload: Record<string, unknown> = {
    chat_id: chatId, text: msg, parse_mode: 'HTML',
    reply_markup: navButtons.length > 0 ? { inline_keyboard: [navButtons] } : undefined,
  };

  if (options?.messageId) {
    payload.message_id = options.messageId;
    await tg('editMessageText', payload, lk, tk);
  } else {
    if (options?.replyToMessageId) payload.reply_to_message_id = options.replyToMessageId;
    await tg('sendMessage', payload, lk, tk);
  }
}

// ─── DM Lines Batch ─────────────────────────────────────────────────
async function sendDmLinesBatch(sb: any, chatId: number, userId: number, lk: string, tk: string) {
  const state = await getUserState(sb, userId);
  const target = decodeTargetPayload(state?.target_name);

  if (!state || !state.mode || !target?.name) { return; }

  const mode = state.mode;
  const index = state.line_index || 0;
  const name = target.name;
  const allLines = await getLines(sb, mode);
  const total = allLines.length;

  if (total === 0) {
    await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '⚠️')} <b>No lines available in this category yet.</b> 😌`, parse_mode: 'HTML' }, lk, tk);
    await setUserState(sb, userId, null, 0, null);
    return;
  }

  if (index >= total) {
    await tg('sendMessage', {
      chat_id: chatId, text: `${ce(E.check, '✅')} <b>All lines have been sent! Want to try a new target?</b> 😏`, parse_mode: 'HTML',
      reply_markup: { inline_keyboard: [[{ text: '🔁 New Target', callback_data: 'restart' }]] },
    }, lk, tk);
    await setUserState(sb, userId, null, 0, null);
    return;
  }

  const end = Math.min(index + BATCH_SIZE, total);
  const batch: Array<{ chatId: number; text: string }> = [];
  const historyRows: Array<{ user_id: number; mode: string; target_name: string; line: string }> = [];

  for (let i = index; i < end; i++) {
    const line = allLines[i].replace(/\{name\}/g, name);
    batch.push({ chatId, text: line });
    historyRows.push({ user_id: userId, mode, target_name: name, line });
  }

  await Promise.all(batch.map((m) => tg('sendMessage', { chat_id: m.chatId, text: m.text, parse_mode: 'HTML' }, lk, tk)));
  saveHistoryBatch(sb, historyRows);

  if (end < total) {
    await setUserState(sb, userId, mode, end, encodeTargetPayload(stripLease(target)));
    await tg('sendMessage', {
      chat_id: chatId, text: `${ce(E.fire, '🔥')} <b>${end}/${total} lines sent. Want more?</b> 😈`, parse_mode: 'HTML',
      reply_markup: { inline_keyboard: [[{ text: '➡️ Next 10 Lines', callback_data: 'next10' }]] },
    }, lk, tk);
  } else {
    await setUserState(sb, userId, null, 0, null);
    await tg('sendMessage', {
      chat_id: chatId, text: `${ce(E.party, '✨')} <b>All lines finished!</b> 😎`, parse_mode: 'HTML',
      reply_markup: { inline_keyboard: [[{ text: '🔁 New Target', callback_data: 'restart' }]] },
    }, lk, tk);
  }
}

// ─── Chat Filters System ────────────────────────────────────────────
type FilterReplyPayload =
  | { kind: 'text'; text: string }
  | { kind: 'sticker'; fileId: string }
  | { kind: 'photo'; fileId: string; caption?: string }
  | { kind: 'animation'; fileId: string; caption?: string }
  | { kind: 'video'; fileId: string; caption?: string }
  | { kind: 'document'; fileId: string; caption?: string };

const filtersCache = new Map<number, { filters: Array<{ keyword: string; action: string }>; ts: number }>();
const FILTERS_CACHE_TTL = 60_000;

function parseFilterReplyPayload(raw: string | null | undefined, keyword: string): FilterReplyPayload {
  const trimmed = String(raw ?? '').trim();
  if (!trimmed || trimmed === 'warn' || trimmed === 'delete') {
    return { kind: 'text', text: keyword };
  }

  try {
    const parsed = JSON.parse(trimmed);
    if (parsed?.kind === 'text' && typeof parsed.text === 'string' && parsed.text.trim()) {
      return { kind: 'text', text: parsed.text };
    }
    if (parsed?.kind === 'sticker' && typeof parsed.fileId === 'string' && parsed.fileId) {
      return { kind: 'sticker', fileId: parsed.fileId };
    }
    if (parsed?.kind === 'photo' && typeof parsed.fileId === 'string' && parsed.fileId) {
      return { kind: 'photo', fileId: parsed.fileId, caption: typeof parsed.caption === 'string' ? parsed.caption : undefined };
    }
    if (parsed?.kind === 'animation' && typeof parsed.fileId === 'string' && parsed.fileId) {
      return { kind: 'animation', fileId: parsed.fileId, caption: typeof parsed.caption === 'string' ? parsed.caption : undefined };
    }
    if (parsed?.kind === 'video' && typeof parsed.fileId === 'string' && parsed.fileId) {
      return { kind: 'video', fileId: parsed.fileId, caption: typeof parsed.caption === 'string' ? parsed.caption : undefined };
    }
    if (parsed?.kind === 'document' && typeof parsed.fileId === 'string' && parsed.fileId) {
      return { kind: 'document', fileId: parsed.fileId, caption: typeof parsed.caption === 'string' ? parsed.caption : undefined };
    }
  } catch {
    return { kind: 'text', text: trimmed };
  }

  return { kind: 'text', text: keyword };
}

function serializeFilterReplyPayload(payload: FilterReplyPayload): string {
  return JSON.stringify(payload);
}

function compactFilterText(value: string, max = 36) {
  const clean = value.replace(/\s+/g, ' ').trim();
  return clean.length > max ? `${clean.slice(0, Math.max(1, max - 1))}…` : clean;
}

function describeFilterReplyPayload(payload: FilterReplyPayload): string {
  switch (payload.kind) {
    case 'text':
      return `text: ${compactFilterText(payload.text)}`;
    case 'sticker':
      return 'sticker';
    case 'photo':
      return payload.caption ? `photo: ${compactFilterText(payload.caption)}` : 'photo';
    case 'animation':
      return payload.caption ? `gif: ${compactFilterText(payload.caption)}` : 'gif';
    case 'video':
      return payload.caption ? `video: ${compactFilterText(payload.caption)}` : 'video';
    case 'document':
      return payload.caption ? `file: ${compactFilterText(payload.caption)}` : 'file';
    default:
      return 'text';
  }
}

function buildFilterReplyPayloadFromMessage(msg: any): FilterReplyPayload | null {
  if (!msg) return null;

  if (msg.sticker?.file_id) {
    return { kind: 'sticker', fileId: msg.sticker.file_id };
  }

  if (Array.isArray(msg.photo) && msg.photo.length > 0) {
    const photo = msg.photo[msg.photo.length - 1];
    return {
      kind: 'photo',
      fileId: photo.file_id,
      caption: typeof msg.caption === 'string' && msg.caption.trim() ? msg.caption : undefined,
    };
  }

  if (msg.animation?.file_id) {
    return {
      kind: 'animation',
      fileId: msg.animation.file_id,
      caption: typeof msg.caption === 'string' && msg.caption.trim() ? msg.caption : undefined,
    };
  }

  if (msg.video?.file_id) {
    return {
      kind: 'video',
      fileId: msg.video.file_id,
      caption: typeof msg.caption === 'string' && msg.caption.trim() ? msg.caption : undefined,
    };
  }

  if (msg.document?.file_id) {
    return {
      kind: 'document',
      fileId: msg.document.file_id,
      caption: typeof msg.caption === 'string' && msg.caption.trim() ? msg.caption : undefined,
    };
  }

  if (typeof msg.text === 'string' && msg.text.trim()) {
    return { kind: 'text', text: msg.text };
  }

  if (typeof msg.caption === 'string' && msg.caption.trim()) {
    return { kind: 'text', text: msg.caption };
  }

  return null;
}

async function sendStoredFilterReply(chatId: number, replyToMessageId: number, payload: FilterReplyPayload, lk: string, tk: string): Promise<boolean> {
  let endpoint = 'sendMessage';
  const body: Record<string, unknown> = { chat_id: chatId, reply_to_message_id: replyToMessageId };

  switch (payload.kind) {
    case 'text':
      body.text = payload.text;
      break;
    case 'sticker':
      endpoint = 'sendSticker';
      body.sticker = payload.fileId;
      break;
    case 'photo':
      endpoint = 'sendPhoto';
      body.photo = payload.fileId;
      if (payload.caption?.trim()) body.caption = payload.caption;
      break;
    case 'animation':
      endpoint = 'sendAnimation';
      body.animation = payload.fileId;
      if (payload.caption?.trim()) body.caption = payload.caption;
      break;
    case 'video':
      endpoint = 'sendVideo';
      body.video = payload.fileId;
      if (payload.caption?.trim()) body.caption = payload.caption;
      break;
    case 'document':
      endpoint = 'sendDocument';
      body.document = payload.fileId;
      if (payload.caption?.trim()) body.caption = payload.caption;
      break;
  }

  const data = await tg(endpoint, body, lk, tk);
  if (!data?.ok) {
    console.error('sendStoredFilterReply failed:', data);
    return false;
  }
  return true;
}

async function getChatFilters(sb: any, chatId: number): Promise<Array<{ keyword: string; action: string }>> {
  const cached = filtersCache.get(chatId);
  if (cached && (Date.now() - cached.ts < FILTERS_CACHE_TTL)) return cached.filters;
  const { data } = await sb.from('chat_filters').select('keyword, action').eq('chat_id', chatId);
  const filters = (data || []).map((r: any) => ({ keyword: String(r.keyword).toLowerCase(), action: r.action || '' }));
  filtersCache.set(chatId, { filters, ts: Date.now() });
  return filters;
}

function invalidateFiltersCache(chatId: number) {
  filtersCache.delete(chatId);
}

async function checkFilters(sb: any, chatId: number, msg: any, lk: string, tk: string): Promise<boolean> {
  if (typeof msg?.text === 'string' && msg.text.trim().startsWith('/')) return false;

  const filters = await getChatFilters(sb, chatId);
  if (filters.length === 0) return false;

  const signals = [
    msg.text,
    msg.caption,
    msg.sticker?.set_name,
    msg.sticker?.emoji,
    msg.document?.file_name,
    msg.video?.file_name,
    msg.animation?.file_name,
  ].filter(Boolean).join(' ').toLowerCase();

  if (!signals) return false;

  for (const f of filters) {
    if (signals.includes(f.keyword)) {
      const payload = parseFilterReplyPayload(f.action, f.keyword);
      const sent = await sendStoredFilterReply(chatId, msg.message_id, payload, lk, tk);
      if (sent) {
        sendLog(lk, tk, `🔍 <b>FILTER REPLY</b>\n📍 ${chatTag(msg.chat)}\n👤 ${userTag(msg.from?.id, msg.from?.first_name, msg.from?.username)}\n🔑 Keyword: <code>${escapeHtml(f.keyword)}</code>\n📦 Reply: <code>${escapeHtml(describeFilterReplyPayload(payload))}</code>`);
      }
      return sent;
    }
  }
  return false;
}

// ─── Sudo / Permission System ───────────────────────────────────────
async function isSudo(sb: any, userId: number): Promise<boolean> {
  if (userId === ADMIN_ID) return true;
  const { data } = await sb.from('sudo_users').select('user_id').eq('user_id', userId).maybeSingle();
  return !!data;
}

async function getSudoUsers(sb: any): Promise<any[]> {
  const { data } = await sb.from('sudo_users').select('*').order('created_at', { ascending: true });
  return data || [];
}

// ─── Admin Command Handler ──────────────────────────────────────────
async function handleAdminCommand(sb: any, chatId: number, rawText: string, userId: number, messageId: number, lk: string, tk: string, replyToMessage?: any) {
  const hasSudo = await isSudo(sb, userId);
  if (!hasSudo) return false;
  const text = cleanCmd(rawText);
  const isOwner = userId === ADMIN_ID;
  const groupMessage = chatId < 0;

  // ─── /info command (reply, @username, or user_id) ────
  if (text === '/info' || text.startsWith('/info ')) {
    const parts = text.split(/\s+/);
    let targetId: number | null = null;
    let targetUsername: string | null = null;

    if (parts.length > 1) {
      if (parts[1].startsWith('@')) {
        targetUsername = parts[1].substring(1).toLowerCase();
      } else if (/^-?\d+$/.test(parts[1])) {
        targetId = parseInt(parts[1]);
      }
    } else if (replyToMessage?.from) {
      // Reply to someone = get their info
      targetId = replyToMessage.from.id;
    }

    if (!targetId && !targetUsername) {
      await tg('sendMessage', { chat_id: chatId, text: `ℹ️ <b>Usage:</b>\n<code>/info @username</code>\n<code>/info 123456789</code>\n<b>Ya kisi ke message pe reply karke /info</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }

    // Try Telegram getChat API for live details
    const lookupId = targetId || targetUsername;
    let tgChat: any = null;
    if (lookupId) {
      const chatRes = await tg('getChat', { chat_id: targetUsername ? `@${targetUsername}` : lookupId }, lk, tk);
      if (chatRes?.ok) tgChat = chatRes.result;
    }

    // Also check bot_users DB
    let query = sb.from('bot_users').select('*');
    if (targetId) query = query.eq('user_id', targetId);
    else if (targetUsername) query = query.eq('username', targetUsername);
    const { data: user } = await query.maybeSingle();

    if (!user && !tgChat) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>User not found.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }

    const resolvedId = tgChat?.id || user?.user_id;
    const sudoCheck = resolvedId > 0 ? await isSudo(sb, resolvedId) : false;
    const { count: histCount } = resolvedId > 0
      ? await sb.from('bot_history').select('id', { count: 'exact', head: true }).eq('user_id', resolvedId)
      : { count: 0 };

    // Build comprehensive info
    const name = tgChat?.first_name || user?.first_name || '—';
    const lastName = tgChat?.last_name || '';
    const uname = tgChat?.username || user?.username;
    const bio = tgChat?.bio || null;
    const photo = tgChat?.photo ? '✅ Has Profile Photo' : '❌ No Profile Photo';
    const isBot = tgChat?.is_bot ? 'Yes 🤖' : 'No';
    const isPremium = tgChat?.is_premium ? 'Yes ⭐' : 'No';
    const hasHiddenNumber = tgChat?.has_private_forwards ? 'Yes 🔒' : 'No';
    const emojiStatus = tgChat?.emoji_status_custom_emoji_id ? `Custom Emoji: <code>${tgChat.emoji_status_custom_emoji_id}</code>` : '—';

    let infoText = `ℹ️ <b>User Info</b>\n\n` +
      `👤 <b>Name:</b> ${escapeHtml(name)}${lastName ? ' ' + escapeHtml(lastName) : ''}\n` +
      `🏷 <b>Username:</b> ${uname ? '@' + uname : '—'}\n` +
      `🆔 <b>ID:</b> <code>${resolvedId}</code>\n`;

    if (tgChat) {
      infoText += `🤖 <b>Bot:</b> ${isBot}\n`;
      infoText += `⭐ <b>Premium:</b> ${isPremium}\n`;
      infoText += `📸 <b>Photo:</b> ${photo}\n`;
      if (bio) infoText += `📝 <b>Bio:</b> <i>${escapeHtml(bio)}</i>\n`;
      infoText += `🔒 <b>Hidden Forwards:</b> ${hasHiddenNumber}\n`;
      infoText += `😀 <b>Emoji Status:</b> ${emojiStatus}\n`;
    }

    if (user) {
      infoText += `📅 <b>Bot Join:</b> ${new Date(user.created_at).toLocaleDateString()}\n`;
      infoText += `🕐 <b>Last Active:</b> ${new Date(user.last_active).toLocaleDateString()}\n`;
      infoText += `🚫 <b>Banned:</b> ${user.is_banned ? 'Yes' : 'No'}\n`;
    }
    if (resolvedId > 0) {
      infoText += `👑 <b>Sudo:</b> ${sudoCheck ? 'Yes' : 'No'}\n`;
      infoText += `📊 <b>Total Commands:</b> ${histCount || 0}`;
    }

    // Add permanent link
    if (resolvedId > 0) {
      infoText += `\n🔗 <b>Link:</b> <a href="tg://user?id=${resolvedId}">Open Profile</a>`;
    }

    await tg('sendMessage', {
      chat_id: chatId,
      text: infoText,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
      disable_web_page_preview: true,
    }, lk, tk);
    return true;
  }

  // ─── /filter command (reply-based keyword filters) ────
  if (text === '/filter' || text.startsWith('/filter ') || text === '/filters' || text.startsWith('/rmfilter ') || text.startsWith('/unfilter ')) {
    if (text === '/filters') {
      const filters = await getChatFilters(sb, chatId);
      if (filters.length === 0) {
        await tg('sendMessage', { chat_id: chatId, text: `🔍 <b>No filters set for this chat.</b>\n\n<b>Usage:</b> <code>/filter keyword</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      } else {
        let msg = `🔍 <b>Active Filters (${filters.length})</b>\n\n`;
        filters.forEach((f, i) => {
          const payload = parseFilterReplyPayload(f.action, f.keyword);
          msg += `<b>${i + 1}.</b> <code>${escapeHtml(f.keyword)}</code> → ${escapeHtml(describeFilterReplyPayload(payload))}\n`;
        });
        msg += `\n<b>Remove:</b> <code>/stop keyword</code>`;
        await tg('sendMessage', { chat_id: chatId, text: msg, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      }
      return true;
    }

    if (text.startsWith('/rmfilter ') || text.startsWith('/unfilter ')) {
      const keyword = text.replace(/^\/(rm|un)filter\s+/i, '').trim().toLowerCase();
      if (!keyword) {
        await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Usage:</b> <code>/rmfilter keyword</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
        return true;
      }
      await sb.from('chat_filters').delete().eq('chat_id', chatId).eq('keyword', keyword);
      invalidateFiltersCache(chatId);
      await tg('sendMessage', { chat_id: chatId, text: `✅ <b>Filter removed:</b> <code>${escapeHtml(keyword)}</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      sendLog(lk, tk, `🔍 <b>FILTER REMOVED</b>\n📍 Chat: <code>${chatId}</code>\n🔑 <code>${escapeHtml(keyword)}</code>`);
      return true;
    }

    const filterArgs = text === '/filter' ? '' : text.replace(/^\/filter\s+/i, '').trim();
    const spaceIdx = filterArgs.indexOf(' ');
    const keyword = (spaceIdx > 0 ? filterArgs.slice(0, spaceIdx) : filterArgs).trim().toLowerCase();
    const inlineReplyText = spaceIdx > 0 ? filterArgs.slice(spaceIdx + 1).trim() : '';
    const repliedPayload = buildFilterReplyPayloadFromMessage(replyToMessage);
    const payload = repliedPayload || (inlineReplyText ? { kind: 'text', text: inlineReplyText } as FilterReplyPayload : null);

    if (!keyword || !payload) {
      await tg('sendMessage', {
        chat_id: chatId,
        text: `🔍 <b>Filter Usage:</b>\n\n<b>Reply method:</b> kisi sticker/photo/gif/video/file/text pe reply karke <code>/filter keyword</code>\n<b>Text method:</b> <code>/filter keyword reply text</code>\n<code>/filters</code> — List all filters\n<code>/stop keyword</code> — Remove filter`,
        parse_mode: 'HTML',
        reply_to_message_id: messageId,
      }, lk, tk);
      return true;
    }

    const serializedPayload = serializeFilterReplyPayload(payload);
    const { error: deleteExistingError } = await sb
      .from('chat_filters')
      .delete()
      .eq('chat_id', chatId)
      .eq('keyword', keyword);

    if (deleteExistingError) {
      console.error('filter delete before save failed:', deleteExistingError);
      await tg('sendMessage', {
        chat_id: chatId,
        text: `❌ <b>Filter save failed.</b> Try again.`,
        parse_mode: 'HTML',
        reply_to_message_id: messageId,
      }, lk, tk);
      return true;
    }

    const { error: insertFilterError } = await sb
      .from('chat_filters')
      .insert({ chat_id: chatId, keyword, action: serializedPayload, added_by: userId });

    if (insertFilterError) {
      console.error('filter insert failed:', insertFilterError);
      await tg('sendMessage', {
        chat_id: chatId,
        text: `❌ <b>Filter save failed.</b> Try again.`,
        parse_mode: 'HTML',
        reply_to_message_id: messageId,
      }, lk, tk);
      return true;
    }

    invalidateFiltersCache(chatId);
    await tg('sendMessage', {
      chat_id: chatId,
      text: `✅ <b>Filter added!</b>\n\n🔑 <b>Keyword:</b> <code>${escapeHtml(keyword)}</code>\n📦 <b>Reply:</b> ${escapeHtml(describeFilterReplyPayload(payload))}`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    sendLog(lk, tk, `🔍 <b>FILTER ADDED</b>\n📍 Chat: <code>${chatId}</code>\n🔑 <code>${escapeHtml(keyword)}</code>\n📦 <code>${escapeHtml(describeFilterReplyPayload(payload))}</code>`);
    return true;
  }

  if (text === '/admin') {
    const sudoLabel = isOwner ? '\n• 👑 Sudo Users' : '';
    await tg('sendMessage', {
      chat_id: chatId,
      text: `${ce(E.crown, '👑')} <b>Admin Panel</b>\n\n• 📊 Stats\n• 👥 Users List\n• 📜 Users History\n• 📢 Broadcast (-pin, -user)\n• 🚫 Ban / ✅ Unban\n• 🔇 Mute / 🔊 Unmute\n• ℹ️ User Info\n• 📝 Lines Manage${sudoLabel}`,
      parse_mode: 'HTML',
      reply_markup: {
        inline_keyboard: [
          [{ text: '📊 Stats', callback_data: 'admin_stats' }, { text: '👥 Users List', callback_data: 'admin_users' }],
          [{ text: 'ℹ️ Info Help', callback_data: 'admin_info' }, { text: '📜 History Help', callback_data: 'admin_history' }],
          [{ text: '📢 Broadcast', callback_data: 'admin_broadcast' }],
          [{ text: '🚫 Ban / ✅ Unban', callback_data: 'admin_ban' }],
          [{ text: '🔇 Mute / 🔊 Unmute', callback_data: 'admin_mute' }],
          [{ text: '📝 Lines Manage', callback_data: 'admin_lines' }],
          ...(isOwner ? [[{ text: '👑 Sudo Users', callback_data: 'admin_sudo' }]] : []),
        ],
      },
    }, lk, tk);
    return true;
  }

  if (text === '/stats' || text === '/users') {
    await sendUsersPage(sb, chatId, 0, lk, tk, { replyToMessageId: messageId, includeStats: true });
    return true;
  }

  if (text.startsWith('/history ')) {
    const targetId = parseInt(text.split(' ')[1]);
    if (isNaN(targetId)) {
      await tg('sendMessage', { chat_id: chatId, text: '<b>Usage:</b> <code>/history user_id</code>', parse_mode: 'HTML' }, lk, tk);
      return true;
    }
    const { data: rows } = await sb.from('bot_history').select('*').eq('user_id', targetId).order('created_at', { ascending: false }).limit(20);
    if (!rows || rows.length === 0) {
      await tg('sendMessage', { chat_id: chatId, text: '<b>No history found for this user.</b>', parse_mode: 'HTML' }, lk, tk);
      return true;
    }
    let msg = `📜 <b>Last 20 lines for ${targetId}:</b>\n`;
    for (const r of rows) msg += `[${r.created_at}] (${r.mode}) ${r.target_name} → ${r.line}\n`;
    await tg('sendMessage', { chat_id: chatId, text: msg, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    return true;
  }

  // ─── Broadcast with -pin and -user flags + reply forward ────
  if (text.startsWith('/broadcast')) {
    let bcRaw = text.substring(10).trim();
    const shouldPin = bcRaw.includes('-pin');
    const userOnly = bcRaw.includes('-user');
    const gcOnly = bcRaw.includes('-gc');
    bcRaw = bcRaw.replace(/-pin/g, '').replace(/-user/g, '').replace(/-gc/g, '').trim();

    // Check if replying to a message (forward that content)
    const hasReply = !!replyToMessage;
    if (!bcRaw && !hasReply) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Usage:</b>\n<code>/broadcast message</code> — All\n<code>/broadcast -user message</code> — Users only\n<code>/broadcast -gc message</code> — Groups only\n<code>/broadcast -pin message</code> — Pin bhi\n\nYa kisi post ko reply karke <code>/broadcast -user -pin</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }

    // Send "processing" status
    const statusRes = await tg('sendMessage', { chat_id: chatId, text: `⏳ <b>Broadcasting...</b> Please wait.`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    const statusMsgId = statusRes?.result?.message_id;

    const { data: allUsers } = await sb.from('bot_users').select('user_id').eq('is_banned', false);
    if (!allUsers || allUsers.length === 0) {
      const noUserText = `${ce(E.stop, '❌')} <b>No users found.</b>`;
      if (statusMsgId) await tg('editMessageText', { chat_id: chatId, message_id: statusMsgId, text: noUserText, parse_mode: 'HTML' }, lk, tk);
      else await tg('sendMessage', { chat_id: chatId, text: noUserText, parse_mode: 'HTML' }, lk, tk);
      return true;
    }

    // Filter: -user = only positive IDs (users), -gc = only negative IDs (groups), default = all
    const targets = userOnly ? allUsers.filter((u: any) => u.user_id > 0) : gcOnly ? allUsers.filter((u: any) => u.user_id < 0) : allUsers;
    const modeLabel = userOnly ? '👤 Users Only' : gcOnly ? '👥 Groups Only' : '👥 All (Users + Groups)';
    const total = targets.length;
    const targetUsers = targets.filter((u: any) => u.user_id > 0).length;
    const targetGroups = targets.filter((u: any) => u.user_id < 0).length;
    let sent = 0, blocked = 0, failed = 0, pinned = 0;
    let sentUsers = 0, sentGroups = 0, blockedUsers = 0, blockedGroups = 0, failedUsers = 0, failedGroups = 0;
    const PARALLEL = 20;

    // Helper to send one broadcast item to a user/group
    async function sendBroadcastItem(targetChatId: number): Promise<{ ok: boolean; msgId?: number; desc?: string; errCode?: number }> {
      let res: any;
      if (hasReply && replyToMessage) {
        // Use forwardMessage to keep buttons, formatting, media exactly as-is
        res = await tg('forwardMessage', {
          chat_id: targetChatId,
          from_chat_id: chatId,
          message_id: replyToMessage.message_id,
        }, lk, tk);
        if (res?.ok) return { ok: true, msgId: res.result?.message_id };
      } else {
        res = await tg('sendMessage', { chat_id: targetChatId, text: `📢 <b>Broadcast:</b>\n${bcRaw}`, parse_mode: 'HTML' }, lk, tk);
        if (res?.ok) return { ok: true, msgId: res.result?.message_id };
      }
      return { ok: false, desc: String(res?.description || '').toLowerCase(), errCode: res?.error_code };
    }

    for (let i = 0; i < targets.length; i += PARALLEL) {
      const batchItems = targets.slice(i, i + PARALLEL);
      const results = await Promise.allSettled(
        batchItems.map(async (u: any) => {
          const targetType = u.user_id > 0 ? 'user' : 'group';
          const r = await sendBroadcastItem(u.user_id);
          if (r.ok) {
            if (shouldPin && r.msgId) {
              const pinRes = await tg('pinChatMessage', { chat_id: u.user_id, message_id: r.msgId, disable_notification: false }, lk, tk);
              if (pinRes?.ok) pinned++;
            }
            return { ok: true, targetType };
          } else {
            const isBlocked = (r.desc || '').includes('bot was blocked') || (r.desc || '').includes('user is deactivated') || (r.desc || '').includes('chat not found') || r.errCode === 403;
            return { ok: false, blocked: isBlocked, targetType };
          }
        }),
      );
      for (const r of results) {
        if (r.status === 'fulfilled') {
          const isUser = r.value.targetType === 'user';
          if (r.value.ok) {
            sent++;
            if (isUser) sentUsers++; else sentGroups++;
          } else if (r.value.blocked) {
            blocked++;
            if (isUser) blockedUsers++; else blockedGroups++;
          } else {
            failed++;
            if (isUser) failedUsers++; else failedGroups++;
          }
        } else {
          failed++;
        }
      }

      if (statusMsgId && (i + PARALLEL) % 100 === 0 && i + PARALLEL < targets.length) {
        await tg('editMessageText', { chat_id: chatId, message_id: statusMsgId, text: `⏳ <b>Broadcasting...</b> ${i + PARALLEL}/${total} done`, parse_mode: 'HTML' }, lk, tk).catch(() => {});
      }
    }

    const replyMsg = replyToMessage;
    const contentType = hasReply ? (replyMsg?.photo ? '🖼 Photo' : replyMsg?.video ? '🎬 Video' : replyMsg?.document ? '📄 Document' : replyMsg?.sticker ? '🎭 Sticker' : replyMsg?.voice ? '🎤 Voice' : replyMsg?.audio ? '🎵 Audio' : replyMsg?.animation ? '🎞 GIF' : '💬 Text') : '💬 Text';
    const summary = `📢 <b>Broadcast Complete!</b>\n\n` +
      `📊 <b>Total Targets:</b> ${total}\n` +
      `👤 <b>Target Users:</b> ${targetUsers}\n` +
      `👥 <b>Target Groups:</b> ${targetGroups}\n` +
      `✅ <b>Sent:</b> ${sent} (Users: ${sentUsers}, Groups: ${sentGroups})\n` +
      `🚫 <b>Blocked:</b> ${blocked} (Users: ${blockedUsers}, Groups: ${blockedGroups})\n` +
      `❌ <b>Failed:</b> ${failed} (Users: ${failedUsers}, Groups: ${failedGroups})\n` +
      (shouldPin ? `📌 <b>Pinned:</b> ${pinned}\n` : '') +
      `${modeLabel.startsWith('👤') ? '👤' : '👥'} <b>Mode:</b> ${modeLabel}\n` +
      `📎 <b>Content:</b> ${contentType}\n` +
      `\n💬 <i>${(bcRaw || replyMsg?.text || replyMsg?.caption || 'media').substring(0, 200)}</i>`;

    if (statusMsgId) {
      await tg('editMessageText', { chat_id: chatId, message_id: statusMsgId, text: summary, parse_mode: 'HTML' }, lk, tk);
    } else {
      await tg('sendMessage', { chat_id: chatId, text: summary, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }

    sendLog(lk, tk, `📢 <b>BROADCAST</b>\n👤 ${userTag(userId, null, null)}\n📎 ${contentType}\n📊 Total: ${total} | Users: ${targetUsers} | Groups: ${targetGroups}\n✅ Sent: ${sent} (U:${sentUsers} G:${sentGroups})\n🚫 Blocked: ${blocked} (U:${blockedUsers} G:${blockedGroups})\n❌ Failed: ${failed} (U:${failedUsers} G:${failedGroups})${shouldPin ? `\n📌 Pinned: ${pinned}` : ''}\n${modeLabel}\n💬 <i>${(bcRaw || replyMsg?.text || replyMsg?.caption || 'media').substring(0, 100)}</i>`);
    return true;
  }

  // ─── /nsfw on/off command ────
  if (text === '/nsfw on' || text === '/nsfw off' || text === '/nsfw') {
    if (text === '/nsfw') {
      const isOn = await isNsfwFilterOn(sb, chatId);
      await tg('sendMessage', {
        chat_id: chatId,
        text: `🔞 <b>NSFW Filter</b>\n\n<b>Status:</b> ${isOn ? '✅ ON' : '❌ OFF'}\n\n<b>Usage:</b>\n<code>/nsfw on</code> — Enable filter\n<code>/nsfw off</code> — Disable filter\n\n<i>When ON, bot auto-deletes 18+ stickers, photos, videos & GIFs in this chat.</i>`,
        parse_mode: 'HTML',
        reply_to_message_id: messageId,
      }, lk, tk);
      return true;
    }
    const turnOn = text === '/nsfw on';
    await sb.from('chat_settings').upsert(
      { chat_id: chatId, nsfw_filter: turnOn, updated_at: new Date().toISOString() },
      { onConflict: 'chat_id' }
    );
    nsfwCache.set(chatId, { on: turnOn, ts: Date.now() });
    await tg('sendMessage', {
      chat_id: chatId,
      text: turnOn
        ? `🔞 <b>NSFW Filter: ✅ ON</b>\n\n<b>18+ stickers, photos, videos & GIFs will be auto-deleted!</b>`
        : `🔞 <b>NSFW Filter: ❌ OFF</b>\n\n<b>NSFW filter disabled for this chat.</b>`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    sendLog(lk, tk, `🔞 <b>NSFW ${turnOn ? 'ON' : 'OFF'}</b>\n👤 ${userTag(userId, null, null)}\n📍 Chat: <code>${chatId}</code>`);
    return true;
  }

  // ─── /addpack command (add sticker pack to NSFW blacklist) ────
  if (text.startsWith('/addpack ') || text === '/addpack') {
    if (text === '/addpack') {
      await tg('sendMessage', { chat_id: chatId, text: `🔞 <b>Add Sticker Pack to Blacklist</b>\n\n<b>Usage:</b>\n<code>/addpack https://t.me/addstickers/packname</code>\n<code>/addpack packname</code>\n\n<i>Us pack ke sare stickers auto-delete honge!</i>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    const packInput = text.replace(/^\/addpack\s+/i, '').trim();
    const setName = extractStickerSetName(packInput);
    if (!setName) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Invalid pack link/name.</b>\n<b>Use:</b> <code>/addpack https://t.me/addstickers/packname</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    const added = await addUserBlacklistPack(sb, chatId, setName);
    if (added) {
      await tg('sendMessage', { chat_id: chatId, text: `✅ <b>Pack blacklisted:</b> <code>${escapeHtml(setName)}</code>\n\n<i>Is pack ke sare stickers ab auto-delete honge!</i>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      sendLog(lk, tk, `🔞 <b>PACK BLACKLISTED</b>\n📦 <code>${escapeHtml(setName)}</code>\n👤 ${userTag(userId, null, null)}\n📍 Chat: <code>${chatId}</code>`);
    } else {
      await tg('sendMessage', { chat_id: chatId, text: `⚠️ <b>Pack already blacklisted:</b> <code>${escapeHtml(setName)}</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
    return true;
  }

  // ─── /removepack command ────
  if (text.startsWith('/removepack ') || text.startsWith('/rmpack ')) {
    const packInput = text.replace(/^\/(remove|rm)pack\s+/i, '').trim();
    const setName = extractStickerSetName(packInput);
    if (!setName) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Invalid pack link/name.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    const removed = await removeUserBlacklistPack(sb, chatId, setName);
    if (removed) {
      await tg('sendMessage', { chat_id: chatId, text: `✅ <b>Pack removed from blacklist:</b> <code>${escapeHtml(setName)}</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      sendLog(lk, tk, `✅ <b>PACK UNBLACKLISTED</b>\n📦 <code>${escapeHtml(setName)}</code>\n👤 ${userTag(userId, null, null)}\n📍 Chat: <code>${chatId}</code>`);
    } else {
      await tg('sendMessage', { chat_id: chatId, text: `⚠️ <b>Pack was not in blacklist:</b> <code>${escapeHtml(setName)}</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
    return true;
  }

  // ─── /packlist command ────
  if (text === '/packlist' || text === '/packs') {
    const packs = await getUserBlacklistedPacks(sb, chatId);
    if (packs.size === 0) {
      await tg('sendMessage', { chat_id: chatId, text: `📦 <b>No sticker packs blacklisted.</b>\n\n<b>Use:</b> <code>/addpack packname</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    } else {
      let msg = `📦 <b>Blacklisted Sticker Packs (${packs.size})</b>\n\n`;
      let i = 1;
      for (const p of packs) {
        msg += `<b>${i}.</b> <code>${escapeHtml(p)}</code> (<a href="https://t.me/addstickers/${p}">view</a>)\n`;
        i++;
      }
      msg += `\n<b>Remove:</b> <code>/removepack packname</code>`;
      await tg('sendMessage', { chat_id: chatId, text: msg, parse_mode: 'HTML', reply_to_message_id: messageId, disable_web_page_preview: true }, lk, tk);
    }
    return true;
  }

  // ─── /pin command (reply to a message to pin it) ────
  if (text === '/pin' || text === '/unpin') {
    if (!replyToMessage) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Reply to a message to ${text === '/pin' ? 'pin' : 'unpin'} it.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    const targetMsgId = replyToMessage.message_id;
    if (text === '/pin') {
      const res = await tg('pinChatMessage', { chat_id: chatId, message_id: targetMsgId, disable_notification: false }, lk, tk);
      if (res?.ok) {
        await tg('sendMessage', { chat_id: chatId, text: `📌 <b>Message pinned!</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
        sendLog(lk, tk, `📌 <b>PIN</b>\n👤 ${userTag(userId, null, null)}\n📍 Chat: <code>${chatId}</code>`);
      } else {
        await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed to pin. Bot needs admin rights.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      }
    } else {
      const res = await tg('unpinChatMessage', { chat_id: chatId, message_id: targetMsgId }, lk, tk);
      if (res?.ok) {
        await tg('sendMessage', { chat_id: chatId, text: `📌 <b>Message unpinned!</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
        sendLog(lk, tk, `📌 <b>UNPIN</b>\n👤 ${userTag(userId, null, null)}\n📍 Chat: <code>${chatId}</code>`);
      } else {
        await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed to unpin.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      }
    }
    return true;
  }

  // ─── Ban ────
  if (text.startsWith('/ban ')) {
    const banId = parseInt(text.split(' ')[1]);
    if (!isNaN(banId)) {
      await sb.from('bot_users').update({ is_banned: true }).eq('user_id', banId);
      await tg('sendMessage', { chat_id: chatId, text: `<b>🚫 User ${banId} banned.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      sendLog(lk, tk, `🚫 <b>BAN</b>\n👤 By: ${userTag(userId, null, null)}\n🎯 Target: <code>${banId}</code>`);
    }
    return true;
  }

  // ─── Unban ────
  if (text.startsWith('/unban ')) {
    const unbanId = parseInt(text.split(' ')[1]);
    if (!isNaN(unbanId)) {
      await sb.from('bot_users').update({ is_banned: false }).eq('user_id', unbanId);
      await tg('sendMessage', { chat_id: chatId, text: `<b>✅ User ${unbanId} unbanned.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      sendLog(lk, tk, `✅ <b>UNBAN</b>\n👤 By: ${userTag(userId, null, null)}\n🎯 Target: <code>${unbanId}</code>`);
    }
    return true;
  }

  // ─── Mute (in group chats via Telegram API) ────
  if (text.startsWith('/mute ')) {
    const parts = text.split(/\s+/);
    const muteTarget = parts[1];
    let muteId: number | null = null;

    if (muteTarget?.startsWith('@')) {
      const { data: u } = await sb.from('bot_users').select('user_id').eq('username', muteTarget.substring(1).toLowerCase()).maybeSingle();
      muteId = u?.user_id || null;
    } else {
      muteId = parseInt(muteTarget) || null;
    }

    if (!muteId) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Usage:</b> <code>/mute @username</code> or <code>/mute user_id</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }

    // Mute = restrict in current group chat
    if (isGroupChat(String(chatId))) {
      const res = await tg('restrictChatMember', {
        chat_id: chatId,
        user_id: muteId,
        permissions: { can_send_messages: false, can_send_media_messages: false, can_send_other_messages: false },
        until_date: Math.floor(Date.now() / 1000) + 86400, // 24h default
      }, lk, tk);
      if (res?.ok) {
        await tg('sendMessage', { chat_id: chatId, text: `🔇 <b>User ${muteTarget} muted for 24h.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
        sendLog(lk, tk, `🔇 <b>MUTE</b>\n👤 By: ${userTag(userId, null, null)}\n🎯 Target: ${muteTarget} (<code>${muteId}</code>)\n📍 Chat: <code>${chatId}</code>`);
      } else {
        await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed to mute. Bot needs admin rights.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      }
    } else {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Mute only works in group chats.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
    return true;
  }

  // ─── Unmute ────
  if (text.startsWith('/unmute ')) {
    const parts = text.split(/\s+/);
    const unmuteTarget = parts[1];
    let unmuteId: number | null = null;

    if (unmuteTarget?.startsWith('@')) {
      const { data: u } = await sb.from('bot_users').select('user_id').eq('username', unmuteTarget.substring(1).toLowerCase()).maybeSingle();
      unmuteId = u?.user_id || null;
    } else {
      unmuteId = parseInt(unmuteTarget) || null;
    }

    if (!unmuteId) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Usage:</b> <code>/unmute @username</code> or <code>/unmute user_id</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }

    if (isGroupChat(String(chatId))) {
      const res = await tg('restrictChatMember', {
        chat_id: chatId,
        user_id: unmuteId,
        permissions: { can_send_messages: true, can_send_media_messages: true, can_send_other_messages: true, can_add_web_page_previews: true },
      }, lk, tk);
      if (res?.ok) {
        await tg('sendMessage', { chat_id: chatId, text: `🔊 <b>User ${unmuteTarget} unmuted.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
        sendLog(lk, tk, `🔊 <b>UNMUTE</b>\n👤 By: ${userTag(userId, null, null)}\n🎯 Target: ${unmuteTarget} (<code>${unmuteId}</code>)\n📍 Chat: <code>${chatId}</code>`);
      } else {
        await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed to unmute. Bot needs admin rights.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      }
    } else {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Unmute only works in group chats.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
    return true;
  }

  // ─── Sudo management (Owner only) ────
  if (text.startsWith('/sudo ') && isOwner) {
    const parts = text.split(/\s+/);
    const action = parts[1]; // add / remove / list
    
    if (action === 'list') {
      const sudos = await getSudoUsers(sb);
      if (sudos.length === 0) {
        await tg('sendMessage', { chat_id: chatId, text: `👑 <b>No sudo users added yet.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      } else {
        let msg = `👑 <b>Sudo Users:</b>\n\n`;
        for (const s of sudos) {
          const { data: u } = await sb.from('bot_users').select('first_name, username').eq('user_id', s.user_id).maybeSingle();
          msg += `• ${userTag(s.user_id, u?.first_name || null, u?.username || null)}\n`;
        }
        await tg('sendMessage', { chat_id: chatId, text: msg, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      }
      return true;
    }

    if (action === 'add' && parts[2]) {
      let sudoId: number | null = null;
      if (parts[2].startsWith('@')) {
        const { data: u } = await sb.from('bot_users').select('user_id').eq('username', parts[2].substring(1).toLowerCase()).maybeSingle();
        sudoId = u?.user_id || null;
      } else {
        sudoId = parseInt(parts[2]) || null;
      }
      if (!sudoId) {
        await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>User not found.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
        return true;
      }
      await sb.from('sudo_users').upsert({ user_id: sudoId, added_by: userId }, { onConflict: 'user_id' });
      await tg('sendMessage', { chat_id: chatId, text: `👑 <b>User <code>${sudoId}</code> added as sudo.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      sendLog(lk, tk, `👑 <b>SUDO ADD</b>\n👤 By: ${userTag(userId, null, null)}\n🎯 Target: <code>${sudoId}</code>`);
      return true;
    }

    if (action === 'remove' && parts[2]) {
      let sudoId: number | null = null;
      if (parts[2].startsWith('@')) {
        const { data: u } = await sb.from('bot_users').select('user_id').eq('username', parts[2].substring(1).toLowerCase()).maybeSingle();
        sudoId = u?.user_id || null;
      } else {
        sudoId = parseInt(parts[2]) || null;
      }
      if (!sudoId) {
        await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>User not found.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
        return true;
      }
      await sb.from('sudo_users').delete().eq('user_id', sudoId);
      await tg('sendMessage', { chat_id: chatId, text: `✅ <b>User <code>${sudoId}</code> removed from sudo.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      sendLog(lk, tk, `❌ <b>SUDO REMOVE</b>\n👤 By: ${userTag(userId, null, null)}\n🎯 Target: <code>${sudoId}</code>`);
      return true;
    }

    await tg('sendMessage', {
      chat_id: chatId,
      text: `👑 <b>Sudo Commands:</b>\n\n<code>/sudo add @username</code> — Add sudo\n<code>/sudo add 123456789</code> — Add sudo\n<code>/sudo remove @username</code> — Remove sudo\n<code>/sudo list</code> — List all sudos`,
      parse_mode: 'HTML',
      reply_to_message_id: messageId,
    }, lk, tk);
    return true;
  }

  // ─── /kick command ────
  if (text === '/kick' || text.startsWith('/kick ')) {
    if (chatId > 0) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Only works in groups.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    let kickId: number | null = null;
    const parts = text.split(/\s+/);
    if (parts[1]?.startsWith('@')) {
      const { data: u } = await sb.from('bot_users').select('user_id').eq('username', parts[1].substring(1).toLowerCase()).maybeSingle();
      kickId = u?.user_id || null;
    } else if (parts[1] && /^\d+$/.test(parts[1])) {
      kickId = parseInt(parts[1]);
    } else if (replyToMessage?.from) {
      kickId = replyToMessage.from.id;
    }
    if (!kickId) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Usage:</b> <code>/kick @username</code> ya reply karke <code>/kick</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    const kickRes = await tg('banChatMember', { chat_id: chatId, user_id: kickId, until_date: Math.floor(Date.now() / 1000) + 60 }, lk, tk);
    if (kickRes?.ok) {
      await tg('unbanChatMember', { chat_id: chatId, user_id: kickId, only_if_banned: true }, lk, tk);
      await tg('sendMessage', { chat_id: chatId, text: `👢 <b>User kicked!</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      sendLog(lk, tk, `👢 <b>KICK</b>\n👤 By: ${userTag(userId, null, null)}\n🎯 Target: <code>${kickId}</code>\n📍 Chat: <code>${chatId}</code>`);
    } else {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed to kick. Bot needs admin rights.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
    return true;
  }

  // ─── /promote command ────
  if (text === '/promote' || text.startsWith('/promote ')) {
    if (chatId > 0) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Only works in groups.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    let promoteId: number | null = null;
    const parts = text.split(/\s+/);
    if (parts[1]?.startsWith('@')) {
      const { data: u } = await sb.from('bot_users').select('user_id').eq('username', parts[1].substring(1).toLowerCase()).maybeSingle();
      promoteId = u?.user_id || null;
    } else if (parts[1] && /^\d+$/.test(parts[1])) {
      promoteId = parseInt(parts[1]);
    } else if (replyToMessage?.from) {
      promoteId = replyToMessage.from.id;
    }
    if (!promoteId) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Usage:</b> <code>/promote @username</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    const promRes = await tg('promoteChatMember', { chat_id: chatId, user_id: promoteId, can_delete_messages: true, can_restrict_members: true, can_pin_messages: true, can_invite_users: true, can_manage_chat: true }, lk, tk);
    if (promRes?.ok) {
      await tg('sendMessage', { chat_id: chatId, text: `⬆️ <b>User promoted to admin!</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      sendLog(lk, tk, `⬆️ <b>PROMOTE</b>\n👤 By: ${userTag(userId, null, null)}\n🎯 Target: <code>${promoteId}</code>\n📍 Chat: <code>${chatId}</code>`);
    } else {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed to promote. Bot needs admin rights.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
    return true;
  }

  // ─── /demote command ────
  if (text === '/demote' || text.startsWith('/demote ')) {
    if (chatId > 0) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Only works in groups.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    let demoteId: number | null = null;
    const parts = text.split(/\s+/);
    if (parts[1]?.startsWith('@')) {
      const { data: u } = await sb.from('bot_users').select('user_id').eq('username', parts[1].substring(1).toLowerCase()).maybeSingle();
      demoteId = u?.user_id || null;
    } else if (parts[1] && /^\d+$/.test(parts[1])) {
      demoteId = parseInt(parts[1]);
    } else if (replyToMessage?.from) {
      demoteId = replyToMessage.from.id;
    }
    if (!demoteId) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Usage:</b> <code>/demote @username</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    const demRes = await tg('promoteChatMember', { chat_id: chatId, user_id: demoteId, can_delete_messages: false, can_restrict_members: false, can_pin_messages: false, can_invite_users: false, can_manage_chat: false, can_change_info: false, can_post_messages: false, can_edit_messages: false }, lk, tk);
    if (demRes?.ok) {
      await tg('sendMessage', { chat_id: chatId, text: `⬇️ <b>User demoted!</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      sendLog(lk, tk, `⬇️ <b>DEMOTE</b>\n👤 By: ${userTag(userId, null, null)}\n🎯 Target: <code>${demoteId}</code>\n📍 Chat: <code>${chatId}</code>`);
    } else {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed to demote.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
    return true;
  }

  // ─── /adminlist command ────
  if (text === '/adminlist' || text === '/admins') {
    if (chatId > 0) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Only works in groups.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    const adminsRes = await tg('getChatAdministrators', { chat_id: chatId }, lk, tk);
    if (adminsRes?.ok && Array.isArray(adminsRes.result)) {
      let msg = `👥 <b>Group Admins (${adminsRes.result.length})</b>\n\n`;
      const creator = adminsRes.result.find((a: any) => a.status === 'creator');
      if (creator) {
        msg += `👑 <b>Creator:</b> ${userTag(creator.user.id, creator.user.first_name, creator.user.username)}\n\n`;
      }
      const admins = adminsRes.result.filter((a: any) => a.status === 'administrator');
      for (const a of admins) {
        const title = a.custom_title ? ` | ${a.custom_title}` : '';
        msg += `• ${userTag(a.user.id, a.user.first_name, a.user.username)}${title}${a.user.is_bot ? ' 🤖' : ''}\n`;
      }
      await tg('sendMessage', { chat_id: chatId, text: msg, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    } else {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed to get admin list.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
    return true;
  }

  // ─── /purge command (delete messages from reply to current) ────
  if (text === '/purge') {
    if (chatId > 0) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Only works in groups.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    if (!replyToMessage) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Reply to a message to purge from that point.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    const startMsgId = replyToMessage.message_id;
    const endMsgId = messageId;
    let deleted = 0;
    const deletePromises: Promise<any>[] = [];
    for (let id = startMsgId; id <= endMsgId; id++) {
      deletePromises.push(deleteTelegramMessage(chatId, id, lk, tk, 0).then(r => { if (r.ok) deleted++; }));
      if (deletePromises.length >= 20) {
        await Promise.all(deletePromises);
        deletePromises.length = 0;
      }
    }
    if (deletePromises.length > 0) await Promise.all(deletePromises);
    const statusMsg = await tg('sendMessage', { chat_id: chatId, text: `🗑 <b>Purged ${deleted} messages!</b>`, parse_mode: 'HTML' }, lk, tk);
    if (statusMsg?.ok) {
      setTimeout(() => deleteTelegramMessage(chatId, statusMsg.result.message_id, lk, tk, 0), 3000);
    }
    sendLog(lk, tk, `🗑 <b>PURGE</b>\n👤 ${userTag(userId, null, null)}\n📍 Chat: <code>${chatId}</code>\n📊 Deleted: ${deleted}`);
    return true;
  }

  // ─── /del command (delete replied message) ────
  if (text === '/del' || text === '/delete') {
    if (!replyToMessage) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Reply to a message to delete it.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    await deleteTelegramMessage(chatId, replyToMessage.message_id, lk, tk);
    await deleteTelegramMessage(chatId, messageId, lk, tk, 0);
    return true;
  }

  // ─── /report command ────
  if (text === '/report') {
    if (chatId > 0) {
      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Only works in groups.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      return true;
    }
    const adminsRes = await tg('getChatAdministrators', { chat_id: chatId }, lk, tk);
    if (adminsRes?.ok && Array.isArray(adminsRes.result)) {
      const adminMentions = adminsRes.result
        .filter((a: any) => !a.user.is_bot)
        .map((a: any) => `<a href="tg://user?id=${a.user.id}">${escapeHtml(a.user.first_name || 'Admin')}</a>`)
        .join(', ');
      const reportedUser = replyToMessage?.from ? userTag(replyToMessage.from.id, replyToMessage.from.first_name, replyToMessage.from.username) : '';
      await tg('sendMessage', {
        chat_id: chatId,
        text: `🚨 <b>Reported to admins!</b>\n\n${reportedUser ? `👤 <b>Reported:</b> ${reportedUser}\n` : ''}👮 ${adminMentions}`,
        parse_mode: 'HTML',
        reply_to_message_id: replyToMessage?.message_id || messageId,
      }, lk, tk);
      sendLog(lk, tk, `🚨 <b>REPORT</b>\n👤 By: ${userTag(userId, null, null)}\n📍 Chat: <code>${chatId}</code>`);
    }
    return true;
  }

    if (text.startsWith('/addroast ')) {
    const lineText = text.substring(10).trim();
    if (lineText) {
      await sb.from('gali_lines').insert({ mode: 'roast', line_text: lineText });
      linesCache.delete('roast');
      const { count } = await sb.from('gali_lines').select('id', { count: 'exact', head: true }).eq('mode', 'roast');
      await tg('sendMessage', { chat_id: chatId, text: `<b>🔥 Light Roast added. Total: ${count}</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
    return true;
  }

  if (text.startsWith('/addfunny ')) {
    const lineText = text.substring(10).trim();
    if (lineText) {
      await sb.from('gali_lines').insert({ mode: 'funny', line_text: lineText });
      linesCache.delete('funny');
      const { count } = await sb.from('gali_lines').select('id', { count: 'exact', head: true }).eq('mode', 'funny');
      await tg('sendMessage', { chat_id: chatId, text: `<b>😂 Heavy Roast added. Total: ${count}</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
    }
    return true;
  }

  if (text.startsWith('/removeroast ')) {
    const idx = parseInt(text.split(' ')[1]);
    if (!isNaN(idx)) {
      const { data: lines } = await sb.from('gali_lines').select('id').eq('mode', 'roast').order('id', { ascending: true });
      if (lines && idx >= 1 && idx <= lines.length) {
        await sb.from('gali_lines').delete().eq('id', lines[idx - 1].id);
        linesCache.delete('roast');
        await tg('sendMessage', { chat_id: chatId, text: `<b>✅ Roast line ${idx} deleted. Total: ${lines.length - 1}</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      } else {
        await tg('sendMessage', { chat_id: chatId, text: `<b>${ce(E.stop, '❌')} Invalid line number.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      }
    }
    return true;
  }

  if (text.startsWith('/removefunny ')) {
    const idx = parseInt(text.split(' ')[1]);
    if (!isNaN(idx)) {
      const { data: lines } = await sb.from('gali_lines').select('id').eq('mode', 'funny').order('id', { ascending: true });
      if (lines && idx >= 1 && idx <= lines.length) {
        await sb.from('gali_lines').delete().eq('id', lines[idx - 1].id);
        linesCache.delete('funny');
        await tg('sendMessage', { chat_id: chatId, text: `<b>✅ Funny line ${idx} deleted. Total: ${lines.length - 1}</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      } else {
        await tg('sendMessage', { chat_id: chatId, text: `<b>${ce(E.stop, '❌')} Invalid line number.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, lk, tk);
      }
    }
    return true;
  }

  return false;
}

// ─── Main Server ────────────────────────────────────────────────────
Deno.serve(async () => {
  const startTime = Date.now();

  const LOVABLE_API_KEY = Deno.env.get('LOVABLE_API_KEY');
  if (!LOVABLE_API_KEY) return new Response(JSON.stringify({ error: 'LOVABLE_API_KEY not configured' }), { status: 500 });

  const TELEGRAM_API_KEY = Deno.env.get('TELEGRAM_API_KEY');
  if (!TELEGRAM_API_KEY) return new Response(JSON.stringify({ error: 'TELEGRAM_API_KEY not configured' }), { status: 500 });

  const SUPABASE_URL = Deno.env.get('SUPABASE_URL');
  const SUPABASE_ANON_KEY = Deno.env.get('SUPABASE_ANON_KEY');
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY || !SUPABASE_SERVICE_ROLE_KEY) {
    return new Response(JSON.stringify({ error: 'Supabase env missing' }), { status: 500 });
  }

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  const workerUrl = `${SUPABASE_URL}/functions/v1/telegram-send-worker`;

  const LK = LOVABLE_API_KEY;
  const TK = TELEGRAM_API_KEY;

  let totalProcessed = 0;

  const { data: state, error: stateErr } = await sb
    .from('telegram_bot_state').select('update_offset, updated_at').eq('id', 1).single();

  if (stateErr) return new Response(JSON.stringify({ error: stateErr.message }), { status: 500 });

  if (isFreshTelegramPollLease(state.updated_at)) {
    return new Response(JSON.stringify({ ok: true, skipped: 'poller_already_running' }));
  }

  let leaseToken = await acquireTelegramPollLease(sb, state.updated_at);
  if (!leaseToken) {
    return new Response(JSON.stringify({ ok: true, skipped: 'lease_conflict' }));
  }

  let currentOffset = state.update_offset;
  let hasActiveGroupJobs = await recoverStalledGroupJobs(sb, workerUrl, SUPABASE_ANON_KEY);

  while (true) {
    const elapsed = Date.now() - startTime;
    const remainingMs = MAX_RUNTIME_MS - elapsed;
    if (remainingMs < MIN_REMAINING_MS) break;

    // Long poll: let Telegram hold the connection until a message arrives (up to 50s)
    const timeout = Math.min(50, Math.max(1, Math.floor((remainingMs - 5000) / 1000)));
    if (timeout < 1) break;

    let updates: any[] = [];

    try {
      const response = await fetch(`${GATEWAY_URL}/getUpdates`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${LK}`,
          'X-Connection-Api-Key': TK,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ offset: currentOffset, timeout, allowed_updates: ['message', 'callback_query', 'inline_query', 'my_chat_member'] }),
      });

      const data = await response.json();
      if (!response.ok) { console.error('getUpdates failed:', data); await sleep(800); }
      else { updates = data.result ?? []; }
    } catch (error) {
      console.error('getUpdates request failed:', error);
      await sleep(800);
    }

    if (updates.length === 0) {
      const nextLeaseToken = await refreshTelegramPollLease(sb, leaseToken);
      if (!nextLeaseToken) break;
      leaseToken = nextLeaseToken;
      hasActiveGroupJobs = await recoverStalledGroupJobs(sb, workerUrl, SUPABASE_ANON_KEY);
      continue;
    }

    for (const update of updates) {
      try {
        if (update.message) {
          const msg = update.message;
          const chatId = msg.chat.id;
          const userId = msg.from?.id;
          const firstName = msg.from?.first_name || null;
          const username = msg.from?.username || null;
          const rawText = (msg.text || '').trim();
          const text = cleanCmd(rawText);
          const messageId = msg.message_id;
          const chatType = msg.chat.type;
          const groupMessage = isGroupChat(chatType);
          if (groupMessage) {
            await addOrUpdateChatTarget(sb, msg.chat);

            // ─── NSFW Filter Check (groups only) ────
            // Check stickers, animations, videos for porn — skip photos, screenshots, documents
            const hasNsfwMedia = !!(msg.sticker || msg.video || msg.animation);
            const isFromBot = msg.from?.is_bot === true;
            console.log('NSFW_GATE', { chatId, hasNsfwMedia, isFromBot });
            if (hasNsfwMedia && !isFromBot && await isNsfwFilterOn(sb, chatId)) {
              console.log('NSFW_FILTER_ON', { chatId, messageId, type: msg.sticker ? 'sticker' : msg.video ? 'video' : 'animation' });
              // Check user-managed sticker pack blacklist first
              if (msg.sticker?.set_name) {
                const userBlacklist = await getUserBlacklistedPacks(sb, chatId);
                if (userBlacklist.has(msg.sticker.set_name.toLowerCase())) {
                  const del = await deleteTelegramMessage(chatId, messageId, LK, TK);
                  sendLog(LK, TK, `🔞 <b>NSFW ${del.ok ? 'DELETED' : 'FAILED'} (user-blacklist)</b>\n👤 ${userTag(userId, firstName, username)}\n📍 ${chatTag(msg.chat)}\n📎 pack:${msg.sticker.set_name}${del.ok ? '' : `\n⚠️ <code>${escapeHtml(del.description).slice(0, 180)}</code>`}`);
                  totalProcessed++;
                  continue;
                }
              }

              const knownStickerSetReason = msg.sticker?.set_name
                ? await getKnownNsfwStickerSet(sb, msg.sticker.set_name)
                : null;
              if (knownStickerSetReason) {
                const del = await deleteTelegramMessage(chatId, messageId, LK, TK);
                sendLog(LK, TK, `🔞 <b>NSFW ${del.ok ? 'DELETED' : 'FAILED'} (known-pack)</b>\n👤 ${userTag(userId, firstName, username)}\n📍 ${chatTag(msg.chat)}\n📎 ${knownStickerSetReason}${del.ok ? '' : `\n⚠️ <code>${escapeHtml(del.description).slice(0, 180)}</code>`}`);
                totalProcessed++;
                continue;
              }

              const knownMediaReason = await getKnownNsfwMedia(sb, msg);
              if (knownMediaReason) {
                const del = await deleteTelegramMessage(chatId, messageId, LK, TK);
                sendLog(LK, TK, `🔞 <b>NSFW ${del.ok ? 'DELETED' : 'FAILED'} (known-media)</b>\n👤 ${userTag(userId, firstName, username)}\n📍 ${chatTag(msg.chat)}\n📎 ${knownMediaReason}${del.ok ? '' : `\n⚠️ <code>${escapeHtml(del.description).slice(0, 180)}</code>`}`);
                totalProcessed++;
                continue;
              }

              const burstInfo = noteNsfwBurst(msg);
              const hotReason = getNsfwHotReason(msg);
              if (hotReason) {
                const del = await deleteTelegramMessage(chatId, messageId, LK, TK);
                if (!del.ok) {
                  console.error('NSFW hot-cache delete failed:', {
                    chatId,
                    messageId,
                    userId,
                    reason: hotReason,
                    telegram: del.data,
                  });
                }
                sendLog(LK, TK, `🔞 <b>NSFW ${del.ok ? 'DELETED' : 'FAILED'} (hot)</b>\n👤 ${userTag(userId, firstName, username)}\n📍 ${chatTag(msg.chat)}\n📎 ${hotReason}${del.ok ? '' : `\n⚠️ <code>${escapeHtml(del.description).slice(0, 180)}</code>`}`);
                totalProcessed++;
                continue;
              }

              // Step 1: Fast keyword/sticker-set check — instant delete
              const fastCheck = quickNsfwCheck(msg);
              if (fastCheck.flagged) {
                EdgeRuntime.waitUntil(Promise.all([
                  rememberKnownNsfwMedia(sb, msg, fastCheck.reason),
                  msg.sticker?.set_name ? rememberNsfwStickerSet(sb, msg.sticker.set_name, fastCheck.reason) : Promise.resolve(),
                ]));
                markNsfwHot(msg, burstInfo.burst ? `bulk:${fastCheck.reason}` : fastCheck.reason);
                const del = await deleteTelegramMessage(chatId, messageId, LK, TK);
                if (!del.ok) {
                  console.error('NSFW fast delete failed:', {
                    chatId,
                    messageId,
                    userId,
                    reason: fastCheck.reason,
                    telegram: del.data,
                  });
                }
                sendLog(LK, TK, `🔞 <b>NSFW ${del.ok ? 'DELETED' : 'FAILED'} (fast)</b>\n👤 ${userTag(userId, firstName, username)}\n📍 ${chatTag(msg.chat)}\n📎 ${fastCheck.reason}${del.ok ? '' : `\n⚠️ <code>${escapeHtml(del.description).slice(0, 180)}</code>`}`);
                totalProcessed++;
                continue;
              }

              if (requiresImmediateNsfwAiCheck(msg)) {
                const verdict = await shouldDeleteForNsfw(msg, LK, TK);
                console.log('NSFW_AI_VERDICT', { chatId, messageId, flagged: verdict.flagged, reason: verdict.reason });
                if (verdict.flagged) {
                  EdgeRuntime.waitUntil(Promise.all([
                    rememberKnownNsfwMedia(sb, msg, verdict.reason),
                    msg.sticker?.set_name ? rememberNsfwStickerSet(sb, msg.sticker.set_name, verdict.reason) : Promise.resolve(),
                  ]));
                  markNsfwHot(msg, burstInfo.burst ? `bulk:${verdict.reason}` : verdict.reason);
                  const del = await deleteTelegramMessage(chatId, messageId, LK, TK);
                  if (!del.ok) {
                    console.error('NSFW sync delete failed:', {
                      chatId,
                      messageId,
                      userId,
                      reason: verdict.reason,
                      telegram: del.data,
                    });
                  }
                  sendLog(LK, TK, `🔞 <b>NSFW ${del.ok ? 'DELETED' : 'FAILED'} (sync-ai)</b>\n👤 ${userTag(userId, firstName, username)}\n📍 ${chatTag(msg.chat)}\n📎 ${verdict.reason}${del.ok ? '' : `\n⚠️ <code>${escapeHtml(del.description).slice(0, 180)}</code>`}`);
                  totalProcessed++;
                  continue;
                }

                if (/download_failed|ai_request_failed|ai_parse_failed/i.test(verdict.reason)) {
                  console.error('NSFW sync check inconclusive:', {
                    chatId,
                    messageId,
                    userId,
                    reason: verdict.reason,
                  });
                }

                continue;
              }

              // Step 2: AI vision check — fire-and-forget background task
              const _chatId = chatId;
              const _messageId = messageId;
              const _userId = userId;
              const _firstName = firstName;
              const _username = username;
              const _chat = msg.chat;
              const _msg = msg;
              EdgeRuntime.waitUntil(
                (async () => {
                  try {
                    const verdict = await shouldDeleteForNsfw(_msg, LK, TK);
                    if (verdict.flagged) {
                      EdgeRuntime.waitUntil(Promise.all([
                        rememberKnownNsfwMedia(sb, _msg, verdict.reason),
                        _msg.sticker?.set_name ? rememberNsfwStickerSet(sb, _msg.sticker.set_name, verdict.reason) : Promise.resolve(),
                      ]));
                      markNsfwHot(_msg, verdict.reason);
                      const del = await deleteTelegramMessage(_chatId, _messageId, LK, TK);
                      if (!del.ok) {
                        console.error('NSFW AI delete failed:', {
                          chatId: _chatId,
                          messageId: _messageId,
                          userId: _userId,
                          reason: verdict.reason,
                          telegram: del.data,
                        });
                      }
                      sendLog(LK, TK, `🔞 <b>NSFW ${del.ok ? 'DELETED' : 'FAILED'} (AI)</b>\n👤 ${userTag(_userId, _firstName, _username)}\n📍 ${chatTag(_chat)}\n📎 ${verdict.reason}${del.ok ? '' : `\n⚠️ <code>${escapeHtml(del.description).slice(0, 180)}</code>`}`);
                    }
                  } catch (err) {
                    console.error('NSFW background check error:', err);
                  }
                })()
              );
            }
          }

          // ─── Keyword Filter Check (groups only, before commands) ────
          if (groupMessage && !msg.from?.is_bot) {
            const filtered = await checkFilters(sb, chatId, msg, LK, TK);
            if (filtered) { totalProcessed++; continue; }
          }

          const hasVoice = !!(msg.voice || msg.audio);
          const replyToBot = msg.reply_to_message?.from?.id ? true : false;

          // ─── Auto Voice-to-Text (no command needed) ────
          let msgHandled = false;
          if (hasVoice) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleVoiceToText(chatId, msg, messageId, LK, TK);
            }
            msgHandled = true;
          }
          // ─── Reply to Bot → AI chat (skip if user is in a flow state) ─
          if (!msgHandled && replyToBot && rawText && !rawText.startsWith('/')) {
            const flowCheck = await getUserState(sb, userId);
            const flowModes = ['short_wait_alias', 'short_add_site', 'short_add_key', 'short_wait_url', 'short_delete', 'short_force_save'];
            const inFlow = flowCheck?.mode && flowModes.includes(flowCheck.mode);
            if (!inFlow) {
              const botId = await getBotId(LK, TK);
              if (msg.reply_to_message?.from?.id === botId && await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
                await handleAiChat(chatId, `/ai ${rawText}`, messageId, LK, TK, true, userId);
                msgHandled = true;
              }
            }
            // If inFlow, msgHandled stays false — falls through to non-command handler
          }
          // ─── Command Routing ────────────────────────────
          const _u = userTag(userId, firstName, username);
          const _c = groupMessage ? `\n📍 <b>Chat:</b> ${chatTag(msg.chat)}` : `\n📍 <b>Chat:</b> Private`;

          if (!msgHandled && text === '/start') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await setUserState(sb, userId, null, 0, null);
              await sendMainMenu(chatId, LK, TK, { userId, firstName, username });
              sendLog(LK, TK, `🟢 <b>START</b>\n👤 ${_u}${_c}`);
            }
          } else if (text === '/help') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await sendHelpMenu(chatId, LK, TK);
              sendLog(LK, TK, `📋 <b>HELP</b>\n👤 ${_u}${_c}`);
            }
          } else if (text === '/end' || text === '/stop') {
            await setUserState(sb, userId, null, 0, null);
            await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '🛑')} <b>Session ended. Send /start to use the bot again.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
            sendLog(LK, TK, `🛑 <b>STOP</b>\n👤 ${_u}${_c}`);
          } else if (text.startsWith('/stop ')) {
            // /stop keyword — remove a filter (works in groups for sudo/admin)
            const keyword = text.replace(/^\/stop\s+/i, '').trim().toLowerCase();
            if (keyword && groupMessage) {
              const hasSudo = await isSudo(sb, userId);
              if (hasSudo) {
                await sb.from('chat_filters').delete().eq('chat_id', chatId).eq('keyword', keyword);
                invalidateFiltersCache(chatId);
                await tg('sendMessage', { chat_id: chatId, text: `✅ <b>Filter stopped:</b> <code>${escapeHtml(keyword)}</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
                sendLog(LK, TK, `🛑 <b>FILTER STOP</b>\n📍 Chat: <code>${chatId}</code>\n🔑 <code>${escapeHtml(keyword)}</code>\n👤 ${_u}`);
              } else {
                await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Only admins can stop filters.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
              }
            } else if (keyword && !groupMessage) {
              await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Filters only work in groups.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
            }
          } else if (text.startsWith('/confess')) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleConfess(chatId, rawText, messageId, chatType, LK, TK);
              sendLog(LK, TK, `🤫 <b>CONFESS</b>\n👤 ${_u}${_c}`);
            }
          } else if (text.startsWith('/secret')) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleSecret(sb, chatId, rawText, messageId, userId, chatType, LK, TK);
              sendLog(LK, TK, `🔒 <b>SECRET</b>\n👤 ${_u}${_c}`);
            }
          } else if (text === '/truthdare' || text === '/tod') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleTruthDare(chatId, LK, TK);
              sendLog(LK, TK, `🎲 <b>TRUTHDARE</b>\n👤 ${_u}${_c}`);
            }
          } else if (text.startsWith('/ai ') || text === '/ai') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleAiChat(chatId, rawText, messageId, LK, TK, false, userId);
              sendLog(LK, TK, `🤖 <b>AI</b>\n👤 ${_u}${_c}\n💬 <i>${rawText.substring(0, 100)}</i>`);
            }
          } else if (text.startsWith('/short')) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleShortener(chatId, rawText, messageId, LK, TK);
              sendLog(LK, TK, `🔗 <b>SHORTENER</b>\n👤 ${_u}${_c}`);
            }
          } else if (text === '/quote' || text === '/shayari') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleQuote(chatId, LK, TK);
              sendLog(LK, TK, `✨ <b>QUOTE</b>\n👤 ${_u}${_c}`);
            }
          } else if (text === '/meme') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleMeme(chatId, LK, TK);
              sendLog(LK, TK, `😂 <b>MEME</b>\n👤 ${_u}${_c}`);
            }
          } else if (text.startsWith('/imagine')) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleImagine(chatId, rawText, messageId, LK, TK);
              sendLog(LK, TK, `🖼 <b>IMAGINE</b>\n👤 ${_u}${_c}\n💬 <i>${rawText.substring(0, 100)}</i>`);
            }
          } else if (text.startsWith('/tts')) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleTTS(chatId, rawText, messageId, LK, TK);
              sendLog(LK, TK, `🔊 <b>TTS</b>\n👤 ${_u}${_c}`);
            }
          } else if (text.startsWith('/roast')) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleAiRoast(chatId, rawText, messageId, LK, TK);
              sendLog(LK, TK, `🔥 <b>AI ROAST</b>\n👤 ${_u}${_c}\n💬 <i>${rawText.substring(0, 100)}</i>`);
            }
          } else if (text.startsWith('/translate')) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleTranslate(chatId, rawText, messageId, LK, TK);
              sendLog(LK, TK, `🌐 <b>TRANSLATE</b>\n👤 ${_u}${_c}`);
            }
          } else if (text === '/joke') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleJoke(chatId, messageId, LK, TK);
              sendLog(LK, TK, `😂 <b>JOKE</b>\n👤 ${_u}${_c}`);
            }
          } else if (text === '/fact') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleFact(chatId, messageId, LK, TK);
              sendLog(LK, TK, `🧠 <b>FACT</b>\n👤 ${_u}${_c}`);
            }
          } else if (text === '/wyr') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleWYR(chatId, messageId, LK, TK);
              sendLog(LK, TK, `🤔 <b>WYR</b>\n👤 ${_u}${_c}`);
            }
          } else if (text === '/pickup') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handlePickup(chatId, messageId, LK, TK);
              sendLog(LK, TK, `💘 <b>PICKUP</b>\n👤 ${_u}${_c}`);
            }
          } else if (text.startsWith('/b ') || text === '/b') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleBypass(chatId, rawText, messageId, LK, TK);
              sendLog(LK, TK, `🔓 <b>BYPASS</b>\n👤 ${_u}${_c}\n💬 <i>${rawText.substring(0, 100)}</i>`);
            }
          } else if (text === '/report') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              if (chatId < 0) {
                const adminsRes = await tg('getChatAdministrators', { chat_id: chatId }, LK, TK);
                if (adminsRes?.ok && Array.isArray(adminsRes.result)) {
                  const adminMentions = adminsRes.result
                    .filter((a: any) => !a.user.is_bot)
                    .map((a: any) => `<a href="tg://user?id=${a.user.id}">${escapeHtml(a.user.first_name || 'Admin')}</a>`)
                    .join(', ');
                  const replyMsg = msg.reply_to_message;
                  const reportedUser = replyMsg?.from ? userTag(replyMsg.from.id, replyMsg.from.first_name, replyMsg.from.username) : '';
                  await tg('sendMessage', {
                    chat_id: chatId,
                    text: `🚨 <b>Reported to admins!</b>\n\n${reportedUser ? `👤 <b>Reported:</b> ${reportedUser}\n` : ''}👮 ${adminMentions}`,
                    parse_mode: 'HTML',
                    reply_to_message_id: replyMsg?.message_id || messageId,
                  }, LK, TK);
                  sendLog(LK, TK, `🚨 <b>REPORT</b>\n👤 By: ${_u}\n📍 ${_c}`);
                }
              } else {
                await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Report only works in groups.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
              }
            }
          } else if (text === '/adminlist' || text === '/admins') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              if (chatId < 0) {
                const adminsRes = await tg('getChatAdministrators', { chat_id: chatId }, LK, TK);
                if (adminsRes?.ok && Array.isArray(adminsRes.result)) {
                  let adminMsg = `👥 <b>Group Admins (${adminsRes.result.length})</b>\n\n`;
                  const creator = adminsRes.result.find((a: any) => a.status === 'creator');
                  if (creator) {
                    adminMsg += `👑 <b>Creator:</b> ${userTag(creator.user.id, creator.user.first_name, creator.user.username)}\n\n`;
                  }
                  for (const a of adminsRes.result.filter((a: any) => a.status === 'administrator')) {
                    const title = a.custom_title ? ` | ${a.custom_title}` : '';
                    adminMsg += `• ${userTag(a.user.id, a.user.first_name, a.user.username)}${title}${a.user.is_bot ? ' 🤖' : ''}\n`;
                  }
                  await tg('sendMessage', { chat_id: chatId, text: adminMsg, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
                }
              } else {
                await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Only works in groups.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
              }
            }
          } else if (text === '/chatid' || text === '/id' || text.startsWith('/id ')) {
            // Enhanced /id — shows user info, chat info, replied user info
            let idText = `🆔 <b>ID Info</b>\n\n`;

            // Current chat info
            idText += `📍 <b>Chat:</b> <code>${chatId}</code>`;
            if (msg.chat.title) idText += ` (${escapeHtml(msg.chat.title)})`;
            if (msg.chat.username) idText += ` @${msg.chat.username}`;
            idText += `\n🏷 <b>Chat Type:</b> ${chatType}\n`;

            // Sender info
            idText += `\n👤 <b>Your ID:</b> <code>${userId}</code>`;
            if (firstName) idText += ` (${escapeHtml(firstName)})`;
            if (username) idText += ` @${username}`;
            idText += `\n`;

            // Replied user info
            if (msg.reply_to_message?.from) {
              const rUser = msg.reply_to_message.from;
              idText += `\n↩️ <b>Replied User ID:</b> <code>${rUser.id}</code>`;
              if (rUser.first_name) idText += ` (${escapeHtml(rUser.first_name)})`;
              if (rUser.username) idText += ` @${rUser.username}`;
              if (rUser.is_bot) idText += ` 🤖`;
              idText += `\n🔗 <a href="tg://user?id=${rUser.id}">Open Profile</a>\n`;
            }

            // If /id @username or /id userId provided
            const idParts = text.split(/\s+/);
            if (idParts.length > 1) {
              const target = idParts[1];
              let lookupId: string | number = target;
              if (target.startsWith('@')) {
                lookupId = target;
              } else if (/^-?\d+$/.test(target)) {
                lookupId = parseInt(target);
              }
              const chatRes = await tg('getChat', { chat_id: lookupId }, LK, TK);
              if (chatRes?.ok) {
                const tc = chatRes.result;
                idText += `\n🔎 <b>Lookup:</b> <code>${tc.id}</code>`;
                if (tc.first_name) idText += ` ${escapeHtml(tc.first_name)}`;
                if (tc.last_name) idText += ` ${escapeHtml(tc.last_name)}`;
                if (tc.title) idText += ` ${escapeHtml(tc.title)}`;
                if (tc.username) idText += ` @${tc.username}`;
                if (tc.type) idText += `\n🏷 Type: ${tc.type}`;
                if (tc.bio) idText += `\n📝 Bio: <i>${escapeHtml(tc.bio)}</i>`;
                idText += `\n`;
              } else {
                idText += `\n🔎 <b>Lookup failed for:</b> <code>${escapeHtml(target)}</code>\n`;
              }
            }

            // Forwarded message info
            if (msg.reply_to_message?.forward_from) {
              const fwd = msg.reply_to_message.forward_from;
              idText += `\n📤 <b>Forwarded From:</b> <code>${fwd.id}</code>`;
              if (fwd.first_name) idText += ` (${escapeHtml(fwd.first_name)})`;
              if (fwd.username) idText += ` @${fwd.username}`;
              idText += `\n`;
            }
            if (msg.reply_to_message?.forward_from_chat) {
              const fwdChat = msg.reply_to_message.forward_from_chat;
              idText += `\n📤 <b>Forwarded From Chat:</b> <code>${fwdChat.id}</code>`;
              if (fwdChat.title) idText += ` (${escapeHtml(fwdChat.title)})`;
              if (fwdChat.username) idText += ` @${fwdChat.username}`;
              idText += `\n`;
            }

            await tg('sendMessage', { chat_id: chatId, text: idText, parse_mode: 'HTML', reply_to_message_id: messageId, disable_web_page_preview: true }, LK, TK);
          } else if (
            text.startsWith('/admin') || text.startsWith('/users') || text.startsWith('/stats') ||
            text.startsWith('/history ') || text.startsWith('/broadcast ') ||
            text.startsWith('/ban ') || text.startsWith('/unban ') ||
            text.startsWith('/mute ') || text.startsWith('/unmute ') ||
            text.startsWith('/sudo ') || text.startsWith('/info') ||
            text === '/pin' || text === '/unpin' || text.startsWith('/nsfw') ||
            text.startsWith('/addroast ') || text.startsWith('/addfunny ') ||
            text.startsWith('/removeroast ') || text.startsWith('/removefunny ') ||
            text === '/filter' || text === '/filters' || text.startsWith('/filter ') || text.startsWith('/rmfilter ') || text.startsWith('/unfilter ')
          ) {
            await handleAdminCommand(sb, chatId, text, userId, messageId, LK, TK, msg.reply_to_message);
            sendLog(LK, TK, `⚙️ <b>ADMIN CMD</b>\n👤 ${_u}${_c}\n💬 <i>${text.substring(0, 80)}</i>`);
          } else if (!msgHandled && !rawText.startsWith('/')) {
            const userState = await getUserState(sb, userId);
            const waitingForTarget = !!(userState && userState.mode && !userState.target_name);

            // Handle shortener flow states
            // Known shortener API format patterns
            const SHORTENER_FORMATS: Record<string, (key: string) => string> = {
              'arolinks.com': (k) => `https://arolinks.com/api?api=${k}&url={url}&alias={alias}`,
              'exe.io': (k) => `https://exe.io/api?api=${k}&url={url}&alias={alias}`,
              'gplinks.in': (k) => `https://gplinks.in/api?api=${k}&url={url}&alias={alias}`,
              'shrinkme.io': (k) => `https://shrinkme.io/api?api=${k}&url={url}&alias={alias}`,
              'shrinke.me': (k) => `https://shrinke.me/api?api=${k}&url={url}&alias={alias}`,
              'ouo.io': (k) => `https://ouo.io/api/${k}?s={url}`,
              'ouo.press': (k) => `https://ouo.press/api/${k}?s={url}`,
              'clicksfly.com': (k) => `https://clicksfly.com/api?api=${k}&url={url}&alias={alias}`,
              'urlshortx.com': (k) => `https://urlshortx.com/api?api=${k}&url={url}&alias={alias}`,
              'shorte.st': (k) => `https://api.shorte.st/s/${k}/{url}`,
              'atglinks.com': (k) => `https://atglinks.com/api?api=${k}&url={url}&alias={alias}`,
              'indianshortner.com': (k) => `https://indianshortner.com/api?api=${k}&url={url}&alias={alias}`,
              'linkvertise.com': (k) => `https://publisher.linkvertise.com/api/v1/redirect/link/static?url={url}&api_token=${k}`,
              'tinyurl.com': (_k) => `https://tinyurl.com/api-create.php?url={url}`,
              'is.gd': (_k) => `https://is.gd/create.php?format=simple&url={url}`,
              'v.gd': (_k) => `https://v.gd/create.php?format=simple&url={url}`,
              'da.gd': (_k) => `https://da.gd/s?url={url}`,
            };

            // Sites that don't need API key
            const NO_KEY_SITES = ['tinyurl.com', 'is.gd', 'v.gd', 'da.gd'];

            if (userState?.mode === 'short_add_site') {
              if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
                let site = rawText.trim().toLowerCase();
                site = site.replace(/^https?:\/\//, '').replace(/\/.*$/, '').replace(/^www\./, '');
                if (!site || !site.includes('.')) {
                  await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Invalid website. Send domain like:</b> <code>arolinks.com</code>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
                } else {
                  const shortName = site.split('.')[0].charAt(0).toUpperCase() + site.split('.')[0].slice(1);
                  const isKnown = !!SHORTENER_FORMATS[site];
                  const noKey = NO_KEY_SITES.includes(site);

                  if (noKey) {
                    // No key needed — save directly
                    const apiUrl = SHORTENER_FORMATS[site]('');
                    await tg('sendChatAction', { chat_id: chatId, action: 'typing' }, LK, TK);
                    const { error } = await sb.from('user_shorteners').upsert(
                      { user_id: userId, name: shortName, api_url: apiUrl },
                      { onConflict: 'user_id,name' }
                    );
                    if (!error) {
                      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.check, '✅')} <b>"${shortName}" saved!</b> No API key needed ✨\n\n<b>/shortener se use karo.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
                    } else {
                      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Save fail.</b>`, parse_mode: 'HTML' }, LK, TK);
                    }
                    await setUserState(sb, userId, null, 0, null);
                  } else {
                    await setUserState(sb, userId, 'short_add_key', 0, JSON.stringify({ site, name: shortName }));
                    const detectMsg = isKnown ? `\n\n✅ <b>${site} auto-detected!</b> Bas API key bhejo.` : '';
                    await tg('sendMessage', {
                      chat_id: chatId,
                      text: `${ce(E.check, '✅')} <b>Website: ${site}</b>${detectMsg}\n\n<b>Step 2/2: Ab apni API Key bhejo</b>\n\n<i>API Key apko ${site} ki settings/dashboard me milegi.</i>`,
                      parse_mode: 'HTML',
                      reply_to_message_id: messageId,
                    }, LK, TK);
                  }
                }
              }
            } else if (userState?.mode === 'short_add_key') {
              if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
                const apiKey = rawText.trim();
                let info: { site: string; name: string };
                try { info = JSON.parse(userState.target_name || '{}'); } catch { info = { site: '', name: 'Custom' }; }
                
                if (!apiKey || apiKey.length < 5) {
                  await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Invalid API key. Sahi key bhejo.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
                } else {
                  // Auto-construct API URL using known format or generic
                  const formatFn = SHORTENER_FORMATS[info.site];
                  const apiUrl = formatFn ? formatFn(apiKey) : `https://${info.site}/api?api=${apiKey}&url={url}&alias={alias}`;
                  
                  // Verify API
                  await tg('sendChatAction', { chat_id: chatId, action: 'typing' }, LK, TK);
                  let verified = false;
                  try {
                    const testUrl = apiUrl.replace(/\{url\}/gi, encodeURIComponent('https://google.com')).replace(/&alias=\{alias\}/gi, '').replace(/\{alias\}/gi, '');
                    const testRes = await fetch(testUrl);
                    const testText = await testRes.text();
                    let testShort = '';
                    try {
                      const j = JSON.parse(testText);
                      testShort = j.shortenedUrl || j.short_url || j.shorturl || j.result || j.link || j.data?.url || j.url || '';
                    } catch { testShort = testText.trim(); }
                    verified = testShort.startsWith('http');
                  } catch (e) {
                    console.error('API verify error:', e);
                  }

                  if (!verified) {
                    await tg('sendMessage', {
                      chat_id: chatId,
                      text: `${ce(E.stop, '⚠️')} <b>API verify fail!</b>\n\n<b>API Key ya website galat ho sakti hai. Check karo.</b>\n\n<b>Phir bhi save karna hai?</b>`,
                      parse_mode: 'HTML',
                      reply_to_message_id: messageId,
                      reply_markup: {
                        inline_keyboard: [
                          [{ text: '✅ Save Anyway', callback_data: `short_force_save:${info.name}` }],
                          [{ text: '❌ Cancel', callback_data: 'short_cancel_add' }],
                        ],
                      },
                    }, LK, TK);
                    await setUserState(sb, userId, 'short_force_save', 0, JSON.stringify({ name: info.name, api_url: apiUrl }));
                  } else {
                    const { error } = await sb.from('user_shorteners').upsert(
                      { user_id: userId, name: info.name, api_url: apiUrl },
                      { onConflict: 'user_id,name' }
                    );
                    if (error) {
                      console.error('Save shortener error:', error);
                      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Save fail. Try again.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
                    } else {
                      await tg('sendMessage', {
                        chat_id: chatId,
                        text: `${ce(E.check, '✅')} <b>"${info.name}" verified & saved!</b> ✨\n\n<b>/shortener se use karo ab.</b>`,
                        parse_mode: 'HTML',
                        reply_to_message_id: messageId,
                      }, LK, TK);
                    }
                    await setUserState(sb, userId, null, 0, null);
                  }
                }
              }
            } else if (userState?.mode === 'short_wait_url') {
              if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
                const urlToShorten = rawText.trim();
                if (!urlToShorten.startsWith('http')) {
                  await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Send a valid URL starting with http:// or https://</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
                } else {
                  const shortenerInfo = userState.target_name;
                  if (shortenerInfo === 'tinyurl') {
                    await shortenWithTinyUrl(chatId, urlToShorten, messageId, LK, TK);
                    await setUserState(sb, userId, null, 0, null);
                  } else {
                    // Check if this shortener supports alias
                    const { data: shortener } = await sb.from('user_shorteners').select('*').eq('user_id', userId).eq('name', shortenerInfo).single();
                    if (shortener) {
                      const supportsAlias = shortener.api_url.includes('{alias}');
                      if (supportsAlias) {
                        // Ask for alias
                        await setUserState(sb, userId, 'short_wait_alias', 0, JSON.stringify({ name: shortener.name, url: urlToShorten, api_url: shortener.api_url }));
                        await tg('sendMessage', {
                          chat_id: chatId,
                          text: `${ce(E.link, '🏷')} <b>Custom Alias (Optional)</b>\n\n<b>Send a custom alias for your short link</b>\n<b>Example:</b> <code>mylink</code>\n\n<b>Or press Skip for auto-generated:</b>`,
                          parse_mode: 'HTML',
                          reply_to_message_id: messageId,
                          reply_markup: { inline_keyboard: [[{ text: '⏭ Skip Alias', callback_data: 'short_skip_alias' }]] },
                        }, LK, TK);
                      } else {
                        await shortenWithCustomApi(chatId, urlToShorten, shortener.api_url, shortener.name, messageId, LK, TK);
                        await setUserState(sb, userId, null, 0, null);
                      }
                    } else {
                      await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Shortener not found.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
                      await setUserState(sb, userId, null, 0, null);
                    }
                  }
                }
              }
            } else if (userState?.mode === 'short_wait_alias') {
              if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
                const alias = rawText.trim();
                try {
                  const info = JSON.parse(userState.target_name || '{}');
                  await shortenWithCustomApi(chatId, info.url, info.api_url, info.name, messageId, LK, TK, alias);
                } catch {
                  await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Something went wrong.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
                }
                await setUserState(sb, userId, null, 0, null);
              }
            } else if (userState?.mode === 'short_delete') {
              if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
                const idx = parseInt(rawText.trim());
                const shorteners = await getUserShorteners(sb, userId);
                if (isNaN(idx) || idx < 1 || idx > shorteners.length) {
                  await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Invalid number.</b>`, parse_mode: 'HTML', reply_to_message_id: messageId }, LK, TK);
                } else {
                  const toDelete = shorteners[idx - 1];
                  await sb.from('user_shorteners').delete().eq('id', toDelete.id);
                  await tg('sendMessage', {
                    chat_id: chatId,
                    text: `${ce(E.check, '✅')} <b>Shortener "${toDelete.name}" deleted!</b>`,
                    parse_mode: 'HTML',
                    reply_to_message_id: messageId,
                  }, LK, TK);
                }
                await setUserState(sb, userId, null, 0, null);
              }
            } else if (groupMessage && !waitingForTarget) {
              // Ignore normal group text
            } else if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              if (!userState || !userState.mode) {
                // Random message - no response
              } else if (!userState.target_name) {
                const name = rawText || 'bhai';
                const targetPayload = encodeTargetPayload({ name, chatId, chatType, runId: crypto.randomUUID() });
                await setUserState(sb, userId, userState.mode, 0, targetPayload);
                await tg('sendMessage', {
                  chat_id: chatId,
                  text: `${ce(E.fire, '🎯')} <b>Target set: ${name}\n\nHere comes the roast!</b> 😈`,
                  parse_mode: 'HTML', reply_to_message_id: messageId,
                }, LK, TK);

                if (groupMessage) {
                  const payload = decodeTargetPayload(targetPayload);
                  EdgeRuntime.waitUntil(triggerGroupWorker(workerUrl, SUPABASE_ANON_KEY, userId, payload?.runId));
                } else {
                  await sendDmLinesBatch(sb, chatId, userId, LK, TK);
                }
              } else if (!groupMessage) {
                await tg('sendMessage', {
                  chat_id: chatId,
                  text: `<b>Use the Restart button for a new target, or send /start</b> 😄`,
                  parse_mode: 'HTML', reply_to_message_id: messageId,
                }, LK, TK);
              }
            }
          }
        } else if (update.callback_query) {
          const cb = update.callback_query;
          const cbData = cb.data || '';
          const chatId = cb.message?.chat?.id;
          const userId = cb.from?.id;
          const firstName = cb.from?.first_name || null;
          const username = cb.from?.username || null;

          // Answer generic callback early for non-reveal actions
          if (!cbData.startsWith('reveal:')) {
            tg('answerCallbackQuery', { callback_query_id: cb.id }, LK, TK);
          }

          // ─── Secret Message Reveal ────────────────────────
          if (cbData.startsWith('reveal:')) {
            const secretId = cbData.substring(7);
            const { data: secret } = await sb.from('secret_messages').select('*').eq('id', secretId).single();

            if (!secret) {
              await tg('answerCallbackQuery', {
                callback_query_id: cb.id,
                text: '❌ Secret message not found or expired.',
                show_alert: true,
              }, LK, TK);
              continue;
            }

            const viewer = await resolveSecretViewer(sb, secret, userId, username);

            if (viewer.canView && !viewer.isSender) {
              await tg('answerCallbackQuery', {
                callback_query_id: cb.id,
                text: buildCallbackAlert('🔓 Secret Message:', secret.message_text),
                show_alert: true,
              }, LK, TK);
              sendLog(LK, TK, `🔓 <b>SECRET REVEALED</b>\n👤 Viewer: ${userTag(userId, firstName, username)}\n🎯 Target was: ${secret.target_username ? '@' + secret.target_username : secret.target_user_id}\n👤 Sender: <code>${secret.sender_id}</code>\n💬 <i>${escapeHtml(secret.message_text).substring(0, 200)}</i>`);
            } else if (viewer.isSender) {
              await tg('answerCallbackQuery', {
                callback_query_id: cb.id,
                text: buildCallbackAlert('📤 Your sent message:', secret.message_text),
                show_alert: true,
              }, LK, TK);
            } else {
              await tg('answerCallbackQuery', {
                callback_query_id: cb.id,
                text: '😂 THIS TEXT IS NOT FOR YOU',
                show_alert: true,
              }, LK, TK);
              sendLog(LK, TK, `🚫 <b>SECRET DENIED</b>\n👤 ${userTag(userId, firstName, username)} tried to read secret meant for ${secret.target_username ? '@' + secret.target_username : secret.target_user_id}`);
            }
            continue;
          }

          if (!chatId) continue;

          if (cbData === 'check_sub') {
            if (await checkSubscription(chatId, userId, LK, TK)) {
              await tg('sendMessage', { chat_id: chatId, text: `${ce(E.check, '✅')} <b>Subscription verified, thank you!</b>`, parse_mode: 'HTML' }, LK, TK);
              await sendMainMenu(chatId, LK, TK);
            } else {
              await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>You haven't joined yet.</b>`, parse_mode: 'HTML' }, LK, TK);
            }
          } else if (cbData === 'mode_roast' || cbData === 'mode_funny') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              const mode = cbData === 'mode_roast' ? 'roast' : 'funny';
              await setUserState(sb, userId, mode, 0, null);
              const modeText = mode === 'roast' ? `${ce(E.skull, '💀')} Light Roasts` : `${ce(E.devil, '😈')} Heavy Roasts`;
              await tg('editMessageText', {
                chat_id: chatId, message_id: cb.message.message_id,
                text: `<b>${modeText} selected!\n\nNow send the target's name</b> (e.g: <code>Abdul</code>)`,
                parse_mode: 'HTML',
              }, LK, TK);
            }
          } else if (cbData === 'next10') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await sendDmLinesBatch(sb, chatId, userId, LK, TK);
            }
          } else if (cbData === 'restart') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await setUserState(sb, userId, null, 0, null);
              await sendMainMenu(chatId, LK, TK);
            }
          } else if (cbData === 'cmd_help') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await sendHelpMenu(chatId, LK, TK, 0, cb.message?.message_id);
            }
          } else if (cbData === 'cmd_truthdare') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleTruthDare(chatId, LK, TK);
            }
          } else if (cbData === 'td_truth') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleTruthOnly(chatId, LK, TK);
            }
          } else if (cbData === 'td_dare') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleDareOnly(chatId, LK, TK);
            }
          } else if (cbData === 'cmd_quote') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleQuote(chatId, LK, TK);
            }
          } else if (cbData === 'cmd_meme') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleMeme(chatId, LK, TK);
            }
          } else if (cbData === 'help_confess') {
            await tg('sendMessage', {
              chat_id: chatId,
              text: `${ce(E.ghost, '🤫')} <b>Anonymous Confession</b>\n\n<b>Send anonymous confessions in groups:</b>\n<code>/confess I love biryani 🍛</code>\n\n<b>Bot will delete your message and post it anonymously!</b>`,
              parse_mode: 'HTML',
            }, LK, TK);
          } else if (cbData === 'help_secret') {
            await tg('sendMessage', {
              chat_id: chatId,
              text: `${ce(E.lock, '🔒')} <b>Secret Message (WhisperBot Style)</b>\n\n<b>Go to any group and type in message box:</b>\n<code>@YourBotUsername @target_username your secret message</code>\n\n<b>A popup will appear — tap it to send!</b>\n<b>Only the target person can read it by clicking the reveal button!</b> 🤫\n\n<b>OR use command:</b>\n<code>/secret @username your secret message</code>`,
              parse_mode: 'HTML',
            }, LK, TK);
          } else if (cbData === 'help_ai') {
            await tg('sendMessage', {
              chat_id: chatId,
              text: `${ce(E.robot, '🤖')} <b>AI Chat</b>\n\n<b>Ask anything to AI:</b>\n<code>/ai tell me a joke</code>\n<code>/ai explain quantum physics simply</code>`,
              parse_mode: 'HTML',
            }, LK, TK);
          } else if (cbData === 'help_shortener') {
            await sendShortenerMenu(chatId, LK, TK);
          } else if (cbData === 'help_imagine') {
            await tg('sendMessage', {
              chat_id: chatId,
              text: `${ce(E.star, '🖼')} <b>AI Image Generation</b>\n\n<b>Generate unique AI images:</b>\n<code>/imagine a cute cat on the moon</code>\n<code>/imagine sunset over mountains</code>\n\n<b>Powered by Lovable AI!</b>`,
              parse_mode: 'HTML',
            }, LK, TK);
          } else if (cbData === 'help_tts') {
            await tg('sendMessage', {
              chat_id: chatId,
              text: `${ce(E.megaphone, '🔊')} <b>Text To Speech</b>\n\n<b>Convert text to voice message:</b>\n<code>/tts Hello, how are you today?</code>\n<code>/tts नमस्ते, आप कैसे हैं?</code>\n\n<b>Supports English & Hindi!</b>`,
              parse_mode: 'HTML',
            }, LK, TK);
          } else if (cbData === 'help_airoast') {
            await tg('sendMessage', {
              chat_id: chatId,
              text: `${ce(E.fire, '🔥')} <b>AI Roast</b>\n\n<b>AI generates savage roast lines:</b>\n<code>/roast Abdul</code>\n<code>/roast Rahul</code>\n\n<b>Every roast is unique & AI-powered!</b>`,
              parse_mode: 'HTML',
            }, LK, TK);
          } else if (cbData === 'help_translate') {
            await tg('sendMessage', {
              chat_id: chatId,
              text: `${ce(E.sparkle, '🌐')} <b>AI Translator</b>\n\n<b>Auto-detects & translates:</b>\n<code>/translate hello how are you</code>\n<code>/translate bhai kya haal hai</code>\n<code>/translate to Spanish: good morning</code>`,
              parse_mode: 'HTML',
            }, LK, TK);
          } else if (cbData.startsWith('help_page:')) {
            const pageIdx = parseInt(cbData.split(':')[1]) || 0;
            await sendHelpMenu(chatId, LK, TK, pageIdx, cb.message?.message_id);
          } else if (cbData === 'help_filters') {
            await tg('sendMessage', {
              chat_id: chatId,
              text: `🔍 <b>Filters System</b>\n\n<b>Set Filter:</b> Reply to any media/text with <code>/filter keyword</code>\n<b>Remove:</b> <code>/stop keyword</code>\n<b>List:</b> <code>/filters</code>\n\n<i>Jab koi us keyword ko type karega, bot wo saved media/text reply karega!</i>`,
              parse_mode: 'HTML',
            }, LK, TK);
          } else if (cbData === 'help_nsfw') {
            await tg('sendMessage', {
              chat_id: chatId,
              text: `🔞 <b>NSFW Filter (Admin Only)</b>\n\n<b>Auto-deletes 18+ stickers, photos, videos & GIFs in groups.</b>\n\n<b>Commands:</b>\n<code>/nsfw on</code> — Enable filter\n<code>/nsfw off</code> — Disable filter\n<code>/nsfw</code> — Check status\n\n<i>Uses AI vision to detect explicit content + keyword matching for instant deletion.</i>`,
              parse_mode: 'HTML',
            }, LK, TK);
          } else if (cbData === 'cmd_joke') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleJoke(chatId, 0, LK, TK);
            }
          } else if (cbData === 'cmd_fact') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleFact(chatId, 0, LK, TK);
            }
          } else if (cbData === 'cmd_wyr') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handleWYR(chatId, 0, LK, TK);
            }
          } else if (cbData === 'wyr_a' || cbData === 'wyr_b') {
            const choice = cbData === 'wyr_a' ? '🅰️ Option A' : '🅱️ Option B';
            await tg('answerCallbackQuery', { callback_query_id: cb.id, text: `You chose ${choice}! 👍`, show_alert: false }, LK, TK);
          } else if (cbData === 'cmd_pickup') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await handlePickup(chatId, 0, LK, TK);
            }
          } else if (cbData.startsWith('ai_roast:')) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              const name = cbData.substring(9);
              await handleAiRoast(chatId, `/roast ${name}`, 0, LK, TK);
            }
          // ─── Shortener Callbacks ─────────────────────────
          } else if (cbData === 'short_default') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              await setUserState(sb, userId, 'short_wait_url', 0, 'tinyurl');
              await tg('sendMessage', {
                chat_id: chatId,
                text: `${ce(E.link, '🔗')} <b>TinyURL Shortener</b>\n\n<b>Send the URL you want to shorten:</b>`,
                parse_mode: 'HTML',
              }, LK, TK);
            }
          } else if (cbData === 'short_add_custom') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              const existing = await getUserShorteners(sb, userId);
              if (existing.length >= 5) {
                await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Maximum 5 custom shorteners reached. Delete one first.</b>`, parse_mode: 'HTML' }, LK, TK);
              } else {
                await setUserState(sb, userId, 'short_add_site', 0, null);
                await tg('sendMessage', {
                  chat_id: chatId,
                  text: `${ce(E.link, '➕')} <b>Add Custom Shortener</b>\n\n<b>Step 1/2: Website URL bhejo</b>\n(e.g: <code>arolinks.com</code>, <code>shrinke.me</code>)`,
                  parse_mode: 'HTML',
                }, LK, TK);
              }
            }
          } else if (cbData === 'short_list_custom') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              const shorteners = await getUserShorteners(sb, userId);
              if (shorteners.length === 0) {
                await tg('sendMessage', {
                  chat_id: chatId,
                  text: `${ce(E.link, '📋')} <b>No custom shorteners yet.</b>\n\n<b>Add one using the button below.</b>`,
                  parse_mode: 'HTML',
                  reply_markup: { inline_keyboard: [[{ text: '➕ Add Custom Shortener', callback_data: 'short_add_custom' }]] },
                }, LK, TK);
              } else {
                let listText = `${ce(E.link, '📋')} <b>Your Custom Shorteners</b>\n\n`;
                const buttons: Array<Array<{ text: string; callback_data: string }>> = [];
                for (let i = 0; i < shorteners.length; i++) {
                  listText += `<b>${i + 1}.</b> ${shorteners[i].name}\n   <code>${shorteners[i].api_url.substring(0, 50)}${shorteners[i].api_url.length > 50 ? '...' : ''}</code>\n\n`;
                  buttons.push([{ text: `🔗 Use ${shorteners[i].name}`, callback_data: `short_use:${shorteners[i].name}` }]);
                }
                buttons.push([{ text: '➕ Add New', callback_data: 'short_add_custom' }, { text: '🗑 Delete', callback_data: 'short_delete_menu' }]);
                await tg('sendMessage', {
                  chat_id: chatId,
                  text: listText,
                  parse_mode: 'HTML',
                  reply_markup: { inline_keyboard: buttons },
                }, LK, TK);
              }
            }
          } else if (cbData === 'short_delete_menu') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              const shorteners = await getUserShorteners(sb, userId);
              if (shorteners.length === 0) {
                await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>No custom shorteners to delete.</b>`, parse_mode: 'HTML' }, LK, TK);
              } else {
                let listText = `${ce(E.link, '🗑')} <b>Delete Custom Shortener</b>\n\n`;
                for (let i = 0; i < shorteners.length; i++) {
                  listText += `<b>${i + 1}.</b> ${shorteners[i].name}\n`;
                }
                listText += `\n<b>Send the number to delete:</b>`;
                await setUserState(sb, userId, 'short_delete', 0, null);
                await tg('sendMessage', { chat_id: chatId, text: listText, parse_mode: 'HTML' }, LK, TK);
              }
            }
          } else if (cbData.startsWith('short_use:')) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              const shortName = cbData.substring(10);
              await setUserState(sb, userId, 'short_wait_url', 0, shortName);
              await tg('sendMessage', {
                chat_id: chatId,
                text: `${ce(E.link, '🔗')} <b>Using: ${shortName}</b>\n\n<b>Send the URL you want to shorten:</b>`,
                parse_mode: 'HTML',
              }, LK, TK);
            }
          } else if (cbData === 'short_skip_alias') {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              const userState = await getUserState(sb, userId);
              if (userState?.mode === 'short_wait_alias') {
                try {
                  const info = JSON.parse(userState.target_name || '{}');
                  await shortenWithCustomApi(chatId, info.url, info.api_url, info.name, 0, LK, TK);
                } catch {
                  await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Something went wrong.</b>`, parse_mode: 'HTML' }, LK, TK);
                }
                await setUserState(sb, userId, null, 0, null);
              }
            }
          } else if (cbData.startsWith('short_force_save:')) {
            if (await handleAccess(sb, chatId, userId, firstName, username, LK, TK)) {
              const userState = await getUserState(sb, userId);
              if (userState?.mode === 'short_force_save') {
                try {
                  const info = JSON.parse(userState.target_name || '{}');
                  const { error } = await sb.from('user_shorteners').upsert(
                    { user_id: userId, name: info.name, api_url: info.api_url },
                    { onConflict: 'user_id,name' }
                  );
                  if (!error) {
                    await tg('sendMessage', { chat_id: chatId, text: `${ce(E.check, '✅')} <b>Shortener "${info.name}" saved!</b>`, parse_mode: 'HTML' }, LK, TK);
                  } else {
                    await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Failed to save.</b>`, parse_mode: 'HTML' }, LK, TK);
                  }
                } catch {
                  await tg('sendMessage', { chat_id: chatId, text: `${ce(E.stop, '❌')} <b>Error saving.</b>`, parse_mode: 'HTML' }, LK, TK);
                }
                await setUserState(sb, userId, null, 0, null);
              }
            }
          } else if (cbData === 'short_cancel_add') {
            await setUserState(sb, userId, null, 0, null);
            await tg('sendMessage', { chat_id: chatId, text: `${ce(E.check, '👍')} <b>Cancelled.</b>`, parse_mode: 'HTML' }, LK, TK);
          } else if (cbData.startsWith('admin_') && await isSudo(sb, userId)) {
            if (cbData === 'admin_stats' || cbData === 'admin_users') {
              await sendUsersPage(sb, chatId, 0, LK, TK, { includeStats: true });
            } else if (cbData.startsWith('admin_users_page:')) {
              const offset = Number(cbData.split(':')[1] ?? 0);
              await sendUsersPage(sb, chatId, offset, LK, TK, { includeStats: true, messageId: cb.message?.message_id });
            } else if (cbData === 'admin_history') {
              await tg('sendMessage', { chat_id: chatId, text: `📜 <b>History Commands:</b>\n• /history &lt;user_id&gt; → last 20 lines\n\n<b>Example:</b>\n<code>/history 123456789</code>`, parse_mode: 'HTML' }, LK, TK);
} else if (cbData === 'admin_broadcast') {
              await tg('sendMessage', { chat_id: chatId, text: `📢 <b>Broadcast Command:</b>\n• <code>/broadcast message</code> — Sabko\n• <code>/broadcast -user message</code> — Sirf users\n• <code>/broadcast -gc message</code> — Sirf groups\n• <code>/broadcast -pin message</code> — Pin bhi\n• Reply karke <code>/broadcast -gc -pin</code>\n\n<b>Flags:</b>\n<code>-user</code> = Sirf users\n<code>-gc</code> = Sirf groups\n<code>-pin</code> = Pin bhi karega\n\nCombine bhi kar sakte ho: <code>-user -pin</code>`, parse_mode: 'HTML' }, LK, TK);
            } else if (cbData === 'admin_ban') {
              await tg('sendMessage', { chat_id: chatId, text: `🚫 <b>Ban / Unban Commands:</b>\n• /ban &lt;user_id&gt;\n• /unban &lt;user_id&gt;\n\n<b>Example:</b>\n<code>/ban 123456789</code>`, parse_mode: 'HTML' }, LK, TK);
            } else if (cbData === 'admin_mute') {
              await tg('sendMessage', { chat_id: chatId, text: `🔇 <b>Mute / Unmute Commands:</b>\n• <code>/mute @username</code> — 24h mute\n• <code>/mute user_id</code>\n• <code>/unmute @username</code>\n• <code>/unmute user_id</code>\n\n⚠️ <b>Group chats me hi kaam karega, bot ko admin hona chahiye.</b>`, parse_mode: 'HTML' }, LK, TK);
            } else if (cbData === 'admin_info') {
              await tg('sendMessage', { chat_id: chatId, text: `ℹ️ <b>User Info Command:</b>\n• <code>/info @username</code>\n• <code>/info user_id</code>\n\n<b>Shows:</b> Name, Username, ID, Join Date, Last Active, Ban Status, Sudo, Command Count`, parse_mode: 'HTML' }, LK, TK);
            } else if (cbData === 'admin_sudo') {
              if (userId === ADMIN_ID) {
                const sudos = await getSudoUsers(sb);
                let sudoMsg = `👑 <b>Sudo Management:</b>\n\n`;
                sudoMsg += `<code>/sudo add @username</code> — Add sudo\n`;
                sudoMsg += `<code>/sudo remove @username</code> — Remove sudo\n`;
                sudoMsg += `<code>/sudo list</code> — View all\n\n`;
                sudoMsg += `<b>Current Sudos:</b> ${sudos.length}`;
                await tg('sendMessage', { chat_id: chatId, text: sudoMsg, parse_mode: 'HTML' }, LK, TK);
              }
            } else if (cbData === 'admin_lines') {
              const [{ count: rc }, { count: fc }] = await Promise.all([
                sb.from('gali_lines').select('id', { count: 'exact', head: true }).eq('mode', 'roast'),
                sb.from('gali_lines').select('id', { count: 'exact', head: true }).eq('mode', 'funny'),
              ]);
              await tg('sendMessage', {
                chat_id: chatId,
                text: `📝 <b>Lines Management:</b>\n\n➕ <code>/addroast {name} line here</code>\n➕ <code>/addfunny {name} line here</code>\n➖ <code>/removeroast 3</code>\n➖ <code>/removefunny 5</code>\n\n🔥 Light Roasts: ${rc || 0}\n😂 Heavy Roasts: ${fc || 0}`,
                parse_mode: 'HTML',
              }, LK, TK);
            }
          }
        // ─── Inline Query (WhisperBot-style Secret Messages) ────
        } else if (update.inline_query) {
          const iq = update.inline_query;
          const queryText = (iq.query || '').trim();
          const senderId = iq.from?.id;
          const senderUsername = iq.from?.username || null;

          if (!queryText || !senderId) {
            // Show usage hint when empty query
            await tg('answerInlineQuery', {
              inline_query_id: iq.id,
              results: [{
                type: 'article',
                id: 'usage_hint',
                title: '🔒 Send Secret Message',
                description: 'Type: @username your secret message',
                input_message_content: {
                  message_text: '🔒 <b>Secret Message Bot</b>\n\nType <code>@bot_username @target message</code> to send a secret!',
                  parse_mode: 'HTML',
                },
              }],
              cache_time: 5,
              is_personal: true,
            }, LK, TK);
          } else {
            // Parse: @username message  OR  userId message
            let targetUsername: string | null = null;
            let targetUserId: number | null = null;
            let secretText = '';

            const parts = queryText.split(/\s+/);
            const firstPart = parts[0];

            if (firstPart.startsWith('@') && parts.length >= 2) {
              targetUsername = firstPart.substring(1).toLowerCase();
              secretText = parts.slice(1).join(' ');
            } else if (/^\d+$/.test(firstPart) && parts.length >= 2) {
              targetUserId = parseInt(firstPart);
              secretText = parts.slice(1).join(' ');
            }

            if (secretText.trim() && (targetUsername || targetUserId)) {
              // If we have username but not userId, try to find from bot_users
              if (targetUsername && !targetUserId) {
                const { data: targetUser } = await sb.from('bot_users').select('user_id').eq('username', targetUsername).single();
                if (targetUser) targetUserId = targetUser.user_id;
              }

              // Save secret to DB
              const { data: secretRow } = await sb.from('secret_messages').insert({
                sender_id: senderId,
                target_username: targetUsername,
                target_user_id: targetUserId,
                message_text: secretText,
                chat_id: 0, // will be in any group
              }).select('id').single();

              const targetDisplay = targetUsername ? `@${targetUsername}` : `User ${targetUserId}`;

              if (secretRow) {
                sendLog(LK, TK, `🔒 <b>INLINE SECRET CREATED</b>\n👤 Sender: <code>${senderId}</code>\n🎯 Target: ${escapeHtml(targetDisplay)}\n💬 <i>${escapeHtml(secretText).substring(0, 200)}</i>`);
                await tg('answerInlineQuery', {
                  inline_query_id: iq.id,
                  results: [{
                    type: 'article',
                    id: secretRow.id,
                    title: `🔒 Send Secret Message to ${targetDisplay}`,
                    description: 'Click to send a secret message',
                    thumbnail_url: 'https://img.icons8.com/emoji/96/locked-emoji.png',
                    input_message_content: {
                      message_text: `🤫 <b>Secret Message for ${targetDisplay}</b>\n\n<i>Only ${targetDisplay} can reveal this message!</i>`,
                      parse_mode: 'HTML',
                    },
                    reply_markup: {
                      inline_keyboard: [
                        [{ text: '🔓 Reveal Secret Message', callback_data: `reveal:${secretRow.id}` }],
                      ],
                    },
                  }],
                  cache_time: 0,
                  is_personal: true,
                }, LK, TK);
              } else {
                await tg('answerInlineQuery', {
                  inline_query_id: iq.id,
                  results: [{
                    type: 'article',
                    id: 'error',
                    title: '❌ Failed to create secret message',
                    description: 'Try again',
                    input_message_content: { message_text: '❌ Failed to create secret message. Try again.' },
                  }],
                  cache_time: 0,
                  is_personal: true,
                }, LK, TK);
              }
            } else {
              // Partial input - show hint
              await tg('answerInlineQuery', {
                inline_query_id: iq.id,
                results: [{
                  type: 'article',
                  id: 'hint',
                  title: '🔒 Send Secret Message',
                  description: 'Format: @username your secret message',
                  input_message_content: {
                    message_text: '🔒 <b>Secret Message</b>\n\nFormat: <code>@bot @username your secret message</code>',
                    parse_mode: 'HTML',
                  },
                }],
                cache_time: 5,
                is_personal: true,
              }, LK, TK);
            }
          }
        // ─── Bot Added/Removed from Group ────
        } else if (update.my_chat_member) {
          const mcm = update.my_chat_member;
          const chat = mcm.chat;
          const from = mcm.from;
          const newStatus = mcm.new_chat_member?.status;
          const oldStatus = mcm.old_chat_member?.status;
          const fromUser = userTag(from?.id || 0, from?.first_name || null, from?.username || null);

          if (newStatus === 'member' || newStatus === 'administrator') {
            await addOrUpdateChatTarget(sb, chat);
            sendLog(LK, TK, `📥 <b>BOT ADDED TO GROUP</b>\n📍 ${chatTag(chat)}\n👤 <b>Added by:</b> ${fromUser}`);
          } else if (newStatus === 'left' || newStatus === 'kicked') {
            await sb.from('bot_users').delete().eq('user_id', chat?.id);
            sendLog(LK, TK, `📤 <b>BOT REMOVED FROM GROUP</b>\n📍 ${chatTag(chat)}\n👤 <b>Removed by:</b> ${fromUser}`);
          }
        }

        totalProcessed++;
      } catch (error) {
        console.error('Error processing update:', error);
      }
    }

    const newOffset = Math.max(...updates.map((u: any) => u.update_id)) + 1;
    const nextLeaseToken = await refreshTelegramPollLease(sb, leaseToken, newOffset);
    if (!nextLeaseToken) break;
    leaseToken = nextLeaseToken;
    currentOffset = newOffset;

    hasActiveGroupJobs = await recoverStalledGroupJobs(sb, workerUrl, SUPABASE_ANON_KEY);
  }

  return new Response(JSON.stringify({ ok: true, processed: totalProcessed }));
});
