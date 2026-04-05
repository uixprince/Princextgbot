# ===== index.py =====

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const GATEWAY_URL = 'https://connector-gateway.lovable.dev/telegram';
const LOGO_API_BASE = 'https://abbas-logo-ai-gen.vercel.app';
const MAX_RUNTIME_MS = 55_000;
const POLL_INTERVAL_MS = 1_000;
const FAST_WINDOW_ATTEMPTS = 30;
const MAX_CHECK_ATTEMPTS = 120;

async function telegramRequest(endpoint: string, body: Record<string, unknown>, lovableKey: string, telegramKey: string) {
  const response = await fetch(`${GATEWAY_URL}/${endpoint}`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${lovableKey}`,
      'X-Connection-Api-Key': telegramKey,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  return response.json();
}

function getProgressStars(attempts: number): string {
  const percent = Math.min(95, Math.floor((attempts / FAST_WINDOW_ATTEMPTS) * 100));
  const filled = Math.min(5, Math.floor(percent / 20));
  return '★'.repeat(filled) + '☆'.repeat(5 - filled);
}

function getProgressMessage(attempts: number): string {
  const percent = Math.min(95, Math.floor((attempts / FAST_WINDOW_ATTEMPTS) * 100));
  const stars = getProgressStars(attempts);
  return `Generating your logo...\n\n${stars} ${percent}%\n\nPlease wait...`;
}

function formatTimestamp(date: Date): string {
  const pad = (n: number) => n.toString().padStart(2, '0');
  return `${pad(date.getDate())}/${pad(date.getMonth() + 1)}/${date.getFullYear()} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

Deno.serve(async () => {
  const startTime = Date.now();

  const LOVABLE_API_KEY = Deno.env.get('LOVABLE_API_KEY');
  if (!LOVABLE_API_KEY) return new Response(JSON.stringify({ error: 'LOVABLE_API_KEY not configured' }), { status: 500 });

  const TELEGRAM_API_KEY = Deno.env.get('TELEGRAM_API_KEY');
  if (!TELEGRAM_API_KEY) return new Response(JSON.stringify({ error: 'TELEGRAM_API_KEY not configured' }), { status: 500 });

  const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
  const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  let processed = 0;

  // Loop: keep checking every 5 seconds until time runs out
  while (Date.now() - startTime < MAX_RUNTIME_MS) {

    // Step 1: Start generation for "pending" jobs
    const { data: pendingJobs } = await supabase
      .from('pending_logos')
      .select('*')
      .eq('status', 'pending')
      .order('created_at', { ascending: true })
      .limit(5);

    for (const job of pendingJobs || []) {
      try {
        await telegramRequest('sendChatAction', {
          chat_id: job.chat_id,
          action: 'upload_photo',
        }, LOVABLE_API_KEY, TELEGRAM_API_KEY);

        const genRes = await fetch(`${LOGO_API_BASE}/gen?prompt=${encodeURIComponent(job.prompt)}`);
        const genData = await genRes.json();

        if (genData.task_id) {
          if (job.status_message_id) {
            await telegramRequest('editMessageText', {
              chat_id: job.chat_id,
              message_id: job.status_message_id,
              text: getProgressMessage(0),
            }, LOVABLE_API_KEY, TELEGRAM_API_KEY);
          }
          await supabase.from('pending_logos').update({
            status: 'checking',
            task_id: genData.task_id,
            updated_at: new Date().toISOString(),
          }).eq('id', job.id);
        } else {
          await supabase.from('pending_logos').update({ status: 'failed', updated_at: new Date().toISOString() }).eq('id', job.id);
          await telegramRequest('sendMessage', { chat_id: job.chat_id, text: 'Logo generation failed. Please try again.' }, LOVABLE_API_KEY, TELEGRAM_API_KEY);
        }
      } catch (e) {
        console.error('Gen error:', e);
        await supabase.from('pending_logos').update({ status: 'failed', updated_at: new Date().toISOString() }).eq('id', job.id);
      }
    }

    // Step 2: Check "checking" jobs
    const { data: checkingJobs } = await supabase
      .from('pending_logos')
      .select('*')
      .eq('status', 'checking')
      .order('created_at', { ascending: true })
      .limit(10);

    if ((!pendingJobs || pendingJobs.length === 0) && (!checkingJobs || checkingJobs.length === 0)) {
      // No work to do, exit early
      break;
    }

    for (const job of checkingJobs || []) {
      try {
        const checkRes = await fetch(`${LOGO_API_BASE}/check?task=${job.task_id}`);
        const checkData = await checkRes.json();

        if (checkData.image_url) {
          const now = new Date();
          const timestamp = formatTimestamp(now);
          const timeTaken = Math.round((now.getTime() - new Date(job.created_at).getTime()) / 1000);

          const text = `Logo ready ✅\n\nPrompt: ${job.prompt}\nTime: ${timeTaken}s\nDate: ${timestamp}\n\nImage URL:\n${checkData.image_url}`;

          await telegramRequest('sendMessage', {
            chat_id: job.chat_id,
            text,
          }, LOVABLE_API_KEY, TELEGRAM_API_KEY);

          if (job.status_message_id) {
            await telegramRequest('deleteMessage', { chat_id: job.chat_id, message_id: job.status_message_id }, LOVABLE_API_KEY, TELEGRAM_API_KEY);
          }

          await supabase.from('pending_logos').update({ status: 'completed', image_url: checkData.image_url, updated_at: new Date().toISOString() }).eq('id', job.id);
          processed++;
        } else {
          const newAttempts = (job.attempts || 0) + 1;

          if (job.status_message_id && newAttempts % 2 === 0) {
            try {
              await telegramRequest('editMessageText', { chat_id: job.chat_id, message_id: job.status_message_id, text: getProgressMessage(newAttempts) }, LOVABLE_API_KEY, TELEGRAM_API_KEY);
            } catch (_) {}
          }

          if (newAttempts >= MAX_CHECK_ATTEMPTS) {
            const statusUrl = job.task_id ? `${LOGO_API_BASE}/check?task=${job.task_id}` : null;
            await supabase
              .from('pending_logos')
              .update({ status: 'failed', attempts: newAttempts, updated_at: new Date().toISOString() })
              .eq('id', job.id);

            const timeoutText = statusUrl
              ? `Still processing on API.\n\nDirect status URL:\n${statusUrl}`
              : 'Timed out, please try again.';

            if (job.status_message_id) {
              await telegramRequest('editMessageText', {
                chat_id: job.chat_id,
                message_id: job.status_message_id,
                text: timeoutText,
              }, LOVABLE_API_KEY, TELEGRAM_API_KEY);
            } else {
              await telegramRequest('sendMessage', {
                chat_id: job.chat_id,
                text: timeoutText,
              }, LOVABLE_API_KEY, TELEGRAM_API_KEY);
            }
          } else {
            await supabase.from('pending_logos').update({ attempts: newAttempts, updated_at: new Date().toISOString() }).eq('id', job.id);
            if (job.status_message_id && newAttempts === FAST_WINDOW_ATTEMPTS) {
              await telegramRequest('editMessageText', {
                chat_id: job.chat_id,
                message_id: job.status_message_id,
                text: 'Generating your logo...\n\n★★★★★ 95%\n\nAlmost done, finalizing...',
              }, LOVABLE_API_KEY, TELEGRAM_API_KEY);
            }
          }
        }
      } catch (e) {
        console.error('Check error:', e);
        const fallbackAttempts = (job.attempts || 0) + 1;

        if (fallbackAttempts >= MAX_CHECK_ATTEMPTS) {
          const statusUrl = job.task_id ? `${LOGO_API_BASE}/check?task=${job.task_id}` : null;
          await supabase
            .from('pending_logos')
            .update({ status: 'failed', attempts: fallbackAttempts, updated_at: new Date().toISOString() })
            .eq('id', job.id);

          await telegramRequest('sendMessage', {
            chat_id: job.chat_id,
            text: statusUrl
              ? `Check request failed repeatedly.\n\nDirect status URL:\n${statusUrl}`
              : 'Check request failed repeatedly. Please try again.',
          }, LOVABLE_API_KEY, TELEGRAM_API_KEY);
        } else {
          await supabase
            .from('pending_logos')
            .update({ attempts: fallbackAttempts, updated_at: new Date().toISOString() })
            .eq('id', job.id);
        }
      }
    }

    // Wait briefly before next check
    await sleep(POLL_INTERVAL_MS);
  }

  return new Response(JSON.stringify({ ok: true, processed }));
});


# ===== index(1).py =====

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
