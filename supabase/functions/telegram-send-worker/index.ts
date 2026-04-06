import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const GATEWAY_URL = 'https://connector-gateway.lovable.dev/telegram';
const MAX_RUNTIME_MS = 45_000;
const LEASE_MS = 45_000;
const MAX_LINES_PER_RUN = 25;

type TargetPayload = {
  name: string;
  chatId?: number;
  chatType?: string;
  runId?: string;
  leaseToken?: string;
  leaseUntil?: string;
};

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isGroupChat(chatType?: string | null) {
  return chatType === 'group' || chatType === 'supergroup';
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

  return res.json();
}

async function sleepWithInterrupt(ms: number, shouldContinue?: () => Promise<boolean>) {
  let remaining = ms;

  while (remaining > 0) {
    if (shouldContinue && !(await shouldContinue())) return false;
    const step = Math.min(250, remaining);
    await sleep(step);
    remaining -= step;
  }

  return true;
}

async function tgSendMessage(
  body: Record<string, unknown>,
  lk: string,
  tk: string,
  retries = 8,
  shouldContinue?: () => Promise<boolean>,
): Promise<boolean> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    if (shouldContinue && !(await shouldContinue())) return false;

    try {
      const data = await tg('sendMessage', body, lk, tk);

      if (data?.ok) return true;

      const retryAfter = Number(data?.parameters?.retry_after ?? 0);
      if (retryAfter > 0) {
        const continued = await sleepWithInterrupt((retryAfter * 1000) + 150, shouldContinue);
        if (!continued) return false;
        continue;
      }

      const description = String(data?.description ?? '').toLowerCase();
      if (data?.error_code === 429 || description.includes('too many requests')) {
        const continued = await sleepWithInterrupt(500 * (attempt + 1), shouldContinue);
        if (!continued) return false;
        continue;
      }

      console.error('sendMessage failed:', data);
      return false;
    } catch (error) {
      if (attempt === retries) {
        console.error('sendMessage request failed:', error);
        return false;
      }

      const continued = await sleepWithInterrupt(250 * (attempt + 1), shouldContinue);
      if (!continued) return false;
    }
  }

  return false;
}

const linesCache = new Map<string, string[]>();

async function getLines(sb: any, mode: string): Promise<string[]> {
  if (linesCache.has(mode)) return linesCache.get(mode)!;

  const { data } = await sb.from('gali_lines').select('line_text').eq('mode', mode).order('id', { ascending: true });
  const lines = (data || []).map((row: any) => row.line_text);
  linesCache.set(mode, lines);
  return lines;
}

async function getUserState(sb: any, userId: number) {
  const { data } = await sb.from('user_states').select('*').eq('user_id', userId).maybeSingle();
  return data;
}

async function claimLease(sb: any, userId: number, expectedRunId?: string) {
  const state = await getUserState(sb, userId);
  if (!state || !state.mode || !state.target_name) return null;

  const payload = decodeTargetPayload(state.target_name);
  if (!payload?.name || !payload.chatId || !isGroupChat(payload.chatType)) return null;
  if (expectedRunId && payload.runId && payload.runId !== expectedRunId) return null;

  const now = Date.now();
  const leaseActive = payload.leaseUntil && Date.parse(payload.leaseUntil) > now && payload.leaseToken;
  if (leaseActive) return null;

  const claimedPayload: TargetPayload = {
    ...payload,
    leaseToken: crypto.randomUUID(),
    leaseUntil: new Date(now + LEASE_MS).toISOString(),
  };
  const claimedRaw = encodeTargetPayload(claimedPayload);

  const { data } = await sb
    .from('user_states')
    .update({ target_name: claimedRaw, updated_at: new Date().toISOString() })
    .eq('user_id', userId)
    .eq('mode', state.mode)
    .eq('line_index', state.line_index || 0)
    .eq('target_name', state.target_name)
    .select('*')
    .maybeSingle();

  if (!data) return null;

  return {
    state: data,
    payload: claimedPayload,
    raw: claimedRaw,
  };
}

async function persistProgress(sb: any, state: any, raw: string, payload: TargetPayload, nextIndex: number) {
  const nextPayload: TargetPayload = {
    ...payload,
    leaseUntil: new Date(Date.now() + LEASE_MS).toISOString(),
  };
  const nextRaw = encodeTargetPayload(nextPayload);

  const { data } = await sb
    .from('user_states')
    .update({ line_index: nextIndex, target_name: nextRaw, updated_at: new Date().toISOString() })
    .eq('user_id', state.user_id)
    .eq('mode', state.mode)
    .eq('line_index', state.line_index || 0)
    .eq('target_name', raw)
    .select('*')
    .maybeSingle();

  if (!data) return null;

  return {
    state: data,
    payload: nextPayload,
    raw: nextRaw,
  };
}

async function isRunStillActive(sb: any, userId: number, mode: string, raw: string) {
  const { data } = await sb
    .from('user_states')
    .select('mode, target_name')
    .eq('user_id', userId)
    .maybeSingle();

  return !!data && data.mode === mode && data.target_name === raw;
}

async function releaseLease(sb: any, state: any, raw: string, payload: TargetPayload) {
  const releasedPayload: TargetPayload = {
    name: payload.name,
    chatId: payload.chatId,
    chatType: payload.chatType,
    runId: payload.runId,
  };

  const releasedRaw = encodeTargetPayload(releasedPayload);

  const { data } = await sb
    .from('user_states')
    .update({ target_name: releasedRaw, updated_at: new Date().toISOString() })
    .eq('user_id', state.user_id)
    .eq('mode', state.mode)
    .eq('line_index', state.line_index || 0)
    .eq('target_name', raw)
    .select('*')
    .maybeSingle();

  return data;
}

async function clearState(sb: any, state: any, raw: string) {
  const { data } = await sb
    .from('user_states')
    .update({ mode: null, line_index: 0, target_name: null, updated_at: new Date().toISOString() })
    .eq('user_id', state.user_id)
    .eq('mode', state.mode)
    .eq('line_index', state.line_index || 0)
    .eq('target_name', raw)
    .select('user_id')
    .maybeSingle();

  return data;
}

function saveHistory(sb: any, userId: number, mode: string, targetName: string, line: string) {
  sb.from('bot_history').insert({ user_id: userId, mode, target_name: targetName, line }).then(() => {}).catch((error: unknown) => {
    console.error('saveHistory failed:', error);
  });
}

async function triggerNextRun(functionUrl: string, anonKey: string, userId: number, runId?: string) {
  await fetch(functionUrl, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${anonKey}`,
      apikey: anonKey,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ userId, runId }),
  });
}

Deno.serve(async (req) => {
  const startTime = Date.now();
  const deadline = startTime + MAX_RUNTIME_MS;

  const LOVABLE_API_KEY = Deno.env.get('LOVABLE_API_KEY');
  const TELEGRAM_API_KEY = Deno.env.get('TELEGRAM_API_KEY');
  const SUPABASE_URL = Deno.env.get('SUPABASE_URL');
  const SUPABASE_ANON_KEY = Deno.env.get('SUPABASE_ANON_KEY');
  const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');

  if (!LOVABLE_API_KEY || !TELEGRAM_API_KEY || !SUPABASE_URL || !SUPABASE_ANON_KEY || !SUPABASE_SERVICE_ROLE_KEY) {
    return new Response(JSON.stringify({ ok: false, error: 'Missing required env' }), { status: 500 });
  }

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  const workerUrl = `${SUPABASE_URL}/functions/v1/telegram-send-worker`;

  const body = req.method === 'POST' ? await req.json().catch(() => ({})) : {};
  const userId = Number(body?.userId ?? 0);
  const runId = typeof body?.runId === 'string' ? body.runId : undefined;

  if (!userId) {
    return new Response(JSON.stringify({ ok: false, error: 'userId required' }), { status: 400 });
  }

  const claimed = await claimLease(sb, userId, runId);
  if (!claimed) {
    return new Response(JSON.stringify({ ok: true, claimed: false }));
  }

  let currentState = claimed.state;
  let currentPayload = claimed.payload;
  let currentRaw = claimed.raw;

  const lines = await getLines(sb, currentState.mode);
  const total = lines.length;

  if (total === 0) {
    await clearState(sb, currentState, currentRaw);
    await tgSendMessage({ chat_id: currentPayload.chatId, text: '⚠️ <b>Abhi is category me koi lines nahi hain.</b>', parse_mode: 'HTML' }, LOVABLE_API_KEY, TELEGRAM_API_KEY);
    return new Response(JSON.stringify({ ok: true, claimed: true, sent: 0, done: true }));
  }

  let sentInThisRun = 0;
  let index = currentState.line_index || 0;

  while (index < total && sentInThisRun < MAX_LINES_PER_RUN && Date.now() < deadline) {
    const stillActiveBeforeSend = await isRunStillActive(sb, currentState.user_id, currentState.mode, currentRaw);
    if (!stillActiveBeforeSend) {
      return new Response(JSON.stringify({ ok: true, claimed: true, sent: sentInThisRun, stopped: true }));
    }

    const line = lines[index].replace(/\{name\}/g, currentPayload.name);
    const sent = await tgSendMessage(
      { chat_id: currentPayload.chatId, text: line, parse_mode: 'HTML' },
      LOVABLE_API_KEY,
      TELEGRAM_API_KEY,
      8,
      () => isRunStillActive(sb, currentState.user_id, currentState.mode, currentRaw),
    );

    if (!sent) {
      const stillActiveAfterFailure = await isRunStillActive(sb, currentState.user_id, currentState.mode, currentRaw);
      if (!stillActiveAfterFailure) {
        return new Response(JSON.stringify({ ok: true, claimed: true, sent: sentInThisRun, stopped: true }));
      }
      break;
    }

    saveHistory(sb, currentState.user_id, currentState.mode, currentPayload.name, line);

    const progressed = await persistProgress(sb, currentState, currentRaw, currentPayload, index + 1);
    if (!progressed) {
      return new Response(JSON.stringify({ ok: true, claimed: true, sent: sentInThisRun + 1, stopped: true }));
    }

    currentState = progressed.state;
    currentPayload = progressed.payload;
    currentRaw = progressed.raw;
    index = currentState.line_index || 0;
    sentInThisRun++;
  }

  if ((currentState.line_index || 0) >= total) {
    const cleared = await clearState(sb, currentState, currentRaw);
    if (cleared) {
      await tgSendMessage({
        chat_id: currentPayload.chatId,
        text: '<b>✨ Saari lines finish ho gayi!</b> 😎',
        parse_mode: 'HTML',
        reply_markup: { inline_keyboard: [[{ text: '🔁 Naya Target', callback_data: 'restart' }]] },
      }, LOVABLE_API_KEY, TELEGRAM_API_KEY);
    }

    return new Response(JSON.stringify({ ok: true, claimed: true, sent: sentInThisRun, done: true }));
  }

  const released = await releaseLease(sb, currentState, currentRaw, currentPayload);
  if (released) {
    EdgeRuntime.waitUntil(triggerNextRun(workerUrl, SUPABASE_ANON_KEY, currentState.user_id, currentPayload.runId));
  }

  return new Response(JSON.stringify({
    ok: true,
    claimed: true,
    sent: sentInThisRun,
    nextIndex: currentState.line_index || 0,
    remaining: Math.max(0, total - (currentState.line_index || 0)),
  }));
});