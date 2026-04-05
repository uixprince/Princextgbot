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


# ===== index3.py =====

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

# ===== config.py =====

project_id = "gfyjcdbntyazwbzjddbt"

[functions.telegram-poll]
verify_jwt = false

[functions.logo-worker]
verify_jwt = false

