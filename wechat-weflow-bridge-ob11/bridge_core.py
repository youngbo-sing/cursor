"""
桥接核心模块：WeFlowBridge 类。

职责：
1. 连接 WeFlow SSE 推送，接收微信消息
2. 消息缓冲合并（BUFFER_SECONDS）
3. 构造 OneBot 事件，推送给 AstrBot
4. 多层消息去重（rawid、内容、自回复）
"""

import json
import logging
import os
import queue
import re
import threading
import time
from collections import defaultdict
from datetime import datetime

import requests

import state
import config
from ob_protocol import push_event, make_message_event

log = logging.getLogger("ob11-bridge")


# ============ 桥接核心 ============


class WeFlowBridge:
    """WeFlow ↔ AstrBot 桥接器（OneBot v11 版）。"""

    def __init__(self, sender):
        self.sender = sender
        self.processed_ids = set()
        self._processed_image_keys = set()  # 已处理过的图片 messageKey，防重复处理
        self._image_key_lock = threading.Lock()
        self.start_timestamp = int(time.time())
        self.pending_buffers = {}
        self.buffer_lock = threading.Lock()
        self.chat_histories = defaultdict(list)
        self.contact_map = {}
        self._sse_session = None
        self._recent_seen = {}
        self._sent_recently = {}
        self._sse_event_keys = {}
        self._pending_image = {}  # talkerId → {"caption": None|str, "event": threading.Event()}
        self._pending_mention_images = {}  # session_id → {"data": data, "time": timestamp} 先图后文暂存
        self._group_awake = {}  # session_id → 群内最后一条被接受消息的时间戳，用于 @触发后的一段时间免@回复

    def should_ignore(self, data):
        content = data.get("content", "")
        msg_type = data.get("type", 0) or data.get("msgType", 0)
        if data.get("sourceName", "") in config.BOT_NICKNAMES:
            return True
        if config.BOT_WXID and data.get("talkerId", "") == config.BOT_WXID:
            return True
        if msg_type in (34,):  # 34=语音
            return True
        if content and "[语音]" in content:
            return True
        if not content or content.strip() == "":
            return True
        return False

    def _is_group(self, data) -> bool:
        """WeFlow 私聊没有 groupName；群聊有 groupName，或 sessionId 带 @chatroom。"""
        session_id = data.get("sessionId", "") or ""
        if data.get("sessionType") == "group":
            return True
        if data.get("groupName"):
            return True
        if "@chatroom" in session_id:
            return True
        return False

    def _speaker_name(self, data) -> str:
        """群成员/私聊对方的显示名。"""
        return (
            data.get("senderName")
            or data.get("sender")
            or data.get("sourceName")
            or data.get("talkerName")
            or "未知"
        )

    def _clean_content(self, content: str) -> str:
        """清理群消息正文：剥掉前导的'发送者wxid:换行'之类的占位前缀。"""
        text = content or ""
        text = re.sub(r"^wxid\S+:\s*\n?", "", text)
        return text.strip()

    def _strip_at(self, content: str) -> str:
        """去掉正文里的 @机器人，只留真正要问的话。"""
        text = content or ""
        for nick in config.BOT_NICKNAMES:
            if nick:
                text = re.sub(rf"[@＠]\s*{re.escape(nick)}\s*", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def _is_mentioned(self, data):
        """检测群消息是否 @ 了机器人。

        WeFlow SSE 推送不含 @ 结构字段，只能从 content 文本检测。
        """
        content = data.get("content", "")
        if not content:
            return False

        for nick in config.BOT_NICKNAMES:
            if not nick:
                continue
            if f"@{nick}" in content or f"＠{nick}" in content:
                return True
            if re.search(rf"[@＠]\s*{re.escape(nick)}", content):
                return True

        if (content.startswith("@") or content.startswith("＠")) and len(content) > 1:
            log.info(f"⚠️ content 以@开头但未匹配昵称: content={content[:40]!r} nicknames={config.BOT_NICKNAMES}")

        return False

    def _is_group_awake(self, session_id_data) -> bool:
        """判断群是否处于唤醒状态（@触发后一段时间内免@回复）。"""
        last_active = self._group_awake.get(session_id_data)
        if last_active is None:
            return False
        return (time.time() - last_active) <= config.MENTION_AWAKE_TIMEOUT

    def _check_group_accept(self, session_id_data, mentioned: bool) -> bool:
        """mention 模式下：判断是否接受该条群消息，并维护群唤醒状态。

        mentioned=True: @了机器人，唤醒群。
        否则: 若群在唤醒期内则续杯接受，否则跳过。
        返回 True 表示接受，False 表示忽略。
        """
        if state.group_reply_mode != "mention":
            return True
        if mentioned:
            self._group_awake[session_id_data] = time.time()
            return True
        if self._is_group_awake(session_id_data):
            self._group_awake[session_id_data] = time.time()  # 唤醒期内消息续杯
            return True
        return False

    def add_to_buffer(self, data):
        """将消息加入缓冲区，等待合并后统一推送给 AstrBot。"""
        content = data.get("content", "")
        source_name = data.get("sourceName", "") or data.get("talkerName", "") or "未知"

        # 先判断群聊/私聊（图片/表情分支也需要用到）
        session_id_data = data.get("sessionId", "") or source_name
        group_name_raw = data.get("groupName", "")
        is_group = self._is_group(data)

        if content == "[图片]":
            # 图片消息（mention 模式下需 @ 或群在唤醒期才处理）
            if is_group and not self._check_group_accept(session_id_data, self._is_mentioned(data)):
                # 先图后文：暂存图片，等后续同人发 @ 文字时合并
                self._pending_mention_images[session_id_data] = {"data": data, "time": time.time()}
                log.info(f"📸 暂存图片，等待关联 @ 文字 (session={session_id_data})")
                return
            threading.Thread(target=self.process_image_message,
                           args=(data,), daemon=True).start()
            return

        if content in ("[动画表情]", "[表情]"):
            # 表情包消息（mention 模式下需 @ 或群在唤醒期才处理）
            if is_group and not self._check_group_accept(session_id_data, self._is_mentioned(data)):
                # 先图后文：暂存表情，等后续同人发 @ 文字时合并
                self._pending_mention_images[session_id_data] = {"data": data, "time": time.time()}
                log.info(f"😀 暂存表情，等待关联 @ 文字 (session={session_id_data})")
                return
            threading.Thread(target=self.process_emoji_message,
                           args=(data,), daemon=True).start()
            return

        now = time.time()
        if content and content in self._sent_recently and now - self._sent_recently[content] < 120:
            log.info(f"⏭️ 自回复去重跳过: {content[:30]}")
            return

        sender_in_group = self._speaker_name(data)

        if is_group:
            if not self._check_group_accept(session_id_data, self._is_mentioned(data)):
                log.info(f"⏭️ 群未唤醒且未 @ 机器人，跳过: [{sender_in_group}] {content[:40]}")
                return
            group_raw = group_name_raw or source_name
            base_name = re.sub(r'\s*\(\d+\)\s*$', '', group_raw).strip()
            contact = base_name
        else:
            contact = source_name

        if is_group and state.group_reply_mode == "batch":
            buffer_key = f"__batch__{base_name}"
        elif is_group and sender_in_group:
            buffer_key = f"{session_id_data}_{sender_in_group}"
        else:
            buffer_key = session_id_data

        with self.buffer_lock:
            if buffer_key not in self.pending_buffers:
                self.pending_buffers[buffer_key] = {
                    "messages": [],
                    "timer": None,
                    "timer_version": 0,
                    "processing": False,
                    "contact": contact,
                    "is_group": is_group,
                    "source_name": source_name,
                    "group_name": base_name if is_group else "",
                    "sender_in_group": sender_in_group if is_group else "",
                    "session_id_data": session_id_data,
                }
            entry = self.pending_buffers[buffer_key]
            if is_group:
                cleaned = self._clean_content(self._strip_at(content)) or content
            else:
                cleaned = content
            entry["messages"].append(cleaned)

            if not entry["processing"]:
                if entry["timer"]:
                    entry["timer"].cancel()
                entry["timer_version"] += 1
                version = entry["timer_version"]

                # 检查是否有暂存的图片（先图后文场景）
                has_pending_image = False
                if is_group and state.group_reply_mode == "mention":
                    cached = self._pending_mention_images.pop(session_id_data, None)
                    if cached and time.time() - cached["time"] < 15:
                        has_pending_image = True
                        log.info(f"📸 检测到关联图片，延长缓冲等待描述")
                        # 异步下载并描述图片，完成后注入 buffer
                        threading.Thread(
                            target=self._inject_cached_image,
                            args=(cached["data"].get("sessionId", session_id_data),
                                  buffer_key, version),
                            daemon=True,
                        ).start()

                if has_pending_image:
                    buffer_delay = 15
                elif cleaned and cleaned[-1] in "。！？!?…～~；;":
                    # 句尾标点：对方大概率说完，短缓冲提前冲刷
                    buffer_delay = 1.0
                else:
                    buffer_delay = config.BUFFER_SECONDS
                log.info(f"📩 收到来自 {contact} 的消息，等待 {buffer_delay}s 后统一推送")
                timer = threading.Timer(buffer_delay, lambda v=version, sid=buffer_key: self.process_sender(sid, v))
                timer.daemon = True
                timer.start()
                entry["timer"] = timer

    def process_sender(self, sender_id, version=None):
        """缓冲到期：通过 OneBot 事件推送给 AstrBot。"""
        with self.buffer_lock:
            if sender_id not in self.pending_buffers:
                return
            entry = self.pending_buffers[sender_id]
            if version is not None and entry.get("timer_version", 0) != version:
                return
            if not entry["messages"]:
                return
            msgs = entry["messages"].copy()
            entry["messages"] = []
            entry["processing"] = True
            if entry["timer"]:
                entry["timer"].cancel()
                entry["timer"] = None

        contact = entry.get("contact", sender_id)
        is_group = entry.get("is_group", False)
        combined = "\n".join(m for m in msgs if m).strip()
        log.info(f"推送 {len(msgs)} 条消息 [{'群' if is_group else '私'}|{contact}]")

        session_id_data = entry.get("session_id_data", sender_id)
        sender_name = entry.get("sender_in_group", "") or entry.get("source_name", "未知")
        speaker_wxid = entry.get("speaker_wxid", "")

        # 构建 OneBot 事件：user_id=发言人，group_id=群会话，正文保持原话
        if is_group:
            sender_key = speaker_wxid or sender_name
            user_id = state._wxid_to_int(sender_key)
            group_key = entry.get("group_name", contact) or session_id_data
            group_id = state._wxid_to_int(group_key)
            text = combined or "你好"

            # 群聊必须带 at 机器人，否则 AstrBot 不当成对自己说，不会回复
            msg_segments = [
                {"type": "at", "data": {"qq": str(state._self_id_int)}},
                {"type": "text", "data": {"text": f" {text}"}},
            ]
            event = make_message_event(
                "group", user_id, msg_segments,
                group_id=group_id,
                group_name=entry.get("group_name", contact),
                nickname=sender_name,
            )
            state._ob_id_to_contact[group_id] = contact
            state._ob_id_to_contact[user_id] = sender_name
            log.info(f"📤 群事件 group_id={group_id} user={sender_name}/{user_id} text={text[:40]}")
        else:
            user_id = state._wxid_to_int(speaker_wxid or session_id_data)
            sender_name = entry.get("source_name", contact)
            event = make_message_event(
                "private", user_id,
                [{"type": "text", "data": {"text": combined}}],
                nickname=sender_name,
            )
            state._ob_id_to_contact[user_id] = contact
            log.info(f"📤 私聊事件 user={sender_name}/{user_id} text={combined[:40]}")

        sent = push_event(event)
        if sent:
            log.info(f"✅ 已推送至 AstrBot [{contact}]")
        else:
            log.warning(f"⚠️ 无 AstrBot 客户端在线 [{contact}]")

        with self.buffer_lock:
            if sender_id in self.pending_buffers:
                self.pending_buffers[sender_id]["processing"] = False

    def listen_sse(self):
        """连接 WeFlow SSE 推送。"""
        sse_url = f"{config.WE_FLOW_BASE_URL}/api/v1/push/messages?access_token={config.ACCESS_TOKEN}"
        log.info(f"连接 WeFlow 推送服务: {sse_url}")
        headers = {"Accept": "text/event-stream", "Cache-Control": "no-cache"}

        try:
            self._sse_session = requests.get(sse_url, headers=headers, stream=True, timeout=None)
            if self._sse_session.status_code != 200:
                log.error(f"连接失败: HTTP {self._sse_session.status_code}")
                return
            log.info("✅ 已连接到 WeFlow 推送")

            for line in self._sse_session.iter_lines(decode_unicode=True):
                if not state.running:
                    break
                if not line:
                    continue
                if line.startswith("data:"):
                    data_str = line[5:].strip()
                    if not data_str:
                        continue
                    try:
                        data = json.loads(data_str)
                    except json.JSONDecodeError:
                        log.warning(f"SSE JSON 无法解析: {data_str[:200]}")
                        continue

                    # WeFlow 推送没有 timestamp；有才做历史过滤（兼容秒/毫秒）
                    msg_time = data.get("timestamp") or data.get("createTime") or 0
                    if msg_time:
                        if msg_time > 1e12:
                            msg_time = msg_time / 1000
                        if msg_time < self.start_timestamp:
                            continue

                    # WeFlow 用 messageKey 去重，旧字段 rawid 仅作兼容
                    raw_id = data.get("messageKey") or data.get("rawid") or ""
                    if raw_id:
                        if raw_id in self.processed_ids:
                            continue
                        self.processed_ids.add(raw_id)
                        if len(self.processed_ids) > 5000:
                            self.processed_ids = set(list(self.processed_ids)[-2500:])

                    if self.should_ignore(data):
                        log.info(
                            f"⏭️ 忽略: {data.get('sourceName', '')} → {str(data.get('content', ''))[:40]}"
                        )
                        continue

                    content = data.get("content", "")
                    is_group = (
                        data.get("sessionType", "") == "group"
                        or bool(data.get("groupName", ""))
                        or "@chatroom" in data.get("sessionId", "")
                    )
                    if is_group:
                        log.info(f"📩 群消息 [{data.get('sourceName','')}]: {content[:60]}")
                        if state.group_reply_mode == "mention":
                            mentioned = any(f"@{n}" in content for n in config.BOT_NICKNAMES)
                            log.info(f"   @={mentioned}")
                    else:
                        log.info(f"📩 收到: {data.get('sourceName','')} → {content[:50]}")
                    self.add_to_buffer(data)

        except requests.exceptions.ConnectionError:
            log.error("无法连接 WeFlow")
        except Exception as e:
            log.error(f"SSE 异常: {e}")
        finally:
            self._sse_session = None

    def _fetch_wechat_image(self, talker: str) -> str | None:
        """从 WeFlow REST API 获取最新图片并保存到本地"""
        try:
            url = f"{config.WE_FLOW_BASE_URL}/api/v1/messages"
            params = {
                "access_token": config.ACCESS_TOKEN,
                "talker": talker,
                "media": "true",
                "limit": 3,
            }
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                log.error(f"WeFlow 消息API: HTTP {resp.status_code}")
                return None

            data = resp.json()
            messages = data if isinstance(data, list) else data.get("messages", data.get("data", []))
            if not isinstance(messages, list):
                messages = []

            for msg in messages:
                if msg.get("mediaType") in ("image", "sticker", "emoji") and msg.get("mediaUrl"):
                    media_url = msg["mediaUrl"]
                    sep = "&" if "?" in media_url else "?"
                    dl_url = f"{media_url}{sep}access_token={config.ACCESS_TOKEN}"

                    img_resp = requests.get(dl_url, timeout=30)
                    if img_resp.status_code != 200:
                        continue

                    # 根据 Content-Type 确定扩展名
                    ct = img_resp.headers.get("Content-Type", "")
                    ext = ".jpg"
                    if "png" in ct: ext = ".png"
                    elif "gif" in ct: ext = ".gif"
                    elif "webp" in ct: ext = ".webp"

                    filename = f"wechat_{int(time.time())}{ext}"
                    save_dir = os.path.join(config.ASTRBOT_ATTACHMENTS, "wechat_images")
                    os.makedirs(save_dir, exist_ok=True)
                    save_path = os.path.join(save_dir, filename)

                    with open(save_path, "wb") as f:
                        f.write(img_resp.content)

                    log.info(f"✅ 微信图片已保存: {save_path}")
                    return save_path

            log.warning(f"消息列表无图片 mediaUrl (talker={talker})")
            return None
        except Exception as e:
            log.error(f"获取微信图片异常: {e}")
            return None

    def process_image_message(self, data):
        """处理图片消息：从 WeFlow 取图 → ollama 描述 → 注入缓冲区"""
        session_id = data.get("sessionId", "")
        source_name = data.get("sourceName", "") or "未知"
        group_name = data.get("groupName", "")
        rawid = data.get("rawid", "")
        msg_key = data.get("messageKey", "") or data.get("rawid", "") or ""

        # 按唯一 messageKey 去重，防止 WeFlow SSE 重复投递导致重复处理
        if msg_key:
            with self._image_key_lock:
                if msg_key in self._processed_image_keys:
                    log.info(f"⏭️ 图片消息重复，跳过 (key={msg_key[:40]})")
                    return
                self._processed_image_keys.add(msg_key)
                # 限长，避免无限增长
                if len(self._processed_image_keys) > 5000:
                    self._processed_image_keys.clear()

        log.info(f"🖼️ 收到图片: {source_name}" +
                 (f" (群:{group_name})" if group_name else ""))

        talker_id = data.get("talkerId", "") or data.get("sessionId", "")
        is_group = bool(group_name) or "@chatroom" in session_id

        # 注册待处理的图片（ollama 完成前标记为 pending）
        img_event = threading.Event()
        self._pending_image[talker_id] = {"caption": None, "event": img_event}

        try:
            # 取图 + ollama 描述
            image_path = self._fetch_wechat_image(session_id)
            caption = None
            if image_path:
                caption = caption_image_via_ollama(image_path)

            caption_text = caption if caption else None
            if caption_text:
                log.info(f"📝 图片描述: {caption_text[:60]}...")
            else:
                log.info("⚠️ 图片描述失败")
                caption_text = "（图片内容无法描述）"

            # 注入图片描述到缓冲区
            with self.buffer_lock:
                self._pending_image[talker_id] = {"caption": caption_text, "event": img_event}

                # 批处理模式用群共享 key
                if is_group and state.group_reply_mode == "batch" and group_name:
                    g_base = re.sub(r'\s*\(\d+\)\s*$', '', group_name).strip()
                    batch_key = f"__batch__{g_base}"
                    if batch_key in self.pending_buffers:
                        entry = self.pending_buffers[batch_key]
                        entry["messages"].insert(0, f'成员"{source_name}"在群"{group_name}"中对你说：[图片: {caption_text}]')
                        entry["image_ready"] = True
                        log.info(f"📝 图片已注入批处理队列")
                        return
                    # 没有文字排队，用 batch key 创建独立条目
                    self.pending_buffers[batch_key] = {
                        "messages": [f'成员"{source_name}"在群"{group_name}"中对你说：[图片: {caption_text}]'],
                        "timer": None,
                        "timer_version": 0,
                        "processing": False,
                        "contact": group_name,
                        "is_group": True,
                        "source_name": source_name,
                        "session_id_data": session_id,
                        "group_name": group_name,
                        "sender_in_group": source_name,
                    }
                    log.info(f"📩 图片无文本跟随，创建批处理图片条目")
                    version = 1
                    timer = threading.Timer(5, lambda v=version, sid=batch_key: self.process_sender(sid, v))
                    timer.daemon = True
                    timer.start()
                    self.pending_buffers[batch_key]["timer"] = timer
                    self.pending_buffers[batch_key]["timer_version"] = version
                elif talker_id in self.pending_buffers:
                    # 已有文本在排队，注入图片上下文
                    entry = self.pending_buffers[talker_id]
                    entry["messages"].insert(0, f"[图片: {caption_text}]")
                    entry["image_ready"] = True
                    log.info(f"📝 图片已注入待处理文本队列")
                    # 若该缓冲无活跃 timer 且未在 processing，补一个 timer 保证推送
                    if not entry.get("processing") and not entry.get("timer"):
                        entry["timer_version"] += 1
                        version = entry["timer_version"]
                        tid_later = talker_id
                        timer = threading.Timer(1.5, lambda v=version, sid=tid_later: self.process_sender(sid, v))
                        timer.daemon = True
                        timer.start()
                        entry["timer"] = timer
                        log.info(f"📤 检测到无活跃 timer，补充推送计时器 ({talker_id})")
                else:
                    # 没有文本排队，创建单条图片消息处理
                    log.info(f"📩 图片无文本跟随，直接处理")
                    self.pending_buffers[talker_id] = {
                        "messages": [f"[图片: {caption_text}]"],
                        "timer": None,
                        "timer_version": 0,
                        "processing": False,
                        "contact": group_name if is_group and group_name else source_name,
                        "is_group": is_group,
                        "source_name": source_name,
                        "session_id_data": session_id,
                        "group_name": group_name if is_group else "",
                        "sender_in_group": "",
                    }
                    version = 1
                    timer = threading.Timer(2, lambda v=version, sid=talker_id: self.process_sender(sid, v))
                    timer.daemon = True
                    timer.start()
                    self.pending_buffers[talker_id]["timer"] = timer
                    self.pending_buffers[talker_id]["timer_version"] = version
        finally:
            # 确保 Event 被设置
            img_event.set()

    def process_emoji_message(self, data):
        """处理表情包消息：尝试下载图片并描述，失败则保留原文转发"""
        session_id = data.get("sessionId", "")
        source_name = data.get("sourceName", "") or "未知"
        group_name = data.get("groupName", "")
        content = data.get("content", "[表情]")

        log.info(f"😀 收到表情包: {source_name}" +
                 (f" (群:{group_name})" if group_name else ""))

        talker_id = data.get("talkerId", "") or data.get("sessionId", "")
        is_group = bool(group_name) or "@chatroom" in session_id

        # 尝试下载图片描述
        try:
            image_path = self._fetch_wechat_image(session_id)
            if image_path:
                caption = caption_image_via_ollama(image_path)
                if caption:
                    content = f"[表情: {caption}]"
                    log.info(f"😀 表情包已描述: {caption[:60]}...")
                else:
                    log.info("😀 表情包图片描述失败，保留原文")
            else:
                log.info("😀 表情包无可用图片，保留原文")

            # 直接注入缓冲区（不等待，立即推送）
            self.add_text_to_buffer(talker_id, source_name, group_name,
                                    session_id, content, is_group, talker_id)
        except Exception as e:
            log.warning(f"😀 表情包处理异常: {e}")
            # 异常时也保底发送原文
            self.add_text_to_buffer(talker_id, source_name, group_name,
                                    session_id, content, is_group, talker_id)

    def _inject_cached_image(self, session_id, buffer_key, version):
        """下载缓存图片 → 描述 → 注入到 buffer 条目（在缓冲计时器到期前完成）"""
        try:
            img_path = self._fetch_wechat_image(session_id)
            caption = None
            if img_path:
                caption = caption_image_via_ollama(img_path)
            text = f"[图片: {caption or '无法描述'}]"

            with self.buffer_lock:
                if buffer_key in self.pending_buffers:
                    entry = self.pending_buffers[buffer_key]
                    # 版本匹配才注入（版本变了说明被新消息重置过）
                    if entry.get("timer_version") == version:
                        entry["messages"].insert(0, text)
                        log.info(f"📸 缓存图片已注入: {text[:60]}")
                    else:
                        log.info(f"📸 缓存图片跳过（buffer 版本已变更）")
        except Exception as e:
            log.warning(f"📸 缓存图片处理异常: {e}")

    def add_text_to_buffer(self, session_id_data, source_name, group_name,
                           session_id, content, is_group, sender_key):
        """通用：将一段文本直接加入缓冲队列（供表情/图片等异步处理完后调用）"""
        with self.buffer_lock:
            buffer_key = sender_key
            if buffer_key not in self.pending_buffers:
                self.pending_buffers[buffer_key] = {
                    "messages": [],
                    "timer": None,
                    "timer_version": 0,
                    "processing": False,
                    "contact": group_name if is_group and group_name else source_name,
                    "is_group": is_group,
                    "source_name": source_name,
                    "session_id_data": session_id,
                    "group_name": group_name if is_group else "",
                    "sender_in_group": source_name if is_group else "",
                }
            entry = self.pending_buffers[buffer_key]
            entry["messages"].append(content)

            if not entry["processing"]:
                if entry["timer"]:
                    entry["timer"].cancel()
                entry["timer_version"] += 1
                version = entry["timer_version"]
                delay = 2  # 表情/图片单独推送，短缓冲
                timer = threading.Timer(delay, lambda v=version, sid=buffer_key: self.process_sender(sid, v))
                timer.daemon = True
                timer.start()
                entry["timer"] = timer
                entry["timer_version"] = version
def caption_image_via_ollama(image_path: str) -> str | None:
    """对图片进行文字描述，支持 ollama 和 OpenAI 兼容 API 两种后端。"""
    try:
        import base64
        with open(image_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode("utf-8")

        if state.image_caption_provider == "openai":
            if not state.image_caption_api_base:
                log.warning("⚠️ provider=openai 但未配置 api_base，跳过图片描述")
                return None
            # OpenAI 兼容 API
            resp = requests.post(
                f"{state.image_caption_api_base}/chat/completions",
                headers={
                    "Authorization": f"Bearer {state.image_caption_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": state.image_caption_model,
                    "messages": [{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": state.image_caption_prompt},
                            {"type": "image_url", "image_url": {
                                "url": f"data:image/jpeg;base64,{img_b64}"
                            }},
                        ],
                    }],
                    "max_tokens": 800,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                caption = resp.json()["choices"][0]["message"]["content"].strip()
                if caption:
                    log.info(f"🖼️ 图片描述: {caption[:80]}...")
                    return caption
                log.warning(f"OpenAI API 返回 200 但内容为空: {str(resp.json())[:200]}")
            else:
                log.warning(f"OpenAI API 返回 HTTP {resp.status_code}: {resp.text[:200]}")
        else:
            # ollama 原生 API
            resp = requests.post(
                f"{state.ollama_base_url}/api/generate",
                json={
                    "model": state.image_caption_model,
                    "prompt": state.image_caption_prompt,
                    "images": [img_b64],
                    "stream": False,
                },
                timeout=state.ollama_timeout,
            )
            if resp.status_code == 200:
                caption = resp.json().get("response", "").strip()
                if caption:
                    log.info(f"🖼️ 图片描述: {caption[:80]}...")
                    return caption
            else:
                log.warning(f"ollama 返回 HTTP {resp.status_code}: {resp.text[:100]}")

    except requests.Timeout:
        log.warning(f"图片描述超时 (30s)")
    except Exception as e:
        log.warning(f"图片描述失败: {e}")
    return None
