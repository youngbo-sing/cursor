"""
桥接核心模块：WeFlowBridge 类。

职责：
1. 连接 WeFlow SSE 推送，接收微信消息
2. 消息缓冲合并（BUFFER_SECONDS）
3. 构造 OneBot 事件，推送给 AstrBot
4. 多层消息去重（messageKey、图片 messageKey）
"""

import json
import logging
import os
import random
import re
import threading
import time
import uuid

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
        self._part_seq = 0  # 同一缓冲内按到达顺序编号，供图片/表情与文字排序
        self.start_timestamp = int(time.time())
        self.pending_buffers = {}
        self.buffer_lock = threading.Lock()
        self._sse_session = None
        self._pending_mention_images = {}  # session_id → sender_key → [{"data": data, "time": ...}]
        self._group_awake = {}  # session_id → 群内最后一条被接受消息的时间戳，用于 @触发后的一段时间免@回复
        self._group_awake_start = {}  # session_id → 当前唤醒期开始时间
        self._group_replied_at = {}  # session_id → 最近一次 AstrBot 成功回复该群的时间
        self._ob_group_session = {}  # OneBot group_id → WeFlow sessionId

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

    def _sender_wxid(self, data) -> str:
        """提取发言人的稳定 wxid。

        WeFlow 群消息没有独立的发言人 wxid 字段：文本 content 会以
        "wxid_xxx:\n" 开头，图片/表情则把 wxid 编进 messageKey，
        两种都会在推送里出现。
        """
        wxid = data.get("talkerId", "") or ""
        if wxid:
            return str(wxid).strip()
        match = re.match(r"^(wxid_[^\s:]+):\s*\n?", data.get("content", "") or "")
        if match:
            return match.group(1)
        raw_key = data.get("messageKey") or data.get("rawid") or ""
        key_parts = str(raw_key).split(":")
        if len(key_parts) >= 3 and key_parts[-2].startswith("wxid_"):
            return key_parts[-2]
        return ""

    def _mention_sender_key(self, data) -> str:
        """先图后文缓存的群内发送者 key：优先 wxid，缺省退回显示名。"""
        wxid = self._sender_wxid(data)
        return wxid or f"name:{self._speaker_name(data)}"

    def _queue_key(self, data, session_id_data, is_group) -> str:
        """文字与图片/表情共用同一个缓冲 key。

        WeFlow 图片/表情没有顶层 talkerId，旧实现分别按 talkerId 和昵称
        查找队列导致图+文经常命中失败；这里群内统一按 sessionId+发送者归类，
        batch 模式整群共用一个队列，私聊直接用 sessionId。
        """
        if is_group and state.group_reply_mode == "batch":
            return f"__batch__{session_id_data}"
        sender_key = self._sender_wxid(data) or self._speaker_name(data)
        if is_group and sender_key:
            return f"{session_id_data}|{sender_key}"
        return session_id_data

    def _create_buffer_entry(self, contact, is_group, source_name, group_name,
                             session_id_data, sender_in_group="", speaker_wxid=""):
        """构建缓冲条目，统一带上群文本合并轮次所需字段。"""
        return {
            "parts": [],  # 有序内容：text 即刻就绪；image/emoji 先占位，描述完成后置 done
            "timer": None,
            "timer_version": 0,
            "timer_kind": None,  # text / media_tail / media_catchup
            "deadline_timer": None,
            "deadline_version": 0,
            "processing": False,
            "media_batch_active": False,
            "media_batch_passed": False,
            "media_batch_start": 0.0,
            "media_running": 0,
            "contact": contact,
            "is_group": is_group,
            "source_name": source_name,
            "group_name": group_name,
            "sender_in_group": sender_in_group,
            "session_id_data": session_id_data,
            "speaker_wxid": speaker_wxid,
            "merge_received": 0,
            "merge_stage": None,
        }

    def _group_text_payload(self, content: str) -> str:
        """群文本正文：先去掉 @机器人；裁剪为空时退回去掉 wxid 前缀的原文。"""
        cleaned = self._clean_content(self._strip_at(content))
        if cleaned:
            return cleaned
        return self._clean_content(content)

    def _group_first_wait_seconds(self, session_id_data) -> float:
        """首等时长：机器人已回复过的唤醒群使用加权随机值，其余固定 5 秒。"""
        reply_time = self._group_replied_at.get(session_id_data, 0.0)
        if state.group_reply_mode == "mention":
            if not self._is_group_awake(session_id_data):
                return config.MERGE_WAIT_FIRST_SECONDS
            wake_start = self._group_awake_start.get(session_id_data, 0.0)
            if reply_time < wake_start:
                return config.MERGE_WAIT_FIRST_SECONDS
        elif not reply_time:
            return config.MERGE_WAIT_FIRST_SECONDS
        try:
            return random.choices(
                config.MERGE_WAIT_RANDOM_VALUES,
                weights=config.MERGE_WAIT_RANDOM_WEIGHTS,
                k=1,
            )[0]
        except (IndexError, ValueError):
            return config.MERGE_WAIT_FIRST_SECONDS

    def _flush_other_group_text_buffers(self, session_id_data, exclude_key):
        """换人打断：同一群内其他成员已就绪的文字/图片立即推送给 AstrBot。"""
        targets = []
        with self.buffer_lock:
            for key, entry in list(self.pending_buffers.items()):
                if key == exclude_key:
                    continue
                if entry.get("session_id_data") != session_id_data:
                    continue
                if not entry.get("is_group"):
                    continue
                if entry.get("processing"):
                    continue
                if not self._entry_has_ready(entry):
                    continue
                targets.append(key)
        for key in targets:
            self.process_sender(key, force=True)

    def _entry_has_ready(self, entry) -> bool:
        """缓冲里是否有可立即推送的内容（文字始终就绪，媒体描述完成后才就绪）。"""
        return any(
            part.get("kind") == "text" or part.get("done")
            for part in entry.get("parts", [])
        )

    def _has_undone_media(self, entry) -> bool:
        return any(
            part.get("kind") in ("image", "emoji") and not part.get("done")
            for part in entry.get("parts", [])
        )

    def _cancel_timer_locked(self, entry):
        if entry.get("timer"):
            entry["timer"].cancel()
        entry["timer"] = None
        entry["timer_kind"] = None
        entry["timer_version"] += 1

    def _cancel_deadline_locked(self, entry):
        if entry.get("deadline_timer"):
            entry["deadline_timer"].cancel()
        entry["deadline_timer"] = None
        entry["deadline_version"] += 1

    def _schedule_buffer_flush_locked(self, buffer_key, delay, deadline=False, kind=None):
        """设置普通合并/补推计时器，或设置媒体批 12 秒强刷计时器。

        调用方必须已持有 buffer_lock。
        """
        entry = self.pending_buffers.get(buffer_key)
        if entry is None or entry.get("processing"):
            return
        if deadline:
            if entry.get("deadline_timer"):
                entry["deadline_timer"].cancel()
            entry["deadline_version"] += 1
            version = entry["deadline_version"]
            timer = threading.Timer(
                delay,
                lambda v=version, sid=buffer_key: self.process_sender(sid, v, deadline=True),
            )
            timer.daemon = True
            timer.start()
            entry["deadline_timer"] = timer
            return
        if entry.get("timer"):
            entry["timer"].cancel()
        entry["timer_version"] += 1
        version = entry["timer_version"]
        entry["timer_kind"] = kind
        timer = threading.Timer(
            delay,
            lambda v=version, sid=buffer_key, tk=kind: self.process_sender(sid, v, kind=tk),
        )
        timer.daemon = True
        timer.start()
        entry["timer"] = timer

    def _end_media_batch_locked(self, entry):
        """媒体批已全部推送或强刷结束时清理批状态。"""
        self._cancel_deadline_locked(entry)
        entry["media_batch_active"] = False
        entry["media_batch_passed"] = False
        entry["media_batch_start"] = 0.0

    def _append_media_part_locked(self, buffer_key, data, kind, front=False):
        """把图片/表情作为待描述占位加入缓冲，并维护当前媒体批。"""
        session_id_data = data.get("sessionId", "") or data.get("sourceName", "") or "未知"
        is_group = self._is_group(data)
        batch_mode = is_group and state.group_reply_mode == "batch"
        now = time.time()
        if buffer_key not in self.pending_buffers:
            source_name = data.get("sourceName", "") or data.get("talkerName", "") or "未知"
            group_raw = (data.get("groupName", "") or "").strip() or source_name
            self.pending_buffers[buffer_key] = self._create_buffer_entry(
                contact=group_raw if is_group else source_name,
                is_group=is_group,
                source_name=source_name,
                group_name=group_raw if is_group else "",
                session_id_data=session_id_data,
                sender_in_group=self._speaker_name(data) if is_group else "",
                speaker_wxid=self._sender_wxid(data) if is_group else "",
            )
        entry = self.pending_buffers[buffer_key]
        # 媒体插入后由媒体批接管：取消仍在排队的文字/收尾/补推计时器
        if entry.get("timer"):
            self._cancel_timer_locked(entry)
        self._part_seq += 1
        part = {
            "seq": self._part_seq,
            "kind": kind,
            "done": False,
            "reserved": False,
            "text": None,
            "media_data": data,
            "raw_content": data.get("content", ""),
            "batch_wrap": batch_mode and kind == "image",
            "source_name": data.get("sourceName", "") or data.get("talkerName", "") or "未知",
            "group_name": (data.get("groupName", "") or "").strip(),
            "sender_wxid": self._sender_wxid(data),
        }
        if front:
            entry["parts"].insert(0, part)
        else:
            entry["parts"].append(part)

        if not entry["media_batch_active"]:
            entry["media_batch_active"] = True
            entry["media_batch_passed"] = False
            entry["media_batch_start"] = now
            self._schedule_buffer_flush_locked(
                buffer_key, config.MEDIA_BATCH_MAX_SECONDS, deadline=True
            )
        elif entry["media_batch_passed"]:
            # 强刷点之后再来媒体就开新一批：旧的在途描述仍可并入新窗口一起推
            entry["media_batch_passed"] = False
            entry["media_batch_start"] = now
            self._schedule_buffer_flush_locked(
                buffer_key, config.MEDIA_BATCH_MAX_SECONDS, deadline=True
            )
        self._kick_media_worker_locked(entry, buffer_key)

    def _kick_media_worker_locked(self, entry, buffer_key):
        """同一 key 同一时间只跑一个媒体 worker，串行取图+描述。"""
        if entry.get("media_running"):
            return
        if not self._has_undone_media(entry):
            return
        entry["media_running"] = 1
        thread = threading.Thread(target=self._media_worker_loop, args=(buffer_key,), daemon=True)
        thread.start()

    def mark_group_replied(self, group_id: int):
        """AstrBot 成功向某群发送回复后记录时间，供随机首等使用。"""
        session_id = self._ob_group_session.get(group_id, "")
        if session_id:
            self._group_replied_at[session_id] = time.time()

    def _cleanup_pending_mention_images(self):
        """清理所有已过期、等待 @ 文字关联的图片/表情，避免无 @ 消息占内存。"""
        now = time.time()
        empty_sessions = []
        for session_id, per_session in list(self._pending_mention_images.items()):
            empty_senders = []
            for key, items in list(per_session.items()):
                fresh = [item for item in items if now - item.get("time", 0) <= 15]
                if fresh:
                    per_session[key] = fresh
                else:
                    empty_senders.append(key)
            for key in empty_senders:
                del per_session[key]
            if not per_session:
                empty_sessions.append(session_id)
        for session_id in empty_sessions:
            self._pending_mention_images.pop(session_id, None)

    def _store_mention_image(self, session_id, data):
        """按群+发送者暂存未关联文字的图片/表情，支持同一人连续多张。"""
        now = time.time()
        per_session = self._pending_mention_images.setdefault(session_id, {})
        sender_key = self._mention_sender_key(data)
        per_session.setdefault(sender_key, []).append({
            "data": data,
            "time": now,
            "sender_wxid": self._sender_wxid(data),
            "sender_name": self._speaker_name(data),
        })

    def _take_mention_images(self, session_id, data):
        """取出当前发言人的全部暂存图片/表情；不匹配则保留给本人。"""
        now = time.time()
        per_session = self._pending_mention_images.get(session_id)
        if not per_session:
            return []
        sender_key = self._mention_sender_key(data)
        items = per_session.pop(sender_key, [])
        fresh = [item for item in items if now - item.get("time", 0) <= 15]
        if not per_session:
            self._pending_mention_images.pop(session_id, None)
        return fresh

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
        now = time.time()
        if mentioned:
            if not self._is_group_awake(session_id_data):
                # 从休眠被 @ 唤醒：新唤醒期开始时清零随机首等资格
                self._group_awake_start[session_id_data] = now
            self._group_awake[session_id_data] = now
            return True
        if self._is_group_awake(session_id_data):
            self._group_awake[session_id_data] = now  # 唤醒期内消息续杯
            return True
        return False

    def add_to_buffer(self, data):
        """将消息加入缓冲区，等待合并后统一推送给 AstrBot。"""
        self._cleanup_pending_mention_images()
        content = data.get("content", "")
        source_name = data.get("sourceName", "") or data.get("talkerName", "") or "未知"

        # 先判断群聊/私聊（图片/表情分支也需要用到）
        session_id_data = data.get("sessionId", "") or source_name
        group_name_raw = data.get("groupName", "")
        is_group = self._is_group(data)
        buffer_key = self._queue_key(data, session_id_data, is_group)
        batch_group = is_group and state.group_reply_mode == "batch"
        interrupt_on_new_speaker = is_group and not batch_group
        accepted = self._check_group_accept(session_id_data, self._is_mentioned(data))

        media_kind = self._media_kind(data)
        if media_kind:
            if is_group and not accepted:
                # 先图后文：暂存图片/表情，等后续本人发 @ 文字时合并
                self._store_mention_image(session_id_data, data)
                label = "图片" if media_kind == "image" else "表情"
                log.info(f"📸 暂存 {source_name} 的{label}，等待本人 @ 文字 (session={session_id_data})")
                return
            if interrupt_on_new_speaker:
                self._flush_other_group_text_buffers(session_id_data, buffer_key)
            with self.buffer_lock:
                self._append_media_part_locked(buffer_key, data, media_kind)
            return

        sender_in_group = self._speaker_name(data)
        speaker_wxid = self._sender_wxid(data) if is_group else ""

        if is_group:
            if not accepted:
                log.info(f"⏭️ 群未唤醒且未 @ 机器人，跳过: [{sender_in_group}] {content[:40]}")
                return
            group_raw = group_name_raw or source_name
            contact = group_raw.strip()
        else:
            contact = source_name

        if interrupt_on_new_speaker:
            self._flush_other_group_text_buffers(session_id_data, buffer_key)

        if is_group:
            cleaned = self._group_text_payload(content)
        else:
            cleaned = content

        immediate_flush = False
        with self.buffer_lock:
            if buffer_key not in self.pending_buffers:
                self.pending_buffers[buffer_key] = self._create_buffer_entry(
                    contact=contact,
                    is_group=is_group,
                    source_name=source_name,
                    group_name=contact if is_group else "",
                    session_id_data=session_id_data,
                    sender_in_group=sender_in_group if is_group else "",
                    speaker_wxid=speaker_wxid,
                )
            entry = self.pending_buffers[buffer_key]
            if is_group:
                if not entry.get("speaker_wxid"):
                    entry["speaker_wxid"] = speaker_wxid

            # 本人“先图后文”的暂存媒体：文字到达后转入同一条 parts 队列
            media_added = False
            if is_group and not batch_group:
                before_count = len(entry["parts"])
                cached_items = self._take_mention_images(session_id_data, data)
                for cached in cached_items:
                    cached_data = cached.get("data") or {}
                    cached_kind = self._media_kind(cached_data) or "image"
                    self._append_media_part_locked(buffer_key, cached_data, cached_kind)
                media_added = len(entry["parts"]) > before_count
                if media_added:
                    log.info(
                        f"📸 关联 {sender_in_group} 的 {len(cached_items)} 条暂存媒体，进入媒体批"
                    )

            if not cleaned:
                if media_added:
                    log.info(f"⏭️ 空 @ 正文：仅关联暂存媒体后等待描述 (session={session_id_data})")
                else:
                    log.info(f"⏭️ 群消息裁剪后为空且无原文，跳过: [{sender_in_group}]")
                return

            immediate_flush = self._append_text_part_locked(buffer_key, cleaned)

        if immediate_flush:
            log.info(f"📨 来自 {contact} 的第 3 段消息在短等待窗口内到达，立即合并推送")
            self.process_sender(buffer_key, force=True)

    def _media_kind(self, data) -> str | None:
        content = data.get("content", "") or ""
        if content == "[图片]":
            return "image"
        if content in ("[动画表情]", "[表情]"):
            return "emoji"
        return None

    def _append_text_part_locked(self, buffer_key, text) -> bool:
        """把文本追加进 parts；返回 True 表示调用方应立即在锁外冲刷。"""
        entry = self.pending_buffers.get(buffer_key)
        if entry is None:
            return False
        is_group = entry.get("is_group", False)
        batch_mode = is_group and state.group_reply_mode == "batch"

        self._part_seq += 1
        entry["parts"].append({
            "seq": self._part_seq,
            "kind": "text",
            "done": True,
            "reserved": False,
            "text": text,
            "media_data": None,
            "raw_content": text,
            "batch_wrap": False,
            "source_name": entry.get("source_name", "") or "未知",
            "group_name": entry.get("group_name", "") or "",
            "sender_wxid": entry.get("speaker_wxid", ""),
        })

        if is_group and not batch_mode:
            entry["merge_received"] = entry.get("merge_received", 0) + 1
            entry["merge_stage"] = "first" if entry["merge_received"] == 1 else "next"

        if entry.get("processing"):
            return False

        batch_live = entry.get("media_batch_active") and not entry.get("media_batch_passed")
        if batch_live:
            if self._has_undone_media(entry):
                # 媒体仍在描述中：文字只合并，交给媒体批的 12s/收尾计时
                self._cancel_timer_locked(entry)
                return False
            # 媒体已全部描述完，2s 收尾期间来了文字：切回正常文字合并轮次
            self._cancel_timer_locked(entry)
            self._end_media_batch_locked(entry)

        if self._has_undone_media(entry):
            # 媒体批已被强刷或换人打断冲刷过，剩余媒体稍后单独补推
            return False

        return self._schedule_text_flush_locked(buffer_key)

    def _schedule_text_flush_locked(self, buffer_key) -> bool:
        """按群 5/3/1 或私聊简单缓冲安排文本推送；返回 True 表示立即冲刷。"""
        entry = self.pending_buffers.get(buffer_key)
        if entry is None or entry.get("processing"):
            return False
        is_group = entry.get("is_group", False)
        batch_mode = is_group and state.group_reply_mode == "batch"

        last_text = ""
        for part in reversed(entry.get("parts", [])):
            if part.get("kind") == "text" and part.get("text"):
                last_text = part["text"]
                break
        ends_punct = bool(last_text and last_text[-1] in "。！？!?…～~；;")

        if not is_group or batch_mode:
            delay = (
                config.MERGE_DONE_PUNCT_SECONDS
                if ends_punct
                else config.BUFFER_SECONDS
            )
            self._schedule_buffer_flush_locked(buffer_key, delay, kind="text")
            return False

        received = entry.get("merge_received", 0)
        stage = entry.get("merge_stage") or ("first" if received <= 1 else "next")
        if stage == "next" and received >= 3:
            return True
        if stage == "first":
            delay = (
                config.MERGE_DONE_PUNCT_SECONDS
                if ends_punct
                else self._group_first_wait_seconds(entry.get("session_id_data", ""))
            )
        else:
            delay = (
                config.MERGE_DONE_PUNCT_SECONDS
                if ends_punct
                else config.MERGE_WAIT_NEXT_SECONDS
            )
        self._schedule_buffer_flush_locked(buffer_key, delay, kind="text")
        return False

    def _after_flush_locked(self, buffer_key) -> bool:
        """推送结束后在锁内重新检查残留 parts，安排收尾/补推/文字轮次。"""
        entry = self.pending_buffers.get(buffer_key)
        if entry is None:
            return False
        if self._has_undone_media(entry):
            if not entry.get("media_batch_active") or entry.get("media_batch_passed"):
                return False
            if not entry.get("deadline_timer"):
                remaining = max(
                    0.1,
                    entry.get("media_batch_start", time.time())
                    + config.MEDIA_BATCH_MAX_SECONDS
                    - time.time(),
                )
                self._schedule_buffer_flush_locked(buffer_key, remaining, deadline=True)
            return False

        self._end_media_batch_locked(entry)
        if not entry.get("parts"):
            return False
        if any(
            part.get("kind") in ("image", "emoji") and part.get("done")
            for part in entry["parts"]
        ) and not any(part.get("kind") == "text" for part in entry["parts"]):
            self._schedule_buffer_flush_locked(
                buffer_key, config.MEDIA_CATCHUP_SECONDS, kind="media_catchup"
            )
            return False
        return self._schedule_text_flush_locked(buffer_key)

    def process_sender(self, sender_id, version=None, deadline=False, force=False, kind=None):
        """缓冲到期/打断/强刷：取走已就绪 parts，构造 OneBot 事件推送给 AstrBot。"""
        with self.buffer_lock:
            entry = self.pending_buffers.get(sender_id)
            if entry is None or entry.get("processing"):
                return
            if deadline:
                if version is not None and entry.get("deadline_version", 0) != version:
                    return
            elif version is not None and entry.get("timer_version", 0) != version:
                return
            self._cancel_timer_locked(entry)

            ready_parts = []
            for part in entry.get("parts", []):
                if part.get("kind") == "text" or part.get("done"):
                    if kind == "media_catchup" and part.get("kind") == "text":
                        continue
                    ready_parts.append(part)
            if not ready_parts:
                if self._has_undone_media(entry):
                    if force or deadline or entry.get("media_batch_passed") or kind == "media_catchup":
                        entry["media_batch_passed"] = True
                        self._cancel_deadline_locked(entry)
                else:
                    self._end_media_batch_locked(entry)
                return

            entry["processing"] = True
            ready_ids = {id(part) for part in ready_parts}
            entry["parts"] = [
                part for part in entry.get("parts", [])
                if id(part) not in ready_ids
            ]
            if any(part.get("kind") == "text" for part in ready_parts):
                entry["merge_received"] = 0
                entry["merge_stage"] = None

            left_undone = self._has_undone_media(entry)
            if not left_undone:
                self._end_media_batch_locked(entry)
            elif force or deadline or entry.get("media_batch_passed") or kind == "media_catchup":
                # 强刷/打断已经冲刷过正文：剩余媒体由 worker 完成后单独补推
                entry["media_batch_passed"] = True
                self._cancel_deadline_locked(entry)

            parts_out = sorted(ready_parts, key=lambda part: part.get("seq", 0))
            contact = entry.get("contact", sender_id)
            is_group = entry.get("is_group", False)
            session_id_data = entry.get("session_id_data", sender_id)
            source_name = entry.get("source_name", "未知")
            sender_in_group = entry.get("sender_in_group", "")
            speaker_wxid = entry.get("speaker_wxid", "")
            group_name = entry.get("group_name", "") or contact

        lines = []
        for part in parts_out:
            if part.get("kind") == "text":
                lines.append(part.get("text") or "")
            elif part.get("done"):
                lines.append(part.get("text") or "")
        combined = "\n".join(line for line in lines if line).strip()
        if not combined:
            log.warning(f"⚠️ {sender_id} 无可推正文，跳过空事件")
            with self.buffer_lock:
                if sender_id in self.pending_buffers:
                    self.pending_buffers[sender_id]["processing"] = False
                return

        log.info(f"推送 {len(parts_out)} 条消息 [{'群' if is_group else '私'}|{contact}]")
        sender_name = sender_in_group or source_name

        if is_group:
            if speaker_wxid:
                sender_key = speaker_wxid
            elif session_id_data:
                sender_key = f"{session_id_data}|{sender_name}"
            else:
                sender_key = sender_name
            user_id = state._wxid_to_int(sender_key)
            group_id = state._wxid_to_int(session_id_data or contact)
            text = combined or "你好"

            # 群聊必须带 at 机器人，否则 AstrBot 不当成对自己说，不会回复
            msg_segments = [
                {"type": "at", "data": {"qq": str(state._self_id_int)}},
                {"type": "text", "data": {"text": f" {text}"}},
            ]
            event = make_message_event(
                "group", user_id, msg_segments,
                group_id=group_id,
                group_name=group_name,
                nickname=sender_name,
            )
            state._ob_id_to_contact[group_id] = group_name
            state._ob_id_to_contact[user_id] = sender_name
            self._ob_group_session[group_id] = session_id_data or contact
            log.info(f"📤 群事件 group_id={group_id} user={sender_name}/{user_id} text={text[:40]}")
        else:
            user_id = state._wxid_to_int(speaker_wxid or session_id_data)
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

        immediate_after = False
        with self.buffer_lock:
            if sender_id in self.pending_buffers:
                self.pending_buffers[sender_id]["processing"] = False
                immediate_after = self._after_flush_locked(sender_id)
        if immediate_after:
            self.process_sender(sender_id, force=True)

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

    def _media_worker_loop(self, buffer_key):
        """串行处理一个缓冲队列里的图片/表情：取图、描述、写回结果。

        同一 key 同一时间只跑一个 worker，避免多张图并发请求；
        全部处理完后再交给收尾计时器决定推送时机。
        """
        try:
            while True:
                with self.buffer_lock:
                    entry = self.pending_buffers.get(buffer_key)
                    if entry is None:
                        return
                    part = next(
                        (
                            p
                            for p in entry.get("parts", [])
                            if p.get("kind") in ("image", "emoji")
                            and not p.get("done")
                            and not p.get("reserved")
                        ),
                        None,
                    )
                    if part is None:
                        self._schedule_media_tail_locked(buffer_key, entry)
                        return
                    part["reserved"] = True
                    media_data = part.get("media_data") or {}
                    log.info(
                        f"🖼️ 开始描述 {part.get('kind')} seq={part.get('seq')} ({buffer_key})"
                    )

                caption = None
                try:
                    image_path = self._fetch_wechat_image(media_data)
                    if image_path:
                        caption = caption_image_via_ollama(image_path)
                except Exception as e:
                    log.warning(f"⚠️ 媒体描述线程异常 ({buffer_key}): {e}")

                text = self._media_part_text(part, caption)
                with self.buffer_lock:
                    entry = self.pending_buffers.get(buffer_key)
                    if entry is None:
                        return
                    if not any(existing is part for existing in entry.get("parts", [])):
                        return
                    part["done"] = True
                    part["reserved"] = False
                    part["text"] = text
                    if not self._has_undone_media(entry):
                        # 最后一张媒体完成即在同一临界区内安排收尾，
                        # 避免中间插入的文字先切回普通轮次又被后面的收尾覆盖。
                        self._schedule_media_tail_locked(buffer_key, entry)
                        return
        finally:
            with self.buffer_lock:
                entry = self.pending_buffers.get(buffer_key)
                if entry is not None:
                    entry["media_running"] = 0
                    if self._has_undone_media(entry):
                        self._kick_media_worker_locked(entry, buffer_key)

    def _schedule_media_tail_locked(self, buffer_key, entry=None):
        """媒体全部描述完成后安排收尾/补推。

        常规媒体批：等 2 秒（不超过 12 秒强刷剩余的收尾时间）后图+文一起推；
        已被 12 秒强刷或换人打断冲刷过：媒体完成后 1.5 秒单独补推。
        """
        if entry is None:
            entry = self.pending_buffers.get(buffer_key)
        if entry is None or entry.get("processing"):
            return
        if self._has_undone_media(entry):
            return
        has_media = any(
            part.get("kind") in ("image", "emoji")
            for part in entry.get("parts", [])
        )
        if not has_media:
            return
        if entry.get("media_batch_passed") or not entry.get("media_batch_active"):
            self._end_media_batch_locked(entry)
            self._schedule_buffer_flush_locked(
                buffer_key, config.MEDIA_CATCHUP_SECONDS, kind="media_catchup"
            )
            return
        remaining = (
            entry.get("media_batch_start", time.time())
            + config.MEDIA_BATCH_MAX_SECONDS
            - time.time()
        )
        delay = min(config.MEDIA_WAIT_LINGER_SECONDS, max(0.1, remaining))
        self._schedule_buffer_flush_locked(buffer_key, delay, kind="media_tail")

    def _media_part_text(self, part, caption) -> str:
        """把描述结果格式化成最终文本；失败时图片给占位文案，表情保留原文。"""
        kind = part.get("kind")
        if kind == "image":
            text = (
                f"[图片: {caption}]"
                if caption
                else config.MEDIA_IMAGE_FAIL_TEXT
            )
        elif caption:
            text = f"[表情: {caption}]"
        else:
            text = part.get("raw_content") or "[表情]"
        if part.get("batch_wrap"):
            source = part.get("source_name") or "未知"
            group = part.get("group_name") or ""
            text = f'成员"{source}"在群"{group}"中对你说：{text}'
        return text

    def _fetch_wechat_image(self, data: dict) -> str | None:
        """按 SSE 的 messageKey 精确反查 WeFlow 图片，避免同会话多图串图。

        图片/表情的 messageKey 里带有 wxid 等身份信息，这里只把结果本地保存；
        描述文本和推送给 AstrBot 的正文都不包含 wxid。
        """
        if not isinstance(data, dict):
            return None
        talker = str(data.get("sessionId") or "").strip()
        target_key = str(data.get("messageKey") or data.get("rawid") or "").strip()
        if not target_key and not talker:
            return None

        def _to_sec(value):
            if value in (None, ""):
                return None
            try:
                number = float(value)
            except (TypeError, ValueError):
                return None
            if number > 1e12:
                number /= 1000.0
            return number

        def _key_fields(message_key):
            match = re.match(
                r"^[^:]+:(?P<remote>\d+):(?P<sec>\d+):(?P<ms>\d+):(?P<local>\d+):(?P<wxid>wxid_[^:]+):\d+$",
                str(message_key or "").strip(),
            )
            if not match:
                return None
            return {
                "remote": match.group("remote"),
                "sec": match.group("sec"),
                "ms": match.group("ms"),
                "local": match.group("local"),
                "wxid": match.group("wxid"),
            }

        def _pick(candidate, names):
            for name in names:
                value = candidate.get(name)
                if value not in (None, "", 0):
                    return str(value)
            return ""

        def _sender_wxid_of(candidate):
            for name in ("senderWxid", "fromUser", "senderId"):
                value = str(candidate.get(name) or "").strip()
                if value.startswith("wxid_"):
                    return value
            content = str(candidate.get("content") or "")
            content_match = re.match(r"^(wxid_[^\s:]+):", content)
            if content_match:
                return content_match.group(1)
            parts = _key_fields(candidate.get("messageKey") or candidate.get("key") or "")
            if parts:
                return parts["wxid"]
            talker_id = str(candidate.get("talkerId") or "").strip()
            return talker_id if talker_id.startswith("wxid_") else ""

        def _remote_of(candidate):
            value = _pick(
                candidate,
                ("msgSvrID", "serverMsgId", "msgId", "newMsgId", "serverId", "svrId"),
            )
            if value:
                return value
            parts = _key_fields(candidate.get("messageKey") or candidate.get("key") or "")
            return parts["remote"] if parts else ""

        def _local_of(candidate):
            value = _pick(
                candidate,
                ("localId", "msgLocalId", "clientMsgId", "id"),
            )
            if value:
                return value
            parts = _key_fields(candidate.get("messageKey") or candidate.get("key") or "")
            return parts["local"] if parts else ""

        def _candidate_time(candidate):
            for name in ("timestamp", "createTime", "msgTime", "serverTime", "time"):
                seconds = _to_sec(candidate.get(name))
                if seconds is not None:
                    return seconds
            parts = _key_fields(candidate.get("messageKey") or candidate.get("key") or "")
            if parts:
                return float(parts["sec"])
            return None

        def _is_media(candidate):
            if not str(candidate.get("mediaUrl") or "").strip():
                return False
            media_type = str(candidate.get("mediaType") or "").lower()
            if not media_type:
                return True
            return any(
                token in media_type
                for token in ("image", "emoji", "sticker", "gif", "picture")
            )

        def _score(candidate):
            candidate_key = str(
                candidate.get("messageKey") or candidate.get("key") or ""
            ).strip()
            if candidate_key and candidate_key == target_key:
                return 100000
            parts = _key_fields(candidate_key)
            score = 0
            if not target_fields:
                return score
            remote = _remote_of(candidate)
            if remote and remote == target_fields["remote"]:
                score += 5000
            local = _local_of(candidate)
            if local and local == target_fields["local"]:
                score += 2000
            candidate_wxid = _sender_wxid_of(candidate)
            if candidate_wxid and candidate_wxid == target_wxid:
                score += 800
            candidate_time = _candidate_time(candidate)
            if candidate_time is not None and target_time is not None:
                if abs(candidate_time - target_time) <= 5:
                    score += 400
            if parts and parts["local"] == target_fields["local"]:
                score += 600
            return score

        try:
            url = f"{config.WE_FLOW_BASE_URL}/api/v1/messages"
            params = {
                "access_token": config.ACCESS_TOKEN,
                "media": "true",
                "limit": 50,
            }
            if talker:
                params["talker"] = talker
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                log.error(f"WeFlow 消息API: HTTP {resp.status_code}")
                return None

            raw = resp.json()
            messages = (
                raw
                if isinstance(raw, list)
                else raw.get("messages", raw.get("data", []))
            )
            if not isinstance(messages, list):
                messages = []

            target_fields = _key_fields(target_key)
            target_wxid = self._sender_wxid(data) or (
                target_fields or {}
            ).get("wxid", "")
            target_time = (
                float(target_fields["sec"]) if target_fields else None
            )

            best = None
            best_score = 0
            for candidate in messages:
                if not isinstance(candidate, dict) or not _is_media(candidate):
                    continue
                score = _score(candidate)
                if score > best_score:
                    best = candidate
                    best_score = score

            if best is None or best_score < 1200:
                log.warning(
                    f"⚠️ 未找到与 SSE 事件唯一匹配的图片 (key={target_key[:60]})"
                )
                return None

            media_url = str(best.get("mediaUrl") or "").strip()
            if not media_url:
                return None
            separator = "&" if "?" in media_url else "?"
            download_url = f"{media_url}{separator}access_token={config.ACCESS_TOKEN}"
            image_resp = requests.get(download_url, timeout=30)
            if image_resp.status_code != 200:
                log.error(f"WeFlow 图片下载: HTTP {image_resp.status_code}")
                return None

            content_type = image_resp.headers.get("Content-Type", "").lower()
            ext = ".jpg"
            if "png" in content_type:
                ext = ".png"
            elif "gif" in content_type:
                ext = ".gif"
            elif "webp" in content_type:
                ext = ".webp"
            elif "jpeg" in content_type or "jpg" in content_type:
                ext = ".jpg"
            else:
                url_match = re.search(
                    r"\.(jpg|jpeg|png|gif|webp)(?:\?|$)",
                    media_url.lower(),
                )
                if url_match:
                    ext = "." + url_match.group(1)
                    if ext == ".jpeg":
                        ext = ".jpg"

            save_dir = os.path.join(config.ASTRBOT_ATTACHMENTS or ".", "wechat_images")
            os.makedirs(save_dir, exist_ok=True)
            filename = f"{uuid.uuid4().hex}{ext}"
            save_path = os.path.join(save_dir, filename)
            with open(save_path, "wb") as f:
                f.write(image_resp.content)
            log.info(f"✅ 微信图片已保存: {save_path}")
            return save_path
        except Exception as e:
            log.error(f"获取微信图片异常: {e}")
            return None


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
