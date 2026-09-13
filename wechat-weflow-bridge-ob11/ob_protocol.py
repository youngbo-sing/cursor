"""
OneBot v11 协议处理模块。

包括：
- make_message_event() — 构造 OneBot 消息事件 JSON
- push_event() — 通过 WebSocket 推送事件给 AstrBot
- _handle_ob_api() — 处理 AstrBot 发来的 API 请求（send_msg 等）
- _extract_text() — 从 OneBot message 段提取纯文本
"""

import asyncio
import base64
import json
import os
import tempfile
import time
import logging

import requests

import state
import config

log = logging.getLogger("ob11-bridge")


def _is_group_send(action: str, params: dict) -> bool:
    """判断 AstrBot 这次发送是群聊还是私聊。"""
    if action == "send_group_msg":
        return True
    if action == "send_private_msg":
        return False
    if params.get("message_type") == "group":
        return True
    if params.get("message_type") == "private":
        return False
    return bool(params.get("group_id"))


def _normalize_message(message) -> list:
    """把 OneBot message 统一成段列表。"""
    if isinstance(message, str):
        return [{"type": "text", "data": {"text": message}}]
    if isinstance(message, dict):
        return [message]
    if isinstance(message, list):
        return message
    return []


def _mark_group_replied(is_group: bool, target_id: int) -> None:
    """AstrBot 成功向群发送内容后记录时间，供群文本随机首等使用。"""
    if not is_group or not target_id:
        return
    bridge = state.bridge_instance
    if bridge is not None:
        bridge.mark_group_replied(target_id)


def _build_api_data(action: str, params: dict):
    """构造常见 OneBot API 的 data，避免 AstrBot 拿到空对象。"""
    if action == "get_login_info":
        nick = config.BOT_NICKNAMES[0] if config.BOT_NICKNAMES else "wechat_bot"
        return {"user_id": state._self_id_int, "nickname": nick}
    if action == "get_status":
        return {"online": True, "good": True}
    if action in ("get_version_info", "get_version"):
        return {
            "app_name": "wechat-weflow-bridge",
            "app_version": "1.0.0",
            "protocol_version": "v11",
        }
    if action == "can_send_image":
        return {"yes": True}
    if action == "can_send_record":
        return {"yes": False}
    if action == "get_friend_list":
        return []
    if action == "get_group_list":
        return []
    if action == "get_group_member_info":
        uid = params.get("user_id", 0)
        return {
            "group_id": params.get("group_id", 0),
            "user_id": uid,
            "nickname": state._ob_id_to_contact.get(uid, str(uid)),
            "card": state._ob_id_to_contact.get(uid, ""),
            "role": "member",
            "sex": "unknown",
            "age": 0,
            "area": "",
            "level": "0",
            "title": "",
        }
    if action == "get_group_member_list":
        return []
    if action in ("send_msg", "send_private_msg", "send_group_msg"):
        return {"message_id": state.next_message_id()}
    return {}


async def _handle_ob_api(data: dict):
    """处理 AstrBot 发来的 API 请求。"""
    action = data.get("action", "")
    params = data.get("params", {}) or {}
    echo = data.get("echo", "")
    log.info(f"[OB11] API: {action} echo={echo} params_keys={list(params.keys())}")

    # 先回响应（必须在处理消息前回，否则 AstrBot 超时）
    resp_sent = False
    resp_data = {"status": "ok", "retcode": 0, "data": _build_api_data(action, params)}
    if echo:
        resp_data["echo"] = echo
    # WS 在线就立即回；掉线时不空等——本地发送（UIA）不依赖 WS，别拖慢消息处理
    for retry in range(3):
        if not state._ob_ws:
            break
        try:
            await state._ob_ws.send(json.dumps(resp_data, ensure_ascii=False))
            resp_sent = True
            log.info(f"[OB11] 已回响应: {action}")
            break
        except Exception as e:
            log.warning(f"[OB11] 回响应失败 (重试 {retry}/3): {e}")
            await asyncio.sleep(0.2)
    if not resp_sent:
        log.warning(f"[OB11] 无法回响应（WS 未连接），消息仍尝试本地处理: {action}")

    if action in ("send_msg", "send_private_msg", "send_group_msg"):
        is_group = _is_group_send(action, params)
        target_id = params.get("group_id") if is_group else params.get("user_id", 0)
        try:
            target_id = int(target_id or 0)
        except (TypeError, ValueError):
            target_id = 0
        message = _normalize_message(params.get("message", []))
        contact = state._ob_id_to_contact.get(target_id, "")
        log.info(f"[OB11] 发送目标: {'群' if is_group else '私'} id={target_id} contact={contact or '?'}")
        if not contact:
            log.warning(f"[OB11] 找不到联系人映射: target_id={target_id} map={dict(list(state._ob_id_to_contact.items())[-8:])}")
            return

        # 逐段处理：文字和图片分别发送
        for seg in message:
            if not isinstance(seg, dict):
                continue
            seg_type = seg.get("type", "")
            seg_data = seg.get("data", {})

            if seg_type == "text":
                text = seg_data.get("text", "")
                if text:
                    await asyncio.to_thread(state.sender_instance.send_text, contact, text)
                    log.info(f"[OB11] 文字已发送至 {contact}: {text[:50]}")
                    _mark_group_replied(is_group, target_id)

            elif seg_type == "image":
                file_val = seg_data.get("file", "")
                if not file_val:
                    continue

                img_path = None

                # AstrBot 通过 aiocqhttp 发图片时用 base64:// 格式
                if file_val.startswith("base64://"):
                    try:
                        # 解码 + 写文件在线程池执行，避免大图卡死事件循环
                        b64_data = file_val[9:]
                        img_path = await asyncio.to_thread(_decode_base64_image, b64_data)
                        if img_path:
                            log.info(f"[OB11] 图片已解码: {os.path.basename(img_path)}")
                    except Exception as e:
                        log.warning(f"[OB11] base64 图片解码失败: {e}")
                else:
                    # 文件名模式：在附件目录找
                    if config.ASTRBOT_ATTACHMENTS:
                        candidates = [
                            os.path.join(config.ASTRBOT_ATTACHMENTS, file_val),
                            os.path.join(config.ASTRBOT_ATTACHMENTS, "wechat_images", file_val),
                        ]
                        for p in candidates:
                            if os.path.exists(p):
                                img_path = p
                                break
                        if not img_path:
                            log.warning(f"[OB11] 图片文件未找到: {file_val}")

                if img_path:
                    try:
                        # 使用线程池执行同步的 UIA 发送，避免阻塞事件循环
                        await asyncio.to_thread(state.sender_instance.send_image, contact, img_path)
                        log.info(f"[OB11] 图片已发送至 {contact}")
                        _mark_group_replied(is_group, target_id)
                    finally:
                        # 临时文件用完删除
                        if img_path and "tmp" in img_path:
                            try:
                                os.unlink(img_path)
                            except Exception:
                                pass

            elif seg_type == "face":
                await asyncio.to_thread(state.sender_instance.send_text, contact, "[表情]")
                log.info(f"[OB11] 表情已发送至 {contact}")
                _mark_group_replied(is_group, target_id)

            # 其他类型（record, video 等）忽略

    else:
        log.debug(f"[OB11] 未处理 API: {action}")

    # 注意：API 响应已在函数开头统一发送，此处不再重复


def _extract_text(message: list) -> str:
    """从 OneBot message 段中提取可发送的文本。"""
    text_parts = []
    for seg in message:
        if isinstance(seg, dict):
            t = seg.get("type", "")
            d = seg.get("data", {})
            if t == "text":
                text_parts.append(d.get("text", ""))
            elif t == "image":
                text_parts.append("[图片]")
            elif t == "face":
                text_parts.append("[表情]")
            elif t == "record":
                text_parts.append("[语音]")
            elif t == "video":
                text_parts.append("[视频]")
            elif t == "reply":
                if d.get("text"):
                    text_parts.append(f'"{d["text"]}"')
            elif t == "at":
                text_parts.append(f"@{d.get('qq', d.get('name', ''))}")
            else:
                # 其他未知类型也尝试提取文本
                text_parts.append(d.get("text", ""))
    return "".join(text_parts).strip()


# ============ OneBot 协议处理 ============


def _raw_message(message: list) -> str:
    """按 OneBot 习惯拼 raw_message，at 段写成 [CQ:at,qq=...]。"""
    parts = []
    for seg in message:
        if not isinstance(seg, dict):
            continue
        t = seg.get("type", "")
        d = seg.get("data", {}) or {}
        if t == "text":
            parts.append(d.get("text", ""))
        elif t == "at":
            parts.append(f"[CQ:at,qq={d.get('qq', '')}]")
        elif t == "image":
            parts.append("[CQ:image]")
        elif t == "face":
            parts.append(f"[CQ:face,id={d.get('id', 0)}]")
    return "".join(parts)


def make_lifecycle_event(sub_type: str = "connect") -> dict:
    """构造 OneBot 生命周期事件，方便 AstrBot 识别 bot 上线。"""
    return {
        "time": int(time.time()),
        "self_id": state._self_id_int,
        "post_type": "meta_event",
        "meta_event_type": "lifecycle",
        "sub_type": sub_type,
    }


def make_heartbeat_event() -> dict:
    return {
        "time": int(time.time()),
        "self_id": state._self_id_int,
        "post_type": "meta_event",
        "meta_event_type": "heartbeat",
        "status": {"online": True, "good": True},
        "interval": 15000,
    }


def make_message_event(message_type: str, user_id: int, message: list,
                       group_id: int = 0, group_name: str = "",
                       nickname: str = "") -> dict:
    """构造符合 OneBot v11 的消息事件。人物身份只放 sender，不改写正文。"""
    raw = _raw_message(message)
    event = {
        "time": int(time.time()),
        "self_id": state._self_id_int,
        "post_type": "message",
        "message_id": state.next_message_id(),
        "user_id": user_id,
        "message": message,
        "raw_message": raw,
        "font": 0,
    }
    if message_type == "group":
        event["message_type"] = "group"
        event["sub_type"] = "normal"
        event["group_id"] = group_id
        event["anonymous"] = None
        event["sender"] = {
            "user_id": user_id,
            "nickname": nickname or str(user_id),
            "card": nickname or "",
            "sex": "unknown",
            "age": 0,
            "area": "",
            "level": "0",
            "role": "member",
            "title": "",
        }
        if group_name:
            event["group_name"] = group_name
    else:
        event["message_type"] = "private"
        event["sub_type"] = "friend"
        event["sender"] = {
            "user_id": user_id,
            "nickname": nickname or str(user_id),
            "sex": "unknown",
            "age": 0,
        }
    return event


def push_event(event: dict) -> bool:
    """通过 WebSocket 客户端连接向 AstrBot 推送事件。"""
    if not state._ob_ws or not state._ob_ws_loop:
        return False
    try:
        future = asyncio.run_coroutine_threadsafe(
            state._ob_ws.send(json.dumps(event, ensure_ascii=False)),
            state._ob_ws_loop,
        )
        future.result(timeout=5)
        return True
    except Exception as e:
        log.warning(f"[OB11] 推送事件失败: {e}")
        return False


def _decode_base64_image(b64_data: str) -> str | None:
    """在线程池中执行：解码 base64 图片并保存为临时文件。

    根据文件头魔数探测真实格式（GIF/PNG/JPEG），
    避免 AstrBot 发来的 GIF 被误存为 .png 导致动图丢失。
    """
    import tempfile
    img_data = base64.b64decode(b64_data)

    # 按文件头判断真实格式
    ext = ".png"
    if img_data[:6] in (b"GIF87a", b"GIF89a"):
        ext = ".gif"
    elif img_data[:2] == b"\xff\xd8":
        ext = ".jpg"
    elif img_data[:8] == b"\x89PNG\r\n\x1a\n":
        ext = ".png"

    tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
    tmp.write(img_data)
    tmp.close()
    return tmp.name
