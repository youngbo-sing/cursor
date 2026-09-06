"""
配置模块：加载 config.json，提供全局配置常量。
"""

import json
import os
import logging
import threading

# ============ 配置 ============

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


config = load_config()

WE_FLOW_BASE_URL = config["weflow_base_url"]
ACCESS_TOKEN = config["access_token"]
ASTRBOT_ATTACHMENTS = config.get("astrbot_attachments", "")
BOT_NICKNAMES = config["bot_nicknames"]
BOT_WXID = config.get("bot_wxid", "")
# 发送方式已固定为 UIA 纯键盘模拟
BUFFER_SECONDS = config.get("buffer_seconds", 5)
# 群聊分段文本合并窗口
MERGE_WAIT_FIRST_SECONDS = float(config.get("merge_wait_first_seconds", 5))
MERGE_WAIT_NEXT_SECONDS = float(config.get("merge_wait_next_seconds", 3))
MERGE_DONE_PUNCT_SECONDS = float(config.get("merge_done_punct_seconds", 1))
MERGE_WAIT_RANDOM_VALUES = [float(v) for v in config.get("merge_wait_random_values", [3, 4, 5])]
MERGE_WAIT_RANDOM_WEIGHTS = [float(v) for v in config.get("merge_wait_random_weights", [30, 50, 20])]
if len(MERGE_WAIT_RANDOM_WEIGHTS) != len(MERGE_WAIT_RANDOM_VALUES):
    MERGE_WAIT_RANDOM_WEIGHTS = [1.0] * len(MERGE_WAIT_RANDOM_VALUES)
# 图片/表情合并窗口
MEDIA_WAIT_LINGER_SECONDS = float(config.get("media_wait_linger_seconds", 2))  # 最后一张媒体描述完成后的收尾等待
MEDIA_BATCH_MAX_SECONDS = float(config.get("media_batch_max_seconds", 12))  # 一批媒体从首张到达起的强刷上限
MEDIA_CATCHUP_SECONDS = float(config.get("media_catchup_seconds", 1.5))  # 文字先走、媒体后完成时的补推等待
MEDIA_IMAGE_FAIL_TEXT = config.get("media_image_fail_text", "（图片内容无法描述）")
WEB_PORT = config.get("web_port", 8766)
GROUP_REPLY_MODE = config.get("group_reply_mode", "mention")  # "mention"(@触发) / "all"
MENTION_AWAKE_TIMEOUT = config.get("mention_awake_timeout", 180)  # @触发后免@回复的时长（秒）

# AstrBot OneBot 连接配置（bridge 作为 WebSocket 客户端连 AstrBot 的 aiocqhttp 服务端）
ASTRBOT_OB_URL = config.get("astrbot_ob_url", "ws://127.0.0.1:19777")

# 图片描述配置（支持 ollama 或 openai 兼容 API）
IMAGE_CAPTION_PROVIDER = config.get("image_caption_provider", "ollama")  # "ollama" / "openai"
IMAGE_CAPTION_MODEL = config.get("image_caption_model", "llava:7b")
IMAGE_CAPTION_API_KEY = config.get("image_caption_api_key", "")
IMAGE_CAPTION_API_BASE = config.get("image_caption_api_base", "")
IMAGE_CAPTION_PROMPT = config.get("image_caption_prompt", "请用中文简短描述这张图片的内容")

# Ollama 图片描述配置（provider=ollama 时使用）
OLLAMA_BASE_URL = config.get("ollama_base_url", "http://127.0.0.1:61000")
OLLAMA_TIMEOUT = config.get("ollama_timeout", 60)

# ============ 日志 ============

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler("bridge.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("ob11-bridge")
