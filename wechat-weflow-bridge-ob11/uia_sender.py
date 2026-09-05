"""
uia_sender.py — 纯键盘模拟的微信消息发送器
=================================================================

原理：
  完全通过键盘快捷键操作微信，无鼠标/无 UIA ValuePattern。
  剪贴板 (pyperclip) + SendKeys 完成一切操作。

工作流：
  1. 定位微信窗口并激活到前台
  2. Ctrl+F 搜索联系人 → Enter 进入聊天
  3. 剪贴板复制消息 → Ctrl+V 粘贴 → Enter 发送
  4. 图片通过 PowerShell 复制到剪贴板 → Ctrl+V → Enter

依赖:
  pip install uiautomation pyperclip
  发送图片需要 PowerShell (Windows 自带)
"""

import ctypes
import logging
import os
import random
import subprocess
import threading
import time

log = logging.getLogger("weflow-bridge")


class BaseSender:
    """消息发送器基类"""
    def send_text(self, contact: str, text: str) -> bool:
        raise NotImplementedError

    def send_image(self, contact: str, image_path: str) -> bool:
        raise NotImplementedError


class UiaSender(BaseSender):
    """
    纯键盘模拟的微信消息发送器。

    不依赖 UIA ValuePattern / InvokePattern / 鼠标点击，
    全程使用剪贴板 + SendKeys 键盘快捷键操作。
    """

    WECHAT_TITLES = ["微信", "WeChat"]

    def __init__(self, search_enabled: bool = True):
        self._lock = threading.Lock()
        self._auto = None
        self._ready = False

        # 微信窗口
        self._window = None

        # 最近联系人缓存（相同目标跳过搜索，快速发送）
        self._last_contact = ""

        self.search_enabled = search_enabled

        self._init()

    # ================================================================
    # 初始化
    # ================================================================

    def _init(self):
        """初始化 uiautomation 并定位微信窗口"""
        try:
            import uiautomation as auto
            self._auto = auto
        except ImportError:
            log.error("请先安装 uiautomation: pip install uiautomation")
            return

        log.info("正在搜索微信窗口...")
        self._find_window()
        if self._window:
            log.info(f"微信窗口: '{self._window.Name}' ClassName={self._window.ClassName}")
            self._ready = True

    def _find_window(self):
        """按窗口类名直接定位微信窗口。

        不用 root.GetChildren() 枚举：枚举要逐个跨进程读每个顶层窗口的
        ClassName/Name，任何一个窗口所属进程无响应就会卡到 UIA 超时（120s）。
        FindWindowW 纯 Win32 调用，不跨进程，瞬间返回。
        """
        import ctypes
        for cls in ("WeChatMainWndForPC", "Qt51514QWindowIcon"):
            hwnd = ctypes.windll.user32.FindWindowW(cls, None)
            if hwnd:
                self._window = self._auto.ControlFromHandle(hwnd)
                return

    def _ensure_window(self) -> bool:
        """确保窗口可用"""
        if not self._ready:
            return False
        if self._window and self._window.Exists(0.2):
            return True
        self._find_window()
        if not self._window:
            log.warning("微信窗口未找到")
            self._ready = False
            return False
        return True

    def _activate(self):
        """激活微信窗口到前台"""
        try:
            self._window.SetActive()
            time.sleep(0.15)
        except Exception:
            try:
                self._window.SwitchToThisWindow()
                time.sleep(0.15)
            except Exception:
                pass
        # AttachThreadInput 绕过 Windows 后台窗口激活限制
        try:
            import ctypes
            hwnd = ctypes.windll.user32.FindWindowW('Qt51514QWindowIcon', None)
            if not hwnd:
                hwnd = ctypes.windll.user32.FindWindowW('WeChatMainWndForPC', None)
            if hwnd:
                WE_CHAT_TID = ctypes.windll.user32.GetWindowThreadProcessId(hwnd, None)
                CURRENT_TID = ctypes.windll.kernel32.GetCurrentThreadId()
                ctypes.windll.user32.AttachThreadInput(CURRENT_TID, WE_CHAT_TID, True)
                ctypes.windll.user32.SetForegroundWindow(hwnd)
                ctypes.windll.user32.BringWindowToTop(hwnd)
                ctypes.windll.user32.AttachThreadInput(CURRENT_TID, WE_CHAT_TID, False)
        except Exception:
            pass

    # ================================================================
    # 联系人切换
    # ================================================================

    def _switch_contact(self, contact: str) -> bool:
        """
        切换到指定联系人/群聊的聊天窗口。

        纯键盘：Ctrl+F → 粘贴联系人名 → Enter
        """
        if not self._ensure_window():
            return False
        self._activate()

        try:
            import ctypes
            from ctypes import wintypes
        except ImportError:
            return False

        hwnd = ctypes.windll.user32.FindWindowW('Qt51514QWindowIcon', None)
        if not hwnd:
            hwnd = ctypes.windll.user32.FindWindowW('WeChatMainWndForPC', None)
        if not hwnd:
            log.warning("找不到微信主窗口句柄")
            return False

        WE_CHAT_TID = ctypes.windll.user32.GetWindowThreadProcessId(hwnd, None)
        CURRENT_TID = ctypes.windll.kernel32.GetCurrentThreadId()
        ctypes.windll.user32.AttachThreadInput(CURRENT_TID, WE_CHAT_TID, True)
        ctypes.windll.user32.SetForegroundWindow(hwnd)
        ctypes.windll.user32.BringWindowToTop(hwnd)
        time.sleep(0.15)

        try:
            auto = self._auto

            # Ctrl+F 打开搜索
            auto.SendKeys('{Ctrl}f')
            time.sleep(0.3)

            # Ctrl+A 全选 → 清空已有内容
            auto.SendKeys('{Ctrl}a')
            time.sleep(0.1)

            # 粘贴联系人名
            import pyperclip
            pyperclip.copy(contact)
            time.sleep(0.1)
            auto.SendKeys('{Ctrl}v')
            time.sleep(0.2)

            # Enter 选中第一个结果
            auto.SendKeys('{Enter}')
            time.sleep(0.5)

            log.info(f"已切到联系人: {contact}")
            return True
        finally:
            ctypes.windll.user32.AttachThreadInput(CURRENT_TID, WE_CHAT_TID, False)

    # ================================================================
    # 发送文字 (纯键盘)
    # ================================================================

    MAX_CHUNK_LEN = 2000  # 微信输入框约 4000 字符上限，留安全余量

    def _send_text_chunk(self, contact: str, text: str, chunk_index: int = 0, total_chunks: int = 1) -> bool:
        """发送单段文字（剪贴板 → Ctrl+V → Enter）。"""
        try:
            import pyperclip

            if total_chunks > 1:
                time.sleep(random.uniform(0.2, 0.4))

            pyperclip.copy(text)
            time.sleep(random.uniform(0.05, 0.15))

            auto = self._auto
            auto.SendKeys('{Ctrl}v')

            time.sleep(random.uniform(0.25, 0.45))

            auto.SendKeys('{Enter}')

            if total_chunks > 1:
                log.info(f"[UIA✓] {contact} ({chunk_index+1}/{total_chunks}): {text[:30]}...")
            else:
                log.info(f"[UIA✓] {contact}: {text[:50]}...")
            return True
        except Exception as e:
            log.error(f"[UIA✗] {contact}: {e}")
            return False

    def send_text(self, contact: str, text: str) -> bool:
        """
        发送文本消息。超长消息自动分段，避免微信输入框截断。

        纯键盘方案：剪贴板 → Ctrl+V → Enter
        """
        with self._lock:
            if not self._ready:
                log.error("UIA Sender 未就绪")
                return False

            if not self._ensure_window():
                return False

            # 安全检查：过滤 PIL 引用
            if "<PIL." in text or "PIL." in text:
                log.warning(f"跳过 PIL 引用消息: {text[:60]}")
                return False

            # 同联系人时窗口可能已失焦，需重新激活；切换路径由 _switch_contact 自行激活
            if self.search_enabled and contact and contact == self._last_contact:
                self._activate()
            auto = self._auto

            # 切换到联系人
            if self.search_enabled and contact:
                if contact != self._last_contact:
                    self._switch_contact(contact)
                    self._last_contact = contact

            # 模拟真人随机延时
            time.sleep(random.uniform(0.1, 0.3))

            # 超长消息分段发送
            if len(text) > self.MAX_CHUNK_LEN:
                chunks = []
                for i in range(0, len(text), self.MAX_CHUNK_LEN):
                    chunks.append(text[i:i + self.MAX_CHUNK_LEN])
                log.info(f"[UIA] 消息过长 ({len(text)} 字符)，分 {len(chunks)} 段发送")
                ok = True
                for i, chunk in enumerate(chunks):
                    if not self._send_text_chunk(contact, chunk, i, len(chunks)):
                        ok = False
                return ok

            return self._send_text_chunk(contact, text)

    # ================================================================
    # 发送图片 (剪贴板 + 纯键盘)
    # ================================================================

    def send_image(self, contact: str, image_path: str, is_group: bool = False) -> bool:
        """
        发送图片/动图。

        - .gif / 内容是 GIF → 走文件粘贴（CF_HDROP），保留动画
        - 其他静态图 → 走图像数据粘贴（CF_BITMAP，原逻辑）
        """
        with self._lock:
            if not self._ready:
                return False
            if not os.path.isfile(image_path):
                log.error(f"图片不存在: {image_path}")
                return False

            try:
                if not self._ensure_window():
                    return False
                self._activate()
                auto = self._auto

                if self.search_enabled and contact:
                    if contact != self._last_contact:
                        self._switch_contact(contact)
                        self._last_contact = contact

                time.sleep(random.uniform(0.15, 0.4))

                # 判断是否 GIF 动图（按扩展名，兼容巧合误名）
                is_gif = self._is_gif_file(image_path)

                if is_gif:
                    # 动图：复制文件本身到剪贴板（保留动画）
                    self._copy_file_to_clipboard(image_path)
                    log.debug(f"GIF 动图 → 以文件形式粘贴: {os.path.basename(image_path)}")
                    wait_time = random.uniform(0.8, 1.3)
                else:
                    # 静态图：复制图像数据到剪贴板（原逻辑）
                    self._copy_image_to_clipboard(image_path)
                    log.debug(f"静态图 → 以图像数据粘贴: {os.path.basename(image_path)}")
                    wait_time = random.uniform(0.6, 1.0)

                time.sleep(0.2)

                # Ctrl+V 粘贴
                auto.SendKeys('{Ctrl}v')
                time.sleep(wait_time)  # 等待微信加载图片预览

                # Enter 发送
                auto.SendKeys('{Enter}')

                log.info(f"[UIA✓] 图片 → {contact}: {os.path.basename(image_path)} ({'GIF动图' if is_gif else '静态图'})")
                return True

            except Exception as e:
                log.error(f"[UIA✗] 图片 → {contact}: {e}")
                return False

    @staticmethod
    def _is_gif_file(path: str) -> bool:
        """判断文件是否为 GIF 动图。优先按扩展名，再用文件头魔数兜底。"""
        if path.lower().endswith(".gif"):
            return True
        # 扩展名不符（如误标 .png）：读文件头判断是不是 GIF
        try:
            with open(path, "rb") as f:
                head = f.read(6)
            if head in (b"GIF87a", b"GIF89a"):
                return True
        except Exception:
            pass
        return False

    def _copy_file_to_clipboard(self, path: str):
        """将文件路径复制到剪贴板（CF_HDROP），粘贴时发送完整文件保留动画"""
        abs_path = os.path.abspath(path)
        try:
            subprocess.run([
                "powershell", "-WindowStyle", "Hidden", "-Command",
                f"Add-Type -AssemblyName System.Windows.Forms;"
                f"$files = New-Object System.Collections.Specialized.StringCollection;"
                f"$files.Add('{abs_path}');"
                f"[System.Windows.Forms.Clipboard]::SetFileDropList($files)"
            ], check=True, timeout=10)
            log.debug(f"文件已复制到剪贴板: {abs_path}")
        except Exception as e:
            log.error(f"复制文件到剪贴板失败: {e}")
            raise

    def _copy_image_to_clipboard(self, path: str):
        """复制图片到剪贴板（通过 PowerShell，避免 PIL 对象被当作文本复制）"""
        abs_path = os.path.abspath(path)
        try:
            subprocess.run([
                "powershell", "-WindowStyle", "Hidden", "-Command",
                f"Add-Type -AssemblyName System.Windows.Forms;"
                f"$img = [System.Drawing.Image]::FromFile('{abs_path}');"
                f"[System.Windows.Forms.Clipboard]::SetImage($img);"
                f"$img.Dispose()"
            ], check=True, timeout=10)
            log.debug("PowerShell 已复制图片到剪贴板")
        except Exception as e:
            log.error(f"复制图片到剪贴板失败: {e}")
            raise
