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
from ctypes import wintypes

log = logging.getLogger("weflow-bridge")

_user32 = ctypes.WinDLL("user32", use_last_error=True)
_kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

_user32.FindWindowW.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR]
_user32.FindWindowW.restype = wintypes.HWND
_user32.IsWindow.argtypes = [wintypes.HWND]
_user32.IsWindow.restype = wintypes.BOOL
_user32.IsWindowVisible.argtypes = [wintypes.HWND]
_user32.IsWindowVisible.restype = wintypes.BOOL
_user32.IsIconic.argtypes = [wintypes.HWND]
_user32.IsIconic.restype = wintypes.BOOL
_user32.GetForegroundWindow.restype = wintypes.HWND
_user32.GetWindowTextW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
_user32.GetWindowTextW.restype = ctypes.c_int
_user32.GetClassNameW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
_user32.GetClassNameW.restype = ctypes.c_int
_user32.GetWindowRect.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.RECT)]
_user32.GetWindowRect.restype = wintypes.BOOL
_user32.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
_user32.GetWindowThreadProcessId.restype = wintypes.DWORD
_user32.AttachThreadInput.argtypes = [wintypes.DWORD, wintypes.DWORD, wintypes.BOOL]
_user32.AttachThreadInput.restype = wintypes.BOOL
_user32.SetForegroundWindow.argtypes = [wintypes.HWND]
_user32.SetForegroundWindow.restype = wintypes.BOOL
_user32.BringWindowToTop.argtypes = [wintypes.HWND]
_user32.BringWindowToTop.restype = wintypes.BOOL
_user32.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
_user32.ShowWindow.restype = wintypes.BOOL
_user32.SetWindowPos.argtypes = [
    wintypes.HWND,
    wintypes.HWND,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_uint,
]
_user32.SetWindowPos.restype = wintypes.BOOL
_user32.GetAncestor.argtypes = [wintypes.HWND, ctypes.c_uint]
_user32.GetAncestor.restype = wintypes.HWND


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

    WECHAT_TITLES = ("微信", "Weixin", "WeChat")
    WECHAT_CLASSES = ("Qt51514QWindowIcon", "WeChatMainWndForPC")

    def __init__(self, search_enabled: bool = True):
        self._lock = threading.Lock()
        self._auto = None
        self._ready = False

        # 只保存 Win32 句柄，不保存跨进程 UIA 窗口对象
        self._hwnd = 0

        # 最近联系人缓存（相同目标跳过搜索，快速发送）
        self._last_contact = ""

        self.search_enabled = search_enabled

        self._init()

    # ================================================================
    # 初始化
    # ================================================================

    def _init(self):
        """初始化 uiautomation（仅用于 SendKeys）并定位微信窗口"""
        try:
            import uiautomation as auto
            self._auto = auto
        except ImportError:
            log.error("请先安装 uiautomation: pip install uiautomation")
            return

        log.info("正在搜索微信窗口...")
        self._find_window()
        if self._hwnd:
            log.info(
                f"微信窗口: '{self._window_text(self._hwnd)}' "
                f"ClassName={self._window_class(self._hwnd)} hwnd={self._hwnd}"
            )
            self._ready = True

    @staticmethod
    def _window_text(hwnd: int) -> str:
        buf = ctypes.create_unicode_buffer(512)
        _user32.GetWindowTextW(hwnd, buf, len(buf))
        return buf.value

    @staticmethod
    def _window_class(hwnd: int) -> str:
        buf = ctypes.create_unicode_buffer(256)
        _user32.GetClassNameW(hwnd, buf, len(buf))
        return buf.value

    @classmethod
    def _find_hwnd(cls) -> int:
        """定位微信主窗口句柄，优先选择前台、可见并带微信标题的窗口。"""
        candidates = []
        foreground = _user32.GetForegroundWindow()

        callback_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

        def collect(hwnd, _):
            class_name = cls._window_class(hwnd)
            if class_name not in cls.WECHAT_CLASSES:
                return True

            title = cls._window_text(hwnd)
            visible = bool(_user32.IsWindowVisible(hwnd))
            minimized = bool(_user32.IsIconic(hwnd))
            rect = wintypes.RECT()
            area = 0
            if _user32.GetWindowRect(hwnd, ctypes.byref(rect)):
                area = max(0, rect.right - rect.left) * max(0, rect.bottom - rect.top)

            score = 0
            if any(keyword in title for keyword in cls.WECHAT_TITLES):
                score += 10000
            score += min(area, 1000000)
            if visible:
                score += 100
            if not minimized:
                score += 1
            if hwnd == foreground:
                score += 10
            if class_name == "WeChatMainWndForPC":
                score += 2

            candidates.append((score, hwnd))
            return True

        _user32.EnumWindows(callback_type(collect), 0)
        if candidates:
            candidates.sort(reverse=True)
            return int(candidates[0][1])

        # EnumWindows 偶尔因桌面/权限状态拿不到目标时，保留 Win32 兜底。
        for class_name in cls.WECHAT_CLASSES:
            hwnd = _user32.FindWindowW(class_name, None)
            if hwnd:
                return int(hwnd)
        return 0

    def _find_window(self) -> bool:
        """重新查找微信主窗口句柄。"""
        self._hwnd = self._find_hwnd()
        return bool(self._hwnd)

    def _ensure_window(self) -> bool:
        """确保窗口可用"""
        if (
            self._hwnd
            and _user32.IsWindow(self._hwnd)
            and self._window_class(self._hwnd) in self.WECHAT_CLASSES
        ):
            self._ready = True
            return True
        self._find_window()
        if not self._hwnd:
            log.warning("微信窗口未找到")
            self._ready = False
            return False
        self._ready = True
        return True

    def _is_foreground(self, hwnd: int) -> bool:
        foreground = _user32.GetForegroundWindow()
        if not foreground:
            return False
        if foreground == hwnd:
            return True
        # 微信搜索等弹出层可能是子窗口，取根窗口后再比较。
        return _user32.GetAncestor(foreground, 2) == hwnd  # GA_ROOT

    def _activate(self) -> bool:
        """激活微信窗口，并确认它已经成为前台窗口。"""
        if not self._ensure_window():
            return False

        hwnd = self._hwnd
        if _user32.IsIconic(hwnd):
            _user32.ShowWindow(hwnd, 9)  # SW_RESTORE
            time.sleep(0.15)

        if self._is_foreground(hwnd):
            return True

        current_tid = _kernel32.GetCurrentThreadId()
        foreground = _user32.GetForegroundWindow()
        foreground_tid = (
            _user32.GetWindowThreadProcessId(foreground, None) if foreground else 0
        )
        target_tid = _user32.GetWindowThreadProcessId(hwnd, None)
        attached_tids = []

        for tid in {foreground_tid, target_tid}:
            if tid and tid != current_tid:
                if _user32.AttachThreadInput(current_tid, tid, True):
                    attached_tids.append(tid)

        try:
            _user32.ShowWindow(hwnd, 5)  # SW_SHOW
            _user32.SetWindowPos(
                hwnd,
                wintypes.HWND(-1),  # HWND_TOPMOST
                0,
                0,
                0,
                0,
                0x0001 | 0x0002 | 0x0040,  # NOSIZE | NOMOVE | SHOWWINDOW
            )
            _user32.SetWindowPos(
                hwnd,
                wintypes.HWND(-2),  # HWND_NOTOPMOST
                0,
                0,
                0,
                0,
                0x0001 | 0x0002 | 0x0040,
            )
            _user32.BringWindowToTop(hwnd)
            _user32.SetForegroundWindow(hwnd)
        finally:
            for tid in reversed(attached_tids):
                _user32.AttachThreadInput(current_tid, tid, False)

        for _ in range(8):
            if self._is_foreground(hwnd):
                time.sleep(0.08)
                return True
            time.sleep(0.08)
            _user32.SetForegroundWindow(hwnd)

        log.warning(
            f"微信窗口未能激活到前台: hwnd={hwnd} "
            f"foreground={_user32.GetForegroundWindow()} "
            f"title='{self._window_text(hwnd)}'"
        )
        return False

    def _send_keys(self, keys: str) -> bool:
        """仅当微信是当前前台窗口时才发送按键。"""
        if self._auto is None:
            log.error("uiautomation 未初始化，无法发送按键")
            return False
        if not self._activate():
            return False
        if not self._is_foreground(self._hwnd):
            return False
        self._auto.SendKeys(keys)
        return self._is_foreground(self._hwnd)

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
        if not self._activate():
            return False

        try:
            # Ctrl+F 打开搜索
            if not self._send_keys('{Ctrl}f'):
                log.warning(f"打开联系人搜索失败: {contact}")
                return False
            time.sleep(0.3)

            # Ctrl+A 全选 → 清空已有内容
            if not self._send_keys('{Ctrl}a'):
                log.warning(f"清空联系人搜索失败: {contact}")
                return False
            time.sleep(0.1)

            # 粘贴联系人名
            import pyperclip
            pyperclip.copy(contact)
            time.sleep(0.1)
            if not self._send_keys('{Ctrl}v'):
                log.warning(f"粘贴联系人名失败: {contact}")
                return False
            time.sleep(0.2)

            # Enter 选中第一个结果
            if not self._send_keys('{Enter}'):
                log.warning(f"选择联系人失败: {contact}")
                return False
            time.sleep(0.5)

            log.info(f"已切到联系人: {contact}")
            return True
        except Exception as e:
            log.error(f"切换联系人失败: {contact}: {e}")
            return False

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

            if not self._send_keys('{Ctrl}v'):
                log.error(f"[UIA✗] {contact}: 粘贴时微信不在前台")
                return False

            time.sleep(random.uniform(0.25, 0.45))

            if not self._send_keys('{Enter}'):
                log.error(f"[UIA✗] {contact}: 发送时微信不在前台")
                return False

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
            if not self._ensure_window():
                log.error("[UIA✗] 微信窗口未就绪")
                return False

            # 安全检查：过滤 PIL 引用
            if "<PIL." in text or "PIL." in text:
                log.warning(f"跳过 PIL 引用消息: {text[:60]}")
                return False

            # 切换到联系人
            if self.search_enabled and contact:
                if contact != self._last_contact:
                    if not self._switch_contact(contact):
                        return False
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
                        break
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
            if not os.path.isfile(image_path):
                log.error(f"图片不存在: {image_path}")
                return False

            try:
                if not self._ensure_window():
                    return False
                if not self._activate():
                    return False

                if self.search_enabled and contact:
                    if contact != self._last_contact:
                        if not self._switch_contact(contact):
                            return False
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
                if not self._send_keys('{Ctrl}v'):
                    log.error(f"[UIA✗] 图片 → {contact}: 粘贴时微信不在前台")
                    return False
                time.sleep(wait_time)  # 等待微信加载图片预览

                # Enter 发送
                if not self._send_keys('{Enter}'):
                    log.error(f"[UIA✗] 图片 → {contact}: 发送时微信不在前台")
                    return False

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
