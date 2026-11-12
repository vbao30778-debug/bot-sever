import os
import sys
import time
import re
import signal
import queue
import threading
import json
import base64
import subprocess
import secrets
import traceback
import html
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

_VIETNAM_TZ = timezone(timedelta(hours=7), name="Asia/Ho_Chi_Minh")
_SCHEDULE_STATE_LOCK = threading.Lock()
_SCHEDULE_PAUSED_STATE = {"VTH": None, "CDTD": None}


def _normalize_daily_pause_window(window):

    if window is None or window == "" or window == 0 or window == "0":
        return None

    def _valid_pair(start, end):
        try:
            start, end = int(start), int(end)
        except (TypeError, ValueError):
            return None
        if 0 <= start < 1440 and 0 <= end < 1440 and start != end:
            return start, end
        return None


    if isinstance(window, (tuple, list)):
        if len(window) == 2 and not isinstance(window[0], (tuple, list)) and not isinstance(window[1], (tuple, list)):
            pair = _valid_pair(window[0], window[1])
            return (pair,) if pair else None
        pairs = []
        for item in window:
            if isinstance(item, (tuple, list)) and len(item) == 2:
                pair = _valid_pair(item[0], item[1])
                if pair and pair not in pairs:
                    pairs.append(pair)
        return tuple(sorted(pairs)) or None

    if not isinstance(window, str):
        return None

    raw = window.strip()
    if not raw or raw == "0":
        return None

    raw = re.sub(r"(?<=\d)\s*-\s*(?=\d)", "-", raw)
    tokens = [token for token in re.split(r"[\s,;]+", raw) if token]
    pairs = []
    pattern = re.compile(r"(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})")
    for token in tokens:
        match = pattern.fullmatch(token)
        if not match:
            return None
        start_h, start_m, end_h, end_m = map(int, match.groups())
        if start_h > 23 or end_h > 23 or start_m > 59 or end_m > 59:
            return None
        pair = _valid_pair(start_h * 60 + start_m, end_h * 60 + end_m)
        if not pair:
            return None
        if pair not in pairs:
            pairs.append(pair)

    return tuple(sorted(pairs)) or None


def _format_daily_pause_window(window):
    windows = _normalize_daily_pause_window(window)
    if windows is None:
        return "0 (chạy liên tục)"
    return " ".join(
        f"{start // 60:02d}:{start % 60:02d}-{end // 60:02d}:{end % 60:02d}"
        for start, end in windows
    )


def _prompt_daily_pause_window(tool_name, step_number):

    while True:
        print(
            f"{step_number}. ⏰ Khung giờ chạy {tool_name} (nhập nhiều khung cách nhau bằng dấu cách, "
            "ví dụ 17:30-20:30 22:10-23:59; nhập 0 = chạy liên tục):",
            flush=True,
        )
        raw = input("").strip()
        if raw == "0":
            return None

        windows = _normalize_daily_pause_window(raw)
        if windows is not None:
            return windows

        print(
            "❌ Khung giờ không hợp lệ. Ví dụ: 17:30-20:30 22:10-23:59 hoặc 0. "
            "Có thể nhập nhiều khung, cách nhau bằng dấu cách, dấu phẩy hoặc dấu chấm phẩy; "
            "mỗi khung phải có giờ hợp lệ và giờ bắt đầu khác giờ kết thúc.",
            flush=True,
        )


def _daily_pause_is_active(window, now=None):
    windows = _normalize_daily_pause_window(window)
    if windows is None:
        return False
    if now is None:
        now = datetime.now(_VIETNAM_TZ)
    current_minute = now.hour * 60 + now.minute

    for start, end in windows:
       
        if start < end and start <= current_minute < end:
            return True
        if start > end and (current_minute >= start or current_minute < end):
            return True
    return False

def _check_daily_pause_window(window, tool_name):
   
    paused = _daily_pause_is_active(window)
    notify = None
    with _SCHEDULE_STATE_LOCK:
        previous = _SCHEDULE_PAUSED_STATE.get(tool_name)
        if paused and previous is not True:
            notify = "pause"
            _SCHEDULE_PAUSED_STATE[tool_name] = True
        elif not paused and previous is True:
            notify = "resume"
            _SCHEDULE_PAUSED_STATE[tool_name] = False
        elif previous is None:
            _SCHEDULE_PAUSED_STATE[tool_name] = paused

    if notify == "pause":
        print(
            f"⏸️ {tool_name}: ĐANG TẠM DỪNG theo lịch "
            f"{_format_daily_pause_window(window)} "
            "Không phân tích và không đặt cược.",
            flush=True,
        )
    elif notify == "resume":
        print(
            f"▶️ {tool_name}: HẾT GIỜ NGHỈ, tự động chạy phân tích và đặt cược lại.",
            flush=True,
        )
    return paused


def _reset_daily_pause_state(tool_name):
    with _SCHEDULE_STATE_LOCK:
        _SCHEDULE_PAUSED_STATE[tool_name] = None

_ORIGINAL_INPUT = input

_RICH_MARKUP_RE = re.compile(r"\[(?:/?[a-zA-Z][^\]]*)\]")

def _strip_rich_markup(value):
    text = str(value or "")
    text = _RICH_MARKUP_RE.sub("", text)
    text = text.replace("\x00", "").replace("\r", "")
    return text

def _telegram_input(prompt=""):
  
    clean_prompt = _strip_rich_markup(prompt)
    if clean_prompt:
        try:
            print(clean_prompt, flush=True)
        except Exception:
            pass
    try:
        return _ORIGINAL_INPUT()
    except EOFError:
        return ""
    except KeyboardInterrupt:
        return ""


class _WorkerSafeOutput:

    def __init__(self, stream):
        self._stream = stream

    def write(self, data):
        try:
            return self._stream.write(data)
        except (BrokenPipeError, OSError, ValueError) as exc:
            raise SystemExit(0) from exc

    def flush(self):
        try:
            return self._stream.flush()
        except (BrokenPipeError, OSError, ValueError) as exc:
            raise SystemExit(0) from exc

    def __getattr__(self, name):
        return getattr(self._stream, name)


def _protect_worker_output_streams():
    if not isinstance(sys.stdout, _WorkerSafeOutput):
        sys.stdout = _WorkerSafeOutput(sys.stdout)
    if not isinstance(sys.stderr, _WorkerSafeOutput):
        sys.stderr = _WorkerSafeOutput(sys.stderr)


input = _telegram_input

_WORKER_CHAT_ID = None


try:
    import requests
except ImportError as exc:
    raise SystemExit("Thiếu requests. Cài: pip install requests") from exc


class _AnsiFore:
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

class _AnsiStyle:
    RESET_ALL = "\033[0m"

Fore = _AnsiFore()
Style = _AnsiStyle()

BOT_TOKEN = os.getenv("BOT_TOKEN", "8344831081:AAFdrGdOnkLFi0CyzY6N1UQC2Jab0nWVutU").strip()
SHOP_VIP_ACTIVATION_WEBHOOK_URL = os.getenv("SHOP_VIP_ACTIVATION_WEBHOOK_URL", "https://shopkey.injectcloud.space/vip-activated").strip()
SHOP_VIP_ACTIVATION_WEBHOOK_SECRET = os.getenv("VIP_ACTIVATION_WEBHOOK_SECRET", "VANBAOTOOL").strip()
POLL_TIMEOUT = 25
MAX_TEXT = 3900
FLUSH_INTERVAL = 0.40

def _parse_chat_ids(value):
    ids = set()
    for item in str(value or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.add(int(item))
        except (TypeError, ValueError):
            pass
    return ids
    
_ALLOWED = os.getenv("ALLOWED_CHAT_IDS", "801844480").strip()
ALLOWED_CHAT_IDS = _parse_chat_ids(_ALLOWED)


_ADMIN = os.getenv("ADMIN_CHAT_IDS", "801844480").strip()
ADMIN_CHAT_IDS = _parse_chat_ids(_ADMIN)
ADMIN_CHAT_IDS.add(8801844480)

TOOLS = {
    "vth": ("⚡ VTH", "main_vth"),
    "lotto": ("🎲 LOTTO", "main_lotto"),
    "cdtd": ("🏎️ CDTD", "main_cdtd"),
    "xworld": ("🎯 CANH CODE", "xw_main"),
    "hilo": ("🃏 HILO", "hilo_main"),
}

ANSI_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
SESSIONS = {}
SESSIONS_LOCK = threading.Lock()
STOP_ALL = threading.Event()
_RENDER_HTTP_SERVER = None

_BOT_POLL_ACTIVITY_LOCK = threading.Lock()
_BOT_POLL_ACTIVITY_TS = time.time()
_BOT_WATCHDOG_THREAD = None
_BOT_WATCHDOG_STOP = threading.Event()


def _mark_bot_poll_activity():
    global _BOT_POLL_ACTIVITY_TS
    with _BOT_POLL_ACTIVITY_LOCK:
        _BOT_POLL_ACTIVITY_TS = time.time()


def _bot_poll_watchdog():

    stale_limit = max(180.0, float(POLL_TIMEOUT) * 6.0)
    while not STOP_ALL.is_set() and not _BOT_WATCHDOG_STOP.wait(15.0):
        with _BOT_POLL_ACTIVITY_LOCK:
            age = time.time() - _BOT_POLL_ACTIVITY_TS
        if age > stale_limit:
            try:
                print(
                    f"[Watchdog] Telegram polling không hoạt động {int(age)}s; thoát process để Render tự khởi động lại.",
                    flush=True,
                )
            except Exception:
                pass

            os._exit(70)


def _start_render_health_server():

    global _RENDER_HTTP_SERVER

    port_raw = os.getenv("PORT", "").strip() or "10000"
    try:
        port = int(port_raw)
        if not (1 <= port <= 65535):
            raise ValueError("PORT ngoài phạm vi")
    except (TypeError, ValueError):
        print(f"[Render] PORT không hợp lệ: {port_raw!r}", flush=True)
        return None

    try:
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        class _HealthHandler(BaseHTTPRequestHandler):
            def _send_ok(self):
                body = b"OK"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                return body

            def do_GET(self):
                body = self._send_ok()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def do_HEAD(self):
                self._send_ok()

            def log_message(self, format, *args):
                return

        class _ReusableHealthServer(ThreadingHTTPServer):
            allow_reuse_address = True

        server = _ReusableHealthServer(("0.0.0.0", port), _HealthHandler)
        server.daemon_threads = True
        _RENDER_HTTP_SERVER = server
        threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.5},
            daemon=True,
            name="render-health-server",
        ).start()
        print(f"[Render] Health server listening on 0.0.0.0:{port}", flush=True)
        return server
    except OSError as exc:
        print(f"[Render] Không mở được PORT {port}: {type(exc).__name__}: {exc}", flush=True)
        return None
    except Exception as exc:
        print(f"[Render] Health server lỗi: {type(exc).__name__}: {exc}", flush=True)
        return None


def _stop_render_health_server():
    global _RENDER_HTTP_SERVER
    server = _RENDER_HTTP_SERVER
    _RENDER_HTTP_SERVER = None
    if server is not None:
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            server.server_close()
        except Exception:
            pass

if Path("/var/data").is_dir():
    BOT_DATA_DIR = Path(os.getenv("VBTOOL_DATA_DIR", "/var/data")).expanduser()
else:
    BOT_DATA_DIR = Path(os.getenv("VBTOOL_DATA_DIR", ".")).expanduser()
BOT_DATA_DIR.mkdir(parents=True, exist_ok=True)

BOT_AUTH_FILE = str(BOT_DATA_DIR / "telegram_auth.json")
BOT_AUTH_HISTORY_FILE = str(BOT_DATA_DIR / "telegram_auth_history.json")
BOT_PENDING_FILE = str(BOT_DATA_DIR / "telegram_pending.json")
BOT_FREE_USED_FILE = str(BOT_DATA_DIR / "telegram_free_used.json")
BOT_EXPIRED_USERS_FILE = str(BOT_DATA_DIR / "telegram_expired_users.json")
BOT_VIP_ACTIVATED_FILE = str(BOT_DATA_DIR / "telegram_vip_activated.json")
BOT_BAN_FILE = str(BOT_DATA_DIR / "telegram_bans.json")
BOT_CHAT_USERS_FILE = str(BOT_DATA_DIR / "telegram_chat_users.json")
BOT_SPAM_WINDOW = 600
BOT_MAX_AUTH_ATTEMPTS = 3
BOT_BAN_MIN_HOURS = 12
BOT_BAN_MAX_HOURS = 48
BOT_AUTH = {}
BOT_EXPIRED_PENDING = {}
BOT_EXPIRED_USERS = {} 
BOT_VIP_ACTIVATED = {}
_CURRENT_CHAT_ID = None
BOT_SPAM = {}
BOT_VIP_SPAM = {}    
BOT_AUTH_LOCK = threading.Lock()
BOT_KEY_EXPIRY_WATCHER = None
BOT_KEY_EXPIRY_STOP = threading.Event()

def _load_json_file(path, default):
    try:
        fp = Path(path)
        if not fp.exists():
            return default
        with fp.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, type(default)) else default
    except Exception:
        return default

def _save_json_file(path, data):
    try:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(target) + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        tmp.replace(target)
        return True
    except Exception:
        return False

BOT_BANS = _load_json_file(BOT_BAN_FILE, {})
BOT_AUTH = _load_json_file(BOT_AUTH_FILE, {})
BOT_AUTH_HISTORY = _load_json_file(BOT_AUTH_HISTORY_FILE, {})
BOT_EXPIRED_USERS = _load_json_file(BOT_EXPIRED_USERS_FILE, {})
BOT_PENDING = _load_json_file(BOT_PENDING_FILE, {})
BOT_CHAT_USERS = _load_json_file(BOT_CHAT_USERS_FILE, [])
if not isinstance(BOT_CHAT_USERS, list):
    BOT_CHAT_USERS = []
BOT_CHAT_USERS = list(dict.fromkeys(str(x) for x in BOT_CHAT_USERS if str(x).strip()))
BOT_CHAT_USERS_LOCK = threading.Lock()

BOT_CHAT_WAIT_STOP = set()
BOT_CHAT_COMPOSING = set()


def _save_bot_pending():
    return _save_json_file(BOT_PENDING_FILE, BOT_PENDING)


def _register_chat_user(chat_id):

    try:
        chat_key = str(int(chat_id))
    except (TypeError, ValueError):
        return False
    with BOT_CHAT_USERS_LOCK:
        if chat_key in BOT_CHAT_USERS:
            return True
        BOT_CHAT_USERS.append(chat_key)
        if not _save_json_file(BOT_CHAT_USERS_FILE, BOT_CHAT_USERS):
            BOT_CHAT_USERS.remove(chat_key)
            return False
    return True


def _chat_broadcast_recipients():

    recipients = set()
    with BOT_CHAT_USERS_LOCK:
        recipients.update(str(x) for x in BOT_CHAT_USERS if str(x).strip())
    recipients.update(str(x) for x in BOT_AUTH.keys())
    recipients.update(str(x) for x in BOT_EXPIRED_USERS.keys())
    recipients.update(str(x) for x in BOT_PENDING.keys())
    try:
        for record in BOT_FREE_USED.values():
            if isinstance(record, dict) and record.get("telegram_id") is not None:
                recipients.add(str(record.get("telegram_id")))
    except (NameError, AttributeError):
        pass
    recipients.update(str(x) for x in ADMIN_CHAT_IDS)
    return recipients


def _broadcast_admin_chat(admin_chat_id, message):

    message = str(message or "").strip()
    if not message:
        send_message(admin_chat_id, "⚠️ Nội dung CHAT không được để trống.", home_markup(admin_chat_id))
        return

    recipients = _chat_broadcast_recipients()
    sender_id = str(admin_chat_id)
    targets = []
    for recipient in sorted(recipients):
        if recipient == sender_id:
            continue
        try:
            targets.append(int(recipient))
        except (TypeError, ValueError):
            continue

    def _send_one(target_id):
        try:
            result = send_message(target_id, message, home_markup(target_id))
            return result is not None
        except Exception:
            return False

    sent = 0
    failed = 0
    if targets:

        worker_count = min(5, len(targets))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(_send_one, target_id) for target_id in targets]
            for future in as_completed(futures):
                try:
                    if future.result():
                        sent += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1

    send_message(
        admin_chat_id,
        "✅ ĐÃ GỬI CHAT\n\n"
        f"👥 Gửi thành công: {sent}\n"
        f"⚠️ Không gửi được: {failed}",
        home_markup(admin_chat_id),
    )


def _admin_stop_all_running_tools():

    with SESSIONS_LOCK:
        active_sessions = list(SESSIONS.items())

    requested = 0
    for target_chat_id, session in active_sessions:
        try:
            if session is None or not session.running:
                continue


            session.set_end_message(
                "🛑 TOOL ĐÃ ĐƯỢC DỪNG THEO YÊU CẦU CỦA ADMIN.\n\n"
                "Bạn có thể chọn tool và chạy lại khi được phép.",
                home_markup(target_chat_id),
            )
            threading.Thread(
                target=session.stop,
                daemon=True,
                name=f"admin-stop-tool-{target_chat_id}",
            ).start()
            requested += 1
        except Exception as exc:
            _admin_only_exception_log(target_chat_id, "Admin dừng toàn bộ tool", exc)

    return requested

BOT_FREE_USED = _load_json_file(BOT_FREE_USED_FILE, {})
BOT_VIP_ACTIVATED = _load_json_file(BOT_VIP_ACTIVATED_FILE, {})

# Key FREE được sử dụng đúng 24 giờ kể từ thời điểm /login thành công.
FREE_KEY_DURATION_HOURS = 24.0
FREE_KEY_DURATION_SECONDS = FREE_KEY_DURATION_HOURS * 3600.0


def _now_ts():
    return time.time()

def _parse_iso_ts(value):
    if value in (None, "", False):
        return None
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            ts = float(value)
        else:
            raw = str(value).strip()
            try:
                ts = float(raw)
            except (TypeError, ValueError):
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return dt.timestamp()
        if ts > 100_000_000_000:  # Unix milliseconds
            ts /= 1000.0
        return ts if ts > 0 else None
    except Exception:
        return None

def _ban_remaining(chat_id):
    rec = BOT_BANS.get(str(chat_id))
    if not isinstance(rec, dict):
        return 0
    if rec.get("permanent") is True:
        return -1
    until = float(rec.get("until", 0) or 0)
    remaining = until - _now_ts()
    if remaining <= 0:
        BOT_BANS.pop(str(chat_id), None)
        _save_json_file(BOT_BAN_FILE, BOT_BANS)
        return 0
    return remaining

def _format_duration(seconds):
    total_minutes = max(1, int(seconds // 60))
    days, rem = divmod(total_minutes, 1440)
    hours, minutes = divmod(rem, 60)
    if days:
        return f"{days} ngày {hours} giờ" if hours else f"{days} ngày"
    return f"{hours} giờ {minutes} phút" if hours else f"{minutes} phút"

def _register_vip_spam(chat_id, reason="Nhập Key VIP không hợp lệ 3 lần liên tiếp"):

    key = str(chat_id)
    now = _now_ts()
    with BOT_AUTH_LOCK:
        arr = [t for t in BOT_VIP_SPAM.get(key, []) if now - t <= BOT_SPAM_WINDOW]
        arr.append(now)
        BOT_VIP_SPAM[key] = arr
        count = len(arr)
        if count < BOT_MAX_AUTH_ATTEMPTS:
            return False, count
        BOT_BANS[key] = {
            "permanent": True,
            "reason": reason,
            "created_at": now,
            "attempts": count,
            "source": "vip_key",
        }
        BOT_VIP_SPAM.pop(key, None)
        BOT_SPAM.pop(key, None)
        _save_json_file(BOT_BAN_FILE, BOT_BANS)
        return True, count

def _reset_vip_spam(chat_id):
    BOT_VIP_SPAM.pop(str(chat_id), None)

_ADMIN_ERROR_LOCK = threading.Lock()
_ADMIN_ERROR_LAST_SENT = {}
_ADMIN_ERROR_COOLDOWN = 15.0

def _notify_admin_code_error(context, exc=None, *, chat_id=None, tool_key=None, traceback_text=None):
    
    try:
        if traceback_text is None:
            if exc is not None:
                traceback_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            else:
                traceback_text = ""

        tool_name = TOOLS.get(str(tool_key).strip().lower(), (str(tool_key or "SYSTEM"), ""))[0]
        source_chat = str(chat_id or _WORKER_CHAT_ID or "").strip()
        key = f"{tool_name}|{source_chat}|{context}|{type(exc).__name__ if exc else 'ERROR'}"
        now = time.time()
        with _ADMIN_ERROR_LOCK:
            last = _ADMIN_ERROR_LAST_SENT.get(key, 0.0)
            if now - last < _ADMIN_ERROR_COOLDOWN:
                return False
            _ADMIN_ERROR_LAST_SENT[key] = now

        header = f"🛑 LỖI CODE / RUNTIME\n\n🧰 Tool: {tool_name}\n"
        if source_chat:
            header += f"👤 Telegram ID: {source_chat}\n"
        header += f"📍 Vị trí: {context}\n"
        body = str(traceback_text or str(exc) or "Không có chi tiết lỗi").strip()
        text = (header + "\n" + body)[-3900:]

        sent = False
        for admin_id in sorted(ADMIN_CHAT_IDS):
            try:
                if send_message(admin_id, text, home_markup(admin_id)) is not None:
                    sent = True
            except Exception:
                pass

   
        try:
            log_file = BOT_DATA_DIR / "bot_runtime_errors.log"
            with log_file.open("a", encoding="utf-8") as f:
                f.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {header}\n{body}\n")
        except Exception:
            pass
        return sent
    except Exception:
        return False

def _admin_only_exception_log(chat_id, context, exc):
   
    return _notify_admin_code_error(context, exc, chat_id=chat_id)


def _bot_banned_message(chat_id):
    remaining = _ban_remaining(chat_id)
    if remaining == -1:
        return "⛔ TÀI KHOẢN ĐÃ BỊ BAN VĨNH VIỄN.\n\n🔒 Lý do: Nhập Key VIP không hợp lệ 3 lần liên tiếp."
    if remaining > 0:
        return (
            "⛔ TÀI KHOẢN ĐÃ BỊ BAN DO SPAM.\n\n"
            f"⏰ Thời gian còn lại: {_format_duration(remaining)}"
        )
    return None

def _bot_device_id(chat_id):
    return f"TG-{int(chat_id)}"

def _bot_pending_record(chat_id):
    rec = BOT_PENDING.get(str(chat_id))
    if not isinstance(rec, dict):
        return {}
    expires = float(rec.get("expires_at", 0) or 0)
    created = float(rec.get("created_at", 0) or 0)
    if expires and _now_ts() >= expires:
        BOT_PENDING.pop(str(chat_id), None)
        _save_bot_pending()
        return {}
    if created and _now_ts() - created > 86400:
        BOT_PENDING.pop(str(chat_id), None)
        _save_bot_pending()
        return {}
    return dict(rec)


_BOT_AUTH_RESTORE_LAST = {}
_BOT_AUTH_RESTORE_LOCK = threading.Lock()
_BOT_AUTH_RESTORE_RESULT = {}


def _server_datetime_to_timestamp(value):
    """Đọc hạn từ Key Server ở dạng ISO, Unix seconds hoặc Unix milliseconds."""
    if value in (None, "", False):
        return None
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            ts = float(value)
        else:
            raw = str(value).strip()
            try:
                ts = float(raw)
            except (TypeError, ValueError):
                if raw.endswith("Z"):
                    raw = raw[:-1] + "+00:00"
                dt = datetime.fromisoformat(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp()
        if ts > 100_000_000_000:  # Unix milliseconds
            ts /= 1000.0
        return ts if ts > 0 else None
    except Exception:
        return None


def _save_auth_history_record(chat_id, record):
    
    tid = str(chat_id or "").strip()
    if not tid or not isinstance(record, dict):
        return False
    key_type = str(record.get("key_type") or "").upper()
    if key_type not in {"VIP", "FREE"}:
        return False
    try:
        expiry_value = float(record.get("expires_at", 0) or 0)
    except (TypeError, ValueError):
        expiry_value = _server_datetime_to_timestamp(record.get("expires_at")) or 0.0
    try:
        activated_value = float(record.get("activated_at", 0) or 0)
    except (TypeError, ValueError):
        activated_value = _server_datetime_to_timestamp(record.get("activated_at")) or 0.0
    clean = {
        "key": str(record.get("key") or "").strip().upper(),
        "key_type": key_type,
        "expires_at": expiry_value,
        "device_id": str(record.get("device_id") or _bot_device_id(tid)),
        "duration_hours": record.get("duration_hours"),
        "is_forever": bool(record.get("is_forever")) and key_type == "VIP",
        "activated_at": activated_value,
        "server_backed": bool(record.get("server_backed", True)),
        "saved_at": _now_ts(),
    }
    if not clean["key"]:
        return False
    BOT_AUTH_HISTORY[tid] = clean
    return _save_json_file(BOT_AUTH_HISTORY_FILE, BOT_AUTH_HISTORY)


def _restore_auth_from_local_history(chat_id):
    tid = str(chat_id or "").strip()
    if not tid:
        return False
    hist = BOT_AUTH_HISTORY.get(tid)
    if not isinstance(hist, dict):
        return False

    key_type = str(hist.get("key_type") or "").upper()
    key = str(hist.get("key") or "").strip().upper()
    if key_type not in {"VIP", "FREE"} or not key:
        return False

    expiry = _server_datetime_to_timestamp(hist.get("expires_at")) or 0.0
    activated_value = _server_datetime_to_timestamp(hist.get("activated_at")) or 0.0
    is_forever = key_type == "VIP" and bool(hist.get("is_forever"))

    if is_forever:
        expiry = 0.0
    elif key_type == "FREE":
        # Với dữ liệu cũ thiếu activated_at, suy ngược từ expires_at đã lưu;
        # không dùng saved_at vì trường đó thay đổi mỗi lần bot đọc trạng thái.
        if not activated_value and expiry:
            activated_value = expiry - FREE_KEY_DURATION_SECONDS
        if not activated_value:
            _BOT_AUTH_RESTORE_RESULT[tid] = "INVALID_EXPIRY"
            return False
        expiry = activated_value + FREE_KEY_DURATION_SECONDS
    elif not expiry:
        return False
    elif not activated_value:
        activated_value = _server_datetime_to_timestamp(hist.get("saved_at")) or _now_ts()

    if not is_forever and (not expiry or _now_ts() >= expiry):
        if BOT_EXPIRED_USERS.get(tid) != key_type:
            BOT_EXPIRED_USERS[tid] = key_type
            _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)
        _BOT_AUTH_RESTORE_RESULT[tid] = "EXPIRED"
        return False

    restored = {
        "key": key,
        "key_type": key_type,
        "expires_at": float(expiry or 0.0),
        "device_id": hist.get("device_id") or _bot_device_id(tid),
        "duration_hours": None if is_forever else (24 if key_type == "FREE" else hist.get("duration_hours")),
        "is_forever": is_forever,
        "activated_at": float(activated_value or _now_ts()),
        "server_backed": bool(hist.get("server_backed", True)),
        "restored_from_history": True,
    }
    with BOT_AUTH_LOCK:
        BOT_AUTH[tid] = restored
        _save_json_file(BOT_AUTH_FILE, BOT_AUTH)
    _save_auth_history_record(tid, restored)
    log_debug(f"Restored unexpired {key_type} auth from local history for Telegram ID {tid}")
    return True


def _clear_cached_vip_auth_on_server_failure(telegram_id, force=False):

    log_debug(
        f"Keeping cached Telegram auth after server-check failure for {str(telegram_id or '').strip()} "
        f"(forced={force})"
    )
    return False

def _restore_telegram_auth_from_server(chat_id, force=False):
    telegram_id = str(chat_id or "").strip()
    if not telegram_id:
        return False
    now = time.time()
    with _BOT_AUTH_RESTORE_LOCK:
        last_check = float(_BOT_AUTH_RESTORE_LAST.get(telegram_id, 0) or 0)
        if not force and now - last_check < 20:
            return False
        _BOT_AUTH_RESTORE_LAST[telegram_id] = now

    try:
        headers = {}
        payload = {"telegram_id": telegram_id}
        if KEY_SERVER_ADMIN_SECRET:
            headers["X-Server-Secret"] = KEY_SERVER_ADMIN_SECRET
            payload["admin_secret"] = KEY_SERVER_ADMIN_SECRET
        response = requests.post(
            f"{KEY_SERVER_URL}/api/telegram/auth_status",
            json=payload,
            headers=headers,
            timeout=8,
        )
        data = response.json() if response.content else {}
        if response.status_code != 200 or not isinstance(data, dict):
            status = "UNAUTHORIZED" if response.status_code in (401, 403) else f"HTTP_{response.status_code}"
            _BOT_AUTH_RESTORE_RESULT[telegram_id] = status
            _clear_cached_vip_auth_on_server_failure(telegram_id, force=force)
            log_debug(
                f"Telegram auth restore unavailable for {telegram_id}: "
                f"HTTP {response.status_code}; forced={force}; cached auth preserved"
            )
            return False
    except Exception as exc:
        _BOT_AUTH_RESTORE_RESULT[telegram_id] = "CONNECTION_ERROR"
        _clear_cached_vip_auth_on_server_failure(telegram_id, force=force)
        log_debug(
            f"Telegram auth restore failed for {telegram_id}: {type(exc).__name__}: {exc}; "
            "cached auth preserved"
        )
        return False

    if not data.get("success"):
        status = str(data.get("status") or "").upper() or "UNKNOWN"
        _BOT_AUTH_RESTORE_RESULT[telegram_id] = status
        log_debug(
            f"Telegram auth status for {telegram_id}: {status or 'UNKNOWN'}; "
            "keeping cached auth"
        )
        return False

    server_rec = data.get("vip") or data.get("free") or data.get("preferred")
    if not isinstance(server_rec, dict):
        _BOT_AUTH_RESTORE_RESULT[telegram_id] = "INVALID_RECORD"
        log_debug(f"Telegram auth response for {telegram_id} has no usable record; cache preserved")
        return False

    key = str(server_rec.get("key") or "").strip().upper()
    key_type = str(server_rec.get("key_type") or "").strip().upper()
    is_forever = bool(server_rec.get("is_forever")) and key_type == "VIP"
    if not key or key_type not in {"VIP", "FREE"}:
        _BOT_AUTH_RESTORE_RESULT[telegram_id] = "INVALID_RECORD"
        log_debug(f"Telegram auth response for {telegram_id} is incomplete; cache preserved")
        return False

    expiry_ts = 0.0 if is_forever else (_server_datetime_to_timestamp(server_rec.get("expires_at")) or 0.0)
    activated_ts = _server_datetime_to_timestamp(server_rec.get("activated_at")) or 0.0

    if key_type == "FREE":
        # Không khôi phục lại cùng một FREE key sau khi thời hạn 24 giờ cục bộ đã hết.
        hist = BOT_AUTH_HISTORY.get(telegram_id)
        if isinstance(hist, dict) and str(hist.get("key") or "").strip().upper() == key:
            hist_activated = _server_datetime_to_timestamp(hist.get("activated_at")) or 0.0
            hist_expiry = _server_datetime_to_timestamp(hist.get("expires_at")) or 0.0
            if not hist_activated and hist_expiry:
                hist_activated = hist_expiry - FREE_KEY_DURATION_SECONDS
            already_expired_locally = str(BOT_EXPIRED_USERS.get(telegram_id) or "").upper() == "FREE"
            elapsed_24h = bool(hist_activated and now >= hist_activated + FREE_KEY_DURATION_SECONDS)
            if already_expired_locally or elapsed_24h:
                BOT_EXPIRED_USERS[telegram_id] = "FREE"
                _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)
                _BOT_AUTH_RESTORE_RESULT[telegram_id] = "EXPIRED"
                log_debug(f"Prevented restoring expired FREE key for Telegram ID {telegram_id}")
                return False

        # Nếu server không trả activated_at, suy ngược từ expires_at để không
        # vô tình bắt đầu lại 24 giờ mỗi lần bot đồng bộ trạng thái.
        if not activated_ts and expiry_ts:
            activated_ts = expiry_ts - FREE_KEY_DURATION_SECONDS
        if not activated_ts:
            _BOT_AUTH_RESTORE_RESULT[telegram_id] = "INVALID_EXPIRY"
            log_debug(f"FREE auth response for {telegram_id} has no activation/expiry timestamp")
            return False
        expiry_ts = activated_ts + FREE_KEY_DURATION_SECONDS
        if expiry_ts <= now:
            BOT_EXPIRED_USERS[telegram_id] = "FREE"
            _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)
            _BOT_AUTH_RESTORE_RESULT[telegram_id] = "EXPIRED"
            return False
        duration_hours = FREE_KEY_DURATION_HOURS
        is_forever = False
    else:
        if not is_forever and not expiry_ts:
            _BOT_AUTH_RESTORE_RESULT[telegram_id] = "INVALID_EXPIRY"
            log_debug(f"Telegram auth response for {telegram_id} has invalid expiry; cache preserved")
            return False
        if expiry_ts and expiry_ts <= now:
            _BOT_AUTH_RESTORE_RESULT[telegram_id] = "EXPIRED"
            log_debug(f"Telegram auth response for {telegram_id} is expired; cache preserved for local expiry handling")
            return False
        duration_hours = server_rec.get("duration")
        try:
            duration_hours = float(duration_hours) if duration_hours is not None else None
        except (TypeError, ValueError):
            duration_hours = None
        if not activated_ts:
            activated_ts = now

    restored = {
        "key": key,
        "key_type": key_type,
        "expires_at": float(expiry_ts or 0.0),
        "device_id": server_rec.get("device") or _bot_device_id(chat_id),
        "duration_hours": None if is_forever else duration_hours,
        "is_forever": is_forever,
        "activated_at": float(activated_ts),
        "restored_from_server": True,
        "server_backed": True,
    }
    with BOT_AUTH_LOCK:
        BOT_AUTH[telegram_id] = restored
        BOT_EXPIRED_PENDING.pop(telegram_id, None)
        BOT_EXPIRED_USERS.pop(telegram_id, None)
        _save_json_file(BOT_AUTH_FILE, BOT_AUTH)
        _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)
    _save_auth_history_record(telegram_id, restored)
    _BOT_AUTH_RESTORE_RESULT[telegram_id] = "ACTIVE"
    return True


def _bot_auth_record(chat_id):
    key = str(chat_id)
    with BOT_AUTH_LOCK:
        rec = BOT_AUTH.get(key)
    if not isinstance(rec, dict):
        _restore_auth_from_local_history(chat_id)
        with BOT_AUTH_LOCK:
            rec = BOT_AUTH.get(key)
    if not isinstance(rec, dict):
        # Không cho server khôi phục lại đúng FREE key đã hết 24 giờ; một key mới
        # vẫn có thể được kích hoạt bình thường qua /login.
        hist = BOT_AUTH_HISTORY.get(key)
        hist_key = str(hist.get("key") or "").strip().upper() if isinstance(hist, dict) else ""
        hist_type = str(hist.get("key_type") or "").upper() if isinstance(hist, dict) else ""
        hist_expiry = (_server_datetime_to_timestamp(hist.get("expires_at")) or 0.0) if isinstance(hist, dict) else 0.0
        hist_activated = (_server_datetime_to_timestamp(hist.get("activated_at")) or 0.0) if isinstance(hist, dict) else 0.0
        if hist_type == "FREE" and hist_key:
            if not hist_activated and hist_expiry:
                hist_activated = hist_expiry - FREE_KEY_DURATION_SECONDS
            already_expired_locally = str(BOT_EXPIRED_USERS.get(key) or "").upper() == "FREE"
            elapsed_24h = bool(hist_activated and _now_ts() >= hist_activated + FREE_KEY_DURATION_SECONDS)
            if already_expired_locally or elapsed_24h:
                BOT_EXPIRED_USERS[key] = "FREE"
                _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)
                _BOT_AUTH_RESTORE_RESULT[key] = "EXPIRED"
                return None
        _restore_telegram_auth_from_server(chat_id)
        with BOT_AUTH_LOCK:
            rec = BOT_AUTH.get(key)
    if not isinstance(rec, dict):
        return None

    with BOT_AUTH_LOCK:
        rec = BOT_AUTH.get(key)
        if not isinstance(rec, dict):
            return None

        key_type = str(rec.get("key_type", "FREE")).upper()
        is_vip_record = key_type == "VIP"
        if rec.get("is_forever") or (is_vip_record and "duration_hours" in rec and rec.get("duration_hours") is None):
            changed = not bool(rec.get("is_forever")) or bool(rec.get("expires_at"))
            rec["is_forever"] = True
            rec["expires_at"] = 0.0
            if changed:
                BOT_AUTH[key] = rec
                _save_json_file(BOT_AUTH_FILE, BOT_AUTH)

        if key_type == "FREE":
            # Chuẩn hóa cả record cũ: hạn FREE luôn là activated_at + đúng 24 giờ.
            activated = _server_datetime_to_timestamp(rec.get("activated_at")) or 0.0
            try:
                expiry = float(rec.get("expires_at", 0) or 0)
            except (TypeError, ValueError):
                expiry = 0.0
            if not activated and expiry:
                activated = expiry - FREE_KEY_DURATION_SECONDS
            if activated:
                expected_expiry = activated + FREE_KEY_DURATION_SECONDS
                changed = (
                    abs(expiry - expected_expiry) > 1.0
                    or rec.get("duration_hours") != FREE_KEY_DURATION_HOURS
                    or bool(rec.get("is_forever"))
                )
                rec["activated_at"] = float(activated)
                rec["expires_at"] = float(expected_expiry)
                rec["duration_hours"] = FREE_KEY_DURATION_HOURS
                rec["is_forever"] = False
                if changed:
                    BOT_AUTH[key] = rec
                    _save_json_file(BOT_AUTH_FILE, BOT_AUTH)
            else:
                # Record FREE thiếu cả thời điểm kích hoạt lẫn hạn: không cho chạy vô thời hạn.
                expiry = 0.0
        else:
            try:
                expiry = float(rec.get("expires_at", 0) or 0)
            except (TypeError, ValueError):
                expiry = 0.0

        if key_type == "FREE" and not activated:
            expiry = 0.0
        if expiry and _now_ts() >= expiry:
            expired_rec = dict(rec)
            expired_rec["__expired__"] = True
            expired_type = key_type
            BOT_EXPIRED_PENDING[key] = expired_type
            if BOT_EXPIRED_USERS.get(key) != expired_type:
                BOT_EXPIRED_USERS[key] = expired_type
                _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)
            return expired_rec
        if key_type == "FREE" and not expiry:
            expired_rec = dict(rec)
            expired_rec["__expired__"] = True
            BOT_EXPIRED_PENDING[key] = "FREE"
            BOT_EXPIRED_USERS[key] = "FREE"
            _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)
            return expired_rec

        _save_auth_history_record(key, rec)
        return dict(rec)


def _bot_auth_valid(chat_id, require_vip=False):
    rec = _bot_auth_record(chat_id)
    if not rec:
        return False, None

    if rec.get("__expired__"):
        return False, rec

    is_vip = str(rec.get("key_type", "FREE")).upper() == "VIP"
    if require_vip and not is_vip:
        return False, rec
    return True, rec

def _set_bot_auth(chat_id, key, key_type, expires_at, server_device_id=None, duration_hours=None, is_forever=False):
    # Key VIP vĩnh viễn dùng expires_at=0; FREE luôn có đúng 24 giờ từ lúc /login thành công.
    key_type_value = str(key_type).upper()
    is_forever = bool(is_forever) and key_type_value == "VIP"
    activated_ts = _now_ts()
    if key_type_value == "FREE":
        duration_hours = FREE_KEY_DURATION_HOURS
        expiry_value = activated_ts + FREE_KEY_DURATION_SECONDS
        is_forever = False
    else:
        expiry_value = 0.0 if is_forever else float(expires_at or 0)
    with BOT_AUTH_LOCK:
        BOT_EXPIRED_PENDING.pop(str(chat_id), None)
        # Xóa dấu hết hạn khi người dùng xác thực key mới thành công.
        if BOT_EXPIRED_USERS.pop(str(chat_id), None) is not None:
            _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)
    rec = {
        "key": str(key).strip().upper(),
        "key_type": str(key_type).upper(),
        "expires_at": expiry_value,
        "device_id": server_device_id or _bot_device_id(chat_id),
        "duration_hours": None if is_forever else duration_hours,
        "is_forever": is_forever,
        "activated_at": activated_ts,
        "server_backed": True,
    }
    with BOT_AUTH_LOCK:
        BOT_AUTH[str(chat_id)] = rec
        _save_json_file(BOT_AUTH_FILE, BOT_AUTH)
    _save_auth_history_record(chat_id, rec)
    if str(key_type).upper() == "VIP":
        _reset_vip_spam(chat_id)
    return rec

def _free_key_used(key):
    return str(key or "").strip() in BOT_FREE_USED


def _mark_free_key_used(key, chat_id, link=None):
    key = str(key or "").strip()
    if not key:
        return False
    BOT_FREE_USED[key] = {
        "telegram_id": str(chat_id),
        "activated_at": _now_ts(),
        "link": str(link or "").strip(),
    }
    return _save_json_file(BOT_FREE_USED_FILE, BOT_FREE_USED)


def _revoke_vip_on_server(target_value, subtract_days=0):
    """Trừ ngày còn hạn hoặc thu hồi VIP ngay trên Key Server."""
    target_value = str(target_value or "").strip()
    if not target_value:
        return False, "Vui lòng nhập Telegram ID hoặc Key VIP", "", False
    if not KEY_SERVER_ADMIN_SECRET:
        return False, "KEY_SERVER_ADMIN_SECRET chưa được cấu hình", "", False
    try:
        subtract_days = int(subtract_days or 0)
    except (TypeError, ValueError):
        return False, "Số ngày không hợp lệ", "", False
    if subtract_days < 0 or subtract_days > 36500:
        return False, "Số ngày phải từ 0 đến 36500", "", False
    payload = {"key_type": "VIP", "admin_secret": KEY_SERVER_ADMIN_SECRET, "subtract_days": subtract_days}
    if re.fullmatch(r"-?\d+", target_value):
        payload["telegram_id"] = target_value
    else:
        payload["key"] = target_value.upper()
    try:
        resp = requests.post(f"{KEY_SERVER_URL}/api/telegram/revoke", json=payload, timeout=15)
        data = resp.json() if resp.content else {}
        if resp.status_code != 200 or not isinstance(data, dict):
            return False, f"HTTP {resp.status_code}", "", False
        if not data.get("success"):
            return False, str(data.get("message", "Server không xác nhận thao tác")), "", False
        owner_id = str(data.get("telegram_id") or (target_value if re.fullmatch(r"-?\d+", target_value) else "")).strip()
        is_reduction = str(data.get("status", "")).upper() == "SUBTRACTED_DAYS"
        if is_reduction:
            # Lấy hạn mới từ server để cập nhật cache bot đang dùng.
            if owner_id:
                verify = requests.post(
                    f"{KEY_SERVER_URL}/api/telegram/auth_status",
                    json={"telegram_id": owner_id, "admin_secret": KEY_SERVER_ADMIN_SECRET},
                    headers={"X-Server-Secret": KEY_SERVER_ADMIN_SECRET}, timeout=10,
                )
                check = verify.json() if verify.content else {}
                if verify.status_code == 200 and isinstance(check, dict) and isinstance(check.get("vip"), dict):
                    expiry = _server_datetime_to_timestamp(check["vip"].get("expires_at"))
                    if expiry:
                        with BOT_AUTH_LOCK:
                            rec = BOT_AUTH.get(owner_id)
                            if isinstance(rec, dict) and str(rec.get("key_type", "")).upper() == "VIP":
                                rec["expires_at"] = float(expiry)
                                _save_json_file(BOT_AUTH_FILE, BOT_AUTH)
                    return True, str(data.get("message", f"Đã trừ {subtract_days} ngày VIP")), owner_id, True
                # Trừ ngày làm hạn VIP chạm/vượt hiện tại: server đã thu hồi hết VIP.
                if verify.status_code == 200 and isinstance(check, dict) and not isinstance(check.get("vip"), dict):
                    return True, str(data.get("message", "Đã trừ ngày; thời hạn VIP đã hết")), owner_id, False
                return False, "Không xác minh được hạn VIP mới trên server.", owner_id, False
            return False, "Không xác định được Telegram ID chủ sở hữu để đồng bộ hạn VIP.", owner_id, False

        if owner_id:
            verify = requests.post(
                f"{KEY_SERVER_URL}/api/telegram/auth_status",
                json={"telegram_id": owner_id, "admin_secret": KEY_SERVER_ADMIN_SECRET},
                headers={"X-Server-Secret": KEY_SERVER_ADMIN_SECRET}, timeout=10,
            )
            check = verify.json() if verify.content else {}
            if verify.status_code != 200 or not isinstance(check, dict):
                return False, "Không xác minh được trạng thái sau thu hồi.", owner_id, False
            if check.get("success") and isinstance(check.get("vip"), dict):
                return False, "Server vẫn trả về Key VIP còn hiệu lực sau thu hồi.", owner_id, False
        return True, str(data.get("message", "Đã thu hồi Key VIP trên server")), owner_id, False
    except Exception as exc:
        return False, str(exc), "", False

def _revoke_bot_auth(target_chat_id, key_type=None):

    target = str(target_chat_id).strip()
    with BOT_AUTH_LOCK:
        rec = BOT_AUTH.get(target)
        if not isinstance(rec, dict):
            return False, None
        if key_type and str(rec.get("key_type", "")).upper() != str(key_type).upper():
            return False, rec
        BOT_AUTH.pop(target, None)
        _save_json_file(BOT_AUTH_FILE, BOT_AUTH)
        BOT_AUTH_HISTORY.pop(target, None)
        _save_json_file(BOT_AUTH_HISTORY_FILE, BOT_AUTH_HISTORY)
    BOT_PENDING.pop(target, None)
    _save_bot_pending()
    return True, rec

def _clear_bot_auth_for_reauth(target_chat_id):
    """Xóa cache xác thực cục bộ để người dùng trở lại menu nhập Key."""
    target = str(target_chat_id or "").strip()
    if not target:
        return None
    with BOT_AUTH_LOCK:
        rec = BOT_AUTH.pop(target, None)
        if rec is not None:
            _save_json_file(BOT_AUTH_FILE, BOT_AUTH)
        # Hàm này chỉ được gọi sau các luồng thu hồi đã xác minh với server.
        BOT_AUTH_HISTORY.pop(target, None)
        _save_json_file(BOT_AUTH_HISTORY_FILE, BOT_AUTH_HISTORY)
    if target in BOT_PENDING:
        BOT_PENDING.pop(target, None)
        _save_bot_pending()
    BOT_EXPIRED_PENDING.pop(target, None)
    if BOT_EXPIRED_USERS.pop(target, None) is not None:
        _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)
    return rec


def _stop_tool_silently(target_chat_id):
    """Dừng tool sau khi thu hồi key mà không gửi log/thông báo dừng cho user."""
    try:
        target = int(target_chat_id)
    except (TypeError, ValueError):
        target = target_chat_id
    session = get_session(target)
    if session is None:
        return False
    try:
        session.suppress_output = True
        # Bỏ các nội dung tool đang chờ gửi để không đẩy thông báo cũ sau khi thu hồi.
        for q in (getattr(session, "send_queue", None),):
            if q is not None:
                while True:
                    try:
                        q.get_nowait()
                    except queue.Empty:
                        break
        session.stop()
    except Exception as exc:
        log_debug(f"Silent tool stop failed for {target}: {type(exc).__name__}: {exc}")
    finally:
        with SESSIONS_LOCK:
            if SESSIONS.get(target) is session:
                SESSIONS.pop(target, None)
    return True


def _send_key_entry_menu_after_revoke(target_chat_id):
    """Quay về menu nhập key bằng thông báo chung, không tiết lộ lý do thu hồi."""
    try:
        target = int(target_chat_id)
    except (TypeError, ValueError):
        target = target_chat_id
    send_message(
        target,
        "🔑 HỆ THỐNG KEY\n\n"
        "Bạn chưa có Key đang hoạt động.\n"
        "Vui lòng chọn FREE hoặc VIP để xác thực.",
        _auth_menu_markup(target),
    )


def _admin_revoke_vip_and_notify(admin_chat_id, target_value, subtract_days=0):
    """Trừ ngày còn hạn VIP hoặc thu hồi ngay, đồng bộ cache và phiên chạy."""
    target_value = str(target_value or "").strip()
    server_ok, server_msg, owner_id, was_reduction = _revoke_vip_on_server(target_value, subtract_days)
    if not server_ok:
        send_message(admin_chat_id, f"❌ Không thể xử lý Key VIP.\n{server_msg}", home_markup(admin_chat_id))
        return False

    if owner_id and not was_reduction:
        _clear_bot_auth_for_reauth(owner_id)
        _stop_tool_silently(owner_id)
        send_message(owner_id, "❌ KEY VIP ĐÃ BỊ THU HỒI. Quyền VIP đã được reset.")
        _send_key_entry_menu_after_revoke(owner_id)
    elif owner_id and was_reduction:
        if subtract_days > 0:
            send_message(owner_id, f"⚠️ Thời hạn Key VIP của bạn vừa được điều chỉnh giảm {subtract_days} ngày theo quản trị viên.")

    BOT_PENDING.pop(str(admin_chat_id), None)
    _save_bot_pending()
    lines = ["✅ ĐÃ TRỪ NGÀY KEY VIP" if was_reduction else "✅ ĐÃ THU HỒI KEY VIP THÀNH CÔNG", ""]
    if owner_id:
        lines.append(f"👤 Telegram ID: {owner_id}")
    if re.fullmatch(r"-?\d+", target_value):
        lines.append(f"🎯 Xử lý theo ID: {target_value}")
    else:
        lines.append(f"🔑 Key VIP: {target_value.upper()}")
    if was_reduction:
        lines.append(f"➖ Số ngày đã trừ: {subtract_days} ngày")
        lines.append("ℹ️ Key vẫn hoạt động nếu còn thời hạn sau khi trừ.")
    lines.append(f"🗑️ Server: {server_msg}")
    send_message(admin_chat_id, "\n".join(lines), home_markup(admin_chat_id))
    return True


def _is_bot_admin(chat_id):
    try:
        return int(chat_id) in ADMIN_CHAT_IDS
    except (TypeError, ValueError):
        return False


SHOP_BOT_URL = os.getenv("VBTOOL_SHOP_BOT_URL", "https://t.me/shopkeyvb_bot").strip() or "https://t.me/shopkeyvb_bot"


def _show_buy_menu(chat_id):
    """Chuyển người dùng sang shop riêng; bot chính không tạo đơn/nhận thanh toán."""
    send_message(
        chat_id,
        "🛒 MUA KEY VIP\n\n"
        "Mở bot bán key để xem các gói Key VIP và mua Key.",
        {"inline_keyboard": [[{"text": "🛒 MỞ BOT BÁN KEY VIP", "url": SHOP_BOT_URL}]]},
    )


def _is_admin_only_message(text):
   
    command = str(text or "").strip()
    if not command:
        return False

   
    if command.upper() in {"💬 CHAT", "🔓 MỞ BAN", "🔑 THU HỒI KEY"}:
        return True

    lower = command.lower()
    upper = command.upper()
    if upper.startswith(("/MO_BAN", "/THUHOI")) or upper == "/VIP":
        return True
    if lower.startswith(("/revoke", "/revoke_key", "/unban")):
        return True
    if lower == "/banlist":
        return True
    if lower.startswith("/ban") and not lower.startswith(("/banlist", "/unban")):
        return True
    return False

def _clear_user_auth_pending(chat_id):
   
    key = str(chat_id)
    pending = BOT_PENDING.get(key)
    if not isinstance(pending, dict):
        return False
    if pending.get("mode") in {"vip", "free", "free_login", "free_select"}:
        BOT_PENDING.pop(key, None)
        _save_bot_pending()
        return True
    return False

def _expire_running_tool_for_key(chat_id, key_type):

    key_type = "VIP" if str(key_type).upper() == "VIP" else "FREE"

    with BOT_AUTH_LOCK:
        BOT_EXPIRED_USERS[str(chat_id)] = key_type
        _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)

   
    try:
        session_chat_id = int(chat_id)
    except (TypeError, ValueError):
        session_chat_id = chat_id

    session = get_session(session_chat_id)
    message = (
        f"⏰ KEY {key_type} ĐÃ HẾT HẠN\n\n"
        "Bạn không thể tiếp tục sử dụng tool. Vui lòng nhập Key mới để tiếp tục."
    )
    target_chat_id = getattr(session, "chat_id", session_chat_id)

    if not session or not session.running:
        # Không có tool chạy: vẫn báo hết hạn và đưa thẳng về màn hình nhập key.
        send_message(target_chat_id, message, _auth_menu_markup(target_chat_id))
        return False

    # Đặt end-message TRƯỚC khi stop để _flusher biết đây là kết thúc do hết hạn.
    session.set_end_message(message, _auth_menu_markup(target_chat_id))
    session.stop()
    return True



def _sync_server_vip_revocations():
    """Đồng bộ VIP an toàn: chỉ reset khi server xác nhận đúng key bị khóa/hết hạn."""
    with BOT_AUTH_LOCK:
        candidates = [
            (str(tid), dict(rec))
            for tid, rec in BOT_AUTH.items()
            if isinstance(rec, dict)
            and str(rec.get("key_type") or "").upper() == "VIP"
            and rec.get("server_backed", True)
        ]

    for telegram_id, local_rec in candidates:
        local_key = str(local_rec.get("key") or "").strip().upper()
        if not local_key:
            # Không thể xác định chính xác key thì không được tự thu hồi cache.
            continue

        try:
            headers = {"X-Server-Secret": KEY_SERVER_ADMIN_SECRET} if KEY_SERVER_ADMIN_SECRET else {}
            payload = {"telegram_id": telegram_id, "key": local_key}
            if KEY_SERVER_ADMIN_SECRET:
                payload["admin_secret"] = KEY_SERVER_ADMIN_SECRET
            response = requests.post(
                f"{KEY_SERVER_URL}/api/telegram/auth_status",
                json=payload,
                headers=headers,
                timeout=8,
            )
            server_data = response.json() if response.content else {}
            if response.status_code != 200 or not isinstance(server_data, dict):
                continue
        except Exception as exc:
            log_debug(f"Scheduled VIP status sync failed for {telegram_id}: {type(exc).__name__}: {exc}")
            continue

        server_vip = server_data.get("vip") if server_data.get("success") is True else None
        server_free = server_data.get("free") if server_data.get("success") is True else None

        # Nếu server xác nhận có VIP đang hoạt động, đồng bộ đúng hồ sơ VIP đó.
        # Cả trường hợp key đã đổi cũng không được giữ cache của key cũ.
        if isinstance(server_vip, dict):
            server_vip_key = str(server_vip.get("key") or "").strip().upper()
            is_forever = bool(server_vip.get("is_forever"))
            expiry = 0.0 if is_forever else _server_datetime_to_timestamp(server_vip.get("expires_at"))
            if not server_vip_key or (not is_forever and not expiry):
                continue
            restored = {
                "key": server_vip_key,
                "key_type": "VIP",
                "expires_at": float(expiry or 0.0),
                "device_id": server_vip.get("device") or _bot_device_id(telegram_id),
                "duration_hours": server_vip.get("duration"),
                "is_forever": is_forever,
                "activated_at": _server_datetime_to_timestamp(server_vip.get("activated_at")) or _now_ts(),
                "server_backed": True,
                "restored_from_server": True,
            }
            with BOT_AUTH_LOCK:
                BOT_AUTH[telegram_id] = restored
                _save_json_file(BOT_AUTH_FILE, BOT_AUTH)
            _save_auth_history_record(telegram_id, restored)
            continue

        # NO_ACTIVE_KEY chỉ nói rằng bộ tra cứu không tìm thấy key đang hoạt động.
        # Chỉ xử lý khi server trả trạng thái cụ thể cho đúng key cục bộ và đúng chủ.
        key_status_data = server_data.get("key_status")
        if not isinstance(key_status_data, dict):
            continue
        status = str(key_status_data.get("status") or "").strip().upper()
        confirmed_key = str(key_status_data.get("key") or "").strip().upper()
        confirmed_owner = str(key_status_data.get("telegram_id") or "").strip()
        if confirmed_key != local_key or confirmed_owner != telegram_id:
            continue
        if status not in {"REVOKED", "EXPIRED"}:
            # ACTIVE / NOT_FOUND / OWNER_MISMATCH / UNKNOWN không phải lệnh thu hồi.
            continue

        _stop_tool_silently(telegram_id)
        _clear_bot_auth_for_reauth(telegram_id)

        # Nếu còn FREE hợp lệ, chuyển về FREE thay vì bắt người dùng xác thực lại.
        free_rec = None
        if isinstance(server_free, dict):
            free_expiry = _server_datetime_to_timestamp(server_free.get("expires_at"))
            if free_expiry and free_expiry > _now_ts():
                free_rec = {
                    "key": str(server_free.get("key") or "").strip().upper(),
                    "key_type": "FREE",
                    "expires_at": float(free_expiry),
                    "device_id": str(server_free.get("device") or _bot_device_id(telegram_id)),
                    "duration_hours": server_free.get("duration"),
                    "is_forever": False,
                    "activated_at": _server_datetime_to_timestamp(server_free.get("activated_at")) or _now_ts(),
                    "server_backed": True,
                }
                if free_rec["key"]:
                    with BOT_AUTH_LOCK:
                        BOT_AUTH[telegram_id] = free_rec
                        _save_json_file(BOT_AUTH_FILE, BOT_AUTH)
                    _save_auth_history_record(telegram_id, free_rec)
                else:
                    free_rec = None

        try:
            target_id = int(telegram_id) if telegram_id.lstrip("-").isdigit() else telegram_id
        except (TypeError, ValueError):
            target_id = telegram_id

        if status == "EXPIRED":
            message = "⏰ KEY VIP ĐÃ HẾT HẠN. Quyền VIP đã được reset."
        else:
            message = "❌ KEY VIP ĐÃ BỊ THU HỒI. Quyền VIP đã được reset."

        if free_rec:
            # Đang còn FREE hợp lệ nên không để cờ hết hạn VIP chặn quyền FREE.
            message += "\n✅ Tài khoản vẫn còn Key FREE đang hoạt động."
            send_message(target_id, message, home_markup(target_id))
        else:
            if status == "EXPIRED":
                with BOT_AUTH_LOCK:
                    BOT_EXPIRED_USERS[telegram_id] = "VIP"
                    _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)
            send_message(target_id, message)
            _send_key_entry_menu_after_revoke(telegram_id)


def _bot_key_expiry_watcher():
    """Watcher trung tâm xử lý hết hạn và thu hồi key cho mọi ToolSession."""
    last_server_revoke_sync = 0.0
    while not STOP_ALL.is_set() and not BOT_KEY_EXPIRY_STOP.wait(0.5):
        now = _now_ts()
        expired = {}

        with BOT_AUTH_LOCK:
            # Process chính là nơi duy nhất quyết định key đã hết hạn.
            # Worker chỉ được đánh dấu __expired__ để báo ngược về đây.
            for chat_id, rec in list(BOT_AUTH.items()):
                if not isinstance(rec, dict):
                    continue
                try:
                    expires_at = float(rec.get("expires_at", 0) or 0)
                except (TypeError, ValueError):
                    expires_at = 0.0

                if expires_at and now >= expires_at:
                    expired[str(chat_id)] = str(
                        rec.get("key_type", "FREE")
                    ).upper()

            # Hỗ trợ cả trường hợp worker đã chạm vào record trước watcher.
            for chat_id, key_type in list(BOT_EXPIRED_PENDING.items()):
                expired.setdefault(
                    str(chat_id),
                    str(key_type).upper(),
                )
                BOT_EXPIRED_PENDING.pop(chat_id, None)

            if expired:
                # Lưu dấu hết hạn trước khi xóa quyền xác thực, để không mất trạng thái
                # nếu tiến trình khởi động lại ngay lúc key vừa hết hạn.
                for chat_id, key_type in expired.items():
                    BOT_EXPIRED_USERS[str(chat_id)] = (
                        "VIP" if str(key_type).upper() == "VIP" else "FREE"
                    )
                _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)

                # Xóa quyền xác thực CHỈ SAU khi đã lấy đủ key_type.
                for chat_id in expired:
                    BOT_AUTH.pop(chat_id, None)
                try:
                    _save_json_file(BOT_AUTH_FILE, BOT_AUTH)
                except Exception:
                    pass

        for chat_id, key_type in expired.items():
            try:
                _expire_running_tool_for_key(chat_id, key_type)
            except Exception as exc:
                _notify_admin_code_error(
                    "Watcher hết hạn key lỗi",
                    exc,
                    chat_id=chat_id,
                    tool_key="SYSTEM",
                )

        if time.time() - last_server_revoke_sync >= 30.0:
            last_server_revoke_sync = time.time()
            try:
                _sync_server_vip_revocations()
            except Exception as exc:
                log_debug(f"Server VIP revocation sync error: {type(exc).__name__}: {exc}")


def start_bot_key_expiry_watcher():
    global BOT_KEY_EXPIRY_WATCHER
    if BOT_KEY_EXPIRY_WATCHER is not None and BOT_KEY_EXPIRY_WATCHER.is_alive():
        return
    BOT_KEY_EXPIRY_STOP.clear()
    BOT_KEY_EXPIRY_WATCHER = threading.Thread(
        target=_bot_key_expiry_watcher,
        daemon=True,
        name="bot-key-expiry-watcher",
    )
    BOT_KEY_EXPIRY_WATCHER.start()


def stop_bot_key_expiry_watcher():
    BOT_KEY_EXPIRY_STOP.set()


def _auth_menu_markup(chat_id=None):
    """Tương thích với các luồng xác thực nhưng luôn giữ nguyên Menu chính."""
    return home_markup(chat_id)

def _auth_menu_text(chat_id):
    ban = _bot_banned_message(chat_id)
    if ban:
        return ban
    valid, rec = _bot_auth_valid(chat_id)
    if valid and rec:
        if rec.get("is_forever"):
            expiry_text = "⏰ Hiệu lực: VĨNH VIỄN — KHÔNG GIỚI HẠN\n"
        else:
            remaining = max(0, float(rec.get("expires_at", 0) or 0) - _now_ts())
            expiry_text = f"⏰ Còn: {_format_duration(remaining)}\n"
        return (
            "✅ ĐÃ XÁC THỰC KEY\n\n"
            f"🔑 Loại Key: {rec.get('key_type', 'FREE')}\n"
            f"{expiry_text}\n"
            "Chọn tool để sử dụng."
        )
    return (
        "🔑 HỆ THỐNG KEY\n\n"
        "Bạn chưa có Key đang hoạt động.\n"
        "Vui lòng chọn FREE hoặc VIP để xác thực.\n"
    )


def tg(method, payload=None, timeout=35):
    
    if not BOT_TOKEN:
        raise RuntimeError("Chưa đặt BOT_TOKEN.")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    attempts = 4 if method == "getUpdates" else 1
    last_exc = None
    for attempt in range(attempts):
        try:
            r = requests.post(url, json=payload or {}, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                raise RuntimeError(str(data))
            return data.get("result")
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            if attempt + 1 >= attempts:
                raise
            # Kết nối TLS/Socket trên Android/Render đôi lúc bị peer đóng.
            # Chờ ngắn rồi tạo lại kết nối ở lần gọi tiếp theo.
            time.sleep(min(2 * (attempt + 1), 6))
    if last_exc is not None:
        raise last_exc
    return None


def send_message(chat_id, text, markup=None, parse_mode=None):
    text = text or " "
    payload = {"chat_id": chat_id, "text": text[:4090],
               "disable_web_page_preview": True}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if markup:
        payload["reply_markup"] = markup
    try:
        return tg("sendMessage", payload, timeout=12)
    except Exception as exc:
        # Khi chạy worker, stderr bị nối vào stdout và sẽ được chuyển về user.
        # Không in exception ra stdout/stderr trong Telegram mode; chỉ lưu nội bộ.
        if True:
            try:
                log_file = BOT_DATA_DIR / "bot_runtime_errors.log"
                with log_file.open("a", encoding="utf-8") as f:
                    f.write(
                        f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                        f"[Telegram sendMessage] {type(exc).__name__}: {exc}\n"
                    )
            except Exception:
                pass
        else:
            print("[Telegram]", exc, file=sys.stderr)
        return None


def delete_message(chat_id, message_id):
    """Xóa một tin nhắn Telegram cũ theo chat_id và message_id."""
    try:
        if message_id is None:
            return False
        tg(
            "deleteMessage",
            {"chat_id": chat_id, "message_id": int(message_id)},
            timeout=12,
        )
        return True
    except Exception as exc:
        try:
            log_file = BOT_DATA_DIR / "bot_runtime_errors.log"
            with log_file.open("a", encoding="utf-8") as f:
                f.write(
                    f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"[Telegram deleteMessage] {type(exc).__name__}: {exc}\n"
                )
        except Exception:
            pass
        return False


def send_photo(chat_id, photo_path, caption="", markup=None, parse_mode=None):
    """Gửi ảnh cho Telegram bằng multipart/form-data (sendPhoto)."""
    if not photo_path or not Path(photo_path).is_file():
        return None
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    data = {
        "chat_id": chat_id,
        "caption": str(caption or "")[:1024],
    }
    if parse_mode:
        data["parse_mode"] = parse_mode
    if markup:
        data["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    try:
        with open(photo_path, "rb") as f:
            resp = requests.post(
                url,
                data=data,
                files={"photo": (Path(photo_path).name, f, "image/jpeg")},
                timeout=20,
            )
        resp.raise_for_status()
        payload = resp.json()
        if not payload.get("ok"):
            raise RuntimeError(str(payload))
        return payload.get("result")
    except Exception as exc:
        try:
            log_file = BOT_DATA_DIR / "bot_runtime_errors.log"
            with log_file.open("a", encoding="utf-8") as f:
                f.write(
                    f"\\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"[Telegram sendPhoto] {type(exc).__name__}: {exc}\\n"
                )
        except Exception:
            pass
        return None


def answer_callback(callback_id, text="", show_alert=False):
    try:
        payload = {"callback_query_id": callback_id, "text": str(text or "")[:190]}
        if show_alert:
            payload["show_alert"] = True
        tg("answerCallbackQuery", payload, timeout=15)
    except Exception:
        pass


def updates(offset=None):
    payload = {
        "timeout": POLL_TIMEOUT,
        "allowed_updates": ["message", "callback_query"],
    }
    if offset is not None:
        payload["offset"] = offset
    return tg("getUpdates", payload, timeout=POLL_TIMEOUT + 10)


def home_markup(chat_id=None):

    is_admin = _is_bot_admin(chat_id) if chat_id is not None else False

    if is_admin:

        keyboard = [
            [{"text": "🧰 CHỌN TOOL"}],
            [{"text": "🆓 NHẬP KEY FREE"}, {"text": "👑 NHẬP KEY VIP"}],
            [{"text": "📊 Trạng thái"}, {"text": "⏹️ Dừng"}],
            [{"text": "🛒 MUA KEY VIP"}, {"text": "💬 CHAT"}],
            [{"text": "🔓 MỞ BAN"}, {"text": "🔑 THU HỒI KEY"}],
        ]
    else:

        keyboard = [
            [{"text": "🧰 CHỌN TOOL"}],
            [{"text": "🛒 MUA KEY VIP"}],
            [{"text": "🆓 NHẬP KEY FREE"}, {"text": "👑 NHẬP KEY VIP"}],
            [{"text": "📊 Trạng thái"}, {"text": "⏹️ Dừng"}],
        ]

    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "is_persistent": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "Chọn chức năng...",
    }

def tool_menu_markup():
    
    return {
        "keyboard": [
            [{"text": "⚡ VTH"}, {"text": "🎲 LOTTO"}],
            [{"text": "🏎️ CDTD"}, {"text": "🎯 CANH CODE"}],
            [{"text": "🃏 HILO"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "Chọn tool muốn sử dụng...",
    }


def clean_output(text):
    text = ANSI_RE.sub("", text or "")
    text = _strip_rich_markup(text)
    return "".join(c for c in text if c in "\n\t" or ord(c) >= 32)


_INTERNAL_ERROR_PATTERNS = (
    "Traceback (most recent call last):",
    "NameError:", "TypeError:", "ValueError:", "KeyError:",
    "AttributeError:", "IndexError:", "RuntimeError:",
    "ZeroDivisionError:", "JSONDecodeError:", "ModuleNotFoundError:",
    "ImportError:", "SyntaxError:", "AssertionError:",
    "ConnectionError:", "TimeoutError:", "FileNotFoundError:",
    "requests.exceptions.", "websocket._exceptions.",
)

def _looks_like_internal_error(text):
    """Nhận diện lỗi kỹ thuật để không chuyển traceback/exception cho user."""
    raw = str(text or "")
    return any(pattern in raw for pattern in _INTERNAL_ERROR_PATTERNS)

def chunks(text, limit=MAX_TEXT):
    text = text.strip()
    out = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut < 200:
            cut = limit
        out.append(text[:cut].strip())
        text = text[cut:].strip()
    if text:
        out.append(text)
    return out


class ToolSession:
    def __init__(self, chat_id, key):
        self.chat_id = chat_id
        self.key = key
        self.name, self.fn_name = TOOLS[key]
        self.proc = None
        # Giới hạn stdout queue để tool in quá nhiều log không làm RAM tăng vô hạn.
        self.q = queue.Queue(maxsize=600)
        self.reader_thread = None
        self.stderr_thread = None
        self.flush_thread = None
        self.sender_thread = None
        self.stop_event = threading.Event()
        self.sender_stop_event = threading.Event()
        self.send_queue = queue.Queue(maxsize=200)
        self._last_sent_text = ""
        self._last_sent_ts = 0.0
        self._stdin_lock = threading.Lock()
        self._expected_stop = False
        self.suppress_output = False
        self._end_message = None
        self._end_markup = None
        self._stderr_lines = []
        self.awaiting_cdtd_coin = False
        self.awaiting_cdtd_bet_type = False
        self.awaiting_cdtd_logic = False
        self.started = time.time()
        # Telegram CDTD: đang chờ nút chọn loại tiền (USDT/BUILD/XWORLD).
        self.awaiting_cdtd_coin = False
        self.input_file = os.path.join(
            "/tmp", f"vbtool_tg_input_{chat_id}_{self.started:.6f}.queue"
        )
        try:
            Path(self.input_file).write_text("", encoding="utf-8")
        except Exception:
            pass

    @property
    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self):
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        worker_script = os.path.abspath(__file__)
        self.proc = subprocess.Popen(
            [sys.executable, "-u", worker_script, "--worker", self.key, str(self.chat_id)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            start_new_session=True,
            env=env,
        )
        self.reader_thread = threading.Thread(
            target=self._reader, daemon=True, name=f"tg-stdout-{self.chat_id}-{self.key}")
        self.reader_thread.start()
        self.stderr_thread = threading.Thread(
            target=self._stderr_reader, daemon=True, name=f"tg-stderr-{self.chat_id}-{self.key}")
        self.stderr_thread.start()
        self.sender_thread = threading.Thread(
            target=self._sender_loop, daemon=True, name=f"tg-send-{self.chat_id}-{self.key}")
        self.sender_thread.start()
        self.flush_thread = threading.Thread(
            target=self._flusher, daemon=True, name=f"tg-flush-{self.chat_id}-{self.key}")
        self.flush_thread.start()

    def _reader(self):
        try:
            while True:
                line = self.proc.stdout.readline()
                if line == "":
                    break
                try:
                    self.q.put_nowait(line)
                except queue.Full:
                    
                    try:
                        self.q.get_nowait()
                    except queue.Empty:
                        pass
                    try:
                        self.q.put_nowait(line)
                    except queue.Full:
                        pass
        except Exception as exc:
            _notify_admin_code_error(
                "ToolSession reader lỗi output",
                exc,
                chat_id=self.chat_id,
                tool_key=self.key,
            )
        finally:
            self.stop_event.set()

    def _stderr_reader(self):
        try:
            while True:
                line = self.proc.stderr.readline()
                if line == "":
                    break
                self._stderr_lines.append(line.rstrip("\n"))
                if len(self._stderr_lines) > 120:
                    self._stderr_lines = self._stderr_lines[-120:]
        except Exception as exc:
            _notify_admin_code_error(
                "ToolSession stderr reader lỗi",
                exc,
                chat_id=self.chat_id,
                tool_key=self.key,
            )

    def _enqueue_message(self, text, markup=None):
        if self.suppress_output:
            return
        text = str(text or " ").strip() or " "
        item = (text, markup)
        try:
            self.send_queue.put_nowait(item)
        except queue.Full:

            try:
                self.send_queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self.send_queue.put_nowait(item)
            except queue.Full:
                pass

    def _sender_loop(self):

        min_interval = 0.95
        while not self.sender_stop_event.is_set() or not self.send_queue.empty():
            try:
                text, markup = self.send_queue.get(timeout=0.25)
            except queue.Empty:
                continue

            now = time.time()
            wait_for = min_interval - (now - self._last_sent_ts)
            if wait_for > 0:
                time.sleep(wait_for)

            now = time.time()
            if text == self._last_sent_text and now - self._last_sent_ts < 2.0:
                continue

            if self.suppress_output:
                continue
            try:
                send_message(self.chat_id, text, markup)
                self._last_sent_text = text
                self._last_sent_ts = time.time()
            except Exception as exc:
                _notify_admin_code_error(
                    "Telegram output sender lỗi",
                    exc,
                    chat_id=self.chat_id,
                    tool_key=self.key,
                )

    def _flusher(self):
        buf = []
        first_line_ts = None
        last_data_ts = None

        def _send_part(part):
            # Tách lỗi lịch không hợp lệ và câu hỏi nhập lịch thành 2 tin Telegram.
            # stdout của worker được gom trong FLUSH_INTERVAL, nên các print liền nhau
            # có thể về chung một `part` nếu không tách ở đây.
            lines = str(part or "").splitlines()
            invalid_i = next(
                (i for i, line in enumerate(lines)
                 if line.strip().startswith("❌ Khung giờ không hợp lệ.")),
                None,
            )
            prompt_i = next(
                (i for i, line in enumerate(lines)
                 if re.match(r"^\d+\.\s*⏰\s*Khung giờ (?:tạm dừng|chạy)", line.strip())),
                None,
            )
            if invalid_i is not None and prompt_i is not None and invalid_i < prompt_i:
                before = "\n".join(lines[:invalid_i]).strip()
                error_msg = "\n".join(lines[invalid_i:prompt_i]).strip()
                prompt_msg = lines[prompt_i].strip()
                after_prompt = "\n".join(lines[prompt_i + 1:]).strip()
                if before:
                    _send_part(before)
                if error_msg:
                    _send_part(error_msg)
                if prompt_msg:
                    _send_part(prompt_msg)
                if after_prompt:
                    _send_part(after_prompt)
                return

            schedule_prefixes = (
                f"⏸️ {self.key.upper()}: ĐANG TẠM DỪNG",
                f"▶️ {self.key.upper()}: HẾT GIỜ NGHỈ",
            )
            schedule_i = next(
                (i for i, line in enumerate(lines)
                 if line.strip().startswith(schedule_prefixes)),
                None,
            )
            if schedule_i is not None and len(lines) > 1:
                before = "\n".join(lines[:schedule_i]).strip()
                status_msg = lines[schedule_i].strip()
                following = "\n".join(lines[schedule_i + 1:]).strip()
                if before:
                    _send_part(before)
                _send_part(status_msg)
                if following:
                    _send_part(following)
                return

            if self.key == "cdtd" and "✅ Đã lưu cấu hình." in part and "9.CHỌN LOGIC" in part:
                left, right = part.split("9.CHỌN LOGIC", 1)
                _send_part(left.rstrip())
                _send_part("9.CHỌN LOGIC" + right)
                return

            if _looks_like_internal_error(part):
                _notify_admin_code_error(
                    "lỗi kỹ thuật",
                    chat_id=self.chat_id,
                    tool_key=self.key,
                    traceback_text=part,
                )
                self._enqueue_message(
                    "BOT LỖI"
                )
                return

            payload_text = part if self.key in ("vth", "cdtd", "lotto") else f"{self.name}\n\n{part}"
            markup = None

            if self.key == "cdtd":
                if "9.CHỌN LOGIC" in part:
                    self.awaiting_cdtd_coin = False
                    self.awaiting_cdtd_bet_type = False
                    self.awaiting_cdtd_logic = True
                    markup = {
                        "keyboard": [
                            [{"text": "🆓 LOGIC FREE"}, {"text": "👑 LOGIC VIP"}]
                        ],
                        "resize_keyboard": True,
                        "one_time_keyboard": True,
                        "input_field_placeholder": "Chọn Logic...",
                    }

                elif "👉 Nhập loại tiền bạn muốn chơi" in part or "CHỌN LOẠI TIỀN CƯỢC" in part:
                    self.awaiting_cdtd_coin = True
                    self.awaiting_cdtd_bet_type = False
                    markup = {
                        "keyboard": [
                            [{"text": "BUILD"}, {"text": "USDT"}, {"text": "XWORLD"}]
                        ],
                        "resize_keyboard": True,
                        "one_time_keyboard": True,
                        "input_field_placeholder": "Chọn loại tiền...",
                    }

                elif "CHỌN LOẠI ĐẶT" in part or "3. Chọn loại đặt" in part:
                    self.awaiting_cdtd_coin = False
                    self.awaiting_cdtd_bet_type = True
                    markup = {
                        "keyboard": [
                            [{"text": "Ai là quán quân"}],
                            [{"text": "Ai không là quán quân"}]
                        ],
                        "resize_keyboard": True,
                        "one_time_keyboard": True,
                        "input_field_placeholder": "Chọn loại đặt...",
                    }

                elif "4. Đặt vào bao nhiêu người:" in part:
                    self.awaiting_cdtd_coin = False
                    self.awaiting_cdtd_bet_type = False
                    self.awaiting_cdtd_logic = False
                    markup = {
                        "keyboard": [
                            [{"text": "1"}, {"text": "2"}, {"text": "3"}],
                            [{"text": "4"}, {"text": "5"}, {"text": "6"}]
                        ],
                        "resize_keyboard": True,
                        "one_time_keyboard": True,
                        "input_field_placeholder": "Chọn số người (1-6)...",
                    }

                elif "✅ Đã chọn Logic" in part:
                    self.awaiting_cdtd_coin = False
                    self.awaiting_cdtd_bet_type = False
                    self.awaiting_cdtd_logic = False
                    # Sau khi chọn Logic CDTD xong, trả Reply Keyboard về Menu chính.
                    markup = home_markup(self.chat_id)

                elif "Đặt vào bao nhiêu người" in part or ("Nhập số" in part and "cược mỗi ván" in part):
                    self.awaiting_cdtd_coin = False
                    self.awaiting_cdtd_bet_type = False
                    self.awaiting_cdtd_logic = False
                    markup = {"remove_keyboard": True}

            self._enqueue_message(payload_text, markup)

        while not self.stop_event.is_set() or not self.q.empty():
            try:
                line = self.q.get(timeout=0.10)
                buf.append(line)
                now = time.time()

                if first_line_ts is None:
                    first_line_ts = now

                last_data_ts = now

            except queue.Empty:
                pass

            now = time.time()

            if buf and (
                (last_data_ts is not None and now - last_data_ts >= FLUSH_INTERVAL)
                or (first_line_ts is not None and now - first_line_ts >= 1.5)
                or (self.stop_event.is_set() and self.q.empty())
            ):
                text = clean_output("".join(buf))
                buf = []
                first_line_ts = None
                last_data_ts = None

                for part in chunks(text):
                    if self.key == "vth" and "🚀 ĐANG KHỞI ĐỘNG VTH..." in part:
                        before, _ = part.split("🚀 ĐANG KHỞI ĐỘNG VTH...", 1)
                        before = before.strip()

                        if before:
                            _send_part(before)

                        _send_part("🚀 ĐANG KHỞI ĐỘNG VTH...")

                    elif self.key == "cdtd" and "🚀 ĐANG KHỞI ĐỘNG CDTD..." in part:
                        before, _ = part.split("🚀 ĐANG KHỞI ĐỘNG CDTD...", 1)
                        before = before.strip()

                        if before:
                            _send_part(before)

                        _send_part("🚀 ĐANG KHỞI ĐỘNG CDTD...")

                    elif self.key == "cdtd" and "4. Đặt vào bao nhiêu người:" in part:
                        before, _ = part.split("4. Đặt vào bao nhiêu người:", 1)
                        before = before.strip()

                        if before:
                            _send_part(before)

                        _send_part("4. Đặt vào bao nhiêu người:")

                    else:
                        _send_part(part)

        if buf:
            text = clean_output("".join(buf))

            for part in chunks(text):
                if self.key == "vth" and "🚀 ĐANG KHỞI ĐỘNG VTH..." in part:
                    before, _ = part.split("🚀 ĐANG KHỞI ĐỘNG VTH...", 1)
                    before = before.strip()

                    if before:
                        _send_part(before)

                    _send_part("🚀 ĐANG KHỞI ĐỘNG VTH...")

                elif self.key == "cdtd" and "🚀 ĐANG KHỞI ĐỘNG CDTD..." in part:
                    before, _ = part.split("🚀 ĐANG KHỞI ĐỘNG CDTD...", 1)
                    before = before.strip()

                    if before:
                        _send_part(before)

                    _send_part("🚀 ĐANG KHỞI ĐỘNG CDTD...")

                elif self.key == "cdtd" and "4. Đặt vào bao nhiêu người:" in part:
                    before, _ = part.split("4. Đặt vào bao nhiêu người:", 1)
                    before = before.strip()

                    if before:
                        _send_part(before)

                    _send_part("4. Đặt vào bao nhiêu người:")

                else:
                    _send_part(part)

        try:
            code = self.proc.wait(timeout=3)
        except Exception:
            code = self.proc.poll()

        stderr_text = "\n".join(self._stderr_lines[-80:]).strip()
        abnormal = (not self._expected_stop) and (code not in (0, None))
        if abnormal:
            _notify_admin_code_error(
                f"Worker {self.name} kết thúc bất thường, exit_code={code}",
                chat_id=self.chat_id,
                tool_key=self.key,
                traceback_text=stderr_text or f"Worker kết thúc với exit code {code}, không có stderr.",
            )

        self.sender_stop_event.set()
        if self.sender_thread is not None and self.sender_thread is not threading.current_thread():
            try:
                self.sender_thread.join(timeout=2)
            except Exception:
                pass

        with SESSIONS_LOCK:
            if SESSIONS.get(self.chat_id) is self:
                SESSIONS.pop(self.chat_id, None)

        try:
            Path(self.input_file).unlink(missing_ok=True)
        except Exception:
            pass

        if not STOP_ALL.is_set():

            try:
                if self._end_message:
                    send_message(self.chat_id, self._end_message, self._end_markup)
                elif abnormal:
                    send_message(self.chat_id, ("⚠️ BOT ĐANG LỖI." if self.key == "lotto" else f"⚠️lỗi."))
                else:
                    send_message(self.chat_id, ("✅ Công cụ đã kết thúc." if self.key == "lotto" else f"✅ {self.name} đã kết thúc."), home_markup(self.chat_id))
            except Exception as exc:
                _notify_admin_code_error(
                    "Gửi thông báo kết thúc worker lỗi",
                    exc,
                    chat_id=self.chat_id,
                    tool_key=self.key,
                )

    def set_end_message(self, text, markup=None):
        self._end_message = str(text or "").strip() or None
        self._end_markup = markup

    def send_input(self, text):
        if not self.running:
            return False
        try:
            if self.proc is None or self.proc.stdin is None:
                return False
            value = str(text).replace("\x00", "").replace("\r", "").strip()

            payload = value + "\n"
            with self._stdin_lock:
                self.proc.stdin.write(payload)
                self.proc.stdin.flush()
            return True
        except (BrokenPipeError, OSError, ValueError):
            return False

    def stop(self):

        self._expected_stop = True
        self.sender_stop_event.set()
        proc = self.proc
        if not proc:
            self.stop_event.set()
            return

        if proc.poll() is not None:
            self.stop_event.set()
            return

        self.stop_event.set()

        try:
            os.killpg(proc.pid, signal.SIGINT)
        except ProcessLookupError:
            pass
        except Exception:
            try:
                proc.terminate()
            except Exception:
                pass

        end = time.time() + 3.0
        while time.time() < end and proc.poll() is None:
            time.sleep(0.1)

        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass

        try:
            proc.wait(timeout=1.0)
        except Exception:
            pass


def get_session(chat_id):
    with SESSIONS_LOCK:
        return SESSIONS.get(chat_id)


def start_tool(chat_id, key):

    _clear_user_auth_pending(chat_id)

    ban = _bot_banned_message(chat_id)
    if ban:
        send_message(chat_id, ban)
        return

   
    valid, rec = _bot_auth_valid(chat_id)
    if not valid or not rec:
        if str(chat_id) in BOT_EXPIRED_USERS:
            send_message(
                chat_id,
                "⚠️ VUI LÒNG XÁC THỰC KEY:\n\n"
                "https://vuotnhanh.com/Zdg7\n\n"
                "📌 Sau khi có Key, gõ:\n"
                "/login KEY_CUA_BAN\n\n"
                "⏰ Key có hiệu lực trong 24h",
                _auth_menu_markup(chat_id),
            )
        else:
            send_message(
                chat_id,
                "🔐 Bạn chưa có key còn hiệu lực.\n\n"
                "Vui lòng xác thực Key FREE hoặc Key VIP trước khi chạy tool.",
                _auth_menu_markup(chat_id),
            )
        return

    if key == "xworld" and str(rec.get("key_type", "FREE")).upper() != "VIP":
        send_message(chat_id, "🔒 CANH CODE chỉ dành cho KEY VIP.\n\nGõ /KEY_VIP để nhập key VIP.", _auth_menu_markup(chat_id))
        return

    if key not in TOOLS:
        send_message(chat_id, "❌ Tool không hợp lệ.", home_markup(chat_id))
        return

    # Chốt kiểm tra hạn ngay trước khi tạo process tool. Khi Key đã hết hạn,
    # kể cả Admin cũng không thể mở tool bằng callback/nút cũ.
    if not rec.get("is_forever"):
        try:
            final_expiry = float(rec.get("expires_at", 0) or 0)
        except (TypeError, ValueError):
            final_expiry = 0.0
        if final_expiry <= _now_ts():
            _expire_running_tool_for_key(chat_id, str(rec.get("key_type", "FREE")).upper())
            return

    old = get_session(chat_id)
    if old and old.running:
        send_message(chat_id,
                     f"⚠️ Bạn đang chạy {old.name}.\n🛑 Hãy Dừng trước.",
                     home_markup(chat_id))
        return

    session = ToolSession(chat_id, key)
    with SESSIONS_LOCK:
        SESSIONS[chat_id] = session
    try:
        session.start()
        # Chọn tool xong -> tự động trả Reply Keyboard về menu chính.
        # Nút "Quay lại" không còn nằm trong menu chọn tool.
        send_message(
            chat_id,
            f"✅ Đã chọn {session.name}.",
            home_markup(chat_id),
        )
    except Exception as exc:
        with SESSIONS_LOCK:
            if SESSIONS.get(chat_id) is session:
                SESSIONS.pop(chat_id, None)
        _admin_only_exception_log(chat_id, "Lỗi khởi động tool", exc)
        send_message(chat_id, "❌ Không thể khởi động tool. Vui lòng thử lại.", home_markup(chat_id))
        return



def stop_menu_markup():
    return {
        "keyboard": [
            [{"text": "⚡ Dừng VTH"}, {"text": "🎲 Dừng LOTTO"}],
            [{"text": "🏎️ Dừng CDTD"}, {"text": "🎯 Dừng CANH CODE"}],
            [{"text": "🃏 Dừng HILO"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "Chọn tool muốn dừng...",
    }


def stop_tool(chat_id, key=None):
    session = get_session(chat_id)
    if not session or not session.running:
        send_message(chat_id, "ℹ️ Không có tool đang chạy.", home_markup(chat_id))
        return

    if key is None:
        send_message(
            chat_id,
            "🛑 CHỌN TOOL MUỐN DỪNG\n\n"
            f"🟢 Đang chạy: {session.name}\n\n"
            "Hãy chọn tool muốn dừng:",
            stop_menu_markup(),
        )
        return

    if key != session.key:
        send_message(
            chat_id,
            f"ℹ️ {TOOLS.get(key, (key, ''))[0]} hiện không chạy.\n\n"
            f"🟢 Tool đang chạy: {session.name}",
            home_markup(chat_id),
        )
        return

    send_message(chat_id, f"⏳ ĐANG DỪNG {session.name}...", None)
    session.stop()

    # Chỉ thông báo đã dừng sau khi tiến trình worker thực sự kết thúc.
    with SESSIONS_LOCK:
        if SESSIONS.get(chat_id) is session:
            SESSIONS.pop(chat_id, None)
    send_message(chat_id, f"✅ ĐÃ DỪNG {session.name}.", home_markup(chat_id))


def show_status(chat_id):
    session = get_session(chat_id)
    if not session or not session.running:
        send_message(chat_id, "⚪ Không có tool nào đang chạy.", home_markup(chat_id))
        return
    send_message(
        chat_id,
        f"🟢 Đang chạy\n"
        f"{session.name}",
        None,
    )


def handle_message(msg):
    global _CURRENT_CHAT_ID
    chat_id = (msg.get("chat") or {}).get("id")
    _CURRENT_CHAT_ID = chat_id
    text = msg.get("text")
    if chat_id is None or text is None:
        return

    is_admin = _is_bot_admin(chat_id)
    cmd = text.strip()
    upper_cmd = cmd.upper()

    # Lệnh/nút liên quan quyền Admin: nếu người gửi không phải Admin thì
    # bỏ qua hoàn toàn, không gửi tin nhắn báo lỗi hay thông báo từ chối.
    if not is_admin and _is_admin_only_message(cmd):
        return

    ban = _bot_banned_message(chat_id)
    if ban and not is_admin:
        send_message(chat_id, ban)
        return

   
    chat_info = msg.get("chat") or {}
    if str(chat_info.get("type", "private")).lower() == "private":
        _register_chat_user(chat_id)

  
    if is_admin and str(chat_id) in BOT_CHAT_WAIT_STOP:
        if cmd.lower() in {"/cancel", "/huy", "hủy", "↩️ quay lại"}:
            BOT_CHAT_WAIT_STOP.discard(str(chat_id))
            BOT_CHAT_COMPOSING.discard(str(chat_id))
            send_message(chat_id, "✅ Đã hủy CHAT.", home_markup(chat_id))
            return

        if cmd == "0":
            BOT_CHAT_WAIT_STOP.discard(str(chat_id))
            BOT_CHAT_COMPOSING.add(str(chat_id))
            requested = 0
            choice_text = "✅ Giữ nguyên: không dừng các tool đang chạy."
        elif cmd == "1":
            BOT_CHAT_WAIT_STOP.discard(str(chat_id))
            requested = _admin_stop_all_running_tools()
            BOT_CHAT_COMPOSING.add(str(chat_id))
            choice_text = (
                "🛑 Đã gửi lệnh dừng tới "
                f"{requested} phiên tool đang chạy."
            )
        else:
            send_message(
                chat_id,
                "⚠️ Vui lòng chỉ nhập 0 hoặc 1.\n\n"
                "0 = Không dừng tool\n"
                "1 = Dừng tất cả tool đang chạy\n\n"
                "Gõ /cancel để hủy CHAT.",
                home_markup(chat_id),
            )
            return

        recipients_count = len(_chat_broadcast_recipients() - {str(chat_id)})
        send_message(
            chat_id,
            f"{choice_text}\n\n"
            "💬 Bây giờ hãy soạn và gửi tin nhắn bất kỳ để phát tới các tài khoản đã tương tác với bot.\n"
            f"👥 Số tài khoản trong danh sách: {recipients_count}\n\n"
            "❌ Gõ /cancel để hủy soạn tin.",
            home_markup(chat_id),
        )
        return

   
    if is_admin and str(chat_id) in BOT_CHAT_COMPOSING:
        if cmd.lower() in {"/cancel", "/huy", "hủy", "↩️ quay lại"}:
            BOT_CHAT_COMPOSING.discard(str(chat_id))
            send_message(chat_id, "✅ Đã hủy soạn CHAT.", home_markup(chat_id))
            return
       
        BOT_CHAT_COMPOSING.discard(str(chat_id))
        _broadcast_admin_chat(chat_id, cmd)
        return

    if cmd == "💬 CHAT":
        if not is_admin:
            send_message(
                chat_id,
                "⛔ CHAT là chức năng riêng của Admin.\n\n"
                "Bạn vẫn có thể xem thông báo CHAT do Admin gửi tới.",
                home_markup(chat_id),
            )
            return
        BOT_CHAT_COMPOSING.discard(str(chat_id))
        BOT_CHAT_WAIT_STOP.add(str(chat_id))
        send_message(
            chat_id,
            "💬 CHAT ADMIN\n\n"
            "Admin có muốn dừng tất cả tool mà người dùng đang chạy không?\n\n"
            "Nhập 0️⃣ để KHÔNG dừng tool.\n"
            "Nhập 1️⃣ để DỪNG tất cả tool đang chạy.\n\n"
            "Sau khi chọn, Admin sẽ soạn tin nhắn để gửi tới các tài khoản đã tương tác với bot.\n"
            "❌ Gõ /cancel để hủy.",
            home_markup(chat_id),
        )
        return

    # ================= MUA KEY VIP =================
    if cmd == "🛒 MUA KEY VIP" or cmd.lower() in {"/mua", "/muakey", "/buy"}:
        _show_buy_menu(chat_id)
        return

    # ================= ADMIN COMMANDS MỚI =================
    if upper_cmd.startswith("/MO_BAN"):
        if not _is_bot_admin(chat_id):
            send_message(chat_id, "⛔ Bạn không có quyền MỞ BAN.", home_markup(chat_id))
            return
        parts = cmd.split(maxsplit=1)
        if len(parts) == 1 or not parts[1].strip():
            BOT_PENDING[str(chat_id)] = {"mode": "admin_mo_ban", "created_at": _now_ts()}
            _save_bot_pending()
            send_message(chat_id, "🔓 Gõ TELEGRAM_ID cần mở BAN.", home_markup(chat_id))
            return
        target_id = parts[1].strip()
        if not re.fullmatch(r"-?\d+", target_id):
            send_message(chat_id, "❌ TELEGRAM_ID không hợp lệ.", home_markup(chat_id))
            return
        existed = str(target_id) in BOT_BANS
        BOT_BANS.pop(str(target_id), None)
        _save_json_file(BOT_BAN_FILE, BOT_BANS)
        BOT_PENDING.pop(str(chat_id), None)
        _save_bot_pending()
        send_message(chat_id, (f"✅ ĐÃ MỞ BAN Telegram ID: {target_id}" if existed else f"ℹ️ Telegram ID {target_id} hiện không bị BAN."), home_markup(chat_id))
        try:
            send_message(int(target_id), "✅ Bạn đã được MỞ BAN.", home_markup(int(target_id)))
        except Exception:
            pass
        return

    if upper_cmd == "/VIP" or upper_cmd == "/THUHOI" or upper_cmd.startswith("/THUHOI "):
        if not _is_bot_admin(chat_id):
            send_message(chat_id, "⛔ Bạn không có quyền THU HỒI KEY.", home_markup(chat_id))
            return
        parts = cmd.split()
        if len(parts) >= 2:
            target_value = parts[1].strip()
            try:
                days = int(parts[2]) if len(parts) >= 3 else 0
            except (TypeError, ValueError):
                send_message(chat_id, "❌ Số ngày không hợp lệ. Ví dụ: /thuhoi 8044480 3 (trừ 3 ngày còn hạn)")
                return
            _admin_revoke_vip_and_notify(chat_id, target_value, days)
            return
        BOT_PENDING[str(chat_id)] = {"mode": "admin_revoke_vip_target", "created_at": _now_ts()}
        _save_bot_pending()
        send_message(
            chat_id,
            "🔑 THU HỒI KEY VIP\n\n"
            "👉 Nhập TELEGRAM ID hoặc KEY VIP cần thu hồi.\n"
            "Ví dụ: 801844480 hoặc ABCD1234\n"
            "Sau đó chọn thu hồi ngay hoặc nhập số ngày muốn trừ khỏi hạn VIP.",
            home_markup(chat_id),
        )
        return

    pending_admin = BOT_PENDING.get(str(chat_id), {})
    admin_mode = pending_admin.get("mode")
    if _is_bot_admin(chat_id) and admin_mode in ("admin_mo_ban", "admin_revoke_vip", "admin_revoke_vip_target", "admin_revoke_vip_days") and not cmd.startswith("/"):
        target_id = cmd.strip()
        if admin_mode == "admin_mo_ban":
            if not re.fullmatch(r"-?\d+", target_id):
                send_message(chat_id, "❌ TELEGRAM_ID không hợp lệ. Hãy nhập lại.", home_markup(chat_id))
                return
            existed = str(target_id) in BOT_BANS
            BOT_BANS.pop(str(target_id), None)
            _save_json_file(BOT_BAN_FILE, BOT_BANS)
            BOT_PENDING.pop(str(chat_id), None)
            _save_bot_pending()
            send_message(chat_id, (f"✅ ĐÃ MỞ BAN Telegram ID: {target_id}" if existed else f"ℹ️ Telegram ID {target_id} hiện không bị BAN."), home_markup(chat_id))
            try:
                send_message(int(target_id), "✅ Bạn đã được MỞ BAN.", home_markup(int(target_id)))
            except Exception:
                pass
            return

        if admin_mode in ("admin_revoke_vip", "admin_revoke_vip_target"):
            # Nhận "ID 3", "KEYVIP 3" hoặc "KEY VIP: ABC123 3" (trừ 3 ngày).
            cleaned = target_id.strip()
            if cleaned.upper().startswith("KEY VIP"):
                cleaned = re.sub(r"^KEY\s+VIP\s*:?\s*", "", cleaned, flags=re.IGNORECASE)
                bits = cleaned.split()
            else:
                bits = cleaned.split()
            target_value = re.sub(r"^KEYVIP\s*:?\s*", "", bits[0], flags=re.IGNORECASE).strip() if bits else ""
            if not target_value:
                send_message(chat_id, "❌ Hãy nhập Telegram ID hoặc Key VIP.", home_markup(chat_id))
                return
            if len(bits) >= 2:
                try:
                    days = int(bits[1])
                except (TypeError, ValueError):
                    send_message(chat_id, "❌ Số ngày không hợp lệ. Hãy nhập lại, ví dụ: ABCD1234 3 (trừ 3 ngày).")
                    return
                BOT_PENDING.pop(str(chat_id), None)
                _save_bot_pending()
                _admin_revoke_vip_and_notify(chat_id, target_value, days)
                return
            BOT_PENDING[str(chat_id)] = {
                "mode": "admin_revoke_vip_days",
                "target_value": target_value,
                "created_at": _now_ts(),
            }
            _save_bot_pending()
            send_message(
                chat_id,
                "➖ ĐIỀU CHỈNH THỜI HẠN KEY VIP\n\n"
                "Nhập số ngày muốn TRỪ khỏi thời hạn còn lại.\n"
                "• 0 = Thu hồi VIP ngay lập tức\n"
                "• 1 = Trừ 1 ngày sử dụng\n"
                "• 3 = Trừ 3 ngày sử dụng\n\n"
                "Ví dụ: nhập 0 hoặc 3.",
                home_markup(chat_id),
            )
            return

        if admin_mode == "admin_revoke_vip_days":
            try:
                days = int(target_id)
            except (TypeError, ValueError):
                send_message(chat_id, "❌ Hãy nhập số ngày dạng số nguyên. Nhập 0 để thu hồi ngay.")
                return
            if days < 0 or days > 36500:
                send_message(chat_id, "❌ Số ngày phải từ 0 đến 36500.")
                return
            target_value = str(pending_admin.get("target_value") or "").strip()
            BOT_PENDING.pop(str(chat_id), None)
            _save_bot_pending()
            _admin_revoke_vip_and_notify(chat_id, target_value, days)
            return

    # Reply Keyboard: bấm CHỌN TOOL mới mở danh sách 5 tool sau khi đã xác thực Key.
    if cmd == "🧰 CHỌN TOOL":
        valid, rec = _bot_auth_valid(chat_id)

        # Nếu key vừa hết hạn, yêu cầu xác thực lại bằng đúng link người dùng cung cấp.
        # Kiểm tra này đứng trước nhánh admin để key hết hạn không thể bỏ qua xác thực.
        if rec and rec.get("__expired__"):
            with BOT_AUTH_LOCK:
                BOT_EXPIRED_USERS[str(chat_id)] = str(rec.get("key_type", "FREE")).upper()
                _save_json_file(BOT_EXPIRED_USERS_FILE, BOT_EXPIRED_USERS)

        if str(chat_id) in BOT_EXPIRED_USERS:
            send_message(
                chat_id,
                "⚠️ VUI LÒNG XÁC THỰC KEY:\n\n"
                "https://vuotnhanh.com/Zdg7\n\n"
                "📌 Sau khi có Key, gõ:\n"
                "/login KEY_CUA_BAN\n\n"
                "⏰ Key có hiệu lực trong 24h",
                _auth_menu_markup(chat_id),
            )
            return

        if not valid or not rec:
            send_message(
                chat_id,
                "🔐 Bạn chưa có key còn hiệu lực.\n\n"
                "Vui lòng xác thực Key FREE hoặc Key VIP trước khi chọn tool.",
                _auth_menu_markup(chat_id),
            )
            return
        send_message(chat_id, "Hãy chọn tool muốn sử dụng:", tool_menu_markup())
        return

    if cmd == "↩️ Quay lại":
        send_message(chat_id, _auth_menu_text(chat_id), home_markup(chat_id))
        return

    # Các nút 5 tool trong Reply Keyboard.
    tool_buttons = {
        "⚡ VTH": "vth",
        "🎲 LOTTO": "lotto",
        "🏎️ CDTD": "cdtd",
        "🎯 CANH CODE": "xworld",
        "🃏 HILO": "hilo",
    }
    if cmd in tool_buttons:
        start_tool(chat_id, tool_buttons[cmd])
        return

    if cmd == "📊 Trạng thái":
        show_status(chat_id)
        return

    if cmd == "⏹️ Dừng":
        stop_tool(chat_id)
        return

    # Các nút dừng tool trong Reply Keyboard.
    stop_buttons = {
        "⚡ Dừng VTH": "vth",
        "🎲 Dừng LOTTO": "lotto",
        "🏎️ Dừng CDTD": "cdtd",
        "🎯 Dừng CANH CODE": "xworld",
        "🃏 Dừng HILO": "hilo",
    }
    if cmd in stop_buttons:
        stop_tool(chat_id, stop_buttons[cmd])
        return

    if cmd == "🆓 NHẬP KEY FREE":
      
        BOT_PENDING.pop(str(chat_id), None)
        _save_bot_pending()
        _reset_vip_spam(chat_id)
        BOT_PENDING[str(chat_id)] = {"mode": "free_select", "created_at": _now_ts()}
        _save_bot_pending()
        _handle_free_key_request(chat_id)
        return

    if cmd == "👑 NHẬP KEY VIP":
       
        BOT_PENDING[str(chat_id)] = {"mode": "vip", "created_at": _now_ts()}
        _save_bot_pending()
        send_message(chat_id,
                     "👑 NHẬP KEY VIP\n\n"
                     "Hãy gửi Key VIP ở tin nhắn tiếp theo.\n"
                     "🔐 Key sẽ được xác thực.", home_markup(chat_id))
        return

    # ================= ADMIN REPLY KEYBOARD =================
    if cmd == "🔓 MỞ BAN":
        if not _is_bot_admin(chat_id):
            send_message(chat_id, "⛔ Bạn không có quyền MỞ BAN.", home_markup(chat_id))
            return
        BOT_PENDING[str(chat_id)] = {"mode": "admin_mo_ban", "created_at": _now_ts()}
        send_message(
            chat_id,
            "🔓 MỞ BAN\n\n"
            "👉 Gõ /mo_ban rồi nhập TELEGRAM_ID.\n"
            "Ví dụ: /mo_ban 1301501405",
            home_markup(chat_id),
        )
        return

    if cmd == "🔑 THU HỒI KEY":
        if not _is_bot_admin(chat_id):
            send_message(chat_id, "⛔ Bạn không có quyền THU HỒI KEY.", home_markup(chat_id))
            return
        BOT_PENDING[str(chat_id)] = {"mode": "admin_revoke_vip_target", "created_at": _now_ts()}
        _save_bot_pending()
        send_message(
            chat_id,
            "🔑 THU HỒI KEY VIP\n\n"
            "👉 Nhập Telegram ID hoặc Key VIP cần thu hồi.\n"
            "Sau đó nhập 0 để thu hồi ngay hoặc số ngày muốn trừ khỏi hạn VIP.",
            home_markup(chat_id),
        )
        return


    if cmd in ("/start", "/menu", "/tools"):

        valid, rec = _bot_auth_valid(chat_id)
        if not valid or not rec:
            _restore_telegram_auth_from_server(chat_id, force=True)
            if not BOT_AUTH.get(str(chat_id)):

                _restore_auth_from_local_history(chat_id)
            valid, rec = _bot_auth_valid(chat_id)
        if is_admin:
            _clear_user_auth_pending(chat_id)
            send_message(chat_id, _auth_menu_text(chat_id), home_markup(chat_id))
        elif valid and rec:
            
            _clear_user_auth_pending(chat_id)
            send_message(chat_id, _auth_menu_text(chat_id), home_markup(chat_id))
        else:
            restore_status = str(_BOT_AUTH_RESTORE_RESULT.get(str(chat_id), "") or "").upper()
            if restore_status in {"UNAUTHORIZED", "CONNECTION_ERROR", "INVALID_RECORD", "INVALID_EXPIRY"} or restore_status.startswith("HTTP_"):
                
                send_message(
                    chat_id,
                    "CHÀO MỪNG BẠN ĐẾN VỚI VBXWORLD-BOT",
                    _auth_menu_markup(chat_id),
                )
            elif restore_status == "NO_ACTIVE_KEY":
                history_rec = BOT_AUTH_HISTORY.get(str(chat_id), {})
                expired_type = str(BOT_EXPIRED_USERS.get(str(chat_id)) or "").upper()
                try:
                    history_expiry = float(history_rec.get("expires_at", 0) or 0) if isinstance(history_rec, dict) else 0.0
                except (TypeError, ValueError):
                    history_expiry = 0.0
                if expired_type in {"FREE", "VIP"} or (history_expiry and _now_ts() >= history_expiry):
                    expired_type = expired_type or str(history_rec.get("key_type") or "FREE").upper()
                    send_message(
                        chat_id,
                        f"⏰ KEY {expired_type} ĐÃ HẾT HẠN\n\nVui lòng lấy hoặc nhập Key mới để tiếp tục.",
                        _auth_menu_markup(chat_id),
                    )
                else:
                    send_message(
                        chat_id,
                        "ADMIN @Vb101014",
                        _auth_menu_markup(chat_id),
                    )
            else:
                send_message(chat_id, _auth_menu_text(chat_id), _auth_menu_markup(chat_id))
        return

    if cmd.lower().startswith(("/revoke", "/revoke_key")):
        if not _is_bot_admin(chat_id):
            send_message(chat_id, "⛔ Bạn không có quyền thu hồi Key.")
            return
        parts = cmd.split()
        if len(parts) < 2:
            send_message(chat_id, "🛑 Cú pháp: /revoke TELEGRAM_ID_OR_KEY [SO_NGAY]\nKhông nhập số ngày = thu hồi ngay.")
            return
        try:
            days = int(parts[2]) if len(parts) >= 3 else 0
        except (TypeError, ValueError):
            send_message(chat_id, "❌ Số ngày không hợp lệ.")
            return
        _admin_revoke_vip_and_notify(chat_id, parts[1].strip(), days)
        return

    if cmd.lower().startswith('/ban') and not cmd.lower().startswith(('/banlist', '/unban')):
        if not _is_bot_admin(chat_id):
            send_message(chat_id, "⛔ Bạn không có quyền BAN người dùng.")
            return
        parts = cmd.split()
        if len(parts) < 2 or not re.fullmatch(r"-?\d+", parts[1].strip()):
            send_message(
                chat_id,
                "🛑 BAN NGƯỜI DÙNG\n\n"
                "Cú pháp:\n"
                "/ban TELEGRAM_ID\n"
                "/ban TELEGRAM_ID 24\n\n"
                "Nếu không nhập số giờ, mặc định 24 giờ."
            )
            return
        target_id = parts[1].strip()
        try:
            ban_hours = float(parts[2]) if len(parts) >= 3 else 24.0
        except ValueError:
            send_message(chat_id, "❌ Số giờ BAN không hợp lệ.")
            return
        if ban_hours <= 0:
            send_message(chat_id, "❌ Số giờ BAN phải lớn hơn 0.")
            return
        if ban_hours > 720:
            send_message(chat_id, "❌ Thời gian BAN tối đa là 720 giờ.")
            return

        target_key = str(target_id)
        now = _now_ts()
        until = now + ban_hours * 3600
        BOT_BANS[target_key] = {
            "until": until,
            "reason": f"Admin BAN bởi {chat_id}",
            "created_at": now,
            "admin_id": str(chat_id),
            "manual": True,
        }
        _save_json_file(BOT_BAN_FILE, BOT_BANS)

        
        try:
            _revoke_bot_auth(target_key)
        except Exception:
            pass
        try:
            running = get_session(int(target_id))
            if running and running.running:
                stop_tool(int(target_id), running.key)
        except Exception:
            pass

        duration_text = _format_duration(ban_hours * 3600)
        send_message(
            chat_id,
            "✅ ĐÃ BAN THÀNH CÔNG\n\n"
            f"👤 Telegram ID: {target_id}\n"
            f"⏰ Thời gian: {duration_text}\n"
            f"🔒 Người dùng sẽ không thể sử dụng bot trong thời gian này."
        )
        try:
            send_message(
                int(target_id),
                "⛔ BẠN ĐÃ BỊ ADMIN BAN\n\n"
                f"⏰ Thời gian: {duration_text}\n"
                "🔒 Bạn không thể sử dụng bot trong thời gian bị khóa."
            )
        except Exception:
            pass
        return

    # ADMIN: /unban <telegram_id>
    if cmd.lower().startswith('/unban'):
        if not _is_bot_admin(chat_id):
            send_message(chat_id, "⛔ Bạn không có quyền GỠ BAN người dùng.")
            return
        parts = cmd.split()
        if len(parts) < 2 or not re.fullmatch(r"-?\d+", parts[1].strip()):
            send_message(chat_id, "🛑 Cú pháp: /unban TELEGRAM_ID")
            return
        target_id = parts[1].strip()
        existed = str(target_id) in BOT_BANS
        BOT_BANS.pop(str(target_id), None)
        _save_json_file(BOT_BAN_FILE, BOT_BANS)
        if existed:
            send_message(chat_id, f"✅ ĐÃ GỠ BAN Telegram ID: {target_id}")
            try:
                send_message(int(target_id), "✅ Bạn đã được GỠ BAN.\n🔓 Có thể sử dụng bot trở lại.", home_markup(int(target_id)))
            except Exception:
                pass
        else:
            send_message(chat_id, f"ℹ️ Telegram ID {target_id} hiện không bị BAN.")
        return

  
    if cmd.lower() == '/banlist':
        if not _is_bot_admin(chat_id):
            send_message(chat_id, "⛔ Bạn không có quyền xem danh sách BAN.")
            return
        active = []
        now = _now_ts()
        for tid, rec in list(BOT_BANS.items()):
            if not isinstance(rec, dict):
                continue
            remaining = float(rec.get('until', 0) or 0) - now
            if remaining > 0:
                active.append((tid, remaining, rec.get('reason', '')))
        if not active:
            send_message(chat_id, "📋 DANH SÁCH BAN\n\n✅ Hiện không có người dùng nào đang bị BAN.")
            return
        lines = ["📋 DANH SÁCH BAN", ""]
        for tid, remaining, reason in active:
            lines.append(f"👤 {tid}\n⏰ Còn: {_format_duration(remaining)}\n📝 {reason}\n")
        send_message(chat_id, "\n".join(lines).strip())
        return

    if cmd.lower().startswith("/login"):
        _reset_vip_spam(chat_id)
        parts = cmd.split(maxsplit=1)
        pending = _bot_pending_record(chat_id)

        if len(parts) < 2 or not parts[1].strip():
            if pending.get("mode") == "free" and pending.get("key"):
                pending["mode"] = "free_login"
                BOT_PENDING[str(chat_id)] = pending
                _save_bot_pending()
            else:
                BOT_PENDING[str(chat_id)] = {"mode": "free_login", "created_at": _now_ts()}
                _save_bot_pending()
            send_message(chat_id, "🔑 Hãy gửi Key FREE để máy chủ xác thực.\n"
                                  "✅ Bot vẫn kiểm tra được key hợp lệ kể cả sau khi khởi động lại.")
            return

        user_key = parts[1].strip().upper()
        telegram_id = str(chat_id).strip()
        if not user_key:
            send_message(chat_id, "❌ KEY FREE KHÔNG HỢP LỆ.", _auth_menu_markup(chat_id))
            return

        # Key Server là nguồn xác thực duy nhất cho các key FREE mới.
        # Không phụ thuộc BOT_PENDING, vì file pending có thể mất khi host restart.
        try:
            verify_response = requests.post(
                f"{KEY_SERVER_URL}/api/telegram/verify_free_key",
                json={"key": user_key, "telegram_id": telegram_id},
                timeout=12,
            )
            verify_data = verify_response.json() if verify_response.content else {}
        except Exception as exc:
            log_debug(f"FREE key verify request failed for {telegram_id}: {type(exc).__name__}: {exc}")
            send_message(chat_id,
                         "⚠️ Chưa kết nối được máy chủ Key. Key chưa bị đánh dấu sai; "
                         "vui lòng thử lại sau.")
            return

        if verify_response.status_code != 200 or not isinstance(verify_data, dict) or not verify_data.get("success"):
            status = str(verify_data.get("status", "INVALID")).upper() if isinstance(verify_data, dict) else "INVALID"
            if status == "EXPIRED":
                message = "❌ KEY FREE ĐÃ HẾT THỜI GIAN CHỜ. Hãy bấm 🆓 NHẬP KEY FREE để tạo link mới."
            elif status in {"INVALID", "KEY_EXISTS", "REVOKED", "ALREADY_ACTIVATED"}:
                message = "❌ KEY FREE KHÔNG HỢP LỆ."
            else:
                message = "❌ KEY FREE KHÔNG HỢP LỆ. Máy chủ không xác nhận được key này."
            send_message(chat_id, message, _auth_menu_markup(chat_id))
            return

        # Máy chủ đã trả success nên key đã được xác thực. Bắt đầu đúng 24 giờ
        # tại thời điểm /login này, không phụ thuộc định dạng expires_at của API.
        _mark_free_key_used(user_key, chat_id, pending.get("link"))
        auth_record = _set_bot_auth(
            chat_id,
            user_key,
            "FREE",
            _now_ts() + FREE_KEY_DURATION_SECONDS,
            verify_data.get("device") or _bot_device_id(chat_id),
            FREE_KEY_DURATION_HOURS,
        )
        expires = float(auth_record["expires_at"])
        BOT_PENDING.pop(str(chat_id), None)
        _save_bot_pending()
        send_message(chat_id,
                     "✅ XÁC THỰC KEY FREE THÀNH CÔNG!\n\n"
                     "🆓 Quyền: LOGIC FREE\n"
                     "⏳ Thời hạn: 24 giờ kể từ lúc xác thực thành công\n"
                     f"⏰ Hiệu lực đến: {datetime.fromtimestamp(expires, _VIETNAM_TZ).strftime('%d/%m/%Y %H:%M:%S')}\n\n"
                     "Bạn có thể chọn tool trong Menu.",
                     home_markup(chat_id))
        return

    # /KEY_VIP hoặc /KEY_VIP ABC123
    if upper_cmd.startswith("/KEY_VIP"):
        parts = cmd.split(maxsplit=1)
        if len(parts) < 2:
            # /KEY_VIP chuyển trạng thái nhập sang VIP, hủy luồng FREE đang chờ.
            BOT_PENDING[str(chat_id)] = {"mode": "vip", "created_at": _now_ts()}
            send_message(chat_id,
                         "👑 NHẬP KEY VIP\n\n"
                         "Gõ /KEY_VIP rồi nhập key VIP.\n"
                         "🔐 Key sẽ được xác thực.")
            return
        user_key = parts[1].strip()
        _verify_bot_vip_key(chat_id, user_key, msg.get("from") if isinstance(msg, dict) else None)
        return

    # Khi bot đang chờ key VIP.
    pending = _bot_pending_record(chat_id)
    if pending.get("mode") == "vip":
        _verify_bot_vip_key(chat_id, cmd, msg.get("from") if isinstance(msg, dict) else None)
        return

    if cmd == "/stop":
        stop_tool(chat_id)
        return
    if cmd == "/status":
        show_status(chat_id)
        return

    session = get_session(chat_id)
    if session and session.running:
        if not session.send_input(text):
            send_message(chat_id,
                         "⚠️ Tool đã đóng hoặc không nhận được dữ liệu.",
                         home_markup(chat_id))
    else:
        send_message(chat_id,
                     "🔐 Bạn chưa chọn hoặc chưa xác thực key.\n\nGõ /start để mở hệ thống key.",
                     _auth_menu_markup(chat_id))


def _notify_shop_bot_vip_activation(chat_id, user_key, user_info=None, auth_record=None, duration_text=None):
    """Chuyển sự kiện kích hoạt VIP đã xác minh sang bot shop để bot shop báo Admin."""
    webhook_url = str(SHOP_VIP_ACTIVATION_WEBHOOK_URL or "").strip()
    webhook_secret = str(SHOP_VIP_ACTIVATION_WEBHOOK_SECRET or "").strip()
    if not webhook_url or not webhook_secret:
        return False

    user_info = user_info if isinstance(user_info, dict) else {}
    auth_record = auth_record if isinstance(auth_record, dict) else {}
    try:
        activated_ts = float(auth_record.get("activated_at", 0) or _now_ts())
    except (TypeError, ValueError):
        activated_ts = _now_ts()
    try:
        activated_at = datetime.fromtimestamp(activated_ts, _VIETNAM_TZ).isoformat(timespec="seconds")
    except Exception:
        activated_at = datetime.now(_VIETNAM_TZ).isoformat(timespec="seconds")

    if duration_text is None:
        if auth_record.get("is_forever"):
            duration_text = "VĨNH VIỄN — KHÔNG GIỚI HẠN"
        else:
            try:
                expiry = float(auth_record.get("expires_at", 0) or 0)
            except (TypeError, ValueError):
                expiry = 0.0
            duration_text = _format_duration(max(0, expiry - _now_ts())) if expiry else "Không xác định"

    # ID ổn định cho đúng một lần kích hoạt: retry cùng sự kiện không gửi trùng,
    # nhưng cùng một key được kích hoạt lại ở thời điểm khác vẫn tạo thông báo mới.
    import hashlib
    stable_event_id = hashlib.sha256(
        f"{str(chat_id)}|{str(user_key).strip().upper()}|{activated_ts:.6f}".encode("utf-8")
    ).hexdigest()[:40]
    payload = {
        "event_id": stable_event_id,
        "event_type": "VIP_ACTIVATED",
        "key_type": "VIP",
        "telegram_id": str(chat_id),
        "username": str(user_info.get("username") or "").strip(),
        "first_name": str(user_info.get("first_name") or "").strip(),
        "last_name": str(user_info.get("last_name") or "").strip(),
        "key": str(user_key).strip().upper(),
        "duration_text": str(duration_text),
        "activated_at": activated_at,
    }
    headers = {"X-VIP-Activation-Secret": webhook_secret}
    try:
        response = requests.post(webhook_url, json=payload, headers=headers, timeout=8)
        try:
            data = response.json() if response.content else {}
        except Exception:
            data = {}
        if response.status_code == 200 and isinstance(data, dict) and data.get("success") is True:
            return True
        log_debug(
            f"Shop VIP activation webhook returned HTTP {response.status_code}: "
            f"{str(data.get('message') if isinstance(data, dict) else data)[:240]}"
        )
    except Exception as exc:
        log_debug(f"Shop VIP activation webhook failed: {type(exc).__name__}: {exc}")
    return False


def _forward_vip_activation_to_shop(chat_id, user_key, user_info=None, auth_record=None):
    """Chỉ chuyển dữ liệu kích hoạt VIP sang bot shop; bot này không gửi nội dung cho Admin."""
    return _notify_shop_bot_vip_activation(
        chat_id, user_key, user_info=user_info, auth_record=auth_record
    )


def _find_active_vip_key_owner(user_key, current_chat_id):
    
    key_to_check = str(user_key or "").strip().upper()
    current_id = str(current_chat_id)
    now = _now_ts()
    with BOT_AUTH_LOCK:
        snapshot = [
            (str(owner_id), dict(record))
            for owner_id, record in BOT_AUTH.items()
            if isinstance(record, dict)
        ]

    for owner_id, record in snapshot:
        if owner_id == current_id:
            continue
        if str(record.get("key", "")).strip().upper() != key_to_check:
            continue
        if str(record.get("key_type", "")).upper() != "VIP":
            continue

        is_forever = bool(record.get("is_forever")) or (
            "duration_hours" in record and record.get("duration_hours") is None
        )
        try:
            expires_at = float(record.get("expires_at", 0) or 0)
        except (TypeError, ValueError):
            expires_at = 0.0

      
        if is_forever or expires_at <= 0 or now < expires_at:
            return owner_id
    return None


def _verify_bot_vip_key(chat_id, user_key, user_info=None):
    
    user_key = str(user_key or "").strip().upper()
    if not user_key:
        send_message(chat_id, "❌ Key VIP không được để trống.", _auth_menu_markup(chat_id))
        return

    local_auth = _bot_auth_record(chat_id)
    if isinstance(local_auth, dict):
        saved_key = str(local_auth.get("key", "")).strip().upper()
        saved_type = str(local_auth.get("key_type", "")).upper()
        is_expired = bool(local_auth.get("__expired__"))
        if saved_key == user_key and saved_type == "VIP" and not is_expired:
            banned, vip_attempts = _register_vip_spam(
                chat_id, "Nhập lại Key VIP đã được kích hoạt 3 lần liên tiếp"
            )
            if banned:
                BOT_PENDING.pop(str(chat_id), None)
                _save_bot_pending()
                send_message(
                    chat_id,
                    "⛔ BẠN ĐÃ BỊ BAN VĨNH VIỄN.\n\n"
                    "🔒 Bạn đã nhập lại Key VIP đã được kích hoạt 3 lần liên tiếp.",
                    None,
                )
            else:
                remain = max(0, BOT_MAX_AUTH_ATTEMPTS - vip_attempts)
                send_message(
                    chat_id,
                    "❌ KEY VIP ĐÃ ĐƯỢC KÍCH HOẠT.\n\n"
                    f"⚠️ BẠN CÒN {remain}/3 LẦN ĐỂ BAN VĨNH VIỄN.\n\n"
                    "🔐 Hãy nhập lại Key VIP.",
                    None,
                )
            return

    send_message(chat_id, "🔎 ĐANG XÁC THỰC KEY VIP...")
    try:
        device_id = _bot_device_id(chat_id)
        result = verify_vip_key_from_server(
            device_id,
            user_key,
            quiet=True,
            telegram_id=str(chat_id),
        )


        if not isinstance(result, dict):
            send_message(
                chat_id,
                "⚠️ CHƯA THỂ XÁC THỰC KEY VIP DO LỖI KẾT NỐI.\n\n"
                "🔄 Vui lòng thử lại sau.",
                None,
            )
            return

        status = str(result.get("status", "INVALID")).upper()

        if not result.get("is_forever"):
            expiry_raw = result.get("expires_at")
            expiry_ts = None
            if expiry_raw not in (None, "", 0, "0", False):
                if isinstance(expiry_raw, (int, float)):
                    expiry_ts = float(expiry_raw)
                else:
                    try:
                        expiry_ts = float(str(expiry_raw).strip())
                    except (TypeError, ValueError):
                        expiry_ts = _parse_iso_ts(expiry_raw)
            if expiry_ts and expiry_ts > 0 and expiry_ts <= _now_ts():
                status = "EXPIRED"
                result["status"] = status

        if result.get("_server_success") and status not in {"EXPIRED", "REVOKED"}:
            server_owner = str(result.get("telegram_id") or "").strip()

            server_confirms_same_owner = bool(server_owner and server_owner == str(chat_id))
            if not server_confirms_same_owner:
                existing_owner = _find_active_vip_key_owner(user_key, chat_id)
                if existing_owner is not None:
                    status = "TELEGRAM_MISMATCH"
                    result["status"] = status

        invalid_statuses = {
            "REVOKED", "EXPIRED", "DEVICE_MISMATCH", "INVALID",
            "TELEGRAM_MISMATCH", "TELEGRAM_VIP_ALREADY_ACTIVATED",
            "ALREADY_ACTIVATED", "KEY_ALREADY_ACTIVATED", "USED",
        }
        if status in invalid_statuses:

            try:
                print(
                    f"[KEY_VERIFY] telegram_id={chat_id} status={status} "
                    f"message={str(result.get('message') or '')[:240]}",
                    flush=True,
                )
            except Exception:
                pass
            banned, vip_attempts = _register_vip_spam(
                chat_id, "Nhập Key VIP không hợp lệ 3 lần liên tiếp"
            )
            if banned:
                BOT_PENDING.pop(str(chat_id), None)
                _save_bot_pending()
                send_message(
                    chat_id,
                    "⛔ BẠN ĐÃ BỊ BAN VĨNH VIỄN.\n\n"
                    "🔒 Bạn đã nhập Key VIP không hợp lệ 3 lần liên tiếp.",
                    None,
                )
            else:
                if status == "REVOKED":
                    msg = "❌ KEY VIP ĐÃ BỊ THU HỒI."
                elif status == "EXPIRED":

                    msg = "❌ KEY VIP KHÔNG HỢP LỆ."
                elif status in {
                    "DEVICE_MISMATCH", "TELEGRAM_MISMATCH",
                    "TELEGRAM_VIP_ALREADY_ACTIVATED", "ALREADY_ACTIVATED",
                    "KEY_ALREADY_ACTIVATED", "USED",
                }:

                    same_saved_key_expired = (
                        isinstance(local_auth, dict)
                        and str(local_auth.get("key", "")).strip() == user_key
                        and str(local_auth.get("key_type", "")).upper() == "VIP"
                        and bool(local_auth.get("__expired__"))
                    )
                    msg = (
                        "❌ KEY VIP KHÔNG HỢP LỆ."
                        if same_saved_key_expired or status == "EXPIRED"
                        else "❌ KEY VIP ĐÃ ĐƯỢC KÍCH HOẠT."
                    )
                else:
                    msg = "❌ KEY VIP KHÔNG HỢP LỆ."
                remain = max(0, BOT_MAX_AUTH_ATTEMPTS - vip_attempts)
                send_message(
                    chat_id,
                    f"{msg}\n\n⚠️ BẠN CÒN {remain}/3 LẦN ĐỂ BAN VĨNH VIỄN.\n\n🔐 Hãy nhập lại Key VIP.",
                    None,
                )
            return

        duration_hours = result.get("duration_hours")
        already_activated_same_account = bool(result.get("already_activated_same_account", False))
        is_forever = bool(result.get("is_forever", False)) or (
            duration_hours is None and not result.get("expires_at")
        )

        if is_forever:
           
            expires = 0.0
            duration_hours = None
        else:
            expires = None
            exp_raw = result.get("expires_at")
            if exp_raw:
                expires = _parse_iso_ts(exp_raw)
            if not expires:
                duration = float(duration_hours or 24)
                expires = _now_ts() + duration * 3600

        current_auth = _bot_auth_record(chat_id)
        current_forever_active = (
            isinstance(current_auth, dict)
            and not current_auth.get("__expired__")
            and str(current_auth.get("key_type", "")).upper() == "VIP"
            and bool(current_auth.get("is_forever"))
        )
        keep_existing_forever = current_forever_active and not is_forever

        if keep_existing_forever:
         
            rec = current_auth
            is_forever = True
            expires = 0.0
            duration_hours = None
        else:

            rec = _set_bot_auth(
                chat_id, user_key, "VIP", expires, device_id,
                duration_hours, is_forever=is_forever
            )

        BOT_PENDING.pop(str(chat_id), None)
        _save_bot_pending()
        expiry_message = (
            "⏰ Hiệu lực: VĨNH VIỄN — KHÔNG GIỚI HẠN"
            if rec.get("is_forever")
            else f"⏰ Còn: {_format_duration(max(0, rec['expires_at'] - _now_ts()))}"
        )

   
        _forward_vip_activation_to_shop(chat_id, user_key, user_info, rec)

        send_message(
            chat_id,
            "✅ XÁC THỰC KEY VIP THÀNH CÔNG!\n\n"
            "💎 Quyền: LOGIC VIP + LOGIC FREE\n"
            f"{expiry_message}\n\n"
            "Bạn có thể dùng các tool và logic VIP được cấp quyền.",
            home_markup(chat_id),
        )
    except Exception as exc:
        _admin_only_exception_log(chat_id, "Lỗi xác thực Key VIP", exc)
        send_message(chat_id, "❌ Xác thực Key VIP thất bại. Vui lòng thử lại.")



def handle_callback(query):
    global _CURRENT_CHAT_ID
    callback_id = query.get("id", "")
    data = str(query.get("data", ""))
    chat_id = ((query.get("message") or {}).get("chat") or {}).get("id")
    if chat_id is None:
        answer_callback(callback_id, "Không xác định được chat.")
        return

   
    if data.startswith(("buy:", "orders:")) or data in {"buy", "buycheck", "buyagain"}:
        answer_callback(callback_id, "Mua Key VIP đã chuyển sang bot shop riêng.")
        _show_buy_menu(chat_id)
        return

    _CURRENT_CHAT_ID = chat_id
    if not _is_bot_admin(chat_id):

        if data.startswith(("buy:approve:", "buy:reject:")):
            answer_callback(callback_id)
            return

    ban = _bot_banned_message(chat_id)
    if ban:
        answer_callback(callback_id, "Bạn đang bị khóa do spam.")
        send_message(chat_id, ban)
        return

    callback_chat = (query.get("message") or {}).get("chat") or {}
    if str(callback_chat.get("type", "private")).lower() == "private":
        _register_chat_user(chat_id)

    answer_callback(callback_id)
    if data == "home":
        valid, rec = _bot_auth_valid(chat_id)
        if valid and rec:
            send_message(chat_id, _auth_menu_text(chat_id), home_markup(chat_id))
        else:
            send_message(chat_id, _auth_menu_text(chat_id), _auth_menu_markup(chat_id))
    elif data == "auth:free":
        BOT_PENDING.pop(str(chat_id), None)
        _save_bot_pending()
        _reset_vip_spam(chat_id)
        BOT_PENDING[str(chat_id)] = {"mode": "free_select", "created_at": _now_ts()}
        _save_bot_pending()
        _handle_free_key_request(chat_id)
    elif data == "auth:vip":
        BOT_PENDING[str(chat_id)] = {"mode": "vip", "created_at": _now_ts()}
        _save_bot_pending()
        send_message(
            chat_id,
            "👑 NHẬP KEY VIP\n\n"
            "Gõ /KEY_VIP rồi nhập key VIP.\n"
            "🔐 Key sẽ được xác thực.",
        )
    elif data == "status":
        show_status(chat_id)
    elif data == "stop":
        stop_tool(chat_id)
    elif data.startswith("stop:"):
        stop_tool(chat_id, data.split(":", 1)[1])
    elif data.startswith("tool:"):
        start_tool(chat_id, data.split(":", 1)[1])


def _handle_free_key_request(chat_id):
    # FREE key được ghi vào Key Server trước khi gửi link cho người dùng.
    pending_current = _bot_pending_record(chat_id)
    if pending_current.get("mode") == "vip":
        BOT_PENDING.pop(str(chat_id), None)
        _save_bot_pending()
        _reset_vip_spam(chat_id)

    ban = _bot_banned_message(chat_id)
    if ban:
        send_message(chat_id, ban)
        return

    valid, rec = _bot_auth_valid(chat_id)
    if valid and rec and str(rec.get("key_type", "FREE")).upper() == "FREE":
        remaining = max(0, rec["expires_at"] - _now_ts())
        send_message(chat_id,
                     "✅ Bạn đang có Key FREE còn hiệu lực.\n"
                     f"⏰ Còn: {_format_duration(remaining)}",
                     home_markup(chat_id))
        return

    now = _now_ts()
    pending = _bot_pending_record(chat_id)
    if pending.get("mode") == "free" and pending.get("link") and now - float(pending.get("created_at", 0)) < 300:
        send_message(chat_id,
                     "🔗 Bạn đã có một link FREE đang chờ.\n\n"
                     f"{pending['link']}\n\n"
                     "📌 Sau khi có Key, gõ:\n/login KEY_CUA_BAN\n\n"
                     "⏰ Key có hiệu lực trong 24h")
        return

    device_id = _bot_device_id(chat_id)
    send_message(chat_id, "⏳ ĐANG TẠO LINK LẤY KEY FREE...")
    try:
  
        key = secrets.token_hex(4).upper()
        telegraph_url = create_telegraph_page(key, device_id)
        if not telegraph_url:
            send_message(chat_id, "❌ Không tạo được trang lấy Key. Vui lòng thử lại sau.")
            return

        params_4m = {"api": LINK4M_TOKEN, "url": telegraph_url}
        r1 = requests.get(LINK4M_API, params=params_4m, timeout=15)
        if r1.status_code != 200:
            raise RuntimeError(f"Link4m HTTP {r1.status_code}")
        data1 = r1.json()
        if data1.get("status") != "success":
            raise RuntimeError("Link4m API không trả về success")
        link4m_url = data1.get("shortenedUrl")
        if not link4m_url:
            raise RuntimeError("Không nhận được shortenedUrl")

        params_vn = {"api": VUOTNHANH_TOKEN, "url": link4m_url}
        r2 = requests.get(VUOTNHANH_API, params=params_vn, timeout=15)
        if r2.status_code != 200:
            raise RuntimeError(f"Vuotnhanh HTTP {r2.status_code}")
        data2 = r2.json()
        final_url = data2.get("shortenedUrl") if data2.get("status") == "success" else None
        final_url = final_url or link4m_url

      
        if not KEY_SERVER_URL or not KEY_SERVER_ADMIN_SECRET:
            raise RuntimeError("Thiếu KEY_SERVER_URL hoặc KEY_SERVER_ADMIN_SECRET")
        register_payload = {
            "key": key,
            "telegram_id": str(chat_id),
            "device_id": device_id,
            "admin_secret": KEY_SERVER_ADMIN_SECRET,
        }
        register_response = requests.post(
            f"{KEY_SERVER_URL}/api/telegram/register_free_key",
            json=register_payload,
            headers={"X-Server-Secret": KEY_SERVER_ADMIN_SECRET},
            timeout=12,
        )
        register_data = register_response.json() if register_response.content else {}
        if register_response.status_code != 200 or not isinstance(register_data, dict) or not register_data.get("success"):
            register_status = str(register_data.get("status", "ERROR")) if isinstance(register_data, dict) else "ERROR"
            if register_status == "TELEGRAM_ACTIVE_KEY":
                _restore_telegram_auth_from_server(chat_id, force=True)
                is_now_valid, current_rec = _bot_auth_valid(chat_id)
                if is_now_valid and current_rec:
                    remaining = max(0, float(current_rec.get("expires_at", 0) or 0) - _now_ts())
                    send_message(chat_id,
                                 "✅ Tài khoản của bạn đã có Key còn hiệu lực.\n"
                                 f"⏰ Còn: {_format_duration(remaining)}",
                                 home_markup(chat_id))
                    return
                send_message(chat_id, "⚠️ Tài khoản này đã có Key FREE đang hoạt động trên máy chủ. Hãy gõ /start để đồng bộ lại.")
                return
            raise RuntimeError(
                f"Key Server không nhận FREE key (HTTP {register_response.status_code}, status={register_status})"
            )

        BOT_PENDING[str(chat_id)] = {
            "mode": "free",
            "created_at": now,
            "key": key,
            "link": final_url,
            "device_id": device_id,
            "expires_at": now + 24 * 3600,
            "server_registered": True,
        }
        _save_bot_pending()

        send_message(chat_id,
                     "🔑 ĐÃ TẠO LINK VƯỢT THÀNH CÔNG:\n\n"
                     f"{final_url}\n\n"
                     "📌 Sau khi có Key, gõ:\n"
                     "/login KEY_CUA_BAN\n\n"
                     "⏰ Key có hiệu lực trong 24h")
    except Exception as exc:
        _admin_only_exception_log(chat_id, "Lỗi tạo link FREE", exc)
        send_message(chat_id, "❌ Không tạo được link FREE hoặc chưa đồng bộ được Key Server. Vui lòng thử lại sau.")


_BOT_HANDLER_LOCKS = {}
_BOT_HANDLER_LOCKS_LOCK = threading.Lock()
_BOT_HANDLER_EXECUTOR = None


def _get_bot_handler_lock(chat_id):
    key = str(chat_id)
    with _BOT_HANDLER_LOCKS_LOCK:
        lock = _BOT_HANDLER_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _BOT_HANDLER_LOCKS[key] = lock
        return lock


def _handle_update_async(update):
 
    try:
        if "callback_query" in update:
            query = update["callback_query"]
            chat_id = ((query.get("message") or {}).get("chat") or {}).get("id")
            chat_id = chat_id if chat_id is not None else ((query.get("from") or {}).get("id"))
            lock = _get_bot_handler_lock(chat_id) if chat_id is not None else threading.Lock()
            with lock:
                handle_callback(query)
        elif "message" in update:
            msg = update["message"]
            chat_id = (msg.get("chat") or {}).get("id")
            lock = _get_bot_handler_lock(chat_id) if chat_id is not None else threading.Lock()
            with lock:
                handle_message(msg)
    except Exception as exc:
        chat_id = None
        try:
            if "message" in update:
                chat_id = (update["message"].get("chat") or {}).get("id")
            elif "callback_query" in update:
                chat_id = ((update["callback_query"].get("message") or {}).get("chat") or {}).get("id")
        except Exception:
            pass
        _notify_admin_code_error("Telegram update handler lỗi", exc, chat_id=chat_id, tool_key="SYSTEM")


def bot_main():
    global _BOT_HANDLER_EXECUTOR, _BOT_WATCHDOG_THREAD
    if not BOT_TOKEN:
        raise SystemExit(
            'Chưa đặt BOT_TOKEN.\n'
            'Hãy cấu hình BOT_TOKEN trong Environment Variables của Render.'
        )
    try:
        tg("deleteWebhook", {"drop_pending_updates": False}, timeout=20)
    except Exception:
        pass

    print("BOT ĐANG CHẠY...", flush=True)
    start_bot_key_expiry_watcher()
    _mark_bot_poll_activity()
    _BOT_WATCHDOG_STOP.clear()
    if _BOT_WATCHDOG_THREAD is None or not _BOT_WATCHDOG_THREAD.is_alive():
        _BOT_WATCHDOG_THREAD = threading.Thread(
            target=_bot_poll_watchdog, daemon=True, name="telegram-poll-watchdog"
        )
        _BOT_WATCHDOG_THREAD.start()
    offset = None
    if _BOT_HANDLER_EXECUTOR is None:
        from concurrent.futures import ThreadPoolExecutor
        _BOT_HANDLER_EXECUTOR = ThreadPoolExecutor(max_workers=12, thread_name_prefix="tg-handler")
    try:
        while not STOP_ALL.is_set():
            try:
                _mark_bot_poll_activity()
                result = updates(offset)
                _mark_bot_poll_activity()
                for update in result or []:
                    offset = int(update["update_id"]) + 1
                    _BOT_HANDLER_EXECUTOR.submit(_handle_update_async, update)
            except KeyboardInterrupt:
                break
            except requests.exceptions.HTTPError as exc:
                _mark_bot_poll_activity()
                response = getattr(exc, "response", None)
                status_code = getattr(response, "status_code", None)
                if status_code == 409:

                    print("[Telegram] 409 Conflict: đang chờ poller khác nhả kết nối...", flush=True)
                    time.sleep(7)
                    continue
                _notify_admin_code_error("Telegram polling HTTP lỗi", exc, tool_key="SYSTEM")
                time.sleep(3)
            except Exception as exc:
                _mark_bot_poll_activity()
                _notify_admin_code_error("Telegram polling lỗi", exc, tool_key="SYSTEM")
                time.sleep(3)
    finally:
        STOP_ALL.set()
        _BOT_WATCHDOG_STOP.set()
        stop_bot_key_expiry_watcher()
        _stop_render_health_server()
        with SESSIONS_LOCK:
            sessions = list(SESSIONS.values())
            SESSIONS.clear()
        for session in sessions:
            try:
                session.stop()
            except Exception:
                pass
        if _BOT_HANDLER_EXECUTOR is not None:
            try:
                _BOT_HANDLER_EXECUTOR.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                _BOT_HANDLER_EXECUTOR.shutdown(wait=False)
            except Exception:
                pass
            _BOT_HANDLER_EXECUTOR = None


def run_worker(tool_key, chat_id=None):
    mapping = {
        "vth": "main_vth",
        "lotto": "main_lotto",
        "cdtd": "main_cdtd",
        "xworld": "xw_main",
        "hilo": "hilo_main",
    }
    fn_name = mapping.get(str(tool_key).strip().lower())
    if not fn_name:
        raise SystemExit(f"Tool không hợp lệ: {tool_key}")
    fn = globals().get(fn_name)
    if not callable(fn):
        raise SystemExit(f"Không tìm thấy {fn_name} trong engine.")

    global _WORKER_CHAT_ID
    _WORKER_CHAT_ID = str(chat_id or "").strip() or None
    _protect_worker_output_streams()


    chat_id = _WORKER_CHAT_ID
    if chat_id:
        safe_chat = re.sub(r"[^0-9A-Za-z_-]+", "_", chat_id)
        worker_dir = BOT_DATA_DIR / "telegram_workers" / f"tg_{safe_chat}"
        try:
            worker_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            worker_dir = BOT_DATA_DIR

        global ACCOUNTS_FILE, CONFIG_FILE, XW_ACCOUNT_FILE, XW_LOG_FILE, STRATEGY_CONFIG_FILE
        ACCOUNTS_FILE = str(worker_dir / "accounts.json")
        CONFIG_FILE = str(worker_dir / "wh_config.json")
        XW_ACCOUNT_FILE = str(worker_dir / "data_xw_confirm_code.txt")
        XW_LOG_FILE = str(worker_dir / "history_nhap_code.log")
        STRATEGY_CONFIG_FILE = str(worker_dir / "strategy.json")

    consecutive_failures = 0
    last_failure_ts = 0.0
    max_consecutive_failures = 10

    while True:
        try:
            fn()
            return
        except KeyboardInterrupt:
            return
        except SystemExit as exc:
            code = exc.code
            if code in (None, 0, "0"):
                return

            now = time.time()
            if now - last_failure_ts > 90:
                consecutive_failures = 0
            consecutive_failures += 1
            last_failure_ts = now

            _notify_admin_code_error(
                f"Worker {tool_key.upper()} kết thúc bằng SystemExit, auto-restart lần {consecutive_failures}",
                exc,
                chat_id=chat_id,
                tool_key=tool_key,
            )

            if consecutive_failures > max_consecutive_failures:
                _notify_admin_code_error(
                    f"Worker {tool_key.upper()} vượt quá {max_consecutive_failures} lần auto-restart liên tiếp",
                    exc,
                    chat_id=chat_id,
                    tool_key=tool_key,
                )
                raise SystemExit(1)

            try:
                print(
                    f"🔄 Tool gặp lỗi tạm thời, đang tự khởi động lại... ({consecutive_failures}/{max_consecutive_failures})",
                    flush=True,
                )
            except Exception:
                pass
            time.sleep(min(2.0 * consecutive_failures, 10.0))
        except BrokenPipeError:

            return
        except Exception as exc:
            now = time.time()
            
            if now - last_failure_ts > 90:
                consecutive_failures = 0
            consecutive_failures += 1
            last_failure_ts = now

            _notify_admin_code_error(
                f"Worker {tool_key.upper()} gặp lỗi runtime, auto-restart lần {consecutive_failures}",
                exc,
                chat_id=chat_id,
                tool_key=tool_key,
            )

            if consecutive_failures > max_consecutive_failures:
                _notify_admin_code_error(
                    f"Worker {tool_key.upper()} vượt quá {max_consecutive_failures} lần auto-restart liên tiếp",
                    exc,
                    chat_id=chat_id,
                    tool_key=tool_key,
                )
                raise SystemExit(1)

            try:
                print(
                    f"🔄 Tool gặp lỗi tạm thời, đang tự khởi động lại... ({consecutive_failures}/{max_consecutive_failures})",
                    flush=True,
                )
            except Exception:
                pass

            time.sleep(min(2.0 * consecutive_failures, 10.0))


import sys
import subprocess
import importlib

REQUIRED_PACKAGES = {
    "requests": "requests",
    "pytz": "pytz",
    "websocket": "websocket-client",
    "colorama": "colorama",
}


def install_missing_packages():
    missing = []

    for module, package in REQUIRED_PACKAGES.items():
        try:
            importlib.import_module(module)
        except ImportError:
            missing.append(package)

    if not missing:
        return

    print("")

    try:
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "-q",
                *missing,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


install_missing_packages()

import base64
import hashlib
import io
import json
import logging
import math
import os
import platform
import random
import re
import secrets
import threading
import json
import subprocess
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
import uuid
import webbrowser

from collections import Counter, defaultdict, deque
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
    wait,
    FIRST_COMPLETED,
)
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse


import pytz
import requests
import urllib3
import websocket

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class _BotConsole:
    width = 80
    def print(self, *objects, sep=" ", end="\n", **kwargs):

        return None
    def clear(self):
        return None

class Text(str):
    def __new__(cls, text="", *args, **kwargs):
        obj = super().__new__(cls, str(text))
        obj._parts = [str(text)] if str(text) else []
        return obj

    def append(self, value, style=None, *args, **kwargs):

        self._parts.append(str(value))
        return None

    @classmethod
    def assemble(cls, *parts):
        out=[]
        for part in parts:
            if isinstance(part, (tuple,list)) and part:
                out.append(str(part[0]))
            else:
                out.append(str(part))
        return cls("".join(out))

    def __str__(self):
        parts = getattr(self, "_parts", None)
        return "".join(parts) if parts is not None else super().__str__()

class _Box:
    ROUNDED = HEAVY = DOUBLE = SIMPLE_HEAVY = None
box = _Box()

class Align:
    @staticmethod
    def center(value, *args, **kwargs): return str(value)
    @staticmethod
    def left(value, *args, **kwargs): return str(value)

class Group:
    def __init__(self, *items): self.items = list(items)
    def __str__(self): return "\n".join(str(x) for x in self.items if str(x))

class Columns:
    def __init__(self, items, *args, **kwargs): self.items=list(items)
    def __str__(self): return "\n".join(str(x) for x in self.items)

class Table:
    def __init__(self, *args, **kwargs): self.rows=[]; self.columns=[]
    @classmethod
    def grid(cls, *args, **kwargs): return cls()
    def add_column(self, *args, **kwargs): self.columns.append(kwargs.get("header", args[0] if args else ""))
    def add_row(self, *values, **kwargs): self.rows.append([str(v) for v in values])
    def __str__(self): return "\n".join(" | ".join(row) for row in self.rows)

class Layout:
    def __init__(self, *args, **kwargs):
        self.name = kwargs.get("name")
        self.rows = []
        self.value = ""

    def split_row(self, *args):
        self.rows.extend(args)
        return self

    def split_column(self, *args):
        self.rows.extend(args)
        return self

    def update(self, value):
        self.value = value
        return self

    def __getitem__(self, name):
        name = str(name)
        if self.name == name:
            return self
        for child in self.rows:
            if isinstance(child, Layout):
                try:
                    return child[name]
                except KeyError:
                    pass
        raise KeyError(name)

    def __str__(self):
        parts = []
        if getattr(self, "value", None):
            parts.append(str(self.value))
        parts.extend(str(x) for x in getattr(self, "rows", []))
        return "\n".join(p for p in parts if p)

class Panel:
    def __init__(self, renderable="", *args, **kwargs): self.renderable=renderable; self.title=kwargs.get("title")
    def __str__(self): return str(self.renderable)

class Rule:
    def __init__(self, title="", *args, **kwargs): self.title=str(title)
    def __str__(self): return ""

class Live:
    def __init__(self, renderable=None, *args, **kwargs): self.renderable=renderable
    def __enter__(self): return self
    def update(self, renderable, *args, **kwargs): self.renderable=renderable
    def __exit__(self, *exc): return False


class _HiloBotLive:
    
    def __init__(self, state):
        self.state = state
        self.renderable = None
        self._last_signature = None
        self._last_emit = 0.0

    def __enter__(self):
        self._emit(force=True)
        return self

    def update(self, renderable=None, *args, **kwargs):
        self.renderable = renderable
        self._emit(force=False)

    def _emit(self, force=False):
        try:
            state = self.state or {}
            balance = state.get("balance", 0)
            current_bet = state.get("current_bet", 0)
            original = state.get("original_balance", 0)
            try:
                profit = float(balance) - float(original)
            except Exception:
                profit = 0.0

            stats = state.get("model_stats") or {}
            current_card = state.get("current_card")
            card_text = hilo__hilo_rank_label(current_card) if current_card is not None else "—"
            last_issue = state.get("last_issue", "—")
            result = state.get("last_result", "—")
            prediction = state.get("next_prediction", "—")
            log_text = str(state.get("log_text", "—"))
            history = state.get("history") or []
            last_hist = history[-1] if history else {}

            signature = (
                str(last_issue), result, card_text, str(prediction), log_text,
                round(float(balance or 0), 8), round(float(current_bet or 0), 8),
                int(stats.get("wins", 0) or 0), int(stats.get("losses", 0) or 0),
                str(last_hist.get("issue_id", "")), str(last_hist.get("result", "")),
            )
            now = time.time()
            if not force and signature == self._last_signature and now - self._last_emit < 5.0:
                return

            if not force and signature == self._last_signature:
                return

            self._last_signature = signature
            self._last_emit = now
            print(
                "🃏 HILO\n"
                f"👤 User: {state.get('account_id', '—')}\n"
                f"💰 Số dư: {float(balance or 0):.2f} | Lãi/Lỗ: {profit:+.2f}\n"
                f"🎴 Lá hiện tại: {card_text}\n"
                f"🎯 Dự đoán: {prediction}\n"
                f"💵 Cược: {float(current_bet or 0):.2f}\n"
                f"📊 Kết quả: {result} | Win {stats.get('wins', 0)} - Lose {stats.get('losses', 0)}\n"
                f"📝 {log_text}",
                flush=True,
            )
        except Exception as exc:
            try:
                _notify_admin_code_error(
                    "HILO Telegram Live output lỗi",
                    exc,
                    chat_id=str(_WORKER_CHAT_ID or ""),
                    tool_key="hilo",
                )
            except Exception:
                pass

    def __exit__(self, *exc):
        return False

class Prompt:
    @staticmethod
    def ask(prompt="", default=None, **kwargs):
        suffix = "" if default is None else f" [{default}]"
        value=input(str(prompt)+suffix+": ")
        return default if value == "" and default is not None else value

class IntPrompt:
    @staticmethod
    def ask(prompt="", default=None, **kwargs):
        while True:
            value=Prompt.ask(prompt, default=default)
            try: return int(value)
            except (TypeError,ValueError): continue

class FloatPrompt:
    @staticmethod
    def ask(prompt="", default=None, **kwargs):
        while True:
            value=Prompt.ask(prompt, default=default)
            try: return float(value)
            except (TypeError,ValueError): continue

console = _BotConsole()

urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)



tool_start_time = time.time()

processed_issue_ids = set()

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
except Exception:
    pass


def force_exit_vbtool(code: int = 0):
    try:
        if threading.current_thread() is threading.main_thread():
            raise SystemExit(code)
        os._exit(code)
    except SystemExit:
        raise
    except Exception:
        try:
            os._exit(code)
        except Exception:
            raise

def show_key_expired_message_and_exit(delay: float = 2.0):
    try:
        console.print("\n❌ KEY ĐÃ HẾT HẠN!")
        console.print("⚠️ Vui lòng nhập key khác để được sử dụng.")

    except Exception:
        pass

    try:
        time.sleep(delay)
    except Exception:
        pass

    force_exit_vbtool(0)

tz = pytz.timezone("Asia/Ho_Chi_Minh")

VBTOOL_COLORS = {k: "" for k in (
    "gold","gold_dark","platinum","diamond","ruby","emerald","sapphire",
    "amethyst","onyx","rose","history_blue","neon_pink","neon_green",
    "neon_orange","crimson","turquoise","lavender","bright_blue","dark_blue",
    "bright_cyan","bold_cyan","neon_blue","white","bright_green","yellow",
    "red","green","bold_green") }
ICONS = {
    "user": "👤",
    "phone": "📱",
    "chart": "📊",
    "check": "✅",
    "clock": "⏱️",
    "diamond": "💎",
    "dice": "🎲",
    "fire": "🔥",
    "money": "💰",
    "target": "🎯",
    "trophy": "🏆",
    "brain": "🧠",
}

LOGO_VBTOOL = ""
LOGO_ULTIMATE = ""

# ================== HỆ THỐNG KEY ==================

DEVICE_ID_FILE = "device_id.txt"
SALT = "VBTOOLKEY"
TEMP_KEY_FILE = str(BOT_DATA_DIR / ".temp_key.txt")
ACTIVATION_FILE = str(BOT_DATA_DIR / "activation.dat")
LINK_HISTORY_FILE = str(BOT_DATA_DIR / "link_history.json")

VUOTNHANH_API = "https://vuotnhanh.com/api"
LINK4M_API = "https://link4m.co/api-shorten/v2"
LINK4M_TOKEN = os.getenv("LINK4M_TOKEN", "6a2391ae3f35952ad60cb3cf").strip()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "fd9a7822bac3990bd0e7e86de2317df0ded0c419ba6a4588baffbedecd06").strip()
VUOTNHANH_TOKEN = os.getenv("VUOTNHANH_TOKEN", "2e82a116-da12-4344-8eb0-936c9274a2f1").strip()

ENCRYPTION_KEY = hashlib.sha256(b"VBTool_VIP").digest()

#========Telegaph================
def create_telegraph_page(key, device_id):
    url = "https://api.telegra.ph/createPage"

    content = json.dumps([
        {
            "tag": "p",
            "children": [
                "🔑 VBTOOL KEY"
            ]
        },
        {
            "tag": "p",
            "children": [
                f"📱 Device ID: {device_id}"
            ]
        },
        {
            "tag": "p",
            "children": [
                f"🔑 Key: {key}"
            ]
        },
        {
            "tag": "p",
            "children": [
                "⚠️ Key chỉ sử dụng trên tài khoản này."
            ]
        },
        {
            "tag": "p",
            "children": [
                "⚠️ Key chỉ được kích hoạt 1 lần, 1 tài khoản duy nhất."
            ]
        }
    ])

    data = {
        "access_token": ACCESS_TOKEN,
        "title": "KEY 24H",
        "author_name": "VBTOOL",
        "content": content,
        "return_content": False
    }

    r = requests.post(url, data=data)

    if r.status_code == 200:
        result = r.json()
        if result["ok"]:
            return result["result"]["url"]

    return None
    
# ================== KEY SERVER ==================
KEY_SERVER_URL = os.getenv("KEY_SERVER_URL", "https://keyvip-sever1.onrender.com").rstrip("/")
KEY_SERVER_ADMIN_SECRET = os.getenv("KEY_SERVER_ADMIN_SECRET", "VBTOOL_VANBAO999").strip()
# ================== HÀM MÃ HÓA ==================


def simple_decrypt(encrypted_str: str) -> Optional[dict]:
    try:
        if not encrypted_str:
            return None
        parts = encrypted_str.split(':', 1)
        if len(parts) != 2:
            return None
        stored_checksum, encrypted_data = parts
        key = ENCRYPTION_KEY
        encrypted = base64.b64decode(encrypted_data.encode())
        decrypted = bytearray()
        for i, char in enumerate(encrypted):
            decrypted.append(char ^ key[i % len(key)])
        json_str = decrypted.decode('utf-8')
        if hashlib.sha256(json_str.encode()).hexdigest() != stored_checksum:
            return None
        return json.loads(json_str)
    except Exception:
        return None

# ================== QUẢN LÝ KEY ==================

def get_device_id() -> str:
    if Path(DEVICE_ID_FILE).exists():
        try:
            with open(DEVICE_ID_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            pass
    new_id = str(uuid.uuid4()).replace("-", "")[:32]
    try:
        with open(DEVICE_ID_FILE, "w", encoding="utf-8") as f:
            f.write(new_id)
    except Exception:
        pass
    return new_id


def check_internet_connection() -> bool:
    try:
        requests.get("https://www.google.com", timeout=3)
        return True
    except Exception:
        return False
def is_vip_activated() -> bool:

    try:
        is_valid, _, is_vip = check_activation_valid(exit_on_expired=False)
        return bool(is_valid and is_vip)
    except Exception:
        return False

def verify_vip_key_from_server(device_id: str, key: str, quiet: bool = False, telegram_id=None) -> Optional[Dict]:

    if not check_internet_connection():
        if not quiet:
            console.print("[bold red]❌ Không có kết nối internet![/bold red]")
        return None

    try:
        effective_device_id = str(device_id or "").strip()
        telegram_id = str(telegram_id or _WORKER_CHAT_ID or "").strip()
        key = str(key or "").strip().upper()
        if not key:
            return {"status": "INVALID", "message": "Thiếu key", "_server_success": False}
        if telegram_id:
            payload = {"telegram_id": telegram_id, "key": key}
            url = f"{KEY_SERVER_URL}/api/telegram/verify_key"
        else:
            payload = {"device_id": effective_device_id, "key": key}
            url = f"{KEY_SERVER_URL}/api/verify_key"
        resp = requests.post(url, json=payload, timeout=15)

        if resp.status_code != 200:
            if not quiet:
                console.print(f"[bold red]❌ HTTP Lỗi: {resp.status_code}[/bold red]")
            return None

        data = resp.json() if resp.content else {}
        if not isinstance(data, dict):
            return None

        status = str(data.get("status", "")).strip().upper()
        if data.get("success"):
            return {
                "status": status or "ACTIVE",
                "_server_success": True,
                "duration_hours": data.get("duration", 24),
                "key_type": data.get("key_type", "VIP"),
                "server_time": data.get("server_time", datetime.now().isoformat()),
                "is_vip": True,
                "is_forever": bool(data.get("is_forever", False)),
                "expires_at": data.get("expires_at"),
                "telegram_id": data.get("telegram_id"),
                "already_activated_same_account": bool(data.get("already_activated_same_account", False)),
                "time_added_to_existing": bool(data.get("time_added_to_existing", False)),
            }

        result = {
            "status": status or "INVALID",
            "_server_success": False,
            "message": data.get("message"),
            "duration_hours": data.get("duration"),
            "key_type": data.get("key_type"),
            "expires_at": data.get("expires_at"),
            "is_forever": bool(data.get("is_forever", False)),
        }

        if not quiet:
            if status == "REVOKED":
                console.print("[bold red]❌ KEY ĐÃ BỊ THU HỒI![/bold red]")
            elif status == "EXPIRED":
                console.print("[bold yellow]❌ KEY ĐÃ HẾT HẠN![/bold yellow]")
            elif status == "DEVICE_MISMATCH":
                console.print("[bold red]❌ KEY ĐÃ ĐƯỢC KÍCH HOẠT TRÊN THIẾT BỊ KHÁC![/bold red]")
            else:
                console.print(f"[bold red]❌ {data.get('message', 'Key không hợp lệ')}[/bold red]")
        return result

    except requests.exceptions.Timeout:
        if not quiet:
            console.print("[bold red]❌ Hết thời gian kiểm tra key.[/bold red]")
        return None
    except Exception as e:
        if not quiet:
            console.print(f"[bold red]❌ Lỗi kiểm tra key: {e}[/bold red]")
        else:
            log_debug(f"VIP verify background error: {e}")
        return None

def remove_local_activation(revoked: bool = False):
   
    try:
        if Path(ACTIVATION_FILE).exists():
            os.remove(ACTIVATION_FILE)
    except Exception:
        pass

 
    if not revoked:
        try:
            clear_link_history()
            if Path(TEMP_KEY_FILE).exists():
                os.remove(TEMP_KEY_FILE)
        except Exception:
            pass
        


def clear_link_history():
    try:
        if Path(LINK_HISTORY_FILE).exists():
            os.remove(LINK_HISTORY_FILE)
    except Exception:
        pass


def load_activation() -> Optional[Dict]:

    if _WORKER_CHAT_ID:
        try:
            return _load_worker_activation()
        except Exception:
            return None

    if not Path(ACTIVATION_FILE).exists():
        return None
    try:
        with open(ACTIVATION_FILE, "r", encoding="utf-8") as f:
            encrypted_data = f.read().strip()
        if not encrypted_data:
            return None
        data = simple_decrypt(encrypted_data)
        if not data:
            try:
                os.remove(ACTIVATION_FILE)
            except Exception:
                pass
            return None
        expected_checksum = hashlib.sha256(
            f"{data.get('device_id', '')}:{data.get('key', '')}:{data.get('activation_time', '')}:{SALT}".encode()
        ).hexdigest()
        if data.get("checksum") != expected_checksum:
            try:
                os.remove(ACTIVATION_FILE)
            except Exception:
                pass
            return None
        return data
    except Exception:
        return None

def _load_worker_activation() -> Optional[Dict]:
    try:
        if not _WORKER_CHAT_ID:
            return None
        rec = _bot_auth_record(_WORKER_CHAT_ID)
        if not isinstance(rec, dict):
            return None
        if rec.get("__expired__"):
            return None
        expires_at = float(rec.get("expires_at", 0) or 0)
        if expires_at and time.time() >= expires_at:
            return None
        is_forever = bool(rec.get("is_forever"))
        return {
            "device_id": get_device_id(),
            "key": rec.get("key", ""),
            "key_type": str(rec.get("key_type", "FREE")).upper(),
            "activation_time": datetime.now().isoformat(),
            "expiry_time": datetime.fromtimestamp(expires_at).isoformat() if expires_at else None,
            "duration_hours": rec.get("duration_hours"),
            "is_forever": is_forever,
            "is_vip": str(rec.get("key_type", "FREE")).upper() == "VIP",
            "_telegram_bot_auth": True,
            "server_device_id": rec.get("device_id"),
        }
    except Exception:
        return None

def check_activation_valid(exit_on_expired: bool = True) -> Tuple[bool, Optional[str], bool]:
   
    if _WORKER_CHAT_ID:
        rec = _bot_auth_record(_WORKER_CHAT_ID)
        if not rec:
         
            return False, "CHƯA CÓ KEY HỢP LỆ — VUI LÒNG NHẬP KEY FREE HOẶC VIP", False
        try:
            if rec.get("__expired__"):
                return False, "KEY ĐÃ HẾT HẠN", False

            key_type = str(rec.get("key_type", "FREE")).strip().upper()
            expires_at = float(rec.get("expires_at", 0) or 0)
            if expires_at and time.time() >= expires_at:
                return False, "KEY ĐÃ HẾT HẠN", False
            is_vip = key_type == "VIP"
            remaining = max(0.0, expires_at - time.time()) if expires_at else 0.0
            if remaining:
                return True, f"Key {key_type} còn {remaining / 3600.0:.1f} giờ", is_vip
            return True, f"Key {key_type} đang hoạt động", is_vip
        except Exception as exc:
            return False, f"Key Telegram không hợp lệ: {exc}", False

    activation = load_activation()
    if not activation:
        return False, "Chưa kích hoạt", False
    try:
        current_device = get_device_id()
        if not activation.get("_telegram_bot_auth") and activation.get("device_id") != current_device:
            try:
                os.remove(ACTIVATION_FILE)
                console.print("[bold red]❌ Device ID không khớp! Key đã dùng trên máy khác.[/bold red]")
            except Exception:
                pass
            return False, "Device ID không khớp", False
        key_type = activation.get("key_type", "FREE")
        is_forever = bool(activation.get("is_forever"))
        expiry_raw = activation.get("expiry_time")
        expiry_time = datetime.fromisoformat(expiry_raw) if expiry_raw else None
        now = datetime.now()
        # FREE hết hạn theo 24 giờ kể từ lúc kích hoạt, không reset theo mốc 00:00.
        if not is_forever and expiry_time is not None and now > expiry_time:
            try:
                os.remove(ACTIVATION_FILE)

                if key_type == "FREE":
                    clear_link_history()
                    if Path(TEMP_KEY_FILE).exists():
                        os.remove(TEMP_KEY_FILE)
            except Exception:
                pass

            if exit_on_expired:
                show_key_expired_message_and_exit(2)
            return False, "KEY ĐÃ HẾT HẠN", False
        is_vip = activation.get("is_vip", False)
        if is_forever:
            return True, "Key VIP vĩnh viễn, không giới hạn thời gian", is_vip
        if expiry_time is None:
            return False, "Thiếu thông tin thời hạn key", False
        remaining = expiry_time - now
        days = remaining.total_seconds() / 86400
        hours = remaining.total_seconds() / 3600

        if days >= 1:
            return True, f"Còn {days:.1f} ngày", is_vip
        else:
            return True, f"Còn {hours:.1f} giờ", is_vip
    except Exception as e:
        try:
            os.remove(ACTIVATION_FILE)
        except Exception:
            pass
        return False, f"Lỗi: {e}", False

_key_monitor_started = False

def start_key_expiry_monitor():
    global _key_monitor_started

    if _WORKER_CHAT_ID:
        return
    if _key_monitor_started:
        return
    _key_monitor_started = True
    threading.Thread(target=key_expiry_monitor, daemon=True).start()

def _activation_is_expired_or_reset() -> bool:
    activation = load_activation()
    if not activation:
        return False
    try:
        if activation.get("is_forever"):
            return False
        expiry_raw = activation.get("expiry_time")
        if not expiry_raw:
            return False
        expiry_time = datetime.fromisoformat(expiry_raw)
        now = datetime.now()
        if now > expiry_time:
            return True
        # Không reset FREE theo nửa đêm; chỉ căn cứ expiry_time (24 giờ từ kích hoạt).
    except Exception:
        return False
    return False

def key_expiry_monitor():

    server_check_interval = 10.0
    last_server_check = 0.0
    terminal_seen = {"REVOKED": 0, "EXPIRED": 0, "DEVICE_MISMATCH": 0}

    while True:
        try:
            if _activation_is_expired_or_reset():
                try:
                    check_activation_valid(exit_on_expired=False)
                except Exception:
                    pass
                try:
                    _shutdown_vth_runtime("[bold yellow]Key đã hết hạn[/bold yellow]")
                except Exception:
                    pass
                return

            now = time.time()
            if now - last_server_check >= server_check_interval:
                last_server_check = now
                activation = load_activation()
                if activation and activation.get("key_type") == "VIP":
                    key = str(activation.get("key", "")).strip()
                    device_id = get_device_id()
                    result = verify_vip_key_from_server(device_id, key, quiet=True) if key else None
                    status = str((result or {}).get("status", "")).upper()

                    if status in terminal_seen:
                        for other in terminal_seen:
                            if other != status:
                                terminal_seen[other] = 0
                        terminal_seen[status] += 1
                    else:
                        for other in terminal_seen:
                            terminal_seen[other] = 0

                    if status == "REVOKED" and terminal_seen[status] >= 2:
                        remove_local_activation(revoked=True)
                        _shutdown_vth_runtime("[bold red]❌ KEY ĐÃ BỊ THU HỒI![/bold red]")
                        return

                    if status == "EXPIRED" and terminal_seen[status] >= 2:
                        remove_local_activation(revoked=False)
                        _shutdown_vth_runtime("[bold yellow]❌ KEY ĐÃ HẾT HẠN![/bold yellow]")
                        return

                    if status == "DEVICE_MISMATCH" and terminal_seen[status] >= 2:
                        remove_local_activation(revoked=True)
                        _shutdown_vth_runtime("[bold red]❌ KEY KHÔNG THUỘC THIẾT BỊ NÀY![/bold red]")
                        return
        except Exception as e:
            log_debug(f"key_expiry_monitor error: {e}")

        time.sleep(1)


# ================== TOOL 1: VUA THOÁT HIỂM ==================

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

BET_API_URL = "https://api.escapemaster.net/escape_game/bet"
WS_URL = "wss://api.escapemaster.net/escape_master/ws"
WALLET_API_URL = "https://wallet.3games.io/api/wallet/user_asset"

HTTP = requests.Session()
try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    adapter = HTTPAdapter(pool_connections=20, pool_maxsize=50, max_retries=Retry(total=3, backoff_factor=0.2, status_forcelist=(500, 502, 503, 504)))
    HTTP.mount("https://", adapter)
    HTTP.mount("http://", adapter)
except Exception:
    pass

ROOM_NAMES = {1: "📦 Nhà kho", 2: "🪑 Phòng họp", 3: "👔 Phòng giám đốc", 4: "💬 Phòng trò chuyện", 5: "🎥 Phòng giám sát", 6: "🏢 Văn phòng", 7: "💰 Phòng tài vụ", 8: "👥 Phòng nhân sự"}
ROOM_ORDER = [1, 2, 3, 4, 5, 6, 7, 8]

USER_ID: Optional[int] = None
SECRET_KEY: Optional[str] = None
IS_VIP_USER: bool = False
issue_id: Optional[int] = None
issue_start_ts: Optional[float] = None
issue_end_ts: Optional[float] = None
count_down: Optional[int] = None
killed_room: Optional[int] = None
round_index: int = 0

room_state: Dict[int, Dict[str, Any]] = {r: {"players": 0, "bet": 0} for r in ROOM_ORDER}
room_stats: Dict[int, Dict[str, Any]] = {r: {"kills": 0, "survives": 0, "last_kill_round": None, "last_players": 0, "last_bet": 0} for r in ROOM_ORDER}

predicted_room: Optional[int] = None
last_killed_room: Optional[int] = None
last_killed_room_delayed: Optional[int] = None
prediction_locked: bool = False

current_build: Optional[float] = None
current_usdt: Optional[float] = None
current_world: Optional[float] = None
last_balance_ts: Optional[float] = None
last_balance_val: Optional[float] = None
starting_balance: Optional[float] = None
cumulative_profit: Optional[float] = None


win_streak: int = 0
lose_streak: int = 0
max_win_streak: int = 0
max_lose_streak: int = 0

base_bet: float = 1.0
multiplier: float = 2.0
current_bet: Optional[float] = None
last_display_bet: float = 0.0
run_mode: str = "AUTO"
bet_rounds_before_skip: int = 0
_rounds_placed_since_skip: int = 0
skip_next_round_flag: bool = False

bet_history: deque = deque(maxlen=200)
bet_sent_for_issue: set = set()

pause_after_losses: int = 0
_skip_rounds_remaining: int = 0
profit_target: Optional[float] = None
stop_when_profit_reached: bool = False
stop_loss_target: Optional[float] = None
stop_when_loss_reached: bool = False
stop_flag: bool = False
_vth_daily_pause_window = None

ui_state: str = "IDLE"
analysis_duration: float = 45.0
analysis_initial_delay: float = 1.5
analysis_ready_ts: Optional[float] = None
analysis_start_ts: Optional[float] = None

def _vth_get_remaining_seconds() -> Optional[float]:
    """
    UI chỉ chạy theo countdown server vừa gửi.
    Không tự trôi theo đồng hồ nội bộ giữa các lần cập nhật.
    """
    try:
        if count_down is not None:
            return max(0.0, float(count_down))
    except Exception:
        pass
    return None

last_msg_ts: float = time.time()
last_balance_fetch_ts: float = 0.0
BALANCE_POLL_INTERVAL: float = 4.0
_ws: Dict[str, Any] = {"ws": None}
active_vth_poller: Optional[Any] = None
# Hoãn kết thúc VTH sau khi vừa chạm mục tiêu/cắt lỗ để Telegram
# kịp gửi bản tin WIN/LOSE của chính ván cuối cùng.
capital_stop_pending: bool = False

def _shutdown_vth_runtime(message: Optional[str] = None):
    global stop_flag, active_vth_poller, capital_stop_pending
    stop_flag = True
    capital_stop_pending = False

    if message:
        try:
            console.print(message)
        except Exception:
            pass

    poller = active_vth_poller
    active_vth_poller = None
    if poller is not None:
        try:
            poller.stop()
        except Exception:
            pass

    try:
        wsobj = _ws.get("ws")
        if wsobj:
            wsobj.close()
    except Exception:
        pass

_sequential_bet_index = 0
killer_history = deque(maxlen=20)
game_kill_log = deque(maxlen=100)
processed_result_issues = set()
history_recorded_issue = set()
bet_selection_by_issue: Dict[int, Dict[str, Any]] = {}

FREE_AI_LIST = [
    "RANDOM",
    "MIN_PLAYER_BET",
    "PROBABILITY",
    "FOLLOW_KILLER",
    "SEQUENTIAL",
    "KILLER_PERSONALITY",
    "SMART_SAFE",
    "FOLLOW_KILLER_DELAYED",
    "HIDE_SEEK_MASTER",
    "BALANCE",
    "MOST_PLAYERS",
    "LEAST_PLAYERS",
    "RICHEST",
    "POOREST",
    "AVOID_RESULT",
    "MEDIAN",
    "PATTERN",
    "ALL",
]

VIP_AI_LIST = [
    "KILLER_WAVE", "PSYCHO_ANALYSIS", "MARKOV_CHAIN", "DEEP_LEARNING",
    "REINFORCEMENT", "BAYESIAN", "K_MEANS", "NEURAL", "FUZZY", "GENETIC",
    "ANT_COLONY", "PARTICLE_SWARM", "KNN", "DECISION_TREE", "RANDOM_FOREST",
    "GRADIENT_BOOST", "LSTM", "TRANSFORMER", "ENSEMBLE", "CYCLE_ANALYSIS",
    "TREND_ANALYSIS", "SAFE_RISK", "TONG_HOP",
]

SELECTION_MODES = {
    "RANDOM": "1. RANDOM (FREE)",
    "MIN_PLAYER_BET": "2. AN TOÀN (FREE)",
    "PROBABILITY": "3. XÁC SUẤT (FREE)",
    "FOLLOW_KILLER": "4. THEO SÁT THỦ (FREE)",
    "SEQUENTIAL": "5. TUẦN TỰ 1→8 (FREE)",
    "KILLER_PERSONALITY": "6. TÍNH CÁCH SÁT THỦ (FREE)",
    "SMART_SAFE": "7. NÉ SÁT THỦ THÔNG MINH (FREE)",
    "FOLLOW_KILLER_DELAYED": "8. THEO VẾT SÁT THỦ (FREE)",
    "HIDE_SEEK_MASTER": "9. BẬC THẦY TRỐN TÌM (FREE)",
    "BALANCE": "10. CÂN BẰNG (FREE)",
    "MOST_PLAYERS": "11. NHIỀU NGƯỜI (FREE)",
    "LEAST_PLAYERS": "12. ÍT NGƯỜI (FREE)",
    "RICHEST": "13. NHIỀU CƯỢC (FREE)",
    "POOREST": "14. ÍT CƯỢC (FREE)",

    "AVOID_RESULT": "15. NÉ KẾT QUẢ (FREE)",
    "MEDIAN": "16. TRUNG VỊ (FREE)",
    "PATTERN": "17. MẪU LẶP (FREE)",
    "ALL": "18. ALL IN (V1 FREE / V2 VIP)",
    "KILLER_WAVE": "19. HOÁ THẦN (VIP)",
    "PSYCHO_ANALYSIS": "20. PHÂN TÍCH TÂM LÝ (VIP)",
    "MARKOV_CHAIN": "21. ADAPTIVE PATTERN (VIP)",
    "DEEP_LEARNING": "22. ENSEMBLE LEARNING (VIP)",
    "REINFORCEMENT": "23. PHÂN TÍCH CHUYÊN SÂU (VIP)",
    "BAYESIAN": "24. MARKOV PROBABILITY (VIP)",
    "K_MEANS": "25. PHÂN TÍCH DỮ LIỆU (VIP)",
    "NEURAL": "26. DATA FUSION (VIP)",
    "FUZZY": "27. X-PMLP (VIP)",
    "GENETIC": "28. H-AITTH (VIP)",
    "ANT_COLONY": "29. TỐI THƯỢNG (VIP)",
    "PARTICLE_SWARM": "30. FREQUENCY ANALYSIS (VIP)",
    "KNN": "31. THỐNG KÊ (VIP)",
    "DECISION_TREE": "32. THDZ-AI (VIP)",
    "RANDOM_FOREST": "33. LMT-AI (VIP)",
    "GRADIENT_BOOST": "34. THẦN THÁNH (VIP)",
    "LSTM": "35. THỢ SĂN (VIP)",
    "TRANSFORMER": "36. VBTTTH (VIP)",
    "ENSEMBLE": "37. SIÊU PHÂN TÍCH V1 (VIP)",
    "CYCLE_ANALYSIS": "38. CHU KỲ (VIP)",
    "TREND_ANALYSIS": "39. BẮT XU HƯỚNG (VIP)",
    "SAFE_RISK": "40. SIÊU PHÂN TÍCH V2 (VIP)",
    "TONG_HOP": "41.TỔNG HỢP (VIP)",
}

settings = {"algo": "RANDOM"}
selected_vth_algo = "RANDOM"
STRATEGY_CONFIG_FILE = "strategy.json"


def get_available_ai_list(key_type: str = "free") -> List[str]:
    
    if str(key_type or "free").strip().lower() == "vip":
        return list(FREE_AI_LIST) + list(VIP_AI_LIST)
    return list(FREE_AI_LIST)


def is_ai_available(ai_key: str, key_type: str = "free") -> bool:
    return str(ai_key or "").upper() in get_available_ai_list(key_type)


def get_vth_algo_display_name(algo_key=None, is_vip=None):
    key = str(algo_key or selected_vth_algo or "RANDOM").upper()
    if key == "ALL":
        if is_vip is None:
            try:
                valid, _reason, detected_vip = check_activation_valid(exit_on_expired=False)
                is_vip = bool(valid and detected_vip)
            except Exception:
                try:
                    is_vip = bool(is_vip_activated())
                except Exception:
                    is_vip = False
        version = "V2" if bool(is_vip) else "V1"
        return f"ALL IN {version} (Cược hết số dư)"
    label = str(SELECTION_MODES.get(key, key))
    # Các nhãn gốc TST có số thứ tự; menu Telegram tự đánh số theo quyền hiện tại.
    return re.sub(r"^\s*\d+\.\s*", "", label).strip()

_spinner = ["📦", "🪑", "👔", "💬", "🎥", "🏢", "💰", "👥"]
_num_re = re.compile(r"-?\d+[\d,]*\.?\d*")
VIP_COLORS = ["#FF00FF", "#D700FF", "#AF00FF", "#8700FF", "#5F00FF", "#0000FF", "#005FFF", "#0087FF", "#00AFFF", "#00D7FF", "#00FFFF"]

# ================== CÁC HÀM HỖ TRỢ VTH ==================


def log_debug(msg: str):
    try:
        logger.debug(msg)
    except Exception:
        pass

def _parse_number(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x)
    m = _num_re.search(s)
    if not m:
        return None
    token = m.group(0).replace(",", "")
    try:
        return float(token)
    except Exception:
        return None

def human_ts() -> str:
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")



def balance_headers_for(uid: Optional[int] = None, secret: Optional[str] = None) -> Dict[str, str]:
    h = {"accept": "*/*", "accept-language": "vi,en;q=0.9", "cache-control": "no-cache", "country-code": "vn", "origin": "https://xworld.info", "pragma": "no-cache", "referer": "https://xworld.info/", "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36", "user-login": "login_v2", "xb-language": "vi-VN"}
    if uid is not None:
        h["user-id"] = str(uid)
    if secret:
        h["user-secret-key"] = str(secret)
    return h

def fetch_balances_3games(retries=3, timeout=8, params=None, uid=None, secret=None):
    global current_build, current_usdt, current_world, last_balance_ts, starting_balance, last_balance_val, cumulative_profit
    uid = uid or USER_ID
    secret = secret or SECRET_KEY
    payload = {"user_id": int(uid) if uid is not None else None, "source": "home"}
    attempt = 0
    while attempt <= retries:
        attempt += 1
        try:
            r = HTTP.post(WALLET_API_URL, json=payload, headers=balance_headers_for(uid, secret), timeout=timeout)
            r.raise_for_status()
            j = r.json()
            data = j.get("data", {}) if isinstance(j, dict) else {}
            ua = data.get("user_asset", {}) if isinstance(data, dict) else {}
            build = _parse_number(ua.get("BUILD"))
            world = _parse_number(ua.get("WORLD"))
            usdt = _parse_number(ua.get("USDT"))
            if build is not None:
                if last_balance_val is None:
                    starting_balance = build
                    last_balance_val = build
                else:
                    last_balance_val = build
                current_build = build
                # Không cập nhật PNL ở đây.
                # Wallet có thể trả về số dư tạm thời sau khi trừ tiền cược;
                # chỉ ván đã được chốt kết quả mới được phép cập nhật cumulative_profit.
            if usdt is not None:
                current_usdt = usdt
            if world is not None:
                current_world = world
            last_balance_ts = time.time()
            return current_build, current_world, current_usdt
        except Exception as e:
            log_debug(f"wallet fetch attempt {attempt} error: {e}")
            time.sleep(min(1.5 * attempt, 4))
    return current_build, current_world, current_usdt


def _vth_total_build_pnl() -> Optional[float]:
    try:
        if starting_balance is None or current_build is None:
            return None
        return float(current_build) - float(starting_balance)
    except Exception:
        return None

# ================== LOGIC PHÂN TÍCH CHỌN PHÒNG ==================
VTH_SKIP_IF_SAFEST_ABOVE = 78.0
VTH_DROP_ROOM_ABOVE = 70.0
AI_PERFORMANCE = defaultdict(lambda: {"wins": 0, "losses": 0, "total": 0})

def compute_vth_room_risk() -> List[Dict[str, Any]]:
    """
    Rủi ro VTH v4 — Bayesian + realtime + volatility.

    P_base   = Dirichlet-smoothed kill rate (ổn định khi ít mẫu)
    P_ewma   = EWMA kill gần (20 ván, decay 0.86)
    P_markov = Markov 1 + 2 bậc
    crowd/money/gap/streak như v3
    vol      = độ biến thiên kill gần → tăng risk (không ổn định)

    conf cao → tin base; conf thấp → tin realtime.
    """
    rows = []
    n_rooms = max(1, len(ROOM_ORDER))
    max_players = max((int(room_state[r].get("players", 0) or 0) for r in ROOM_ORDER), default=0) or 1
    max_bet = max((float(room_state[r].get("bet", 0) or 0) for r in ROOM_ORDER), default=0.0) or 1.0
    log = list(game_kill_log) if game_kill_log else []
    n_log = len(log)

    # EWMA 20
    ewma = {r: 0.0 for r in ROOM_ORDER}
    if log:
        window = log[-20:]
        decay = 0.86
        w = 1.0
        wsum = 0.0
        for k in reversed(window):
            if k in ewma:
                ewma[k] += w
            wsum += w
            w *= decay
        if wsum > 0:
            for r in ewma:
                ewma[r] /= wsum

    # Markov 1 + 2
    markov = {r: 1.0 / n_rooms for r in ROOM_ORDER}
    if len(log) >= 2:
        last = log[-1]
        cnt = defaultdict(float)
        tot = 0.0
        for a, b in zip(log, log[1:]):
            if a == last:
                cnt[b] += 1.0
                tot += 1.0
        alpha = 0.85
        for r in ROOM_ORDER:
            markov[r] = (cnt.get(r, 0.0) + alpha) / (tot + alpha * n_rooms)
    if len(log) >= 3:
        a2, b2 = log[-2], log[-1]
        cnt2 = defaultdict(float)
        tot2 = 0.0
        for i in range(len(log) - 2):
            if log[i] == a2 and log[i + 1] == b2:
                cnt2[log[i + 2]] += 1.0
                tot2 += 1.0
        if tot2 > 0:
            alpha2 = 0.55
            for r in ROOM_ORDER:
                p2 = (cnt2.get(r, 0.0) + alpha2) / (tot2 + alpha2 * n_rooms)
                markov[r] = 0.60 * markov[r] + 0.40 * p2

    recent8 = log[-8:] if log else []
    streak_map = {r: recent8.count(r) for r in ROOM_ORDER}

    # volatility: biến thiên tần suất kill theo nửa log
    vol_map = {r: 0.0 for r in ROOM_ORDER}
    if n_log >= 8:
        half = n_log // 2
        first, second = log[:half], log[half:]
        for r in ROOM_ORDER:
            f1 = first.count(r) / float(len(first) or 1)
            f2 = second.count(r) / float(len(second) or 1)
            vol_map[r] = abs(f2 - f1)

    for r in ROOM_ORDER:
        kills = int(room_stats[r].get("kills", 0) or 0)
        survives = int(room_stats[r].get("survives", 0) or 0)
        samples = kills + survives
        k_log = log.count(r) if log else 0

        # Dirichlet prior alpha=1 (đều)
        if samples >= 2:
            p_base = (kills + 1.0) / (samples + float(n_rooms))
            conf = min(1.0, samples / 24.0)
        elif n_log >= 4:
            p_base = (k_log + 1.0) / (n_log + float(n_rooms))
            conf = min(1.0, n_log / 20.0) * 0.75
        else:
            p_base = 1.0 / n_rooms
            conf = 0.12

        p_ewma = float(ewma.get(r, 0.0))
        p_mk = float(markov.get(r, 1.0 / n_rooms))
        players = int(room_state[r].get("players", 0) or 0)
        bet = float(room_state[r].get("bet", 0) or 0)
        crowd = players / max_players
        money = bet / max_bet

        gap = float(n_rooms + 3)
        if log:
            for i in range(len(log) - 1, -1, -1):
                if log[i] == r:
                    gap = float(len(log) - 1 - i)
                    break
        gap_factor = min(1.0, gap / 12.0)
        streak_n = min(1.0, float(streak_map.get(r, 0)) / 3.0)
        vol = float(vol_map.get(r, 0.0))

        w_base = 0.24 + 0.26 * conf
        w_ewma = 0.18 + 0.10 * (1.0 - conf)
        w_mk = 0.14 + 0.06 * (1.0 - conf)
        w_crowd = 0.09
        w_money = 0.07
        w_gap = 0.09
        w_streak = 0.08
        w_vol = 0.05
        wsum = w_base + w_ewma + w_mk + w_crowd + w_money + w_gap + w_streak + w_vol
        p_lose = (
            w_base * p_base
            + w_ewma * p_ewma
            + w_mk * p_mk
            + w_crowd * crowd
            + w_money * money
            + w_gap * gap_factor * (1.0 / n_rooms) * 2.2
            + w_streak * streak_n * 0.48
            + w_vol * min(1.0, vol * 4.0) * 0.35
        ) / max(wsum, 1e-6)
        p_lose = max(0.02, min(0.95, p_lose))
        p_win = 1.0 - p_lose

        rows.append({
            "id": r,
            "name": ROOM_NAMES.get(r, f"Phòng {r}"),
            "kills": kills,
            "survives": survives,
            "samples": samples,
            "kill_rate": p_base,
            "players": players,
            "bet": bet,
            "risk_pct": p_lose * 100.0,
            "win_prob": p_win * 100.0,
            "lose_prob": p_lose * 100.0,
            "ewma": p_ewma,
            "markov": p_mk,
            "gap": gap,
            "streak": streak_map.get(r, 0),
            "volatility": vol,
            "confidence": conf,
            "last_kill_round": room_stats[r].get("last_kill_round"),
        })

    if rows:
        mx = max(x["risk_pct"] for x in rows) or 1.0
        mn = min(x["risk_pct"] for x in rows)
        span = max(1e-6, mx - mn)
        for x in rows:
            rel = (x["risk_pct"] - mn) / span * 100.0
            x["risk_pct"] = 0.78 * x["risk_pct"] + 0.22 * rel
            x["risk_pct"] = max(1.0, min(99.0, x["risk_pct"]))
            x["lose_prob"] = x["risk_pct"]
            x["win_prob"] = 100.0 - x["risk_pct"]
        s = sum(float(x["win_prob"]) for x in rows) or 1.0
        for x in rows:
            x["win_prob_rel"] = float(x["win_prob"]) / s * 100.0
    rows.sort(key=lambda x: x["risk_pct"], reverse=True)
    return rows

def _vth_kill_gap(room: int) -> float:
    """Số ván từ lần kill gần nhất (cao = an toàn hơn)."""
    if not game_kill_log:
        return 8.0
    log = list(game_kill_log)
    for i in range(len(log) - 1, -1, -1):
        if log[i] == room:
            return float(len(log) - 1 - i)
    return float(min(12, len(log) + 2))

def _vth_transition_safe(room: int) -> float:
    """P(không bị kill ngay sau last) — điểm an toàn Markov."""
    if len(game_kill_log) < 2:
        return 0.5
    last = game_kill_log[-1]
    trans = defaultdict(int)
    total = 0
    for a, b in zip(game_kill_log, list(game_kill_log)[1:]):
        if a == last:
            trans[b] += 1
            total += 1
    if total == 0:
        return 0.5
    # xác suất room bị kill tiếp
    p_kill = (trans.get(room, 0) + 0.35) / (total + 0.35 * len(ROOM_ORDER))
    return 1.0 - p_kill

def _vth_unified_scores(noise: float = 0.04) -> Dict[int, float]:
    """
    Điểm an toàn hợp nhất (cao = nên cược):
    survival + gap + markov + anti-crowd + anti-money + risk% (nếu có).
    """
    max_p = max((room_state[r].get("players", 0) or 0) for r in ROOM_ORDER) or 1
    max_b = max((room_state[r].get("bet", 0) or 0) for r in ROOM_ORDER) or 1.0
    risk_by = {}
    try:
        for row in compute_vth_room_risk() or []:
            rid = int(row.get("id") or row.get("room") or 0)
            if rid:
                risk_by[rid] = float(row.get("risk_pct", 50) or 50) / 100.0
    except Exception:
        pass

    scores = {}
    for r in ROOM_ORDER:
        kills = int(room_stats[r].get("kills", 0) or 0)
        survives = int(room_stats[r].get("survives", 0) or 0)
        survival = (survives + 1.2) / (kills + survives + 2.4)
        gap = min(1.0, _vth_kill_gap(r) / 8.0)
        markov_safe = _vth_transition_safe(r)
        crowd = 1.0 - (float(room_state[r].get("players", 0) or 0) / max_p)
        money = 1.0 - (float(room_state[r].get("bet", 0) or 0) / max_b)
        risk_safe = 1.0 - risk_by.get(r, 0.5)

        scores[r] = (
            0.24 * survival
            + 0.18 * gap
            + 0.18 * markov_safe
            + 0.14 * crowd
            + 0.12 * money
            + 0.14 * risk_safe
        )
        if noise:
            scores[r] += random.uniform(-noise, noise)
    return scores

def _vth_pick_max(scores: Dict[int, float]) -> int:
    if not scores:
        return random.choice(ROOM_ORDER)
    best = max(scores.values())
    tops = [r for r, v in scores.items() if abs(v - best) < 1e-9]
    return random.choice(tops)

def _vth_risk_map() -> Dict[int, float]:
    try:
        return {int(r["id"]): float(r.get("risk_pct", 50) or 50) for r in (compute_vth_room_risk() or [])}
    except Exception:
        return {}

def _vth_conservative_pick(candidates: List[int], n: int = 1) -> List[int]:
    """Ưu tiên risk thấp — luôn trả đủ n phòng (không bỏ ván)."""
    n = max(1, min(4, int(n or 1)))
    rm = _vth_risk_map()
    pool = list(candidates) if candidates else list(ROOM_ORDER)
    pool = sorted(set(pool), key=lambda r: (rm.get(r, 55.0), r))
    out = list(pool)
    if len(out) < n:
        for r in sorted(ROOM_ORDER, key=lambda x: (rm.get(x, 55.0), x)):
            if r not in out:
                out.append(r)
            if len(out) >= n:
                break
    if not out:
        out = list(ROOM_ORDER)[:n]
    return out[:n]

def _vth_round_danger_check() -> tuple:
    """
    Phân tích độ ổn định kỳ hiện tại.
    Returns: (is_danger: bool, message: str)
    """
    try:
        rows = compute_vth_room_risk() or []
    except Exception:
        rows = []
    if not rows:
        return True, "Chưa đủ dữ liệu — phân tích không ổn"
    risks = [float(r.get("risk_pct", 50) or 50) for r in rows]
    safest = min(risks)
    avg = sum(risks) / max(1, len(risks))
    high_n = sum(1 for x in risks if x >= 70)
    # mẫu kill quá ít → kém tin cậy
    samples = sum(int(r.get("samples", 0) or 0) for r in rows)
    if samples < 8 and len(list(game_kill_log or [])) < 5:
        return True, f"Dữ liệu mỏng (mẫu {samples}) — chưa ổn để cược"
    if safest >= VTH_SKIP_IF_SAFEST_ABOVE:
        return True, f"Nguy hiểm: phòng an toàn nhất vẫn {safest:.0f}% risk"
    if avg >= 66.0:
        return True, f"Nguy hiểm: risk trung bình {avg:.0f}% quá cao"
    if high_n >= 6:
        return True, f"Nguy hiểm: {high_n}/8 phòng risk CAO"
    # chênh lệch quá nhỏ + avg cao → không có chỗ trú
    if (max(risks) - min(risks)) < 8 and avg >= 58:
        return True, "Nguy hiểm: mọi phòng risk gần nhau, không có lựa chọn ổn"
    return False, ""

# LOGIC 43 — RỦI RO AN TOÀN (VIP / SAFE RISK)
def choose_safe_risk() -> int:
    """VIP: tối ưu giảm thua — risk thấp là trọng số chính."""
    scores = _vth_unified_scores(noise=0.015)
    try:
        for row in compute_vth_room_risk() or []:
            rid = int(row.get("id") or 0)
            if rid in scores:
                rp = float(row.get("risk_pct", 50) or 50) / 100.0
                scores[rid] += 0.55 * (1.0 - rp)
                if rp > 0.70:
                    scores[rid] -= 0.45
    except Exception:
        pass
    # conservative: chỉ xét top risk thấp
    picks = _vth_conservative_pick(list(scores.keys()), 1)
    if picks:
        return picks[0]
    return _vth_pick_max(scores)

# LOGIC 1 — PHẬT ĐỘ (RANDOM)
def choose_random() -> int:
    return random.choice(ROOM_ORDER)

# LOGIC 2 — AN TOÀN (MIN PLAYERS & BET)
def choose_min_player_bet() -> int:
    if not any(rs.get('players', 0) > 0 or rs.get('bet', 0) > 0 for rs in room_state.values()):
        return choose_random()
    player_ranks = sorted(ROOM_ORDER, key=lambda r: room_state[r]['players'])
    bet_ranks = sorted(ROOM_ORDER, key=lambda r: room_state[r]['bet'])
    scores = defaultdict(int)
    for i, r in enumerate(player_ranks):
        scores[r] += i
    for i, r in enumerate(bet_ranks):
        scores[r] += i
    return min(scores, key=scores.get)

# LOGIC 3 — XÁC SUẤT (PROBABILITY)
def choose_probability() -> int:
    scores = {}
    for r in ROOM_ORDER:
        kills = room_stats[r].get('kills', 0)
        survives = room_stats[r].get('survives', 0)
        survival_rate = (survives + 1) / (kills + survives + 2)
        scores[r] = survival_rate
    return max(scores, key=scores.get)

# LOGIC 4 — THEO SÁT THỦ (FOLLOW KILLER)
def choose_follow_killer() -> int:
    if last_killed_room is not None and last_killed_room in ROOM_ORDER:
        return last_killed_room
    return random.choice(ROOM_ORDER)

# LOGIC 5 — TUẦN TỰ
def choose_sequential() -> int:
    global _sequential_bet_index
    room_to_bet = ROOM_ORDER[_sequential_bet_index]
    _sequential_bet_index = (_sequential_bet_index + 1) % len(ROOM_ORDER)
    return room_to_bet

# LOGIC 6 — TÍNH CÁCH SÁT THỦ
def choose_killer_personality_enhanced() -> int:
    if len(killer_history) < 3:
        return choose_random()
    
    recent_killers = list(killer_history)[-15:]
    avg_players = sum(h['players'] for h in recent_killers) / len(recent_killers)
    avg_bet = sum(h['bet'] for h in recent_killers) / len(recent_killers)
    player_var = sum((h['players'] - avg_players) ** 2 for h in recent_killers) / len(recent_killers)
    bet_var = sum((h['bet'] - avg_bet) ** 2 for h in recent_killers) / len(recent_killers)
    stability_factor = 1.5 if player_var < 2 and bet_var < 100 else 0.8
    
    avoidance_scores = {}
    for r in ROOM_ORDER:
        current_players = room_state[r]['players']
        current_bet = room_state[r]['bet']
        player_dist = abs(current_players - avg_players) / (avg_players + 1)
        bet_dist = abs(current_bet - avg_bet) / (avg_bet + 1)
        base_score = (player_dist + bet_dist) * stability_factor
        kills = room_stats[r].get('kills', 0)
        survives = room_stats[r].get('survives', 0)
        survival_rate = (survives + 1) / (kills + survives + 2)
        base_score += survival_rate * 0.3
        avoidance_scores[r] = base_score
    
    return max(avoidance_scores, key=avoidance_scores.get)

# LOGIC 7 — THÔNG MINH (SMART SAFE)
def choose_smart_safe_enhanced() -> int:
    """Smart Safe v2 — unified score + lệch pattern sát thủ."""
    scores = _vth_unified_scores(noise=0.05)
    if killer_history:
        avg_p = sum(h.get("players", 0) for h in killer_history) / len(killer_history)
        avg_b = sum(h.get("bet", 0) for h in killer_history) / len(killer_history)
        for r in ROOM_ORDER:
            # càng khác profile sát thủ càng an toàn
            pd = abs((room_state[r].get("players", 0) or 0) - avg_p) / (avg_p + 1)
            bd = abs((room_state[r].get("bet", 0) or 0) - avg_b) / (avg_b + 1)
            scores[r] += 0.12 * (pd + bd)
    return _vth_pick_max(scores)

# LOGIC 8 — THEO VẾT SÁT THỦ (TRỄ 1 VÁN)
def choose_follow_killer_delayed() -> int:
    global last_killed_room_delayed
    if last_killed_room_delayed is not None and last_killed_room_delayed in ROOM_ORDER:
        return last_killed_room_delayed
    return random.choice(ROOM_ORDER)

# LOGIC 9 — THÁNH TRỐN TÌM (MASTER AI)
def choose_hide_seek_master() -> int:
    danger_scores = {}
    max_players = max(rs['players'] for rs in room_state.values()) or 1
    max_bet = max(rs['bet'] for rs in room_state.values()) or 1
    avg_players_killed = 0
    avg_bet_killed = 0
    if killer_history:
        avg_players_killed = sum(h['players'] for h in killer_history) / len(killer_history)
        avg_bet_killed = sum(h['bet'] for h in killer_history) / len(killer_history)
    for r in ROOM_ORDER:
        kills = room_stats[r].get('kills', 0)
        survives = room_stats[r].get('survives', 0)
        hist_danger = (kills + 1) / (kills + survives + 2)
        crowd_danger = room_state[r]['players'] / max_players
        money_danger = room_state[r]['bet'] / max_bet
        personality_danger = 0
        if killer_history:
            player_sim = 1 - (abs(room_state[r]['players'] - avg_players_killed) / (avg_players_killed + max_players + 1))
            bet_sim = 1 - (abs(room_state[r]['bet'] - avg_bet_killed) / (avg_bet_killed + max_bet + 1))
            personality_danger = (player_sim + bet_sim) / 2
        recency_penalty = 0.0  # không chặn phòng vừa kill
        total_danger = (0.3 * hist_danger) + (0.2 * crowd_danger) + (0.2 * money_danger) + (0.3 * personality_danger) + recency_penalty
        danger_scores[r] = total_danger
    return min(danger_scores, key=danger_scores.get)

# LOGIC 10 — CÂN BẰNG (BALANCE)
def choose_balance() -> int:
    scores = {}
    total_players = sum(rs['players'] for rs in room_state.values())
    total_bet = sum(rs['bet'] for rs in room_state.values())
    avg_players = total_players / len(ROOM_ORDER) if total_players > 0 else 0
    avg_bet = total_bet / len(ROOM_ORDER) if total_bet > 0 else 0
    for r in ROOM_ORDER:
        players = room_state[r]['players']
        bet = room_state[r]['bet']
        score = abs(players - avg_players) / (avg_players + 1) + abs(bet - avg_bet) / (avg_bet + 1)
        scores[r] = score
    return min(scores, key=scores.get)

# LOGIC 11 — ĐÔNG NHẤT (MOST PLAYERS)
def choose_most_players() -> int:
    return max(ROOM_ORDER, key=lambda r: room_state[r]['players'])

# LOGIC 12 — ÍT NHẤT (LEAST PLAYERS)
def choose_least_players() -> int:
    return min(ROOM_ORDER, key=lambda r: room_state[r]['players'])

# LOGIC 13 — GIÀU NHẤT (RICHEST)
def choose_richest() -> int:
    return max(ROOM_ORDER, key=lambda r: room_state[r]['bet'])

# LOGIC 14 — NGHÈO NHẤT (POOREST)
def choose_poorest() -> int:
    return min(ROOM_ORDER, key=lambda r: room_state[r]['bet'])



# LOGIC 16 — TRÁNH KẾT QUẢ (AVOID RESULT)
def choose_avoid_result() -> int:
    if last_killed_room is None:
        return random.choice(ROOM_ORDER)
    candidates = [r for r in ROOM_ORDER if r != last_killed_room]
    if not candidates:
        return random.choice(ROOM_ORDER)
    return random.choice(candidates)

# LOGIC 19 — TRUNG VỊ (MEDIAN)
def choose_median() -> int:
    if not any(rs['players'] > 0 for rs in room_state.values()):
        return random.choice(ROOM_ORDER)
    players_list = sorted(ROOM_ORDER, key=lambda r: room_state[r]['players'])
    bet_list = sorted(ROOM_ORDER, key=lambda r: room_state[r]['bet'])
    median_players = players_list[len(players_list) // 2]
    median_bet = bet_list[len(bet_list) // 2]
    if median_players == median_bet:
        return median_players
    scores = {}
    for r in ROOM_ORDER:
        dist_players = abs(room_state[r]['players'] - room_state[median_players]['players'])
        dist_bet = abs(room_state[r]['bet'] - room_state[median_bet]['bet'])
        scores[r] = dist_players + dist_bet
    return min(scores, key=scores.get)

# LOGIC 20 — MẪU LẶP (PATTERN)
def choose_pattern() -> int:
    if len(game_kill_log) < 3:
        return random.choice(ROOM_ORDER)
    last_3 = list(game_kill_log)[-3:]
    if len(last_3) == 3 and last_3[0] == last_3[2]:
        return last_3[1]
    return random.choice(ROOM_ORDER)

# LOGIC 41 — PHÂN TÍCH CHU KỲ (CYCLE ANALYSIS)
def choose_cycle_analysis() -> int:
    if len(game_kill_log) < 6:
        return choose_random()
    
    history = list(game_kill_log)
    best_cycle = None
    best_score = -1
    
    for cycle_len in range(2, 11):
        if len(history) >= cycle_len * 2:
            matches = 0
            total = len(history) - cycle_len
            for i in range(total):
                if history[i] == history[i + cycle_len]:
                    matches += 1
            score = matches / max(1, total)
            if score > best_score:
                best_score = score
                best_cycle = cycle_len
    
    if best_cycle and best_score > 0.6:
        last_cycle_start = len(history) - best_cycle
        if last_cycle_start >= 0:
            predicted = history[last_cycle_start]
            if predicted in ROOM_ORDER:
                if predicted == last_killed_room:
                    alternatives = [r for r in ROOM_ORDER if r != predicted]
                    if alternatives:
                        return random.choice(alternatives)
                return predicted
    
    return choose_smart_safe_enhanced()

# LOGIC 42 — PHÂN TÍCH XU HƯỚNG (TREND ANALYSIS)
def choose_trend_analysis() -> int:
    if len(killer_history) < 5:
        return choose_random()
    
    player_trends = []
    bet_trends = []
    for h in list(killer_history)[-10:]:
        player_trends.append(h['players'])
        bet_trends.append(h['bet'])
    
    if len(player_trends) >= 3:
        player_slope = (player_trends[-1] - player_trends[0]) / max(1, len(player_trends))
        bet_slope = (bet_trends[-1] - bet_trends[0]) / max(1, len(bet_trends))
        next_players = player_trends[-1] + player_slope * 1.5
        next_bet = bet_trends[-1] + bet_slope * 1.5
        
        scores = {}
        for r in ROOM_ORDER:
            if r == last_killed_room:
                scores[r] = -999999
                continue
            players = room_state[r]['players']
            bet = room_state[r]['bet']
            player_dist = abs(players - next_players) / (next_players + 1)
            bet_dist = abs(bet - next_bet) / (next_bet + 1)
            scores[r] = player_dist + bet_dist
        
        return max(scores, key=scores.get)
    
    return choose_random()

def choose_vip_random() -> int:
    logic_list = [
        choose_random, choose_min_player_bet, choose_probability,
        choose_follow_killer, choose_sequential, choose_killer_personality_enhanced,
        choose_smart_safe_enhanced, choose_follow_killer_delayed, choose_hide_seek_master,
        choose_balance, choose_most_players, choose_least_players,
        choose_richest, choose_poorest, choose_alternate,
        choose_avoid_result, choose_cold, choose_hot, choose_median, choose_pattern,
        choose_cycle_analysis, choose_trend_analysis
    ]
    sys_random = random.SystemRandom()
    chosen_func = sys_random.choice(logic_list)
    return chosen_func()

# LOGIC 22 — BẮT SÓNG SÁT THỦ (KILLER WAVE)
def choose_killer_wave() -> int:
    if len(game_kill_log) < 4:
        return choose_random()
    last_4 = list(game_kill_log)[-4:]
    for i in range(1, 4):
        if len(last_4) >= i*2 and last_4[-i:] == last_4[-i*2:-i]:
            predicted = last_4[-i-1] if len(last_4) > i else last_4[-1]
            return predicted
    return choose_smart_safe_enhanced()

# LOGIC 23 — PHÂN TÍCH TÂM LÝ (PSYCHO ANALYSIS)
def choose_psycho_analysis() -> int:
    max_players_room = max(ROOM_ORDER, key=lambda r: room_state[r]['players'])
    max_bet_room = max(ROOM_ORDER, key=lambda r: room_state[r]['bet'])
    crowd_favorite = max_players_room if room_state[max_players_room]['players'] > room_state[max_bet_room]['players'] else max_bet_room
    candidates = [r for r in ROOM_ORDER if r != crowd_favorite]
    if candidates:
        return min(candidates, key=lambda r: room_state[r]['players'] + room_state[r]['bet'] * 0.01)
    return choose_random()

# LOGIC 24 — CHUỖI MARKOV (MARKOV CHAIN)
def choose_markov_chain() -> int:
    """Markov v2: dự đoán phòng kill kế → cược phòng KHÁC (an toàn)."""
    if len(game_kill_log) < 3:
        return _vth_pick_max(_vth_unified_scores())
    # bậc 1
    t1 = defaultdict(lambda: defaultdict(float))
    for a, b in zip(game_kill_log, list(game_kill_log)[1:]):
        t1[a][b] += 1.0
    last = game_kill_log[-1]
    # bậc 2 nếu đủ dữ liệu
    pred_kill_scores = {r: 0.15 for r in ROOM_ORDER}
    if len(game_kill_log) >= 3:
        t2 = defaultdict(lambda: defaultdict(float))
        log = list(game_kill_log)
        for i in range(len(log) - 2):
            t2[(log[i], log[i + 1])][log[i + 2]] += 1.0
        key = (log[-2], log[-1])
        if t2[key]:
            for r, c in t2[key].items():
                pred_kill_scores[r] += c
    if t1[last]:
        for r, c in t1[last].items():
            pred_kill_scores[r] += c * 0.85
    # chọn phòng ít khả năng bị kill nhất
    return min(pred_kill_scores, key=pred_kill_scores.get)

# LOGIC 25 — HỌC SÂU (DEEP LEARNING)
def choose_deep_learning_enhanced() -> int:
    if len(killer_history) < 5:
        return choose_random()
    
    weights = {}
    for r in ROOM_ORDER:
        kills = room_stats[r].get('kills', 0)
        survives = room_stats[r].get('survives', 0)
        survival_rate = (survives + 1) / (kills + survives + 2)
        
        recent_boost = 0
        if r in game_kill_log:
            recent_count = list(game_kill_log).count(r)
            recent_boost = -0.3 * min(1, recent_count / 3)
        
        trend_boost = 0
        if len(game_kill_log) >= 5:
            last_5 = list(game_kill_log)[-5:]
            if r in last_5:
                recent_appear = last_5.count(r)
                trend_boost = -0.2 * recent_appear
            else:
                trend_boost = 0.1
        
        max_players = max(rs['players'] for rs in room_state.values()) or 1
        max_bet = max(rs['bet'] for rs in room_state.values()) or 1
        crowd_boost = 1 - (room_state[r]['players'] / max_players)
        money_boost = 1 - (room_state[r]['bet'] / max_bet)
        
        killer_pattern_boost = 0
        if len(game_kill_log) >= 3:
            last_3 = list(game_kill_log)[-3:]
            if len(last_3) == 3 and last_3[0] == last_3[2] and last_3[0] == r:
                killer_pattern_boost = -0.4
        
        weights[r] = (0.25 * survival_rate + 0.20 * recent_boost + 0.15 * trend_boost + 0.15 * crowd_boost + 0.10 * money_boost + 0.15 * killer_pattern_boost)
        weights[r] += random.uniform(-0.08, 0.08)
    
    return max(weights, key=weights.get)

# LOGIC 26 — HỌC TĂNG CƯỜNG (REINFORCEMENT)
def choose_reinforcement() -> int:
    if len(bet_history) < 3:
        return choose_random()
    action_scores = {r: 0 for r in ROOM_ORDER}
    for b in list(bet_history)[-10:]:
        room = b.get('room')
        result = b.get('result')
        if room in ROOM_ORDER and result:
            if result == "Thắng":
                action_scores[room] += 1
            else:
                action_scores[room] -= 0.5
    max_score = max(action_scores.values())
    if max_score <= 0:
        return choose_random()
    best_rooms = [r for r, s in action_scores.items() if s == max_score]
    return random.choice(best_rooms)

# LOGIC 27 — XÁC SUẤT BAYES (BAYESIAN)
def choose_bayesian() -> int:
    """Bayes v2: posterior P(kill) kết hợp form realtime → chọn min."""
    if len(game_kill_log) < 2:
        return _vth_pick_max(_vth_unified_scores())
    room_counts = Counter(game_kill_log)
    total_kills = len(game_kill_log)
    posterior = {}
    for r in ROOM_ORDER:
        prior = 1.0 / len(ROOM_ORDER)
        like = (room_counts.get(r, 0) + 0.8) / (total_kills + 0.8 * len(ROOM_ORDER))
        # evidence: đông / nhiều tiền → sát thủ hay chọn
        max_p = max(room_state[x].get("players", 0) or 0 for x in ROOM_ORDER) or 1
        max_b = max(room_state[x].get("bet", 0) or 0 for x in ROOM_ORDER) or 1
        crowd = (room_state[r].get("players", 0) or 0) / max_p
        money = (room_state[r].get("bet", 0) or 0) / max_b
        evidence = 0.55 + 0.25 * crowd + 0.20 * money
        posterior[r] = prior * like * evidence
    s = sum(posterior.values()) or 1
    for r in posterior:
        posterior[r] /= s
    return min(posterior, key=posterior.get)

# LOGIC 28 — PHÂN CỤM K-MEANS
def choose_k_means() -> int:
    if len(game_kill_log) < 6:
        return choose_random()
    from collections import defaultdict
    room_features = defaultdict(lambda: [0, 0])
    for i, room in enumerate(list(game_kill_log)[-10:]):
        room_features[room][0] += 1
        room_features[room][1] = i
    cluster_1 = set()
    cluster_2 = set()
    for room, features in room_features.items():
        if features[0] < 2:
            cluster_1.add(room)
        else:
            cluster_2.add(room)
    if cluster_1:
        return random.choice(list(cluster_1))
    return choose_random()

# LOGIC 29 — MẠNG NƠ-RON (NEURAL)
def choose_neural() -> int:
    if len(killer_history) < 3:
        return choose_random()
    scores = {}
    for r in ROOM_ORDER:
        players = room_state[r]['players']
        bet = room_state[r]['bet']
        kills = room_stats[r].get('kills', 0)
        survives = room_stats[r].get('survives', 0)
        layer1 = (0.3 * survives) - (0.5 * kills) + (0.2 * players) - (0.3 * bet)
        layer2 = (0.4 * layer1) + (0.2 * (survives - kills))
        layer3 = (0.5 * layer2) + (0.3 * (1 - players/max(1, max(rs['players'] for rs in room_state.values()))))
        output = 1 / (1 + math.exp(-layer3))
        scores[r] = output
    return max(scores, key=scores.get)

# LOGIC 30 — LOGIC MỜ (FUZZY)
def choose_fuzzy() -> int:
    if len(killer_history) < 2:
        return choose_random()
    scores = {}
    for r in ROOM_ORDER:
        players = room_state[r]['players']
        bet = room_state[r]['bet']
        players_young = max(0, 1 - players/2) if players < 2 else 0
        players_mid = max(0, 1 - abs(players-3)/2) if 1 < players < 5 else 0
        players_old = max(0, (players-4)/2) if players > 4 else 0
        bet_low = max(0, 1 - bet/100) if bet < 100 else 0
        bet_mid = max(0, 1 - abs(bet-300)/200) if 100 < bet < 500 else 0
        bet_high = max(0, (bet-400)/200) if bet > 400 else 0
        rule1 = min(players_young, bet_low)
        rule2 = min(players_old, bet_high)
        rule3 = min(players_mid, bet_mid)
        safety_score = (rule1 * 1.0 + rule2 * 0.0 + rule3 * 0.5) / (rule1 + rule2 + rule3 + 0.01)
        scores[r] = safety_score
    return max(scores, key=scores.get)

# LOGIC 31 — THUẬT TOÁN DI TRUYỀN (GENETIC)
def choose_genetic() -> int:
    if len(killer_history) < 5:
        return choose_random()
    population = list(game_kill_log)[-10:]
    if not population:
        return choose_random()
    fitness = {}
    for r in ROOM_ORDER:
        kills = room_stats[r].get('kills', 0)
        survives = room_stats[r].get('survives', 0)
        fitness[r] = (survives + 1) / (kills + survives + 2)
    return max(fitness, key=fitness.get)

# LOGIC 32 — KIẾN BÒ (ANT COLONY)
def choose_ant_colony() -> int:
    if len(game_kill_log) < 3:
        return choose_random()
    pheromone = {}
    for r in ROOM_ORDER:
        count = list(game_kill_log).count(r)
        pheromone[r] = count / len(game_kill_log) if game_kill_log else 0
    return min(pheromone, key=pheromone.get)

# LOGIC 33 — BẦY ĐÀN (PARTICLE SWARM)
def choose_particle_swarm() -> int:
    if len(killer_history) < 3:
        return choose_random()
    scores = {}
    for r in ROOM_ORDER:
        kills = room_stats[r].get('kills', 0)
        survives = room_stats[r].get('survives', 0)
        survival_rate = (survives + 1) / (kills + survives + 2)
        recent_trend = 0.3 if r in list(game_kill_log)[-3:] else 0
        scores[r] = survival_rate + recent_trend
    return max(scores, key=scores.get)

# LOGIC 34 — K-NEAREST NEIGHBORS (KNN)
def choose_knn() -> int:
    if len(game_kill_log) < 3:
        return choose_random()
    k = min(3, len(game_kill_log))
    nearest = list(game_kill_log)[-k:]
    counts = Counter(nearest)
    min_count = min(counts.values())
    candidates = [r for r, c in counts.items() if c == min_count]
    if candidates:
        return random.choice(candidates)
    return choose_random()

# LOGIC 35 — CÂY QUYẾT ĐỊNH (DECISION TREE)
def choose_decision_tree() -> int:
    if len(killer_history) < 5:
        return choose_random()
    return choose_probability() if killer_history else choose_random()

# LOGIC 36 — RỪNG NGẪU NHIÊN (RANDOM FOREST)
def choose_random_forest() -> int:
    if len(killer_history) < 3:
        return choose_random()
    predictions = []
    for _ in range(5):
        if random.random() > 0.5:
            predictions.append(choose_probability())
        else:
            predictions.append(choose_min_player_bet())
    counts = Counter(predictions)
    return max(counts, key=counts.get)

# LOGIC 37 — TĂNG CƯỜNG GRADIENT (GRADIENT BOOST)
def choose_gradient_boost() -> int:
    if len(killer_history) < 3:
        return choose_random()
    scores = {}
    for r in ROOM_ORDER:
        base_score = 0.5
        kills = room_stats[r].get('kills', 0)
        survives = room_stats[r].get('survives', 0)
        survival_rate = (survives + 1) / (kills + survives + 2)
        base_score += 0.3 * survival_rate
        base_score -= 0.1 * (room_state[r]['players'] / max(1, max(rs['players'] for rs in room_state.values())))
        base_score -= 0.1 * (room_state[r]['bet'] / max(1, max(rs['bet'] for rs in room_state.values())))
        scores[r] = base_score
    return max(scores, key=scores.get)

# LOGIC 38 — LSTM
def choose_lstm() -> int:
    if len(game_kill_log) < 4:
        return choose_random()
    last_5 = list(game_kill_log)[-5:]
    if len(last_5) == 5 and last_5[0] == last_5[3] and last_5[1] == last_5[4]:
        return last_5[2]
    return choose_markov_chain()

# LOGIC 39 — TRANSFORMER
def choose_transformer() -> int:
    if len(game_kill_log) < 4:
        return choose_random()
    attention_scores = {}
    for r in ROOM_ORDER:
        kills = room_stats[r].get('kills', 0)
        survives = room_stats[r].get('survives', 0)
        recency = 1 - (list(game_kill_log).count(r) / max(1, len(game_kill_log)))
        attention_scores[r] = (0.4 * recency) + (0.3 * (survives / max(1, kills + survives))) + (0.3 * (1 - room_state[r]['players'] / max(1, max(rs['players'] for rs in room_state.values()))))
    return max(attention_scores, key=attention_scores.get)

# Logic 8: ALL IN
# LOGIC 21 — ALL IN (FREE: V1 / VIP: V2)
def choose_all():
    if not ROOM_ORDER:
        return 1

    if len(ROOM_ORDER) == 1:
        return ROOM_ORDER[0]

    max_players = max(
        (
            room_state.get(r, {}).get("players", 0)
            for r in ROOM_ORDER
        ),
        default=1
    ) or 1

    max_bet = max(
        (
            room_state.get(r, {}).get("bet", 0)
            for r in ROOM_ORDER
        ),
        default=1
    ) or 1

    full_history = list(game_kill_log)
    history100 = full_history[-100:]
    history10 = history100[-10:]

    scores = {}
    current_gaps = {}

    for room in ROOM_ORDER:
        state = room_state.get(room, {})

        players = max(
            0.0,
            float(state.get("players", 0))
        )

        bet = max(
            0.0,
            float(state.get("bet", 0))
        )

        bet_score = 1.0 - (
            bet / max_bet
        )

        bet_score = max(
            0.0,
            min(
                1.0,
                bet_score
            )
        )

        player_score = (
            players / max_players
        )

        player_score = max(
            0.0,
            min(
                1.0,
                player_score
            )
        )

        hits100 = history100.count(room)

        if history100:
            kill_safety100 = 1.0 - (
                hits100 / len(history100)
            )
        else:
            kill_safety100 = 0.5

        kill_safety100 = max(
            0.0,
            min(
                1.0,
                kill_safety100
            )
        )

        hits10 = history10.count(room)

        if history10:
            kill_safety10 = 1.0 - (
                hits10 / len(history10)
            )
        else:
            kill_safety10 = 0.5

        kill_safety10 = max(
            0.0,
            min(
                1.0,
                kill_safety10
            )
        )

        current_gap = len(history100)

        for i in range(
            len(history100) - 1,
            -1,
            -1
        ):
            if history100[i] == room:
                current_gap = (
                    len(history100) - 1 - i
                )
                break

        current_gaps[room] = current_gap

        if history100:
            delay_score = min(
                current_gap / len(history100),
                1.0
            )
        else:
            delay_score = 0.5

        delay_score = max(
            0.0,
            min(
                1.0,
                delay_score
            )
        )

        score = (
            bet_score * 0.55 +
            player_score * 0.05 +
            kill_safety100 * 0.15 +
            kill_safety10 * 0.10 +
            delay_score * 0.15
        )

        if hits10 == 0 and len(history10) >= 5:
            score += 0.05

        if hits100 <= 2 and len(history100) >= 30:
            score += 0.04

        if (
            current_gap >= 10
            and
            len(history100) >= 20
        ):
            score += 0.04

        if (
            current_gap >= 20
            and
            len(history100) >= 30
        ):
            score += 0.03

        scores[room] = score

    available_rooms = [
        room
        for room in ROOM_ORDER
        if room != last_killed_room
    ]

    if not available_rooms:
        available_rooms = list(ROOM_ORDER)

    best_score = max(
        scores.get(
            room,
            float("-inf")
        )
        for room in available_rooms
    )

    candidates = [
        room
        for room in available_rooms
        if scores.get(
            room,
            float("-inf")
        ) >= best_score - 0.02
    ]

    if not candidates:
        candidates = available_rooms

    candidates.sort(
        key=lambda room: (
            -scores.get(
                room,
                float("-inf")
            ),
            current_gaps.get(
                room,
                0
            ),
            -history100.count(room),
            -history10.count(room),
            room_state.get(
                room,
                {}
            ).get(
                "bet",
                float("inf")
            )
        )
    )

    return candidates[0]

# LOGIC 40 — TỔNG HỢP ENHANCED (ENSEMBLE)
def choose_ensemble_enhanced() -> int:
    """Ensemble v3: multi-AI + unified score + adaptive performance + anti-kill."""
    base = _vth_unified_scores(noise=0.02)
    votes = defaultdict(float)
    for r, sc in base.items():
        votes[r] += sc * 1.15

    ai_weights = {
        "choose_safe_risk": 1.45,
        "choose_smart_safe_enhanced": 1.15,
        "choose_probability": 0.9,
        "choose_markov_chain": 1.05,
        "choose_bayesian": 1.0,
        "choose_killer_personality_enhanced": 0.7,
        "choose_deep_learning_enhanced": 0.7,
        "choose_hide_seek_master": 0.5,
        "choose_neural": 0.65,
        "choose_cycle_analysis": 0.6,
        "choose_trend_analysis": 0.55,
        "choose_transformer": 0.55,
    }
    for ai_name, weight in ai_weights.items():
        try:
            func = globals().get(ai_name)
            if not func:
                continue
            room = func()
            w = float(weight)
            perf = AI_PERFORMANCE.get(ai_name, {}) or AI_PERFORMANCE.get(ai_name.replace("choose_", "").upper(), {})
            total_p = int(perf.get("total", 0) or 0)
            if total_p >= 4:
                wr = float(perf.get("wins", 0)) / total_p
                w *= 0.5 + 1.0 * wr
            votes[room] += w
        except Exception:
            continue

    if not votes:
        picks = _vth_conservative_pick(list(ROOM_ORDER), 1)
        return picks[0] if picks else random.choice(ROOM_ORDER)
    top = _vth_pick_max(dict(votes))
    picks = _vth_conservative_pick([top] + sorted(votes.keys(), key=lambda r: votes[r], reverse=True), 1)
    return picks[0] if picks else top

ENHANCED_LOGIC_MAP = {
    "RANDOM": choose_random,
    "MIN_PLAYER_BET": choose_min_player_bet,
    "PROBABILITY": choose_probability,
    "FOLLOW_KILLER": choose_follow_killer,
    "SEQUENTIAL": choose_sequential,
    "KILLER_PERSONALITY": choose_killer_personality_enhanced,
    "SMART_SAFE": choose_smart_safe_enhanced,
    "FOLLOW_KILLER_DELAYED": choose_follow_killer_delayed,
    "HIDE_SEEK_MASTER": choose_hide_seek_master,
    "BALANCE": choose_balance,
    "MOST_PLAYERS": choose_most_players,
    "LEAST_PLAYERS": choose_least_players,
    "RICHEST": choose_richest,
    "POOREST": choose_poorest,
    "AVOID_RESULT": choose_avoid_result,
    "MEDIAN": choose_median,
    "PATTERN": choose_pattern,
    "VIP_RANDOM": choose_vip_random,
    "KILLER_WAVE": choose_killer_wave,
    "PSYCHO_ANALYSIS": choose_psycho_analysis,
    "MARKOV_CHAIN": choose_markov_chain,
    "DEEP_LEARNING": choose_deep_learning_enhanced,
    "REINFORCEMENT": choose_reinforcement,
    "BAYESIAN": choose_bayesian,
    "K_MEANS": choose_k_means,
    "NEURAL": choose_neural,
    "FUZZY": choose_fuzzy,
    "GENETIC": choose_genetic,
    "ANT_COLONY": choose_ant_colony,
    "PARTICLE_SWARM": choose_particle_swarm,
    "KNN": choose_knn,
    "DECISION_TREE": choose_decision_tree,
    "RANDOM_FOREST": choose_random_forest,
    "GRADIENT_BOOST": choose_gradient_boost,
    "LSTM": choose_lstm,
    "TRANSFORMER": choose_transformer,
    "ENSEMBLE": choose_ensemble_enhanced,
    "SAFE_RISK": choose_safe_risk,
    "CYCLE_ANALYSIS": choose_cycle_analysis,
    "TREND_ANALYSIS": choose_trend_analysis,
    "ALL": choose_all,
}


# LOGIC 44 — TỔNG HỢP VIP (ĐẶT CUỐI MENU)
def choose_tong_hop() -> int:
    """Logic 44 — Tổng Hợp VIP: bỏ phiếu từ logic VIP 22-43 và cộng điểm rủi ro.

    Đây là lựa chọn tổng hợp ở cuối menu VIP; ALL IN V2 cũng dùng hàm này
    để chọn phòng khi key VIP đã được xác thực.
    """
    rooms = [int(r) for r in ROOM_ORDER]
    if not rooms:
        return 1
    candidates = [r for r in rooms if r != last_killed_room]
    if not candidates:
        candidates = list(rooms)

    vote_scores = {r: 0.0 for r in candidates}
    vote_counts = {r: 0 for r in candidates}

    # Tổng hợp các logic VIP 22-43. ENSEMBLE vẫn là một phiếu với trọng số thấp hơn.
    # Bỏ TONG_HOP/ALL để tránh tự gọi đệ quy khi chạy Logic 44 hoặc ALL IN V2.
    for ai_key in VIP_AI_LIST:
        if ai_key in {"TONG_HOP", "ALL"}:
            continue
        func = ENHANCED_LOGIC_MAP.get(ai_key)
        if not callable(func):
            continue
        try:
            room = int(func())
            if room not in vote_scores:
                continue
            weight = 0.55 if ai_key == "ENSEMBLE" else 1.0
            try:
                perf = AI_PERFORMANCE.get(func.__name__, {}) or AI_PERFORMANCE.get(ai_key, {}) or {}
                total = int(perf.get("total", 0) or 0)
                if total >= 4:
                    win_rate = float(perf.get("wins", 0) or 0) / max(1, total)
                    weight *= 0.5 + win_rate
            except Exception:
                pass
            vote_scores[room] += weight
            vote_counts[room] += 1
        except Exception as exc:
            log_debug(f"TONG_HOP VIP: lỗi ở {ai_key}: {exc}")

    # Bổ sung điểm an toàn tổng quát và ước lượng risk realtime.
    try:
        unified = _vth_unified_scores(noise=0.0)
        vals = [float(unified.get(r, 0.0) or 0.0) for r in candidates]
        lo = min(vals) if vals else 0.0
        hi = max(vals) if vals else 1.0
        span = hi - lo
        for room in candidates:
            raw = float(unified.get(room, 0.0) or 0.0)
            normalized = ((raw - lo) / span) if span > 1e-12 else 0.5
            vote_scores[room] += 0.35 * normalized
    except Exception as exc:
        log_debug(f"TONG_HOP VIP: unified score lỗi: {exc}")

    try:
        for row in compute_vth_room_risk() or []:
            rid = int(row.get("id") or row.get("room") or 0)
            if rid not in vote_scores:
                continue
            risk_pct = max(0.0, min(100.0, float(row.get("risk_pct", 50) or 50)))
            vote_scores[rid] += 0.75 * (1.0 - risk_pct / 100.0)
    except Exception as exc:
        log_debug(f"TONG_HOP VIP: risk score lỗi: {exc}")

    # Phạt nhẹ phòng vừa bị hạ gần đây và phòng tập trung quá đông/tiền.
    try:
        recent = list(game_kill_log)[-10:]
        max_players = max((float(room_state.get(r, {}).get("players", 0) or 0) for r in candidates), default=1.0) or 1.0
        max_bet = max((float(room_state.get(r, {}).get("bet", 0) or 0) for r in candidates), default=1.0) or 1.0
        for room in candidates:
            state = room_state.get(room, {})
            players = max(0.0, float(state.get("players", 0) or 0))
            bet = max(0.0, float(state.get("bet", 0) or 0))
            vote_scores[room] -= 0.15 * (players / max_players)
            vote_scores[room] -= 0.15 * (bet / max_bet)
            vote_scores[room] -= 0.10 * recent.count(room)
    except Exception as exc:
        log_debug(f"TONG_HOP VIP: điều chỉnh phòng lỗi: {exc}")

    best_score = max(vote_scores.values())
    best_rooms = [r for r, score in vote_scores.items() if abs(score - best_score) < 1e-9]
    if len(best_rooms) > 1:
        # Khi điểm bằng nhau ưu tiên phòng có nhiều phiếu, rồi chọn ngẫu nhiên nếu vẫn hòa.
        max_votes = max(vote_counts.get(r, 0) for r in best_rooms)
        best_rooms = [r for r in best_rooms if vote_counts.get(r, 0) == max_votes]
    return int(random.choice(best_rooms or candidates))


ENHANCED_LOGIC_MAP["TONG_HOP"] = choose_tong_hop


def _vth_current_key_type(is_vip=None) -> str:
    """Lấy quyền FREE/VIP hiện tại từ trạng thái xác thực của bot."""
    if is_vip is None:
        try:
            valid, _reason, detected_vip = check_activation_valid(exit_on_expired=False)
            if valid:
                is_vip = bool(detected_vip)
            else:
                is_vip = bool(IS_VIP_USER or is_vip_activated())
        except Exception:
            try:
                is_vip = bool(IS_VIP_USER or is_vip_activated())
            except Exception:
                is_vip = bool(IS_VIP_USER)
    return "vip" if bool(is_vip) else "free"


def choose_rooms_multi(mode: str, n: int = 1, is_vip=None) -> Tuple[List[int], str]:
  
    n = max(1, min(4, int(n or 1)))
    mode = str(mode or "ENSEMBLE").upper()
    key_type = _vth_current_key_type(is_vip)
    if not is_ai_available(mode, key_type):
        mode = "RANDOM"
    try:
        scores = _vth_unified_scores(noise=0.02)
    except Exception:
        scores = {r: random.random() for r in ROOM_ORDER}
    try:
        for row in compute_vth_room_risk() or []:
            rid = int(row.get("id") or 0)
            if rid in scores:
                scores[rid] += 0.40 * (1.0 - float(row.get("risk_pct", 50) or 50) / 100.0)
    except Exception:
        pass
    try:
        if mode != "SAFE_RISK":
            primary, _ = choose_room_tn(mode, is_vip=(key_type == "vip"))
            if primary in scores:
                scores[primary] += 0.25
    except Exception:
        pass
    ranked = sorted(scores.keys(), key=lambda r: scores[r], reverse=True)
    filtered = _vth_conservative_pick(ranked, n)
    return filtered, mode


def choose_room_tn(mode: str, is_vip: Optional[bool] = None) -> Tuple[int, str]:
    """Chạy logic VTH, khóa VIP cho FREE và giữ ALL IN V1/V2 đúng quyền."""
    mode = str(mode or "RANDOM").upper()
    key_type = _vth_current_key_type(is_vip)

    # ALL IN không thay đổi cách cược: V1 dùng chọn phòng FREE, V2 dùng Tổng Hợp VIP.
    # Nhãn trả về vẫn là ALL để phần đặt cược lấy toàn bộ số dư tài khoản.
    if mode == "ALL":
        try:
            chosen_room = choose_tong_hop() if key_type == "vip" else choose_all()
            chosen_room = int(chosen_room)
            if chosen_room not in ROOM_ORDER:
                raise ValueError(f"ALL IN trả về phòng không hợp lệ: {chosen_room!r}")
            return chosen_room, "ALL"
        except Exception as exc:
            log_debug(f"Lỗi phân tích ALL IN {key_type.upper()}: {exc}")
            try:
                fallback_room = int(choose_safe_risk() if key_type == "vip" else choose_random())
            except Exception:
                fallback_room = ROOM_ORDER[0] if ROOM_ORDER else 1
            if ROOM_ORDER and fallback_room not in ROOM_ORDER:
                fallback_room = ROOM_ORDER[0]
            return fallback_room, "ALL"

    if not is_ai_available(mode, key_type):
        print(f"⚠️ AI {mode} không khả dụng với key {key_type.upper()}. Chuyển sang RANDOM.", flush=True)
        mode = "RANDOM"
    func = ENHANCED_LOGIC_MAP.get(mode, choose_random)
    try:
        chosen_room = int(func())
        if chosen_room not in ROOM_ORDER:
            raise ValueError(f"{mode} trả về phòng không hợp lệ: {chosen_room!r}")
        return chosen_room, mode
    except Exception as exc:
        log_debug(f"Lỗi thuật toán VTH {mode}: {exc}")
        try:
            fallback_room = int(choose_random())
        except Exception:
            fallback_room = ROOM_ORDER[0] if ROOM_ORDER else 1
        if ROOM_ORDER and fallback_room not in ROOM_ORDER:
            fallback_room = ROOM_ORDER[0]
        return fallback_room, "RANDOM"


def update_ai_performance(algo_name: str, is_win: bool):
    if is_win:
        AI_PERFORMANCE[algo_name]["wins"] += 1
    else:
        AI_PERFORMANCE[algo_name]["losses"] += 1
    AI_PERFORMANCE[algo_name]["total"] += 1
    try:
        with open('ai_performance.json', 'w', encoding='utf-8') as f:
            json.dump(dict(AI_PERFORMANCE), f, indent=2)
    except:
        pass

def get_best_ai() -> str:
    best_ai = "RANDOM"
    best_rate = 0
    for ai_name, stats in AI_PERFORMANCE.items():
        if stats["total"] >= 5:
            rate = stats["wins"] / stats["total"]
            if rate > best_rate:
                best_rate = rate
                best_ai = ai_name
    return best_ai


# ================== API VÀ ĐẶT CƯỢC VTH ==================

def api_headers() -> Dict[str, str]:
    return {"content-type": "application/json", "user-agent": "Mozilla/5.0", "user-id": str(USER_ID) if USER_ID else "", "user-secret-key": SECRET_KEY if SECRET_KEY else ""}

def place_bet_http(issue: int, room_id: int, amount: float) -> dict:
    payload = {"asset_type": "BUILD", "user_id": USER_ID, "room_id": int(room_id), "bet_amount": float(amount)}
    try:
        r = requests.post(BET_API_URL, headers=api_headers(), json=payload, timeout=8)
        try:
            return r.json()
        except Exception:
            return {"raw": r.text, "http_status": r.status_code}
    except Exception as e:
        return {"error": str(e)}

def record_bet(issue: int, room_id: int, amount: float, resp: dict, algo_used: Optional[str] = None) -> dict:
    """Ghi lịch sử bằng đúng snapshot phòng đã chốt cho ván.

    Không lấy lại predicted_room hiện tại vì sau khi sang ván mới biến này
    có thể đã đổi sang phòng khác. Snapshot theo issue giúp tránh tình trạng
    UI hiển thị P6 nhưng lịch sử lại ghi P7.
    """
    try:
        issue = int(issue)
    except (TypeError, ValueError):
        return {}

    if issue in history_recorded_issue:
        return next((r for r in bet_history if r.get("issue") == issue), {})

    snapshot = bet_selection_by_issue.get(issue) or {}
    selected_room = snapshot.get("room", room_id)
    try:
        selected_room = int(selected_room)
    except (TypeError, ValueError):
        selected_room = None

    # Tuyệt đối không ghi ID ngoài P1-P8. Nếu snapshot lỗi thì dùng room_id
    # truyền vào nhưng vẫn validate lại trước khi đưa vào lịch sử.
    if selected_room not in ROOM_ORDER:
        try:
            selected_room = int(room_id)
        except (TypeError, ValueError):
            selected_room = None
    if selected_room not in ROOM_ORDER:
        log_debug(f"Bỏ ghi lịch sử: room không hợp lệ issue={issue}, room={room_id!r}")
        return {}

    if room_id != selected_room:
        log_debug(
            f"ROOM SNAPSHOT mismatch issue={issue}: request_room={room_id!r}, "
            f"selected_room={selected_room}. Dùng selected_room."
        )

    history_recorded_issue.add(issue)
    now = datetime.now(tz).strftime("%H:%M:%S")
    display_algo = algo_used or snapshot.get("algo") or selected_vth_algo or settings.get("algo")
    if display_algo == "RANDOM" and selected_vth_algo and selected_vth_algo != "RANDOM":
        display_algo = selected_vth_algo

    rec = {
        "issue": issue,
        "room": selected_room,
        "amount": float(amount),
        "time": now,
        "resp": resp,
        "result": "Đang",
        "algo": display_algo,
        "delta": None,
        "balance_before_bet": None,
        "win_streak": win_streak,
        "lose_streak": lose_streak,
    }
    bet_history.append(rec)
    return rec

def place_bet_async(issue: int, room_id: int, amount: float, algo_used: Optional[str] = None):
    if _check_daily_pause_window(_vth_daily_pause_window, "VTH"):
        return
    try:
        issue = int(issue)
    except (TypeError, ValueError):
        return

    # Luôn ưu tiên snapshot của đúng ván.
    snapshot_room = (bet_selection_by_issue.get(issue) or {}).get("room")
    if snapshot_room in ROOM_ORDER:
        room_id = int(snapshot_room)
    else:
        try:
            room_id = int(room_id)
        except (TypeError, ValueError):
            return
        if room_id not in ROOM_ORDER:
            log_debug(f"place_bet_async bỏ qua room không hợp lệ issue={issue}: {room_id!r}")
            return

    if issue in bet_sent_for_issue:
        return
    bet_sent_for_issue.add(issue)
    def worker():

        # Kiểm tra lại trong thread để không gửi cược nếu khung nghỉ vừa bắt đầu.
        if _check_daily_pause_window(_vth_daily_pause_window, "VTH"):
            bet_sent_for_issue.discard(issue)
            return

        # Chụp số dư ngay trước khi gửi lệnh để tính lãi/lỗ BUILD thực tế.
        try:
            balance_before_bet = float(current_build) if current_build is not None else None
        except Exception:
            balance_before_bet = None

        res = place_bet_http(issue, room_id, amount)
        rec = record_bet(issue, room_id, amount, res, algo_used=algo_used)
        if rec is not None:
            rec["balance_before_bet"] = balance_before_bet
        if isinstance(res, dict) and (res.get("msg") == "ok" or res.get("code") == 0 or res.get("status") in ("ok", 1)):
            bet_sent_for_issue.add(issue)

        else:
            bet_sent_for_issue.discard(issue)
            history_recorded_issue.discard(issue)
            console.print(f"[red]❌ Đặt lỗi v{issue}: {res}[/]")
    threading.Thread(target=worker, daemon=True).start()

def lock_prediction_if_needed(force: bool = False):
    global prediction_locked, predicted_room, ui_state
    global current_bet, _rounds_placed_since_skip
    global skip_next_round_flag, _skip_rounds_remaining
    global stop_flag
    global last_display_bet
    global count_down
    global analysis_ready_ts

    if stop_flag or capital_stop_pending:
        return

    # Trong khung giờ đã cài, VTH không phân tích/chốt dự đoán.
    if _check_daily_pause_window(_vth_daily_pause_window, "VTH"):
        return

    if prediction_locked and not force:
        return

    if issue_id is None:
        return

    if not force and analysis_ready_ts is not None and time.time() < analysis_ready_ts:
        return

    if not force:
        remaining = _vth_get_remaining_seconds()
        # Tự động chốt dự đoán và đặt cược khi countdown còn 10 giây.
        if remaining is not None and remaining > 10:
            return

    mode = settings.get("algo", "RANDOM").upper()
    is_vip = _vth_current_key_type() == "vip"

    if not is_ai_available(mode, "vip" if is_vip else "free"):
        print("❌ Logic này chỉ dành cho Key VIP! Chuyển về RANDOM.", flush=True)
        mode = "RANDOM"
        settings["algo"] = "RANDOM"
   
    try:
        chosen, algo_used = choose_room_tn(mode, is_vip=is_vip)
        if chosen not in ROOM_ORDER:
            raise ValueError(f"Phòng không hợp lệ: {chosen!r}")
    except Exception as e:
        log_debug(f"Lỗi chọn phòng ({mode}): {e}")
        chosen = choose_random()
        if chosen not in ROOM_ORDER:
            chosen = ROOM_ORDER[0]
        algo_used = "RANDOM"
    predicted_room = int(chosen)
    prediction_locked = True

    all_in_bet_amount = None
    if mode == "ALL":
        try:
            all_in_balance, _, _ = fetch_balances_3games(retries=1, timeout=3)
            if all_in_balance is not None:
                all_in_bet_amount = float(all_in_balance)
        except Exception as exc:
            log_debug(f"ALL IN balance snapshot error: {exc}")

    try:
        room_label = ROOM_NAMES.get(predicted_room, f"Phòng {predicted_room}")
        algo_label = get_vth_algo_display_name(algo_used, is_vip)

        if mode == "ALL":
            current_bet_display = all_in_bet_amount if all_in_bet_amount is not None else 0.0
        else:
            current_bet_display = current_bet if current_bet not in (None, 0) else base_bet
        try:
            last_display_bet = float(current_bet_display)
        except Exception:
            pass
        if True:
            prediction_message = (
                f"📌 Ván: {issue_id or '-'}\n"
                f"🏠 Đánh: {room_label} (P{predicted_room})\n"
                f"🧠 Thuật toán: {algo_label}\n"
                f"💰 Cược: {float(current_bet_display or 0):,.2f} BUILD"
            )
            print(prediction_message, flush=True)
    except Exception as exc:
        log_debug(f"Telegram prediction display error: {exc}")

    try:
        current_issue = int(issue_id)
        bet_selection_by_issue[current_issue] = {
            "room": int(predicted_room),
            "algo": str(algo_used),
            "created_at": time.time(),
        }

        if len(bet_selection_by_issue) > 100:
            for old_issue in sorted(bet_selection_by_issue)[:-60]:
                bet_selection_by_issue.pop(old_issue, None)
    except (TypeError, ValueError):
        pass

    if _skip_rounds_remaining > 0:
        console.print(f"[yellow]⏸️ Đang nghỉ {_skip_rounds_remaining} ván theo cấu hình sau khi thua.[/]")
        _skip_rounds_remaining -= 1
        return
    if skip_next_round_flag:
        console.print("[yellow]⏸️ TẠM DỪNG THEO DÕI SÁT THỦ[/]")
        skip_next_round_flag = False
        return
    if run_mode == "AUTO":
        bld = current_build
        if bld is None:
            bld, _, _ = fetch_balances_3games(retries=1, timeout=3)
            if bld is None:
                console.print("[yellow]⚠️ Không lấy được số dư, không thể đặt cược. Sẽ thử lại...[/]")
                prediction_locked = False
                predicted_room = None
                ui_state = "ANALYZING"
                return
        if current_bet is None:
            current_bet = base_bet
        if mode == "ALL":
            if all_in_bet_amount is None:
                bld_fresh, _, _ = fetch_balances_3games(retries=1, timeout=3)
                if bld_fresh is None:
                    console.print("[yellow]⚠️ Không lấy được số dư BUILD cho ALL IN.[/]")
                    prediction_locked = False
                    predicted_room = None
                    ui_state = "ANALYZING"
                    return
                all_in_bet_amount = float(bld_fresh)
            amt = float(all_in_bet_amount)
            last_display_bet = amt
        else:
            amt = float(current_bet)
            last_display_bet = amt
        if amt <= 0:
            console.print("[yellow]⚠️ Số tiền đặt không hợp lệ. Bỏ qua.[/]")
            return
        if mode != "ALL" and amt > bld:
            console.print(f"[red]🔥 VỐN KHÔNG ĐỦ ĐỂ GẤP THẾP! Cần {amt:,.2f} nhưng chỉ có {bld:,.2f}. Reset về cược gốc.[/red]")
            current_bet = float(base_bet)
            amt = float(current_bet)
            last_display_bet = amt
            if amt > bld:
                console.print(f"[red]💀 Vốn không đủ để đặt cược gốc ({amt:,.2f}). Dừng tool.[/red]")
                stop_flag = True
                return
                
        place_bet_async(issue_id, predicted_room, amt, algo_used=algo_used)
        ui_state = "PREDICTED"
        _rounds_placed_since_skip += 1

        if (
            bet_rounds_before_skip > 0
            and _rounds_placed_since_skip >= bet_rounds_before_skip
        ):
            completed_rounds = _rounds_placed_since_skip
            skip_next_round_flag = True
            _rounds_placed_since_skip = 0
            try:
                snapshot = bet_selection_by_issue.setdefault(int(issue_id), {})
                snapshot["anti_soi_notice"] = (
                    f"🛡️ CHỐNG SOI KÍCH HOẠT\n"
                    f"✅ Đã cược đủ {completed_rounds} ván theo cấu hình.\n"
                    f"⏸️ Ván kế tiếp sẽ nghỉ 1 ván."
                )
            except Exception:
                pass

# ================== WEBSOCKET VTH ==================

def safe_send_enter_game(ws):
    if not ws:
        log_debug("safe_send_enter_game: ws None")
        return
    try:
        payload = {"msg_type": "handle_enter_game", "asset_type": "BUILD", "user_id": USER_ID, "user_secret_key": SECRET_KEY}
        ws.send(json.dumps(payload))
        log_debug("Sent enter_game")
    except Exception as e:
        log_debug(f"safe_send_enter_game err: {e}")

def _extract_issue_id(d: Dict[str, Any]) -> Optional[int]:
    if not isinstance(d, dict):
        return None
    possible = []
    for key in ("issue_id", "issueId", "issue", "id"):
        v = d.get(key)
        if v is not None:
            possible.append(v)
    if isinstance(d.get("data"), dict):
        for key in ("issue_id", "issueId", "issue", "id"):
            v = d["data"].get(key)
            if v is not None:
                possible.append(v)
    for p in possible:
        try:
            return int(p)
        except Exception:
            try:
                return int(str(p))
            except Exception:
                continue
    return None

def on_open(ws):
    _ws["ws"] = ws
    # Không gửi thông báo kết nối lặp lại qua Telegram mỗi lần websocket reconnect.
    safe_send_enter_game(ws)
def on_message(ws, message):
    global issue_id, count_down, killed_room, round_index, ui_state, analysis_start_ts, issue_start_ts, issue_end_ts
    global prediction_locked, predicted_room, last_killed_room, last_killed_room_delayed, last_msg_ts, current_bet
    global win_streak, lose_streak, max_win_streak, max_lose_streak, cumulative_profit, _skip_rounds_remaining, stop_flag

    last_msg_ts = time.time()

    try:
        if isinstance(message, bytes):
            try:
                message = message.decode("utf-8", errors="replace")
            except Exception:
                message = str(message)

        data = None
        try:
            data = json.loads(message)
        except Exception:
            try:
                data = json.loads(message.replace("'", '"'))
            except Exception:
                log_debug(f"on_message non-json: {str(message)[:200]}")
                return

        if isinstance(data, dict) and isinstance(data.get("data"), str):
            try:
                inner = json.loads(data.get("data"))
                merged = dict(data)
                merged.update(inner)
                data = merged
            except Exception:
                pass

        msg_type = str(data.get("msg_type") or data.get("type") or "")
        new_issue = _extract_issue_id(data)

        if msg_type == "notify_enter_game":
            info = data.get("info", {})
            if isinstance(info, dict):
                if info.get("start_time"):
                    st = float(info.get("start_time"))
                    if st > time.time() * 500:
                        st /= 1000.0
                    issue_start_ts = st

                if info.get("end_time"):
                    et = float(info.get("end_time"))
                    if et > time.time() * 500:
                        et /= 1000.0
                    issue_end_ts = et

            if data.get("last_killed_room_id") is not None:
                try:
                    candidate_kill = int(data["last_killed_room_id"])
                except (TypeError, ValueError):
                    candidate_kill = None
                last_killed_room = candidate_kill if candidate_kill in ROOM_ORDER else None

            room_stat = data.get("room_stat", [])
            if isinstance(room_stat, list):
                for rm in room_stat:
                    _process_room_update(rm)

        elif msg_type == "notify_issue_stat" or "issue_stat" in msg_type:
            rooms = data.get("rooms") or []
            if not rooms and isinstance(data.get("data"), dict):
                rooms = data["data"].get("rooms", [])

            for rm in (rooms or []):
                _process_room_update(rm)
                try:
                    rid = int(rm.get("room_id") or rm.get("roomId") or rm.get("id"))
                except Exception:
                    continue

                # _process_room_update đã parse và validate dữ liệu.
                state = room_state.get(rid)
                if state is None:
                    continue
                room_stats[rid]["last_players"] = state.get("players", 0)
                room_stats[rid]["last_bet"] = state.get("bet", 0)

            if new_issue is not None and new_issue != issue_id:
                log_debug(f"New issue: {issue_id} -> {new_issue}")
                issue_id = new_issue
                processed_result_issues.clear()
                history_recorded_issue.clear()

                if data.get("start_time"):
                    st = float(data.get("start_time"))
                    if st > time.time() * 500:
                        st /= 1000.0
                    issue_start_ts = st
                else:
                    issue_start_ts = time.time()

                if data.get("end_time"):
                    et = float(data.get("end_time"))
                    if et > time.time() * 500:
                        et /= 1000.0
                    issue_end_ts = et
                else:
                    issue_end_ts = time.time() + 60

                count_down = 60
                round_index += 1
                killed_room = None
                prediction_locked = False
                predicted_room = None
                ui_state = "ANALYZING"
                analysis_start_ts = time.time()
                analysis_ready_ts = analysis_start_ts + analysis_initial_delay

        elif msg_type == "notify_count_down" or "count_down" in msg_type:
            try:
                new_count = (
                    data.get("count_down")
                    or data.get("countDown")
                    or data.get("count")
                    or data.get("data", {}).get("count_down")
                    or data.get("data", {}).get("countDown")
                )

                if new_count is not None:
                    # Đồng bộ countdown ngay khi server gửi
                    count_down = max(0, int(float(new_count)))
                    last_msg_ts = time.time()

                    # Không để countdown tự trôi giữa các lần update
                    issue_end_ts = None

                    # Khóa dự đoán khi countdown còn 10 giây
                    if count_down <= 10 and not prediction_locked:
                        lock_prediction_if_needed()

            except Exception as e:
                log_debug(f"notify_count_down error: {e}")

        elif msg_type == "notify_result":
            if issue_id in processed_result_issues:
                return

            processed_result_issues.add(issue_id)
            kr = None
            possible_keys = ["killed_room", "killed_room_id", "killedRoom", "killedRoomId", "kill_room"]

            for key in possible_keys:
                if data.get(key) is not None:
                    kr = data.get(key)
                    break

            if kr is None and isinstance(data.get("data"), dict):
                for key in possible_keys:
                    if data["data"].get(key) is not None:
                        kr = data["data"].get(key)
                        break

            if kr is not None:
                try:
                    krid = int(kr)
                except Exception:
                    krid = kr

                if krid not in ROOM_ORDER:
                    log_debug(f"Bỏ qua killed_room_id không hợp lệ: {krid!r}")
                    return

                killed_room = krid
                game_kill_log.append(krid)
                update_killer_history(krid)

                last_killed_room = krid
                last_killed_room_delayed = krid

                for rid in ROOM_ORDER:
                    if rid == krid:
                        room_stats[rid]["kills"] += 1
                        room_stats[rid]["last_kill_round"] = round_index
                    else:
                        room_stats[rid]["survives"] += 1

                balance_before_payout = current_build
                rec = None
                for b in reversed(bet_history):
                    if b.get("issue") == issue_id:
                        rec = b
                        break

                if rec is not None:
                    try:
                        placed_room = int(rec.get("room"))
                        if placed_room != int(killed_room):
                            rec["result"] = "Thắng"
                            current_bet = float(base_bet)
                            last_display_bet = float(base_bet)
                            win_streak += 1
                            lose_streak = 0
                            if win_streak > max_win_streak:
                                max_win_streak = win_streak
                        else:
                            rec["result"] = "Thua"

                            try:
                                previous_bet = float(rec.get("amount", base_bet) or base_bet)

                                factor = float(multiplier or 1.0)
                                if factor <= 0:
                                    factor = 1.0
                                next_bet = previous_bet * factor
                                if next_bet <= 0:
                                    next_bet = float(base_bet)
                                current_bet = next_bet

                                last_display_bet = next_bet
                            except Exception:
                                current_bet = float(base_bet)
                                last_display_bet = float(base_bet)

                            lose_streak += 1
                            win_streak = 0
                            if lose_streak > max_lose_streak:
                                max_lose_streak = lose_streak

                            if pause_after_losses > 0:
                                _skip_rounds_remaining = pause_after_losses
                                rec["pause_after_loss_notice"] = (
                                    f"⏸️ NGHỈ SAU THUA KÍCH HOẠT\n"
                                    f"❌ Ván vừa rồi: THUA.\n"
                                    f"⏸️ Sẽ nghỉ {pause_after_losses} ván theo cấu hình."
                                )

                        threading.Thread(
                            target=_background_update_balance_after_result,
                            args=(rec, balance_before_payout),
                            daemon=True
                        ).start()

                        rec["win_streak"] = win_streak
                        rec["lose_streak"] = lose_streak

                    except Exception as e:
                        log_debug(f"result handle err: {e}")

                    try:
                        if True:
                            _placed_room_snapshot = int(placed_room)
                            _killed_room_snapshot = int(killed_room)
                            _issue_snapshot = issue_id
                            _result_label_snapshot = rec.get("result", "Đang")
                            _win_streak_snapshot = int(win_streak)
                            _lose_streak_snapshot = int(lose_streak)
                            _max_win_streak_snapshot = int(max_win_streak)
                            _max_lose_streak_snapshot = int(max_lose_streak)
                            _tool_start_snapshot = tool_start_time
                            try:
                                _algo_snapshot = (
                                    (bet_selection_by_issue.get(int(_issue_snapshot)) or {}).get("algo")
                                    or rec.get("algo")
                                    or selected_vth_algo
                                    or settings.get("algo")
                                    or "RANDOM"
                                )
                                _algo_snapshot = get_vth_algo_display_name(_algo_snapshot)
                            except Exception:
                                _algo_snapshot = "RANDOM"

                            def _emit_vth_result_telegram():
                                try:
                                    for _wait in range(32):
                                        if rec.get("delta") is not None:
                                            break
                                        time.sleep(0.25)

                                    placed_label = ROOM_NAMES.get(
                                        _placed_room_snapshot,
                                        f"Phòng {_placed_room_snapshot}"
                                    )
                                    killed_label = ROOM_NAMES.get(
                                        _killed_room_snapshot,
                                        f"Phòng {_killed_room_snapshot}"
                                    )
                                    result_word = "WIN" if _result_label_snapshot == "Thắng" else "LOSE"

                                    anti_soi_notice = ""
                                    try:
                                        anti_soi_notice = str(
                                            (bet_selection_by_issue.get(int(_issue_snapshot)) or {}).get("anti_soi_notice") or ""
                                        ).strip()
                                    except Exception:
                                        anti_soi_notice = ""

                                    pause_after_loss_notice = str(
                                        rec.get("pause_after_loss_notice") or ""
                                    ).strip()

                                    capital_limit_notice = str(
                                        rec.get("capital_limit_notice") or ""
                                    ).strip()

                                    try:
                                        start_balance_display = (
                                            float(starting_balance)
                                            if starting_balance is not None else None
                                        )
                                    except Exception:
                                        start_balance_display = None

                                    try:
                                        current_balance_display = (
                                            float(current_build)
                                            if current_build is not None else None
                                        )
                                    except Exception:
                                        current_balance_display = None

                                    def _fmt_balance_display(value):
                                        if value is None:
                                            return "--"
                                        try:
                                            return f"{value:,.2f}"
                                        except Exception:
                                            return str(value)

                                    try:
                                        cumulative_build_pnl = _vth_total_build_pnl()
                                    except Exception:
                                        cumulative_build_pnl = None

                                    pnl_text = (
                                        f"{cumulative_build_pnl:+,.4f}"
                                        if cumulative_build_pnl is not None else "--"
                                    )

                                    try:
                                        elapsed = max(
                                            0,
                                            int(time.time() - _tool_start_snapshot)
                                        )
                                    except Exception:
                                        elapsed = 0
                                    run_h, rem = divmod(elapsed, 3600)
                                    run_m, _ = divmod(rem, 60)

                                    result_message = (
                                        f"👤 User:{USER_ID}\n"
                                        f"🧠 Thuật toán : {_algo_snapshot}\n"
                                        f"📌 Ván: {_issue_snapshot or '-'}\n"
                                        f"🎯 ĐÁNH:{placed_label} (P{_placed_room_snapshot})\n"
                                        f"☠️ PHÒNG VỪA BỊ KILL:{killed_label} (P{_killed_room_snapshot})\n"
                                        f"🏆 WIN:{_win_streak_snapshot}(MAX:{_max_win_streak_snapshot}) | 💀 LOSE:{_lose_streak_snapshot}(MAX:{_max_lose_streak_snapshot})\n"
                                        f"{'✅ WIN' if result_word == 'WIN' else '❌ LOSE'}\n"
                                        f"💸 LÃI VÁN: {float(rec.get('delta') or 0):.4f}\n"
                                        f"📊 LÃI/LỖ: {pnl_text}\n"
                                        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                        f"💳 SỐ DƯ: {_fmt_balance_display(start_balance_display)} -> {_fmt_balance_display(current_balance_display)}\n"
                                        f"⏳ CHẠY BOT: {run_h} Giờ {run_m} Phút"
                                    )

                                    notices = [x for x in (
                                        anti_soi_notice,
                                        pause_after_loss_notice,
                                        capital_limit_notice,
                                    ) if x]
                                    capital_limit_reached = bool(rec.get("capital_limit_reached"))

                                    print(result_message, flush=True)
                                    if notices:

                                        time.sleep(max(FLUSH_INTERVAL + 0.15, 0.55))
                                        print("\n\n".join(notices), flush=True)

                                    if capital_limit_reached:
                                        time.sleep(max(1.20, FLUSH_INTERVAL + 0.70))
                                        _stop_for_capital_limit()
                                except Exception as exc:
                                    log_debug(f"Telegram result display error: {exc}")

                            threading.Thread(
                                target=_emit_vth_result_telegram,
                                daemon=True
                            ).start()
                    except Exception as exc:
                        log_debug(f"Telegram result display setup error: {exc}")

            ui_state = "RESULT"

    except Exception as e:
        log_debug(f"on_message err: {e}")

def _stop_for_capital_limit(message: str = None):
    if stop_flag:
        return
    _shutdown_vth_runtime(message)


def _check_capital_limit_now(rec: Optional[dict] = None) -> bool:
    global capital_stop_pending

    if (
        stop_when_profit_reached
        and profit_target is not None
        and isinstance(current_build, (int, float))
        and current_build >= profit_target
    ):
        notice = (
            f"🎯 ĐẠT MỤC TIÊU LÃI\n"
            f"💳 Số dư hiện tại: {current_build:,.2f} BUILD\n"
            f"🏁 Mục tiêu: {profit_target:,.2f} BUILD\n"
            f"✅ ĐỪNG VTH."
        )
        if isinstance(rec, dict):

            rec["capital_limit_notice"] = notice
            rec["capital_limit_reached"] = True
            capital_stop_pending = True
        else:
            _stop_for_capital_limit(notice)
        return True

    if (
        stop_when_loss_reached
        and stop_loss_target is not None
        and isinstance(current_build, (int, float))
        and current_build <= stop_loss_target
    ):
        notice = (
            f"🛑 ĐẠT MỨC CẮT LỖ\n"
            f"💀 Số dư hiện tại: {current_build:,.2f} BUILD\n"
            f"📉 Mức cắt lỗ: {stop_loss_target:,.2f} BUILD\n"
            f"🛑 Tool đã dừng."
        )
        if isinstance(rec, dict):

            rec["capital_limit_notice"] = notice
            rec["capital_limit_reached"] = True
            capital_stop_pending = True
        else:
            _stop_for_capital_limit(notice)
        return True

    return False

def _background_update_balance_after_result(rec: dict, balance_before: Optional[float]):

    global cumulative_profit
    global current_build, current_usdt, current_world
    global stop_flag

    try:

        if rec and rec.get("balance_before_bet") is not None:
            balance_before = rec.get("balance_before_bet")
        elif balance_before is None and rec:
            balance_before = rec.get("balance_before_bet")

        result_status = str(rec.get("result", "") if rec else "").strip().lower()
        is_win = result_status in ("thắng", "win")
        is_lose = result_status in ("thua", "lose")

        before = float(balance_before) if isinstance(balance_before, (int, float)) else None
        accepted_balance = None
        consecutive_same = 0
        last_value = None


        time.sleep(0.20)

        for _ in range(24):
            new_balance, usdt, world = fetch_balances_3games(
                retries=0,
                timeout=2
            )

            if isinstance(new_balance, (int, float)):
                new_balance = float(new_balance)
                current_usdt = usdt
                current_world = world

                if before is None:
                    accepted_balance = new_balance
                    break

                diff = new_balance - before

                if is_win:
                    valid_direction = diff > 1e-9
                elif is_lose:
                    valid_direction = diff < -1e-9
                else:
                    valid_direction = abs(diff) <= 1e-9

                if valid_direction:

                    if last_value is not None and abs(new_balance - last_value) <= 1e-9:
                        consecutive_same += 1
                    else:
                        consecutive_same = 1
                    last_value = new_balance

                    if consecutive_same >= 2:
                        accepted_balance = new_balance
                        break
                else:
                    consecutive_same = 0
                    last_value = new_balance

            time.sleep(0.30)

        if accepted_balance is None:
            if is_win:
                if before is not None:
                    current_build = before
                rec["delta"] = None
                log_debug(
                    f"WIN issue={rec.get('issue')} chưa thấy payout; không chốt PNL từ số dư tạm thời."
                )
                _check_capital_limit_now(rec)
                return
            accepted_balance = last_value

        if isinstance(accepted_balance, (int, float)):
            current_build = float(accepted_balance)

            if starting_balance is not None:
                cumulative_profit = _vth_total_build_pnl()

            capital_reached = _check_capital_limit_now(rec)

            if before is not None:
                rec["delta"] = float(accepted_balance) - before
            else:
                rec["delta"] = None

            if capital_reached:
                return

    except Exception as e:
        log_debug(f"background balance update error: {e}")

def update_killer_history(killed_room_id):
    if killed_room_id in room_state:
        killer_history.append({'players': room_state[killed_room_id].get('players', 0), 'bet': room_state[killed_room_id].get('bet', 0)})
def _process_room_update(room_data: dict):
    if not isinstance(room_data, dict):
        return
    try:
        rid = int(room_data.get("room_id") or room_data.get("roomId") or room_data.get("id"))
        if rid not in ROOM_ORDER:
            return

        players = int(_parse_number(
            room_data.get("user_cnt")
            if room_data.get("user_cnt") is not None
            else room_data.get("userCount", 0)
        ) or 0)
        bet = _parse_number(
            room_data.get("total_bet_amount")
            if room_data.get("total_bet_amount") is not None
            else (room_data.get("totalBet") if room_data.get("totalBet") is not None else room_data.get("bet", 0))
        ) or 0.0

        room_state[rid] = {"players": players, "bet": float(bet)}
        if rid not in room_stats:
            room_stats[rid] = {"kills": 0, "survives": 0, "last_kill_round": None, "last_players": 0, "last_bet": 0}
        room_stats[rid]["last_players"] = players
        room_stats[rid]["last_bet"] = float(bet)
    except (ValueError, TypeError):
        pass

def on_close(ws, code, reason):
    log_debug(f"WS closed: {code} {reason}")
def on_error(ws, err):
    log_debug(f"WS error: {err}")
def start_ws():
    backoff = 1.0
    while not stop_flag:
        try:
            ws_app = websocket.WebSocketApp(WS_URL, on_open=on_open, on_message=on_message, on_close=on_close, on_error=on_error)
            _ws["ws"] = ws_app
            ws_app.run_forever(ping_interval=15, ping_timeout=6)
        except Exception as e:
            log_debug(f"start_ws exception: {e}")
        t = min(backoff + random.random() * 0.8, 30)
        log_debug(f"Reconnect WS after {t}s")
        time.sleep(t)
        backoff = min(backoff * 1.8, 30)

class BalancePoller(threading.Thread):
    def __init__(self, uid: Optional[int], secret: Optional[str], poll_seconds: int = 2, on_balance=None, on_error=None, on_status=None):
        super().__init__(daemon=True)
        self.uid = uid
        self.secret = secret
        self.poll_seconds = max(1, int(poll_seconds))
        self._running = True
        self._last_balance_local: Optional[float] = None
        self.on_balance = on_balance
        self.on_error = on_error
        self.on_status = on_status

    def stop(self):
        self._running = False

    def run(self):
        if self.on_status:
            self.on_status("Kết nối...")
        while self._running and not stop_flag:
            try:
                build, world, usdt = fetch_balances_3games(params={"userId": str(self.uid)} if self.uid else None, uid=self.uid, secret=self.secret)
                if build is None:
                    raise RuntimeError("Không đọc được balance từ response")
                delta = 0.0 if self._last_balance_local is None else (build - self._last_balance_local)
                first_time = (self._last_balance_local is None)
                if first_time or abs(delta) > 0:
                    self._last_balance_local = build
                    if self.on_balance:
                        self.on_balance(float(build), float(delta), {"ts": human_ts()})
                    if self.on_status:
                        self.on_status("Đang theo dõi")
                else:
                    if self.on_status:
                        self.on_status("Đang theo dõi (không đổi)")
            except Exception as e:
                if self.on_error:
                    self.on_error(str(e))
                if self.on_status:
                    self.on_status("Lỗi kết nối vui lòng thử lại...")
            for _ in range(max(1, int(self.poll_seconds * 5))):
                if not self._running or stop_flag:
                    break
                time.sleep(0.2)
        if self.on_status:
            self.on_status("Đã dừng")

def monitor_loop():
    global last_balance_fetch_ts, last_msg_ts, stop_flag

    while not stop_flag:
        # Theo dõi chuyển trạng thái theo lịch để thông báo lúc nghỉ/chạy lại.
        _check_daily_pause_window(_vth_daily_pause_window, "VTH")
        now = time.time()

        if now - last_balance_fetch_ts >= BALANCE_POLL_INTERVAL:
            last_balance_fetch_ts = now
            try:
                fetch_balances_3games(
                    params={"userId": str(USER_ID)} if USER_ID else None
                )
            except Exception as e:
                log_debug(f"monitor fetch err: {e}")

        if now - last_msg_ts > 12:
            log_debug("No ws msg >12s, send enter_game")
            try:
                safe_send_enter_game(_ws.get("ws"))
            except Exception as e:
                log_debug(f"monitor send err: {e}")

        if now - last_msg_ts > 45:
            log_debug("No ws msg >45s, force reconnect")
            try:
                wsobj = _ws.get("ws")
                if wsobj:
                    try:
                        wsobj.close()
                    except Exception:
                        pass
            except Exception:
                pass

        try:
            # Luôn dùng countdown / end_time từ server
            remaining = _vth_get_remaining_seconds()

            if (
                analysis_start_ts
                and (time.time() - analysis_start_ts >= analysis_duration)
                and not prediction_locked
                and remaining is not None
                and remaining <= 10
            ):
                lock_prediction_if_needed()

        except Exception as e:
            log_debug(f"monitor loop err: {e}")

        time.sleep(1)

def build_config_header():
    return Text("")
def build_step_indicator(current_step: int, total_steps: int):
    return ""
def prompt_settings() -> bool:
    global base_bet, multiplier, run_mode
    global selected_vth_algo
    global bet_rounds_before_skip, current_bet
    global pause_after_losses
    global profit_target, stop_when_profit_reached
    global stop_loss_target, stop_when_loss_reached
    global _vth_daily_pause_window

    # ================== MENU THUẬT TOÁN VTH ==================

    key_valid, key_reason, is_vip = check_activation_valid(
        exit_on_expired=False
    )

    if not key_valid:
        print(
            f"🔒 {key_reason}. Vui lòng nhập lại Key FREE hoặc Key VIP.",
            flush=True,
        )
        return False

    modes = list(SELECTION_MODES.items())
    vip_keys = set(VIP_AI_LIST)

    if is_vip:
        permission_line = "🔑 Quyền hiện tại: VIP — Logic 1-41"
    else:
        permission_line = "🔑 Quyền hiện tại: FREE — Dùng được Logic 1-18"

    lines = [
        "🧠 CHỌN LOGIC VUA THOÁT HIỂM",
        permission_line,
        "",
    ]

    for i, (key, _label) in enumerate(modes, 1):
        display_name = get_vth_algo_display_name(key, is_vip)

        if key in vip_keys and not is_vip:
            lines.append(
                f"{i}. {display_name} (🔒 CHỈ KEY VIP)"
            )
        else:
            lines.append(f"{i}. {display_name}")

    lines.extend([
        "",
        f">> Chọn số thứ tự (1-{len(modes)}):",
    ])

    print("\n".join(lines), flush=True)

    while True:
        raw = input("").strip()

        key_valid, key_reason, current_is_vip = check_activation_valid(
            exit_on_expired=False
        )

        if not key_valid:
            print(
                f"🔒 {key_reason}. Vui lòng nhập lại Key FREE hoặc Key VIP.",
                flush=True,
            )
            return False

        current_key_type = "vip" if current_is_vip else "free"
        current_available = get_available_ai_list(current_key_type)

        try:
            choice = int(raw)
        except ValueError:
            print(
                f"❌ Vui lòng nhập số từ 1 đến {len(modes)}.",
                flush=True,
            )
            continue

        if not 1 <= choice <= len(modes):
            print(
                f"❌ Vui lòng nhập từ 1 đến {len(modes)}.",
                flush=True,
            )
            continue

        selected_key = modes[choice - 1][0]

        if selected_key not in current_available:
            print("❌ Logic này chỉ dành cho Key VIP!", flush=True)
            continue

        is_vip = current_is_vip
        break

    selected_vth_algo = selected_key
    settings["algo"] = selected_key

    selected_name = get_vth_algo_display_name(
        selected_key,
        is_vip,
    )

    print(
        f"✅ Đã chọn thuật toán: {selected_name}",
        flush=True,
    )

    if selected_key == "ALL":
        pt_str = Prompt.ask("🎯 Mục tiêu lãi (0 = tắt)").strip()

        if pt_str and pt_str != "0":
            try:
                profit_target = float(pt_str)
                stop_when_profit_reached = profit_target > 0

                print(
                    f"✅ Đã đặt mục tiêu lãi: dừng khi số dư đạt "
                    f"{profit_target:,.2f} BUILD",
                    flush=True,
                )
            except ValueError:
                profit_target = None
                stop_when_profit_reached = False
                print(
                    "⚠️ Mục tiêu lãi không hợp lệ, đã tắt.",
                    flush=True,
                )
        else:
            profit_target = None
            stop_when_profit_reached = False

        stop_loss_target = None
        stop_when_loss_reached = False
        _vth_daily_pause_window = _prompt_daily_pause_window("VTH", "2")
        _reset_daily_pause_state("VTH")
        base_bet = 0.0
        multiplier = 1.0
        current_bet = 0.0
        bet_rounds_before_skip = 0
        pause_after_losses = 0
        run_mode = "AUTO"

        return True

    # ================== QUẢN LÝ VỐN ==================
    print("\n💰 QUẢN LÝ VỐN", flush=True)

    while True:
        try:
            base_bet = FloatPrompt.ask(
                "1. Cược gốc (BUILD)"
            )

            if base_bet >= 1:
                break

            print(
                "❌ Cược gốc tối thiểu là 1.0 BUILD.",
                flush=True,
            )

        except Exception:
            print(
                "❌ Vui lòng nhập số hợp lệ.",
                flush=True,
            )

    while True:
        try:
            multiplier = FloatPrompt.ask(
                "2. Hệ số nhân sau khi thua",
                default=10,
            )

            if multiplier > 0:
                break

            print(
                "❌ Hệ số phải lớn hơn 0.",
                flush=True,
            )

        except Exception:
            print(
                "❌ Vui lòng nhập số hợp lệ.",
                flush=True,
            )

    current_bet = base_bet

    # ================== QUẢN LÝ RỦI RO ==================
    print("\n🛡️ QUẢN LÝ RỦI RO", flush=True)

    bet_rounds_before_skip = IntPrompt.ask(
        "3. Chống soi: nghỉ 1 ván sau bao nhiêu ván cược? "
        "(0 = Tắt)",
        default=0,
    )

    pause_after_losses = IntPrompt.ask(
        "4. Nghỉ sau khi thua: nghỉ bao nhiêu ván? "
        "(0 = Tắt)",
        default=0,
    )

    # ================== ĐẶT MỤC TIÊU ==================
    print("\n🎯 ĐẶT MỤC TIÊU", flush=True)

    pt_str = Prompt.ask(
        "5. Mục tiêu lãi: nhập số BUILD muốn đạt (0 = Tắt)"
    ).strip()

    if pt_str and pt_str != "0":
        try:
            profit_target = float(pt_str)
            stop_when_profit_reached = profit_target > 0
        except ValueError:
            profit_target = None
            stop_when_profit_reached = False

            print(
                "⚠️ Mục tiêu lãi không hợp lệ, đã tắt.",
                flush=True,
            )
    else:
        profit_target = None
        stop_when_profit_reached = False

    sl_str = Prompt.ask(
        "6. Cắt lỗ: số BUILD tối thiểu còn lại để dừng "
        "(0 = Tắt)"
    ).strip()

    if sl_str and sl_str != "0":
        try:
            stop_loss_target = float(sl_str)
            stop_when_loss_reached = stop_loss_target > 0
        except ValueError:
            stop_loss_target = None
            stop_when_loss_reached = False

            print(
                "⚠️ Cắt lỗ không hợp lệ, đã tắt.",
                flush=True,
            )
    else:
        stop_loss_target = None
        stop_when_loss_reached = False

    _vth_daily_pause_window = _prompt_daily_pause_window("VTH", "7")
    _reset_daily_pause_state("VTH")

    run_mode = "AUTO"

    return True

def load_accounts() -> list:
    acc_file = Path(ACCOUNTS_FILE)
    if not acc_file.exists():
        return []
    try:
        return json.loads(acc_file.read_text())
    except (json.JSONDecodeError, IOError):
        return []

def save_accounts(accounts: list):
    acc_file = Path(ACCOUNTS_FILE)
    with acc_file.open("w", encoding="utf-8") as f:
        json.dump(accounts, f, indent=2)

def add_new_account(accounts: list) -> bool:
    while True:
        console.clear()

        print(
            "📌 DÁN LINK TÀI KHOẢN VUA THOÁT HIỂM\n\n"
            "Gửi link game có dạng chứa userId và secretKey.",
            flush=True,
        )

        link = Prompt.ask(
            "👉 Dán link game vào đây (Nhập 0 để huỷ)"
        ).strip()

        if link == "0":
            print("🛑 Đã huỷ.", flush=True)
            return False

        if not link:
            print(
                "⚠️ Vui lòng dán link hoặc nhập 0 để huỷ.",
                flush=True,
            )
            continue

        try:
            params = parse_qs(urlparse(link).query)

            uid_raw = params.get("userId", [""])[0]
            skey = params.get("secretKey", [""])[0].strip()

            if not uid_raw or not skey:
                raise ValueError

            uid = int(uid_raw)

            accounts.append({
                "userId": uid,
                "secretKey": skey,
            })

            save_accounts(accounts)

            print(
                f"✅ TÀI KHOẢN {uid} ĐÃ ĐƯỢC THÊM THÀNH CÔNG!\n"
                "🧠 Bây giờ hãy chọn thuật toán để cài cấu hình.",
                flush=True,
            )

            time.sleep(0.5)
            return True

        except (KeyError, IndexError):
            print(
                "❌ Link game không hợp lệ!",
                flush=True,
            )

        except ValueError:
            print(
                "❌ UserID hoặc SecretKey không hợp lệ!",
                flush=True,
            )

        except Exception as e:
            print(
                f"❌ Lỗi: {e}",
                flush=True,
            )

        time.sleep(0.5)

def start_threads():
    threading.Thread(target=start_ws, daemon=True).start()
    threading.Thread(target=monitor_loop, daemon=True).start()

def _send_vth_start_config(balance=None):
    """Gửi một lần cấu hình VTH vừa cài kèm số dư BUILD thực tế."""
    try:
        algo_name = get_vth_algo_display_name(selected_vth_algo, is_vip_activated())
    except Exception:
        algo_name = get_vth_algo_display_name(selected_vth_algo, False)

    def _fmt_num(value):
        try:
            return f"{float(value):,.2f}"
        except Exception:
            return "0.00"

    balance_text = _fmt_num(balance) if balance is not None else "Không lấy được"
    config_lines = [
        "📜 TỔNG KẾT CẤU HÌNH VTH",
        "",
        f"👤 Tài khoản:{USER_ID}",
        f"💳 Số dư tài khoản: {balance_text} BUILD",
        f"🧠 Thuật toán: {algo_name}",
        ("💰 Cược mỗi ván: ALL IN — toàn bộ số dư BUILD" if str(selected_vth_algo).upper() == "ALL" else f"💰 Cược mỗi ván: {_fmt_num(base_bet)} BUILD"),
        f"📈 Hệ số sau thua: x{_fmt_num(multiplier)}",
        f"🛡️ Chống soi: {int(bet_rounds_before_skip or 0)} ván rồi nghỉ 1 ván" if int(bet_rounds_before_skip or 0) > 0 else "🛡️ Chống soi: Tắt",
        f"⏸️ Nghỉ sau thua: {int(pause_after_losses or 0)} ván" if int(pause_after_losses or 0) > 0 else "⏸️ Nghỉ sau thua: Tắt",
        f"🎯 Mục tiêu lãi: Dừng khi số dư đạt {_fmt_num(profit_target)} BUILD" if stop_when_profit_reached and profit_target else "🎯 Mục tiêu lãi: Tắt",
        f"🛑 Cắt lỗ: Dừng khi số dư còn {_fmt_num(stop_loss_target)} BUILD" if stop_when_loss_reached and stop_loss_target else "🛑 Cắt lỗ: Tắt",
        f"⏰ Khung giờ chạy: {_format_daily_pause_window(_vth_daily_pause_window)}",
    ]
    print("\n".join(config_lines), flush=True)
    print("🚀 ĐANG KHỞI ĐỘNG VTH...", flush=True)


def start_game_flow():
    global stop_flag, IS_VIP_USER, active_vth_poller, capital_stop_pending
    global starting_balance, cumulative_profit, last_balance_val
    if USER_ID is None or SECRET_KEY is None:
        print("❌ Chưa chọn tài khoản.")
        return
    _, _, IS_VIP_USER = check_activation_valid()
    capital_stop_pending = False

    global _sequential_bet_index, killed_room, round_index
    global predicted_room, last_killed_room, last_killed_room_delayed
    global prediction_locked, issue_id, count_down
    global win_streak, lose_streak, max_win_streak, max_lose_streak
    global _rounds_placed_since_skip, skip_next_round_flag, _skip_rounds_remaining
    global analysis_start_ts, analysis_ready_ts, issue_start_ts, issue_end_ts
    _sequential_bet_index = 0
    killer_history.clear()
    game_kill_log.clear()
    bet_history.clear()
    bet_sent_for_issue.clear()
    processed_result_issues.clear()
    history_recorded_issue.clear()
    bet_selection_by_issue.clear()
    killed_room = None
    round_index = 0
    predicted_room = None
    last_killed_room = None
    last_killed_room_delayed = None
    prediction_locked = False
    issue_id = None
    count_down = None
    win_streak = lose_streak = 0
    max_win_streak = max_lose_streak = 0
    _rounds_placed_since_skip = 0
    skip_next_round_flag = False
    _skip_rounds_remaining = 0
    analysis_start_ts = None
    analysis_ready_ts = None
    issue_start_ts = None
    issue_end_ts = None

    starting_balance = None
    cumulative_profit = 0.0
    last_balance_val = None

    try:
        initial_balance, _, _ = fetch_balances_3games(
            retries=2,
            timeout=5,
            params={"userId": str(USER_ID)},
            uid=USER_ID,
            secret=SECRET_KEY,
        )
        if initial_balance is not None:
            initial_balance = float(initial_balance)
            globals()["current_build"] = initial_balance

            starting_balance = initial_balance
            cumulative_profit = 0.0
    except Exception as e:
        initial_balance = None
        log_debug(f"Lỗi lấy số dư BUILD lúc khởi động VTH: {e}")

    try:
        globals()["current_bet"] = float(base_bet)
        globals()["last_display_bet"] = float(base_bet)
    except Exception:
        pass

    _send_vth_start_config(initial_balance)

    start_threads()
    poller = BalancePoller(USER_ID, SECRET_KEY, poll_seconds=max(1, int(BALANCE_POLL_INTERVAL)), on_balance=None, on_error=None, on_status=None)
    active_vth_poller = poller
    poller.start()
    last_status_issue = None
    try:
        while not stop_flag:
            try:

                current_issue = issue_id
                if current_issue is not None and current_issue != last_status_issue:
                    b = current_build if isinstance(current_build, (int, float)) else 0.0
                    bet = last_display_bet if isinstance(last_display_bet, (int, float)) else current_bet
                    last_status_issue = current_issue
            except Exception as e:
                log_debug(f"telegram text loop: {e}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        _shutdown_vth_runtime("Đã dừng tool.")
        try: poller.stop()
        except Exception: pass
        if active_vth_poller is poller:
            active_vth_poller = None

def main_vth():
    start_key_expiry_monitor()

    if not check_activation_valid()[0]:
        console.print("[bold red]❌ Key của bạn không còn hiệu lực.[/bold red]")
        time.sleep(2)
        return

    global stop_flag, USER_ID, SECRET_KEY
    stop_flag = False

    console.clear()
    save_accounts([])
    if not add_new_account([]):
        return

    accounts = load_accounts()
    if not accounts:
        return

    USER_ID = accounts[0]["userId"]
    SECRET_KEY = accounts[0]["secretKey"]

    console.clear()
    if not prompt_settings():
        return

    console.clear()
    start_game_flow()

# ================== TOOL 2: LOTTO ==================

ACCOUNTS_FILE = str(BOT_DATA_DIR / "accounts.json")
CONFIG_FILE = "wh_config.json"
API_URL = "https://api.winhash.net/lucky_game/hourly_issue_list"
HOME_URL = "https://api.winhash.net/lucky_game/home"
API_URL_GET_BALANCE = "https://wallet.3games.io/api/wallet/user_asset"
HOME_POLL_INTERVAL = 3
UI_REFRESH = 6
DECIMALS = 4

ICON_LOTTO = {"nho": "🔵", "lon": "🔴", "hoa": "⚖️", 1:"1", 2:"2", 3:"3", 4:"4", 5:"5", 6:"6"}
NUMBER_COLORS = {1:"bright_blue", 2:"bright_cyan", 3:"bright_green", 4:"yellow", 5:"bright_magenta", 6:"bright_red"}

_num_re_local = re.compile(r"-?\d+[\d,]*\.?\d*")

# ================== CLASS PHÂN TÍCH LOTTO ==================

class ThreeNumberAnalyzer:
    def __init__(self):
        self.position_stats = {0: defaultdict(lambda: Counter()), 1: defaultdict(lambda: Counter()), 2: defaultdict(lambda: Counter())}
        self.position_names = {0: "ĐẦU", 1: "GIỮA", 2: "CUỐI"}
        self.pattern_history = []
        self.last_seen_index = {0: {}, 1: {}, 2: {}}

    def _build_position_stats(self, history: List[Dict], decay: float = 0.06):
        self.pattern_history = []
        for p in [0,1,2]:
            self.position_stats[p] = defaultdict(lambda: Counter())
            self.last_seen_index[p] = {}
        n = max(1, len(history) - 1)
        for i in range(len(history) - 1):
            current = history[i].get('lucky_codes', [])
            next_codes = history[i + 1].get('lucky_codes', [])
            if len(current) == 3 and len(next_codes) == 3:
                distance = (n - 1) - i
                weight = float(math.exp(-decay * max(0, distance)))
                for pos in [0, 1, 2]:
                    trigger_num = current[pos]
                    result_num = next_codes[pos]
                    self.position_stats[pos][trigger_num][result_num] += weight
                self.pattern_history.append({'current': tuple(current), 'next': tuple(next_codes), 'current_sum': sum(current), 'next_sum': sum(next_codes)})
        for pos in [0,1,2]:
            seen = {}
            for dist, rec in enumerate(reversed(history)):
                codes = rec.get('lucky_codes', [])
                if not isinstance(codes, list) or len(codes) != 3:
                    continue
                num = codes[pos]
                if num not in seen:
                    seen[num] = dist
            self.last_seen_index[pos] = seen

    def _predict_position(self, position: int, current_num: int, context: Optional[Tuple[int,int,int]] = None) -> Dict:
        stats = self.position_stats[position][current_num]
        total = float(sum(stats.values()))
        if total == 0:
            return {i: 1/6 for i in range(1, 7)}
        probs = {i: stats.get(i, 0) / total for i in range(1, 7)}
        alpha = 0.5
        last_seen = self.last_seen_index.get(position, {})
        recency_boost = {}
        for i in range(1,7):
            dist = last_seen.get(i)
            if dist is None:
                recency_boost[i] = 1.0
            else:
                recency_boost[i] = 1.0 + max(0.0, (6.0 - float(dist)) / 12.0)
        smoothed = {}
        denom = total + 6 * alpha
        for i in range(1,7):
            base = (stats.get(i, 0) + alpha) / denom
            smoothed[i] = base * recency_boost.get(i, 1.0)
        s_sum = sum(smoothed.values())
        if s_sum <= 0:
            return {i: 1/6 for i in range(1, 7)}
        base_probs = {i: smoothed[i] / s_sum for i in range(1,7)}
        if context and self.pattern_history:
            boost = defaultdict(float)
            total_ctx = 0.0
            for idx, ph in enumerate(reversed(self.pattern_history)):
                cur = ph.get('current')
                nxt = ph.get('next')
                if not cur or not nxt: continue
                match = 0
                for p in range(3):
                    if p == position: continue
                    if context[p] == cur[p]:
                        match += 1
                if match >= 1:
                    recency_w = math.exp(-0.06 * idx)
                    observed = nxt[position]
                    boost[observed] += recency_w * (1.0 + match * 0.25)
                    total_ctx += recency_w
            if total_ctx > 0:
                ctx_probs = {i: (boost.get(i, 0.0) / total_ctx) for i in range(1,7)}
                mix = 0.25
                blended = {i: base_probs.get(i,0.0) * (1.0 - mix) + ctx_probs.get(i,0.0) * mix for i in range(1,7)}
                s = sum(blended.values())
                if s > 0:
                    return {i: blended[i]/s for i in range(1,7)}
        return base_probs

    def analyze(self, history: List[Dict]) -> Dict:
        if len(history) < 2:
            return {"last_three": [], "position_probs": [{}, {}, {}], "predicted_numbers": [], "predicted_sum": 0, "prediction": None, "confidence": 0.0, "bet_action": "small"}
        self._build_position_stats(history)
        last_record = history[-1]
        last_three = last_record.get('lucky_codes', [])
        if len(last_three) != 3:
            return {"last_three": last_three, "position_probs": [{}, {}, {}], "predicted_numbers": [], "predicted_sum": 0, "prediction": None, "confidence": 0.0, "bet_action": "small"}
        position_probs = []
        predicted_numbers = []
        total_confidence = 0.0
        per_number_details = []
        for pos in [0, 1, 2]:
            current_num = last_three[pos]
            probs = self._predict_position(pos, current_num, context=last_three)
            details = {}
            counts = self.position_stats[pos]
            sample_size = float(sum(counts.get(current_num, Counter()).values())) if counts.get(current_num) else 0.0
            for num in range(1,7):
                prob = probs.get(num, 0.0)
                dist = self.last_seen_index.get(pos, {}).get(num)
                recency_score = 0.0 if dist is None else max(0.0, (12.0 - float(dist)) / 12.0)
                freq = 0
                for k,v in self.position_stats[pos].items():
                    freq += v.get(num, 0)
                freq_score = float(freq) / max(1.0, sum(sum(c.values()) for c in self.position_stats[pos].values()))
                score = prob * 0.6 + recency_score * 0.25 + freq_score * 0.15
                details[num] = {'prob': prob, 'recency_score': recency_score, 'freq': freq, 'freq_score': freq_score, 'score': score}
            position_probs.append(probs)
            best_num = max(details.items(), key=lambda x: x[1]['score'])[0]
            predicted_numbers.append(best_num)
            total_confidence += details[best_num]['prob']
            per_number_details.append({'position': pos, 'current': current_num, 'sample_size': sample_size, 'details': details})
        predicted_sum = sum(predicted_numbers)
        avg_confidence = total_confidence / 3
        if 3 <= predicted_sum <= 9:
            bet_action = "nho"
            prediction = "Nho"
        elif 10 <= predicted_sum <= 11:
            bet_action = "hoa"
            prediction = "Hoa"
        elif 12 <= predicted_sum <= 18:
            bet_action = "lon"
            prediction = "Lon"
        else:
            bet_action = "nho"
            prediction = "Nho"
        sample_sizes = []
        for pos in [0, 1, 2]:
            current_num = last_three[pos]
            stats = self.position_stats[pos][current_num]
            sample_sizes.append(sum(stats.values()))
        avg_sample = sum(sample_sizes) / 3 if sample_sizes else 0
        try:
            ent = 0.0
            for probs in position_probs:
                s = 0.0
                for p in probs.values():
                    if p > 0: s -= p * math.log(p)
                ent += s
            avg_entropy = ent / 3.0 if position_probs else 0.0
            entropy_penalty = avg_entropy / max(1.0, math.log(6))
            avg_confidence *= max(0.25, 1.0 - 0.5 * entropy_penalty)
        except Exception:
            pass
        if avg_sample < 3:
            avg_confidence *= 0.55
        elif avg_sample < 6:
            avg_confidence *= 0.8
        return {"last_three": last_three, "position_probs": position_probs, "predicted_numbers": predicted_numbers, "predicted_sum": predicted_sum, "prediction": prediction, "confidence": avg_confidence, "sample_sizes": sample_sizes, "bet_action": bet_action, "per_position_details": per_number_details}

class AccountManager:
    def __init__(self, path: Optional[str] = None):
        # Resolve path at runtime: Telegram worker dùng file riêng theo chat_id.
        self.path = path or ACCOUNTS_FILE
        self.accounts = self._load()

    def _load(self) -> List[Dict]:
        if not os.path.exists(self.path):
            return []
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.accounts, f, ensure_ascii=False, indent=2)

    def parse_link_to_account(self, link: str) -> Optional[Dict]:
        try:
            p = urlparse(link)
            qs = parse_qs(p.query)

            user_id = qs.get("userId", [None])[0]
            secret = qs.get("secretKey", [None])[0]

            if not user_id or not secret:
                return None

            return {
                "user-id": user_id,
                "user-secret-key": secret,
                "country-code": "vn",
                "user-agent": "Mozilla/5.0",
                "asset": "BUILD",
            }

        except Exception:
            return None

    def add_account(self) -> Dict:
        while True:
            raw = Prompt.ask("👉 Dán link game")

            if raw.strip() == "":
                raise KeyboardInterrupt

            # Chỉ chấp nhận link có userId và secretKey
            if (
                raw.startswith("http")
                and "userId=" in raw
                and "secretKey=" in raw
            ):
                acc = self.parse_link_to_account(raw)

                if acc:
                    self.accounts.clear()
                    self.accounts.append(acc)
                    self.save()

                    if True:
                        print(f"✅ Đăng nhập thành công\n👤 userId={acc['user-id']}", flush=True)
                    else:
                        console.print(
                            Panel(
                                f"✅ Đăng nhập thành công\nUser ID: {acc['user-id']}",
                                style="bold green",
                                border_style="green",
                            )
                        )

                    return acc

            if True:
                print("❌ Link không hợp lệ! Hãy gửi lại link game có userId và secretKey.", flush=True)
            else:
                console.print("[bold red]❌ Link không hợp lệ![/bold red]")

    def remove_account(self, index: int) -> bool:
        if 0 <= index < len(self.accounts):
            self.accounts.pop(index)
            self.save()
            return True
        return False

class MartingaleBetting:

    def __init__(self, initial_bet: float, multiplier: float, asset: str):
        self.initial_bet = float(initial_bet)
        self.multiplier = float(multiplier)
        self.asset = asset
        self.current_bet = float(initial_bet)
        self.total_wagered = 0.0
        self.total_won = 0.0
        self.win_count = 0
        self.loss_count = 0
        self.current_win_streak = 0
        self.current_loss_streak = 0
        self.max_win_streak = 0
        self.max_loss_streak = 0
        self.net_profit = 0.0

    def get_bet_amount(self) -> float:
        return float(self.current_bet)

    def on_win(self, payout: float):
        try:
            self.total_won += float(payout)
        except Exception:
            pass
        self.win_count += 1
        self.current_bet = float(self.initial_bet)
        self.current_win_streak += 1
        self.current_loss_streak = 0
        if self.current_win_streak > self.max_win_streak:
            self.max_win_streak = self.current_win_streak

    def on_loss(self):
        self.loss_count += 1
        self.current_bet = float(self.current_bet) * float(self.multiplier)
        self.current_loss_streak += 1
        self.current_win_streak = 0
        if self.current_loss_streak > self.max_loss_streak:
            self.max_loss_streak = self.current_loss_streak

    def record_wager(self, amount: float):
        try:
            self.total_wagered += float(amount)
        except Exception:
            pass

    def get_stats(self) -> Dict:
        net_profit = self.total_won - self.total_wagered
        total_bets = self.win_count + self.loss_count
        return {"total_bets": total_bets, "wins": self.win_count, "losses": self.loss_count, "total_wagered": self.total_wagered, "total_won": self.total_won, "net_profit": net_profit, "current_bet": self.current_bet, "asset": self.asset, "current_win_streak": self.current_win_streak, "current_loss_streak": self.current_loss_streak, "max_win_streak": self.max_win_streak, "max_loss_streak": self.max_loss_streak}

def build_headers(config: Dict[str, str]) -> Dict[str, str]:
    return {'accept': '*/*', 'accept-language': 'en-US,en;q=0.9', 'country-code': config.get('country-code', 'vn'), 'origin': 'https://winhash.io', 'referer': 'https://winhash.io/', 'user-agent': config.get('user-agent', 'Mozilla/5.0'), 'user-id': config['user-id'], 'user-login': 'login_v2', 'user-secret-key': config['user-secret-key'], 'xb-language': config.get('xb-language', 'en-US')}

def fetch_issue_list(session: requests.Session, headers: Dict[str, str], ts: int) -> List[Dict]:
    params = {'ts': str(ts)}
    try:
        resp = session.get(API_URL, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get('code') is not None and data.get('code') != 0:
            return []
        return data.get('data', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    except Exception:
        return []

def fetch_home(session: requests.Session, headers: Dict[str, str], asset: str) -> Optional[Dict]:
    params = {'game_id': '1', 'asset': asset}
    try:
        resp = session.get(HOME_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get('code') is not None and data.get('code') != 0:
            return None
        return data.get('data') if isinstance(data, dict) else None
    except Exception:
        return None

def get_balance(s: requests.Session, headers: Dict[str, str], uid: str, key: str) -> Dict[str, Dict[str, float]]:
    h = headers.copy()
    h["user-id"] = uid
    h["user-secret-key"] = key

    try:
        payload = {
            "user_id": int(uid) if str(uid).isdigit() else uid,
            "source": "home"
        }

        r = s.post(
            API_URL_GET_BALANCE,
            headers=h,
            json=payload,
            timeout=10
        )
        r.raise_for_status()

        d = r.json()

        if isinstance(d, dict) and d.get("code") == 0:

            data = d.get("data") or {}
            ua = data.get("user_asset") or data.get("assets") or data

            build = world = usdt = None

            if isinstance(ua, list):
                for item in ua:
                    asset = str(item.get("asset", "")).upper()
                    bal = _safe_round(
                        item.get("balance")
                        or item.get("amount")
                        or 0
                    )

                    if asset == "BUILD":
                        build = bal
                    elif asset in ("WORLD", "XWORLD"):
                        world = bal
                    elif asset == "USDT":
                        usdt = bal

            elif isinstance(ua, dict):
                for k, v in ua.items():

                    name = str(k).upper()

                    if isinstance(v, dict):
                        bal = _safe_round(
                            v.get("balance")
                            or v.get("amount")
                            or 0
                        )
                    else:
                        bal = _safe_round(v)

                    if name == "BUILD":
                        build = bal
                    elif name in ("WORLD", "XWORLD"):
                        world = bal
                    elif name == "USDT":
                        usdt = bal

            build = 0.0 if build is None else build
            world = 0.0 if world is None else world
            usdt = 0.0 if usdt is None else usdt

            return {
                "BUILD": {"balance": build},
                "XWORLD": {"balance": world},
                "USDT": {"balance": usdt},
            }

    except Exception as e:
        logger.debug(f"get_balance error: {e}")

    return {
        "BUILD": {"balance": 0.0},
        "XWORLD": {"balance": 0.0},
        "USDT": {"balance": 0.0},
    }

def _safe_round(val: Any, ndigits: int = DECIMALS) -> float:
    try:
        return round(float(val), ndigits)
    except Exception:
        return 0.0

def _parse_number_local(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x)
    m = _num_re_local.search(s)
    if not m:
        return None
    token = m.group(0).replace(",", "")
    try:
        return float(token)
    except Exception:
        return None

def sum_to_label(total: int) -> str:
    try:
        total = int(total)
    except Exception:
        return "unknown"
    if 3 <= total <= 9:
        return "nho"
    if 10 <= total <= 11:
        return "hoa"
    if 12 <= total <= 18:
        return "lon"
    return "unknown"

def get_balances_from_home(home: Dict) -> Dict[str, Optional[float]]:
    build = None
    world = None
    usdt = None
    if not isinstance(home, dict):
        return {"BUILD": None, "XWORLD": None, "USDT": None}
    ua = home.get('user_asset') or home.get('wallet') or home.get('assets') or {}
    if isinstance(ua, dict):
        if 'BUILD' in ua:
            build = _parse_number_local(ua.get('BUILD'))
        if 'WORLD' in ua:
            world = _parse_number_local(ua.get('WORLD'))
        if 'USDT' in ua:
            usdt = _parse_number_local(ua.get('USDT'))
    for k in ('build','ctoken','ctoken_contribute','balance','amount'):
        if build is None and k in home:
            build = _parse_number_local(home.get(k))
    for k in ('world','xworld','WORLD','XWORLD'):
        if world is None and k in home:
            world = _parse_number_local(home.get(k))
    for k in ('usdt','kusdt','USDT'):
        if usdt is None and k in home:
            usdt = _parse_number_local(home.get(k))
    if build is None or world is None or usdt is None:
        try:
            s = json.dumps(home)
            if build is None:
                m = re.search(r'(?:BUILD|build)[:\"\']*\s*([0-9\.,]+)', s, re.I)
                if m: build = _parse_number_local(m.group(1))
            if world is None:
                m = re.search(r'(?:WORLD|world|xworld)[:\"\']*\s*([0-9\.,]+)', s, re.I)
                if m: world = _parse_number_local(m.group(1))
            if usdt is None:
                m = re.search(r'(?:USDT|usdt|kusdt)[:\"\']*\s*([0-9\.,]+)', s, re.I)
                if m: usdt = _parse_number_local(m.group(1))
        except Exception:
            pass
    return {"BUILD": build, "XWORLD": world, "USDT": usdt}

def get_hour_start_timestamp(hour_offset: int = 0):
    now = datetime.now(timezone.utc)
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    hour_start = hour_start - timedelta(hours=hour_offset)
    return int(hour_start.timestamp())

def compute_number_counts(history: List[Dict]) -> Dict[str, Any]:
    overall = Counter()
    per_pos = {0: Counter(), 1: Counter(), 2: Counter()}
    for rec in history:
        codes = rec.get('lucky_codes') or []
        if not isinstance(codes, list) or len(codes) != 3:
            continue
        for pos, num in enumerate(codes):
            per_pos[pos][num] += 1
            overall[num] += 1
    return {'overall': overall, 'per_pos': per_pos}

def issue_to_record(issue: Dict) -> Optional[Dict]:
    lucky_codes = issue.get('lucky_codes') or []
    if len(lucky_codes) != 3:
        return None
    total = sum(lucky_codes)
    return {'issue_id': issue.get('issue_id'), 'lucky_codes': lucky_codes, 'sum': total, 'result': sum_to_label(total)}

def home_to_record(home: Dict) -> Optional[Dict]:
    if not isinstance(home, dict):
        return None
    raw_issue_id = home.get('last_issue_id')
    try:
        last_issue_id = int(raw_issue_id) if raw_issue_id is not None else None
    except (TypeError, ValueError):
        return None

    raw_codes = home.get('last_issue_lucky_code') or []
    if last_issue_id is None or not isinstance(raw_codes, (list, tuple)) or len(raw_codes) != 3:
        return None
    try:
        lucky_codes = [int(x) for x in raw_codes]
    except (TypeError, ValueError):
        return None
    if any(x < 1 or x > 6 for x in lucky_codes):
        return None

    total = sum(lucky_codes)
    return {'issue_id': last_issue_id, 'lucky_codes': lucky_codes, 'sum': total, 'result': sum_to_label(total)}

def place_bet(session: requests.Session, headers: Dict[str, str], issue_id: int, bet_type: str, amount: float, asset: str) -> tuple:
    bet_ids = {'nho': 70309, 'lon': 71218, 'hoa': 71011}
    if bet_type not in bet_ids:
        return False, "Invalid bet type"
    payload = {"game_id": 1, "issue_id": issue_id, "items": [{"id": bet_ids[bet_type], "amount": str(amount), "asset": asset}]}
    try:
        resp = session.post('https://api.winhash.net/lucky_game/v2/create_order', headers=headers, json=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            success = data.get('code') == 0
            error_msg = data.get('msg', 'Unknown error')
            return success, error_msg if not success else "Success"
        else:
            return False, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, str(e)

def make_vbtool_compact_panel(state: Dict, tick: int = 0) -> Panel:
    balances = state.get('balances') or {}
    def _fmt_bal(b):
        try:
            if b is None: return '—'
            if isinstance(b, dict): return f"{_safe_round(b.get('balance',0)):.2f}"
            return f"{_safe_round(b):.2f}"
        except: return '—'
    b_build = _fmt_bal(balances.get('BUILD'))
    b_x = _fmt_bal(balances.get('XWORLD'))
    last_issue = state.get('last_issue', '—')
    last_result = state.get('last_result', '—')
    next_pred = state.get('next_prediction', '—')
    conf = state.get('next_confidence', 0.0)
    analysis = state.get('three_number_analysis', {})
    last_three = analysis.get('last_three', [])
    predicted_numbers = analysis.get('predicted_numbers', [])
    predicted_sum = analysis.get('predicted_sum', 0)
    start_bal = None
    if isinstance(state.get('start_balances'), dict):
        try:
            start_bal = float(state['start_balances'].get('BUILD', {}).get('balance'))
        except: pass
    cur_bal = None
    try:
        b = balances.get('BUILD')
        if isinstance(b, dict): cur_bal = b.get('balance')
        else: cur_bal = float(b) if b else None
    except: pass
    model_stats = state.get('model_stats', {})
    wins = model_stats.get('wins', 0)
    losses = model_stats.get('losses', 0)
    total_pred = model_stats.get('predictions', 0)
    win_rate = (wins / total_pred * 100) if total_pred > 0 else 0.0

    t = Text()
    t.append(f"{ICONS['user']} User: {state.get('account_id', '—')}", style=VBTOOL_COLORS["history_blue"])
    t.append("  |  ", style="dim")
    t.append(f"{ICONS['diamond']} BUILD: {b_build}", style=f"bold {VBTOOL_COLORS['emerald']}")
    t.append("  |  ", style="dim")
    t.append(f"🌍 XW: {b_x}", style=VBTOOL_COLORS["history_blue"])
    t.append("\n" + "━" * 68 + "\n", style=VBTOOL_COLORS["gold"])
    t.append(f"📊 Kết quả #{last_issue}: {last_result}", style=VBTOOL_COLORS["platinum"])
    t.append(f"  |  Tỷ lệ: ", style="dim")
    t.append(f"{win_rate:.0f}%", style=f"bold {VBTOOL_COLORS['history_blue']}")
    if start_bal is not None and cur_bal is not None:
        diff = _safe_round(cur_bal - start_bal, DECIMALS)
        pct = (diff / start_bal * 100.0) if (start_bal and start_bal != 0) else 0.0
        sign = "+" if diff > 0 else ""
        if diff > 0: arrow, style = "🚀", f"bold {VBTOOL_COLORS['emerald']}"
        elif diff < 0: arrow, style = "💔", f"bold {VBTOOL_COLORS['ruby']}"
        else: arrow, style = "➡️", VBTOOL_COLORS["gold"]
        t.append("  |  ", style="dim")
        t.append(f"{arrow} {sign}{diff:.2f} ({sign}{pct:.1f}%)", style=style)
    t.append("\n" + "─" * 68 + "\n", style="dim")
    t.append(f"{ICONS['dice']} Hiện tại: ", style="dim")
    for num in last_three:
        icon = ICON_LOTTO.get(num, str(num))
        color = NUMBER_COLORS.get(num, "white")
        t.append(f"{icon} ", style=f"bold {color}")
    t.append("  →  ", style="dim")
    t.append("Dự đoán: ", style="dim")
    for num in predicted_numbers:
        icon = ICON_LOTTO.get(num, str(num))
        color = NUMBER_COLORS.get(num, "white")
        t.append(f"{icon} ", style=f"bold {color}")
    t.append(f"(Σ={predicted_sum})", style=VBTOOL_COLORS["gold"])
    t.append("\n")
    t.append(f"{ICONS['target']} Cược tiếp: ", style="dim")
    t.append(f"{next_pred}", style=f"bold {VBTOOL_COLORS['neon_pink']}")
    if conf:
        conf_bar = int(conf * 12)
        conf_style = VBTOOL_COLORS["emerald"] if conf >= 0.7 else (VBTOOL_COLORS["gold"] if conf >= 0.5 else VBTOOL_COLORS["ruby"])
        t.append("  [", style="dim")
        t.append("█" * conf_bar, style=conf_style)
        t.append("░" * (12 - conf_bar), style="dim")
        t.append(f" {conf*100:.0f}%]", style=conf_style)
    hist = state.get('history', []) or []
    counts = compute_number_counts(hist) if hist else {'overall': Counter(), 'per_pos': {0: Counter(), 1: Counter(), 2: Counter()}}
    overall = counts.get('overall', {})
    if overall:
        t.append("\n", "")
        t.append("🔥❄️ Hot/Cold: ", style="dim")
        maxc = max(overall.values()) if overall else 1
        for n in range(1,7):
            c = overall.get(n, 0)
            color = NUMBER_COLORS.get(n, 'white')
            bl = int((c / maxc) * 6) if maxc else 0
            bars = '█' * bl + ' ' * (6 - bl)
            t.append(f" {ICON_LOTTO.get(n)}", style=f"bold {color}")
            t.append(bars, style=color)
        t.append("\n", "")
    t.append(f"{ICONS['chart']} Tổng: ", style="dim")
    t.append(f"{total_pred} ván", style=VBTOOL_COLORS["platinum"])
    t.append("  |  🏆 Win ", style="dim")
    t.append(f"{wins}", style=VBTOOL_COLORS["emerald"])
    t.append("  💀 Lose ", style="dim")
    t.append(f"{losses}", style=VBTOOL_COLORS["ruby"])
    bet_stats = state.get('bet_stats')
    if bet_stats:
        win_streak = bet_stats.get('current_win_streak', 0)
        loss_streak = bet_stats.get('current_loss_streak', 0)
        if win_streak > 0:
            t.append("\n", "")
            t.append(f"{ICONS['fire']} Chuỗi thắng: ", style=VBTOOL_COLORS["emerald"])
            t.append(f"{win_streak}", style=f"bold {VBTOOL_COLORS['emerald']}")
            if win_streak >= 3:
                t.append(" 🏆", style=VBTOOL_COLORS["gold"])
        if loss_streak > 0:
            t.append("\n", "")
            t.append(f"💀 Chuỗi thua: ", style=VBTOOL_COLORS["ruby"])
            t.append(f"{loss_streak}", style=f"bold {VBTOOL_COLORS['ruby']}")
            if loss_streak >= 3:
                t.append(" ⚠️", style=VBTOOL_COLORS["gold"])
        t.append("\n", "")
        total_wagered = bet_stats.get('total_wagered', 0)
        if start_bal is not None and cur_bal is not None:
            net_profit = _safe_round(cur_bal - start_bal, DECIMALS)
        else:
            net_profit = bet_stats.get('net_profit', 0)
        asset = bet_stats.get('asset', 'BUILD')
        t.append(f"{ICONS['money']} Đã cược: ", style="dim")
        t.append(f"{total_wagered:.2f} {asset}", style=VBTOOL_COLORS["gold"])
        profit_style = f"bold {VBTOOL_COLORS['emerald']}" if net_profit > 0 else (f"bold {VBTOOL_COLORS['ruby']}" if net_profit < 0 else VBTOOL_COLORS["gold"])
        t.append(f"  |  {ICONS['chart']} Lãi/Lỗ: ", style="dim")
        t.append(f"{net_profit:+.2f} {asset}", style=profit_style)
    log_text = state.get('log_text', '—')
    t.append("\n" + "─" * 68 + "\n", style="dim")
    t.append(log_text, style=VBTOOL_COLORS["platinum"])
    t.append("\n", "")
    return Panel(t, border_style=VBTOOL_COLORS["bright_cyan"], box=box.SIMPLE_HEAVY, padding=(0, 1))

def make_vbtool_history_cards(history: List[Dict], limit: int = 5, cols: int = 1, tick: int = 0, highlight_issue: Optional[int] = None) -> Panel:
    recent = list(reversed(history[-limit:]))
    cards = []
    for rec in recent:
        issue = rec.get('issue_id')
        ts = rec.get('timestamp', '')
        short_ts = ts[11:19] if isinstance(ts, str) and len(ts) >= 19 else ''
        codes = rec.get('lucky_codes', [])
        code_line = Text()
        for i, n in enumerate(codes):
            color = NUMBER_COLORS.get(n, 'white')
            code_line.append(f" {ICON_LOTTO.get(n)} ", style=f"bold {color}")
            if i < len(codes)-1:
                code_line.append(' ', style='dim')
        kq = rec.get('result', '—')
        if kq == 'nho':
            kq_style = f"bold white on {VBTOOL_COLORS['sapphire']}"
            kq_icon = '🔵'
        elif kq == 'lon':
            kq_style = f"bold white on {VBTOOL_COLORS['ruby']}"
            kq_icon = '🔴'
        else:
            kq_style = f"bold black on {VBTOOL_COLORS['gold']}"
            kq_icon = '⚖️'
        bet = rec.get('bet') or {}
        pl = None
        try:
            bb = bet.get('balance_before')
            ba = bet.get('balance_after')
            if bb is not None and ba is not None:
                pl = float(ba) - float(bb)
            else:
                pl = bet.get('profit')
        except Exception:
            pl = bet.get('profit') if bet else None
        if pl is None:
            pl_text = Text('—', style='dim')
        elif pl > 0:
            pl_text = Text(f'+{pl:.2f}', style=f'bold {VBTOOL_COLORS["emerald"]}')
        elif pl < 0:
            pl_text = Text(f'{pl:.2f}', style=f'bold {VBTOOL_COLORS["ruby"]}')
        else:
            pl_text = Text(f'{pl:.2f}', style=VBTOOL_COLORS["gold"])
        bs = VBTOOL_COLORS["onyx"]
        if highlight_issue is not None and issue == highlight_issue:
            bs = VBTOOL_COLORS["gold"]
        card = Panel(Align.left(Text.assemble(Text(f"#{str(issue)[-4:]} ", style=f'bold {VBTOOL_COLORS["history_blue"]}'), Text(short_ts + '\n', style='dim'), code_line, Text('\n', ''), Text.assemble(Text(kq_icon + ' ', style=kq_style), Text(' ', ''), pl_text))), box=box.ROUNDED, padding=(0, 1), border_style=bs)
        cards.append(card)
    if not cards:
        return Panel(Text('Không có lịch sử', style='dim'), border_style=VBTOOL_COLORS["onyx"])
    return Panel(Columns(cards, equal=True, expand=True), title=f"LỊCH SỬ GẦN ĐÂY", border_style=VBTOOL_COLORS["history_blue"], box=box.ROUNDED)

def make_vbtool_layout(state: Dict) -> Layout:
    tick = state.get("tick", 0)

    layout = Layout(name="root")
    layout.split_column(
        Layout(name="main", ratio=1),
        Layout(name="history", size=12),
    )

    layout["main"].update(
        make_vbtool_compact_panel(state, tick)
    )

    layout["history"].update(
        make_vbtool_history_cards(
            state.get("history", []),
            limit=5,
            cols=1,
            tick=tick,
            highlight_issue=state.get("last_issue"),
        )
    )

    return layout   

def main_lotto():
    start_key_expiry_monitor()

    print(
        "📋 Gửi link game có userId và secretKey.\n"
        "Ví dụ: link game chứa ?userId=...&secretKey=...",
        flush=True,
    )

    if not check_activation_valid()[0]:
        print("❌ Key của bạn không còn hiệu lực.", flush=True)
        time.sleep(2)
        return

    console.clear()
    am = AccountManager()

    console.print(
        Panel(
            "📋 Vui lòng dán link game để đăng nhập",
            title="LOTTO",
            border_style=VBTOOL_COLORS["history_blue"],
            box=box.ROUNDED,
        )
    )

    try:
        selected_account = am.add_account()
    except KeyboardInterrupt:
        return

    if not selected_account:
        print("❌ Không thêm được tài khoản!", flush=True)
        return

    uid = (
        selected_account.get("user-id")
        or selected_account.get("userId")
        or selected_account.get("uid")
    )

    secret = (
        selected_account.get("user-secret-key")
        or selected_account.get("secretKey")
        or selected_account.get("user_secret_key")
    )

    if not uid:
        print("❌ Không tìm thấy User ID!", flush=True)
        return

    if not secret:
        print("❌ Không tìm thấy Secret Key!", flush=True)
        return

    config = {
        "user-id": uid,
        "user-secret-key": secret,
        "country-code": selected_account.get("country-code", "vn"),
        "user-agent": selected_account.get("user-agent", "Mozilla/5.0"),
        "asset": selected_account.get("asset", "BUILD"),
    }

    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    headers = build_headers(config)
    session = requests.Session()
    asset = config.get("asset", "BUILD")

    while True:
        try:
            bet_per_round = float(
                Prompt.ask(
                    f"[bold {VBTOOL_COLORS['white']}]1. Cược mỗi ván bao nhiêu BUILD?[/bold {VBTOOL_COLORS['white']}]"
                )
            )
            break
        except ValueError:
            print("❌ Vui lòng nhập số hợp lệ!", flush=True)

    while True:
        try:
            multiplier = float(
                Prompt.ask(
                    f"[bold {VBTOOL_COLORS['white']}]2. Nhập số nhân sau khi thua[/bold {VBTOOL_COLORS['white']}]"
                )
            )
            break
        except ValueError:
            print("❌ Vui lòng nhập số hợp lệ!", flush=True)

    while True:
        answer = Prompt.ask(
            f"[bold {VBTOOL_COLORS['white']}]3. Bật auto cược? (y/n)[/bold {VBTOOL_COLORS['white']}]"
        ).strip().lower()

        if answer in ("y", "yes"):
            enable_betting = True
            break

        if answer in ("n", "no"):
            enable_betting = False
            break

        print("❌ Vui lòng nhập y hoặc n!", flush=True)

    betting_system = (
        MartingaleBetting(bet_per_round, multiplier, asset)
        if enable_betting
        else None
    )

    current_ts = get_hour_start_timestamp(0)
    previous_ts = get_hour_start_timestamp(1)
    issues = {}

    for issue in (
        fetch_issue_list(session, headers, previous_ts)
        + fetch_issue_list(session, headers, current_ts)
    ):
        if issue.get("issue_id"):
            issues[issue["issue_id"]] = issue

    history = [
        issue_to_record(i)
        for i in sorted(
            issues.values(),
            key=lambda x: x["issue_id"],
        )
        if issue_to_record(i)
    ]

    if not history:
        print(
            "❌ Không có lịch sử. Kiểm tra kết nối/config.",
            flush=True,
        )
        return

    three_number_analyzer = ThreeNumberAnalyzer()
    last_processed = max(h["issue_id"] for h in history)
    pending = {}
    pending_bets = {}
    processed_issue_ids = set()
    stop = False

    state = {
        "account_id": selected_account.get("user-id") if selected_account else None,
        "balances": {
            "BUILD": None,
            "XWORLD": None,
            "USDT": None,
        },
        "start_balances": {},
        "last_result": "—",
        "last_issue": "—",
        "next_prediction": "—",
        "next_confidence": None,
        "planned_bet": 0.0,
        "history": history,
        "three_number_analysis": three_number_analyzer.analyze(history),
        "bet_stats": None,
        "model_stats": {
            "predictions": 0,
            "wins": 0,
            "losses": 0,
            "current_streak": 0,
        },
        "log_text": "Sẵn sàng ✅",
        "tick": 0,
        "asset": asset,
    }

    console.clear()

    print(
        "🚀 LOTTO ĐANG CHẠY\n"
        f"👤 userId={uid}\n"
        f"💳 Loại tiền: {asset}\n"
        f"💰 Cược mỗi ván: {bet_per_round:.4f} {asset}\n"
        f"📈 Hệ số sau thua: x{multiplier:.2f}\n"
        f"🤖 Auto cược: {'BẬT' if enable_betting else 'TẮT'}\n"
        "⏳ Đang chờ dữ liệu ván mới...",
        flush=True,
    )

    with Live(
        make_vbtool_layout(state),
        console=console,
        refresh_per_second=UI_REFRESH,
    ) as live:
        loop_error_count = 0

        while not stop:
            try:
                if not check_activation_valid()[0]:
                    stop = True
                    break

                state["tick"] = (
                    state.get("tick", 0) + 1
                ) % 1000000

                try:
                    uid = selected_account.get("user-id")
                    key = selected_account.get("user-secret-key")

                    if uid and key:
                        bmap = get_balance(
                            session,
                            headers,
                            uid,
                            key,
                        )

                        if isinstance(bmap, dict):
                            state["balances"] = bmap

                            if not state.get("start_balances"):
                                state["start_balances"] = {
                                    k: {
                                        "balance": (
                                            float(v.get("balance", 0))
                                            if isinstance(v, dict)
                                            else _safe_round(v)
                                        )
                                    }
                                    for k, v in bmap.items()
                                }
                                state["log_text"] = "Đã lưu số dư khởi điểm"

                except Exception:
                    logger.debug("Wallet API read failed")

                home_data = fetch_home(
                    session,
                    headers,
                    asset,
                )

                if not home_data:
                    state["log_text"] = "Chờ dữ liệu..."

                    try:
                        live.update(make_vbtool_layout(state))
                    except Exception:
                        pass

                    try:
                        time.sleep(HOME_POLL_INTERVAL)
                    except KeyboardInterrupt:
                        return

                    continue

                try:
                    if state["balances"] and all(
                        _safe_round(
                            state["balances"]
                            .get(k, {})
                            .get("balance", 0)
                        ) == 0.0
                        for k in (
                            "BUILD",
                            "XWORLD",
                            "USDT",
                        )
                    ):
                        hb = get_balances_from_home(home_data)

                        for k in (
                            "BUILD",
                            "XWORLD",
                            "USDT",
                        ):
                            v = hb.get(k)

                            if v is not None:
                                state["balances"][k] = {
                                    "balance": _safe_round(v)
                                }

                        if (
                            state["balances"]
                            and not state.get("start_balances")
                        ):
                            state["start_balances"] = {
                                k: {
                                    "balance": (
                                        float(v.get("balance", 0))
                                        if isinstance(v, dict)
                                        else _safe_round(v)
                                    )
                                }
                                for k, v in state["balances"].items()
                            }

                except Exception:
                    pass

                last_id = home_data.get("last_issue_id")

                if last_id and last_id > last_processed:
                    record = home_to_record(home_data)

                    if record:
                        last_processed = record["issue_id"]
                        actual = record["result"]
                        decision = pending.pop(
                            record["issue_id"],
                            None,
                        )
                        bet_info = pending_bets.pop(
                            record["issue_id"],
                            None,
                        )

                        state["last_issue"] = record["issue_id"]
                        state["last_result"] = (
                            f"{actual.upper()} ({record['sum']})"
                        )
                        if decision:
                            predicted_bet = decision.get(
                                "bet_action",
                                "small",
                            )
                            correct = actual == predicted_bet
                            result_emoji = "✅" if correct else "❌"

                            state["log_text"] = (
                                f"{result_emoji} #{record['issue_id']}: "
                                f"{predicted_bet.upper()} → {actual.upper()}"
                            )

                            if betting_system and bet_info:
                                if isinstance(bet_info, dict):
                                    bet_type = bet_info.get("type")
                                    bet_amount = float(
                                        bet_info.get("amount", 0.0)
                                    )
                                    b_before = bet_info.get(
                                        "balance_before"
                                    )
                                else:
                                    try:
                                        bet_type, bet_amount = bet_info
                                        b_before = None
                                    except Exception:
                                        bet_type = None
                                        bet_amount = 0.0
                                        b_before = None

                                try:
                                    b_after = None
                                    bmap = state.get("balances") or {}
                                    bval = bmap.get("BUILD")

                                    if isinstance(bval, dict):
                                        b_after = float(
                                            bval.get("balance", 0.0)
                                        )
                                    else:
                                        b_after = _safe_round(bval)

                                except Exception:
                                    b_after = None

                                if correct:
                                    payout = bet_amount * 2
                                    betting_system.on_win(payout)
                                else:
                                    betting_system.on_loss()

                                try:
                                    if (
                                        b_before is not None
                                        and b_after is not None
                                    ):
                                        profit_value = (
                                            float(b_after)
                                            - float(b_before)
                                        )
                                    elif correct:
                                        profit_value = (
                                            payout - bet_amount
                                        )
                                    else:
                                        profit_value = -bet_amount

                                except Exception:
                                    profit_value = (
                                        payout - bet_amount
                                        if correct
                                        else -bet_amount
                                    )

                                record["bet"] = {
                                    "type": bet_type,
                                    "amount": float(bet_amount),
                                    "profit": _safe_round(
                                        profit_value,
                                        DECIMALS,
                                    ),
                                    "balance_before": b_before,
                                    "balance_after": b_after,
                                }

                                state["bet_stats"] = (
                                    betting_system.get_stats()
                                )

                            ms = state.get("model_stats") or {}
                            ms["predictions"] = (
                                ms.get("predictions", 0) + 1
                            )

                            if correct:
                                ms["wins"] = (
                                    ms.get("wins", 0) + 1
                                )
                                ms["current_streak"] = 0
                            else:
                                ms["losses"] = (
                                    ms.get("losses", 0) + 1
                                )
                                ms["current_streak"] = (
                                    ms.get("current_streak", 0) + 1
                                )

                            state["model_stats"] = ms

                        try:
                            record["timestamp"] = (
                                datetime.utcnow().isoformat() + "Z"
                            )
                        except Exception:
                            record["timestamp"] = None

                        if (
                            record.get("issue_id")
                            in processed_issue_ids
                        ):
                            continue

                        processed_issue_ids.add(
                            record.get("issue_id")
                        )

                        existing = next(
                            (
                                i
                                for i, h in enumerate(history)
                                if h.get("issue_id")
                                == record.get("issue_id")
                            ),
                            None,
                        )

                        if existing is None:
                            history.append(record)
                        else:
                            history[existing] = record

                        if len(history) > 500:
                            history = history[-500:]

                        state["history"] = history
                        state["three_number_analysis"] = (
                            three_number_analyzer.analyze(history)
                        )

                        ms_now = state.get("model_stats") or {}
                        bet_desc = "Không cược"

                        if record.get("bet"):
                            rb = record.get("bet") or {}
                            bet_desc = (
                                f"{str(rb.get('type', '—')).upper()} "
                                f"{float(rb.get('amount', 0.0)):.4f} "
                                f"{asset}"
                            )

                        print(
                            f"📊 KẾT QUẢ #{record.get('issue_id')}: "
                            f"{str(actual).upper()} "
                            f"({record.get('sum', '—')})\n"
                            f"🎯 Cược: {bet_desc}\n"
                            f"📈 Win: {ms_now.get('wins', 0)} "
                            f"| Lose: {ms_now.get('losses', 0)}",
                            flush=True,
                        )

                        try:
                            live.update(
                                make_vbtool_layout(state)
                            )
                        except Exception:
                            pass

                next_id = (last_id or 0) + 1

                if next_id not in pending:
                    analysis = three_number_analyzer.analyze(history)
                    state["three_number_analysis"] = analysis

                    predicted_outcome = analysis.get(
                        "bet_action",
                        "nho",
                    )
                    prediction_label = analysis.get(
                        "prediction",
                        "1-3",
                    )
                    conf = analysis.get(
                        "confidence",
                        0.5,
                    )

                    decision = {
                        "action": "PREDICT",
                        "bet_action": predicted_outcome,
                        "prediction": prediction_label,
                        "confidence": conf,
                    }

                    pending[next_id] = decision

                    icon = ICON_LOTTO.get(
                        predicted_outcome,
                        "",
                    )
                    state["next_prediction"] = (
                        f"{icon} {predicted_outcome.upper()}"
                    )
                    state["next_confidence"] = conf
                    state["log_text"] = (
                        f"Dự #{next_id}: "
                        f"{predicted_outcome.upper()} "
                        f"(tin cậy {conf * 100:.1f}%)"
                    )

                    if betting_system:
                        bet_amount = (
                            betting_system.get_bet_amount()
                        )
                        state["planned_bet"] = bet_amount

                        success, message = place_bet(
                            session,
                            headers,
                            next_id,
                            predicted_outcome,
                            bet_amount,
                            asset,
                        )

                        if success:
                            betting_system.record_wager(bet_amount)

                            try:
                                b_before = None
                                bmap = state.get("balances") or {}
                                bval = bmap.get("BUILD")

                                if isinstance(bval, dict):
                                    b_before = float(
                                        bval.get("balance", 0.0)
                                    )
                                else:
                                    b_before = _safe_round(bval)

                            except Exception:
                                b_before = None

                            pending_bets[next_id] = {
                                "type": predicted_outcome,
                                "amount": bet_amount,
                                "balance_before": b_before,
                            }

                            state["bet_stats"] = (
                                betting_system.get_stats()
                            )
                            state["log_text"] = (
                                f"💰Cược #{next_id}: "
                                f"{bet_amount:.2f} {asset} → "
                                f"{predicted_outcome.upper()}"
                            )

                        else:
                            state["log_text"] = (
                                f"⚠️ Lỗi: {message[:30]}"
                            )

                    else:
                        state["log_text"] = (
                            f"🎯 Dự đoán #{next_id}: "
                            f"{predicted_outcome.upper()}"
                        )
                        state["planned_bet"] = 0.0

                    pred_nums = ", ".join(
                        str(n)
                        for n in analysis.get(
                            "predicted_numbers",
                            [],
                        )
                    )

                    print(
                        f"🎯 DỰ ĐOÁN #{next_id}: "
                        f"{predicted_outcome.upper()}\n"
                        f"🔢 Bộ số: {pred_nums or '—'} "
                        f"| Σ={analysis.get('predicted_sum', '—')}\n"
                        f"🧠 Độ tin cậy: "
                        f"{float(conf) * 100:.1f}%",
                        flush=True,
                    )

                try:
                    live.update(
                        make_vbtool_layout(state)
                    )
                except Exception:
                    pass

                try:
                    time.sleep(HOME_POLL_INTERVAL)
                except KeyboardInterrupt:
                    return

            except KeyboardInterrupt:
                return

            except SystemExit:
                raise

            except Exception as exc:
                loop_error_count += 1
                log_debug(
                    f"LOTTO polling loop error #{loop_error_count}: "
                    f"{type(exc).__name__}: {exc}"
                )

                try:
                    _notify_admin_code_error(
                        "LOTTO polling loop lỗi — worker sẽ tự tiếp tục",
                        exc,
                        chat_id=str(_WORKER_CHAT_ID or ""),
                        tool_key="lotto",
                    )
                except Exception:
                    pass

                state["log_text"] = (
                    "⚠️ Kết nối/API gặp lỗi tạm thời, "
                    "đang tự chạy lại... "
                    f"({type(exc).__name__})"
                )

                try:
                    live.update(
                        make_vbtool_layout(state)
                    )
                except Exception:
                    pass

                if loop_error_count >= 3:
                    try:
                        session.close()
                    except Exception:
                        pass

                    session = requests.Session()
                    loop_error_count = 0

                try:
                    time.sleep(
                        min(
                            10.0,
                            2.0 + loop_error_count,
                        )
                    )
                except KeyboardInterrupt:
                    return

    console.clear()
    return

# ================== TOOL 3: CHẠY ĐUA TỐC ĐỘ ==================

def prints(r, g, b, text="text", end="\n"):
    if True:
        print(_strip_rich_markup(text), end=end, flush=True)
        return
    print(f"\033[38;2;{r};{g};{b}m{text}\033[0m", end=end)

def cprints(color_name, text="", end="\n"):

    try:
        h = str(VBTOOL_COLORS.get(color_name, "") or "").lstrip("#")
        if len(h) != 6 or any(ch not in "0123456789abcdefABCDEF" for ch in h):
            print(text, end=end)
            return
        r = int(h[0:2], 16)
        g = int(h[2:4], 16)
        b = int(h[4:6], 16)
        prints(r, g, b, text=text, end=end)
    except (TypeError, ValueError):
        print(text, end=end)

NV = {
    1: "Bậc Thầy Tấn Công",
    2: "Quyền Sắt",
    3: "Thợ Lặn Sâu",
    4: "Cơn Lốc Sân Cỏ",
    5: "Hiệp Sĩ Phi Nhanh",
    6: "Vua Home Run",
}

def _sprint_history_sequence(history):
    seq = []
    for item in history or []:
        if not isinstance(item, dict):
            continue
        top1 = item.get("top1")
        if isinstance(top1, int):
            seq.append(top1)
        elif isinstance(top1, str) and top1.isdigit():
            seq.append(int(top1))
    return seq

def _sprint_delay(seq, nv):
    delay = 0
    for x in seq:
        if x == nv:
            break
        delay += 1
    return delay

def _sprint_current_streak(seq, nv):
    streak = 0
    for x in seq:
        if x == nv:
            streak += 1
        else:
            break
    return streak

def _sprint_weighted_recent_hits(seq, nv, limit=30, decay=0.92):
    score = 0.0
    weight = 1.0
    for x in seq[:limit]:
        if x == nv:
            score += weight
        weight *= decay
    return score

def _sprint_average_gap(seq, nv):
    positions = [i for i, x in enumerate(seq) if x == nv]
    if len(positions) < 2:
        return None
    gaps = [b - a for a, b in zip(positions, positions[1:])]
    return sum(gaps) / len(gaps)

def _sprint_follow_probability(seq, prev_nv, nv):
    after = []
    for i in range(len(seq) - 1):
        if seq[i + 1] == prev_nv:
            after.append(seq[i])
    if not after:
        return 0.0
    return after.count(nv) / len(after)

def choose_free_cdtd(history, so_nv):
    seq = _sprint_history_sequence(history)
    if not seq:
        return random.sample(range(1, 7), so_nv)

    scores = {}
    total = len(seq)

    for nv in range(1, 7):
        score = 56.0

        # 1) Tần suất tổng thể
        wins = seq.count(nv)
        rate = wins / total
        score += (0.50 - rate) * 34

        # 2) Độ trễ kể từ lần xuất hiện gần nhất
        delay = _sprint_delay(seq, nv)
        if delay >= 18:
            score += 16
        elif delay >= 12:
            score += 12
        elif delay >= 8:
            score += 8
        elif delay >= 5:
            score += 4
        elif delay <= 1:
            score -= 7

        score += min(delay * 0.45, 8.5)

        # 3) Chuỗi hiện tại
        streak = _sprint_current_streak(seq, nv)
        if streak:
            score += min(streak * 6.5, 22)

        # 4) Xu hướng 30 ván gần nhất
        recent_score = _sprint_weighted_recent_hits(seq, nv, limit=30, decay=0.90)
        score += recent_score * 4.0

        # 5) Cửa sổ 10 ván gần
        last10 = seq[:10]
        count10 = sum(1 for x in last10 if x == nv)
        if count10 == 0:
            score += 5
        elif count10 == 1:
            score += 3
        elif count10 >= 4:
            score -= 9

        # 6) Mô hình sau người đang dẫn đầu gần nhất
        if len(seq) >= 2:
            prev_top = seq[0]
            score += _sprint_follow_probability(seq, prev_top, nv) * 32

        # 7) Chu kỳ xuất hiện
        avg_gap = _sprint_average_gap(seq, nv)
        if avg_gap:
            if delay >= avg_gap * 1.4:
                score += 8
            elif delay >= avg_gap:
                score += 4
            elif delay <= avg_gap * 0.55:
                score -= 4

        # 8) Tránh chọn mã quá nóng trong lịch sử dài
        history50 = seq[:50]
        if history50:
            wins50 = history50.count(nv)
            if wins50 >= 14:
                score -= 10
            elif wins50 == 0:
                score += 5

        # 9) Chống lặp gần nhất
        if len(seq) >= 2 and seq[0] == seq[1] == nv:
            score += 5

        scores[nv] = score

    min_score = min(scores.values())
    max_score = max(scores.values())
    if max_score != min_score:
        for nv in scores:
            scores[nv] = ((scores[nv] - min_score) / (max_score - min_score)) * 100

    # Lớp CDTD-AI bổ sung cho logic FREE.
    try:
        ai = analyze_cdtd_ai(history, "winner", "free")
        for nv in scores:
            scores[nv] += float(ai["scores"].get(nv, 50.0)) * 0.22
    except Exception:
        pass

    return sorted(scores, key=scores.get, reverse=True)[:so_nv]

#=================NOT WINNER FREE=================

# 1
def choose_not_winner_frequency(history, so_nv):

    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    total = len(seq)

    recent10 = seq[:10]
    recent20 = seq[:20]
    recent50 = seq[:50]

    scores = {}

    for nv in range(1, 7):

        wins_all = seq.count(nv)

        not_win_all = (
            (total - wins_all) / total
            if total > 0 else 0.0
        )

        n10 = len(recent10)
        wins10 = recent10.count(nv)

        not_win10 = (
            (n10 - wins10) / n10
            if n10 > 0 else 0.0
        )

        n20 = len(recent20)
        wins20 = recent20.count(nv)

        not_win20 = (
            (n20 - wins20) / n20
            if n20 > 0 else 0.0
        )

        n50 = len(recent50)
        wins50 = recent50.count(nv)

        not_win50 = (
            (n50 - wins50) / n50
            if n50 > 0 else 0.0
        )


        delay = 0

        for x in seq:
            if x == nv:
                break
            delay += 1

        delay_score = min(
            delay / max(total, 1),
            1.0
        )

        score = (
            not_win_all * 0.30
            + not_win10 * 0.25
            + not_win20 * 0.20
            + not_win50 * 0.15
            + delay_score * 0.10
        )

        scores[nv] = score

    ranking = sorted(
        scores,
        key=lambda nv: scores[nv],
        reverse=True
    )

    return ranking[:so_nv]
    
# 2

def choose_xac_suat_free(history, so_nv):

    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    total = len(seq)

    recent30 = seq[:30]
    recent10 = seq[:10]

    scores = {}

    last_winner = seq[0] if seq else None

    for nv in range(1, 7):

        wins_all = seq.count(nv)

        p_all = (
            (total - wins_all + 1)
            / (total + 2)
        )

        n30 = len(recent30)
        wins30 = recent30.count(nv)

        p30 = (
            (n30 - wins30 + 1)
            / (n30 + 2)
        )

        n10 = len(recent10)
        wins10 = recent10.count(nv)

        p10 = (
            (n10 - wins10 + 1)
            / (n10 + 2)
        )

        transition_total = 0
        transition_wins = 0

        if len(seq) >= 2:
            for i in range(len(seq) - 1):
                previous_winner = seq[i + 1]
                current_winner = seq[i]

                if previous_winner == last_winner:
                    transition_total += 1

                    if current_winner == nv:
                        transition_wins += 1

        if transition_total > 0:
            p_transition = (
                (transition_total - transition_wins + 1)
                / (transition_total + 2)
            )
        else:
            p_transition = p_all

        probability = (
            p_all * 0.40
            + p30 * 0.30
            + p10 * 0.20
            + p_transition * 0.10
        )

        scores[nv] = probability

    result = sorted(
        scores,
        key=lambda nv: scores[nv],
        reverse=True
    )

    return result[:so_nv]
    
# 3
def choose_not_winner_transition(history, so_nv):

    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    current_winner = seq[0]

    scores = {}

    for nv in range(1, 7):

        transition_total = 0
        transition_win = 0

        for i in range(len(seq) - 1):

            newer = seq[i]
            older = seq[i + 1]

            if older == current_winner:
                transition_total += 1

                if newer == nv:
                    transition_win += 1

        if transition_total > 0:
            not_win_probability = (
                transition_total - transition_win + 1
            ) / (
                transition_total + 2
            )
        else:
            not_win_probability = 0.5

        recent_transitions = 0
        recent_not_win = 0

        for i in range(
            min(len(seq) - 1, 5)
        ):
            newer = seq[i]
            older = seq[i + 1]

            if older == current_winner:
                recent_transitions += 1

                if newer != nv:
                    recent_not_win += 1

        if recent_transitions > 0:
            recent_probability = (
                recent_not_win
                / recent_transitions
            )
        else:
            recent_probability = not_win_probability

        if current_winner == nv:
            current_penalty = 0.15
        else:
            current_penalty = 0.0

        score = (
            not_win_probability * 0.65
            + recent_probability * 0.35
            - current_penalty
        )

        scores[nv] = max(0.0, score)

    ranking = sorted(
        scores,
        key=lambda nv: scores[nv],
        reverse=True
    )

    return ranking[:so_nv]
    
#4
def choose_not_winner_markov(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    current = seq[0]
    scores = {}

    for nv in range(1, 7):
        total = 0
        win = 0

        for i in range(len(seq) - 1):
            if seq[i + 1] == current:
                total += 1
                if seq[i] == nv:
                    win += 1

        p_not = (total - win + 1) / (total + 2) if total else 0.5

        scores[nv] = p_not

    return sorted(scores, key=scores.get, reverse=True)[:so_nv]
    
#5
def choose_not_winner_bayesian(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    scores = {}

    for nv in range(1, 7):
        wins = seq.count(nv)
        losses = len(seq) - wins

        probability = (losses + 1) / (wins + losses + 2)

        scores[nv] = probability

    return sorted(scores, key=scores.get, reverse=True)[:so_nv]
    
#6
def choose_not_winner_momentum(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    recent = seq[:15]
    scores = {}

    for nv in range(1, 7):
        score = 0.0

        for i, winner in enumerate(recent):
            weight = 1 / (i + 1)

            if winner == nv:
                score -= weight
            else:
                score += weight / 5

        scores[nv] = score

    return sorted(scores, key=scores.get, reverse=True)[:so_nv]
    
#7
def choose_not_winner_cycle(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    scores = {}

    for nv in range(1, 7):
        positions = [
            i for i, x in enumerate(seq)
            if x == nv
        ]

        if len(positions) < 2:
            scores[nv] = 0.5
            continue

        gaps = [
            positions[i + 1] - positions[i]
            for i in range(len(positions) - 1)
        ]

        avg_gap = sum(gaps) / len(gaps)
        current_gap = positions[0]

        distance = abs(current_gap - avg_gap)

        scores[nv] = min(
            distance / max(avg_gap, 1),
            1.0
        )

    return sorted(scores, key=scores.get, reverse=True)[:so_nv]
    
#8
def choose_not_winner_balance(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    total = len(seq)
    expected = total / 6
    scores = {}

    for nv in range(1, 7):
        wins = seq.count(nv)

        deviation = abs(wins - expected)

        scores[nv] = (
            deviation / max(expected, 1)
        )

    return sorted(scores, key=scores.get, reverse=True)[:so_nv]
    
#9
def choose_not_winner_multilevel(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    windows = [5, 10, 20, 40]
    scores = {}

    for nv in range(1, 7):
        score = 0.0
        weight_total = 0.0

        for size in windows:
            data = seq[:size]

            if not data:
                continue

            weight = 1 / size
            not_win = 1 - (
                data.count(nv) / len(data)
            )

            score += not_win * weight
            weight_total += weight

        scores[nv] = (
            score / weight_total
            if weight_total else 0.0
        )

    return sorted(scores, key=scores.get, reverse=True)[:so_nv]
 
#10
def choose_not_winner_stability(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    windows = [
        seq[:10],
        seq[:20],
        seq[:30]
    ]

    scores = {}

    for nv in range(1, 7):
        values = []

        for data in windows:
            if data:
                values.append(
                    1 - data.count(nv) / len(data)
                )

        if not values:
            scores[nv] = 0
            continue

        average = sum(values) / len(values)

        variance = sum(
            (x - average) ** 2
            for x in values
        ) / len(values)

        scores[nv] = average - variance

    return sorted(scores, key=scores.get, reverse=True)[:so_nv]
    
#11
def choose_not_winner_bayes_multi(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    windows = [10, 20, 30, 50]
    scores = {}

    for nv in range(1, 7):
        total_score = 0.0
        total_weight = 0.0

        for size in windows:
            data = seq[:size]

            if not data:
                continue

            wins = data.count(nv)
            losses = len(data) - wins

            # Beta posterior
            probability = (
                losses + 1
            ) / (
                len(data) + 2
            )

            weight = 1 / size

            total_score += probability * weight
            total_weight += weight

        scores[nv] = (
            total_score / total_weight
            if total_weight
            else 0.5
        )

    return sorted(scores, key=scores.get, reverse=True)[:so_nv]
    
#12
def choose_not_winner_entropy(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    recent = seq[:30]
    scores = {}

    for nv in range(1, 7):
        p = recent.count(nv) / len(recent)

        if p <= 0:
            scores[nv] = 1.0
            continue
        scores[nv] = -p * math.log(p)
    return sorted(scores, key=scores.get, reverse=True)[:so_nv]
    
#13
def choose_not_winner_opponent(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    seq = _sprint_history_sequence(history)

    if not seq:
        return random.sample(range(1, 7), so_nv)

    current = seq[0]
    scores = {}

    for nv in range(1, 7):
        if nv == current:
            scores[nv] = 0.25
            continue

        occurrences = 0
        non_winner = 0

        for i in range(len(seq) - 1):
            if seq[i] == current:
                occurrences += 1

                if seq[i + 1] != nv:
                    non_winner += 1

        scores[nv] = (
            non_winner / occurrences
            if occurrences
            else 0.5
        )

    return sorted(scores, key=scores.get, reverse=True)[:so_nv]
#================TỔNG HỢP NOT_WINNER_FREE ================================
def choose_not_winner_free(history, so_nv):
    try:
        so_nv = max(1, min(int(so_nv), 6))
    except Exception:
        so_nv = 1

    if not history:
        return random.sample(range(1, 7), so_nv)

    logic_funcs = [
        choose_not_winner_frequency,
        choose_xac_suat_free,
        choose_not_winner_transition,
        choose_not_winner_markov,
        choose_not_winner_bayesian,
        choose_not_winner_momentum,
        choose_not_winner_cycle,
        choose_not_winner_balance,
        choose_not_winner_multilevel,
        choose_not_winner_stability,
        choose_not_winner_bayes_multi,
        choose_not_winner_entropy,
        choose_not_winner_opponent,
    ]

    # Điểm tổng hợp từ kết quả của các logic
    scores = {nv: 0.0 for nv in range(1, 7)}
    valid_results = 0

    for func in logic_funcs:
        try:
            result = func(history, so_nv)

            if not result:
                continue

            # Chấp nhận kết quả dạng danh sách số
            ranking = []
            for item in result:
                try:
                    nv = int(item)
                    if 1 <= nv <= 6 and nv not in ranking:
                        ranking.append(nv)
                except (TypeError, ValueError):
                    continue

            if not ranking:
                continue

            valid_results += 1


            for rank, nv in enumerate(ranking):
                rank_score = (len(ranking) - rank) / len(ranking)
                scores[nv] += rank_score

        except Exception:
            continue

    if valid_results == 0:
        return random.sample(range(1, 7), so_nv)

    # Chuẩn hóa điểm theo số logic chạy thành công
    for nv in scores:
        scores[nv] /= valid_results

    # Xếp hạng theo điểm tổng hợp của 13 logic
    ranking = sorted(
        scores,
        key=lambda nv: scores[nv],
        reverse=True
    )

    return ranking[:so_nv]

# ================== LOGIC VIP ==================

def choose_vip_cdtd(history, so_nv):
    seq = _sprint_history_sequence(history)
    if not seq:
        return random.sample(range(1, 7), so_nv)
    try:
        so_nv = max(1, min(6, int(so_nv)))
    except Exception:
        so_nv = 1

    total = len(seq)
    fs = {
        "frequency": {nv: 0.0 for nv in range(1, 7)},
        "delay": {nv: 0.0 for nv in range(1, 7)},
        "streak": {nv: 0.0 for nv in range(1, 7)},
        "recent": {nv: 0.0 for nv in range(1, 7)},
        "markov1": {nv: 0.0 for nv in range(1, 7)},
        "markov2": {nv: 0.0 for nv in range(1, 7)},
        "last10": {nv: 0.0 for nv in range(1, 7)},
        "long_history": {nv: 0.0 for nv in range(1, 7)},
        "cycle": {nv: 0.0 for nv in range(1, 7)},
        "repeat": {nv: 0.0 for nv in range(1, 7)},
        "ai_module": {nv: 0.0 for nv in range(1, 7)},
    }

    for nv in range(1, 7):
        wins = seq.count(nv)
        rate = wins / total
        fs["frequency"][nv] = (0.50 - rate) * 46

        delay = _sprint_delay(seq, nv)
        d = min(delay * 1.0, 16)
        if delay >= 25:
            d += 18
        elif delay >= 18:
            d += 14
        elif delay >= 12:
            d += 10
        elif delay >= 6:
            d += 5
        elif delay <= 1:
            d -= 6
        fs["delay"][nv] = d

        streak = _sprint_current_streak(seq, nv)
        fs["streak"][nv] = min(streak * 8.0, 28) if streak else 0.0
        fs["recent"][nv] = _sprint_weighted_recent_hits(seq, nv, limit=50, decay=0.92) * 4.2

        if len(seq) >= 2:
            fs["markov1"][nv] = _sprint_follow_probability(seq, seq[0], nv) * 42

        if len(seq) >= 3:
            a, b = seq[1], seq[0]
            total_pair = hit = 0
            for i in range(len(seq) - 2):
                if seq[i + 2] == a and seq[i + 1] == b:
                    total_pair += 1
                    if seq[i] == nv:
                        hit += 1
            if total_pair:
                fs["markov2"][nv] = (hit / total_pair) * 58

        count10 = sum(1 for x in seq[:10] if x == nv)
        fs["last10"][nv] = 8 if count10 == 0 else 4 if count10 == 1 else -12 if count10 >= 4 else 0

        long_score = 0.0
        h100 = seq[:100]
        if h100:
            w100 = h100.count(nv)
            r100 = w100 / len(h100)
            long_score += r100 * 20
            if w100 >= 30:
                long_score += 10
            elif w100 >= 20:
                long_score += 6
            elif w100 == 0:
                long_score -= 8

        h200 = seq[:200]
        if h200:
            w200 = h200.count(nv)
            r200 = w200 / len(h200)
            long_score += r200 * 14
            if w200 >= 50:
                long_score += 12
            elif w200 >= 35:
                long_score += 8
        fs["long_history"][nv] = long_score

        avg_gap = _sprint_average_gap(seq, nv)
        if avg_gap:
            fs["cycle"][nv] = (
                10 if delay >= avg_gap * 1.45
                else 5 if delay >= avg_gap
                else -5 if delay <= avg_gap * 0.6
                else 0
            )

        if len(seq) >= 2 and seq[0] == seq[1] == nv:
            fs["repeat"][nv] = 6

    try:
        ai = analyze_cdtd_ai(history, "winner", "vip")
        fs["ai_module"] = {
            nv: (float(ai["scores"].get(nv, 50.0)) - 50.0) * 1.15
            for nv in range(1, 7)
        }
    except Exception:
        pass

    scores = {nv: 96.0 for nv in range(1, 7)}
    for feature_scores in fs.values():
        for nv, value in feature_scores.items():
            scores[nv] += float(value)

    items = list(scores.items())
    random.shuffle(items)
    items.sort(key=lambda item: item[1], reverse=True)
    return [nv for nv, _ in items[:so_nv]]


def choose_not_winner_vip(history, so_nv):
    seq = _sprint_history_sequence(history)
    if not seq:
        return random.sample(range(1, 7), so_nv)
    try:
        so_nv = max(1, min(6, int(so_nv)))
    except Exception:
        so_nv = 1

    total = len(seq)
    fs = {
        "frequency": {nv: 0.0 for nv in range(1, 7)},
        "delay": {nv: 0.0 for nv in range(1, 7)},
        "streak": {nv: 0.0 for nv in range(1, 7)},
        "recent": {nv: 0.0 for nv in range(1, 7)},
        "markov1": {nv: 0.0 for nv in range(1, 7)},
        "markov2": {nv: 0.0 for nv in range(1, 7)},
        "last10": {nv: 0.0 for nv in range(1, 7)},
        "long_history": {nv: 0.0 for nv in range(1, 7)},
        "cycle": {nv: 0.0 for nv in range(1, 7)},
        "repeat": {nv: 0.0 for nv in range(1, 7)},
        "ai_module": {nv: 0.0 for nv in range(1, 7)},
    }

    for nv in range(1, 7):
        wins = seq.count(nv)
        rate = wins / total
        fs["frequency"][nv] = (1 - rate) * 42

        delay = _sprint_delay(seq, nv)
        d = min(delay * 0.45, 8)
        if delay >= 25:
            d += 20
        elif delay >= 18:
            d += 16
        elif delay >= 12:
            d += 11
        elif delay >= 6:
            d += 6
        elif delay <= 1:
            d -= 12
        fs["delay"][nv] = d

        streak = _sprint_current_streak(seq, nv)
        if streak:
            fs["streak"][nv] = -min(streak * 9.0, 30)
        fs["recent"][nv] = -_sprint_weighted_recent_hits(seq, nv, limit=50, decay=0.92) * 5.5

        if len(seq) >= 2:
            fs["markov1"][nv] = -_sprint_follow_probability(seq, seq[0], nv) * 40

        if len(seq) >= 3:
            a, b = seq[1], seq[0]
            total_pair = hit = 0
            for i in range(len(seq) - 2):
                if seq[i + 2] == a and seq[i + 1] == b:
                    total_pair += 1
                    if seq[i] == nv:
                        hit += 1
            if total_pair:
                fs["markov2"][nv] = -(hit / total_pair) * 60

        count10 = sum(1 for x in seq[:10] if x == nv)
        fs["last10"][nv] = 10 if count10 == 0 else 5 if count10 == 1 else -15 if count10 >= 4 else 0

        long_score = 0.0
        h100 = seq[:100]
        if h100:
            w100 = h100.count(nv)
            r100 = w100 / len(h100)
            long_score += (1 - r100) * 26
            if w100 == 0:
                long_score += 12
            elif w100 <= 3:
                long_score += 7
            elif w100 >= 20:
                long_score -= 12

        h200 = seq[:200]
        if h200:
            w200 = h200.count(nv)
            r200 = w200 / len(h200)
            long_score += (1 - r200) * 16
            if w200 >= 45:
                long_score -= 14
        fs["long_history"][nv] = long_score

        avg_gap = _sprint_average_gap(seq, nv)
        if avg_gap:
            fs["cycle"][nv] = (
                8 if delay >= avg_gap * 1.5
                else -6 if delay <= avg_gap * 0.7
                else 0
            )

        if len(seq) >= 2 and seq[0] == seq[1] == nv:
            fs["repeat"][nv] = -8

    try:
        ai = analyze_cdtd_ai(history, "not_winner", "vip")
        fs["ai_module"] = {
            nv: (float(ai["scores"].get(nv, 50.0)) - 50.0) * 1.15
            for nv in range(1, 7)
        }
    except Exception:
        pass

    scores = {nv: 104.0 for nv in range(1, 7)}
    for feature_scores in fs.values():
        for nv, value in feature_scores.items():
            scores[nv] += float(value)

    items = list(scores.items())
    random.shuffle(items)
    items.sort(key=lambda item: item[1], reverse=True)
    return [nv for nv, _ in items[:so_nv]]


class SprintGame:

    def __init__(self):
        self.session = requests.Session()
        self.headers = {}
        self.coin = ""
        self.bet_amount = 0
        self.he_so = 0
        self.type_bet = ""
        self.so_nv = 0
        self.logic_type = "free"
        self.target_profit = 0.0
        self.stop_loss = 0.0
        self.rest_after_lose = 0
        self.anti_soi_after = 0
        self.auto_bet = True
        self.daily_pause_window = None

    def home(self, coin):
        params = {
            "asset": coin
        }

        while True:
            try:
                response = self.session.get(
                    "https://api.sprintrun.win/sprint/home",
                    params=params,
                    headers=self.headers,
                    timeout=10
                )

                return response.text

            except requests.exceptions.RequestException as e:
                cprints("ruby", f"⚠️ Lỗi kết nối API: {e}")
                cprints("yellow", "🔄 Đang kết nối lại...")
                time.sleep(2)

    def info(self, ki, coin):
        while True:
            try:
                params = {
                    "issue": str(ki),
                    "asset": coin,
                }

                response = self.session.get(
                    "https://api.sprintrun.win/sprint/issue_result",
                    params=params,
                    headers=self.headers,
                    timeout=10
                )

                return json.loads(response.text)

            except Exception as e:
                cprints("ruby", f"⚠️ Lỗi lấy kết quả: {e}")
                time.sleep(2)

    def bet(self, ki, kq, coin, bet_amount, type_bet):
        # Lớp bảo vệ cuối cùng: không gửi lệnh cược trong giờ nghỉ đã cài.
        if _check_daily_pause_window(
            getattr(self, "daily_pause_window", None), "CDTD"
        ):
            return False
        try:
            json_data = {
                "issue_id": int(ki),
                "bet_group": type_bet,
                "asset_type": coin,
                "athlete_id": kq,
                "bet_amount": bet_amount,
            }

            response = self.session.post(
                "https://api.sprintrun.win/sprint/bet",
                headers=self.headers,
                json=json_data,
                timeout=10
            )

            res = json.loads(response.text)

            if res.get("code") == 0:
                return True

        except Exception as e:
            cprints("ruby", f"❌ Lỗi đặt cược {coin}: {e}")

        return False

    def random_result(self, so_nguoi):
        li = [1, 2, 3, 4, 5, 6]
        kq = []

        for _ in range(int(so_nguoi)):
            x = random.choice(li)
            kq.append(x)
            li.remove(x)

        return kq

    def check_result(self, coin, bot_chon, type_game):
        try:
            res = json.loads(self.home(coin))
        except Exception as e:
            cprints("ruby", f"❌ Lỗi lấy dữ liệu trận đấu: {e}")
            return None

        try:
            ki = res["data"]["issue_id"]
        except Exception:
            cprints("ruby", "❌ Không lấy được mã ván đấu")
            return None

        while True:
            try:
                res = json.loads(self.home(coin))
                raw_sec = int(res.get("data", {}).get("expire_seconds", 0))
                expire_sec = max(0, raw_sec - 1)
                if expire_sec <= 1:
                    break
                time.sleep(1)
            except Exception as e:
                cprints("ruby", f"⚠️ Lỗi đếm thời gian: {e}")
                time.sleep(2)

        time.sleep(8)
        re_end = self.info(ki, coin)

        try:
            rank = [int(x) for x in re_end["data"]["athlete_rank"]]
            top1 = rank[0]
            award = re_end["data"]["my_total_award"]
            won = top1 in bot_chon if type_game == "winner" else top1 not in bot_chon

            return {
                "issue_id": re_end["data"]["issue_id"],
                "top1": top1,
                "my_total_bet": re_end["data"]["my_total_bet"],
                "my_total_award": re_end["data"]["my_total_award"],
                "kq": top1 in bot_chon if type_game == "winner" else top1 not in bot_chon,
                "bot_chon": bot_chon,
            }

        except Exception as e:
            cprints("ruby", f"❌ Lỗi xử lý kết quả: {e}")
            return None

    def vanchoi(self, coin, history, he_so, bet_amount0, type_bet, so_nv):

        if _check_daily_pause_window(
            getattr(self, "daily_pause_window", None), "CDTD"
        ):
            return None

        if self.logic_type == "vip":
            try:
                is_valid, msg, is_vip = check_activation_valid(exit_on_expired=False)
                if not is_valid:
                    cprints("ruby", f"❌ {msg or 'Key không còn hiệu lực.'}")
                    self.logic_type = "free"
                elif not is_vip:
                    cprints("ruby", "❌ Key FREE không được sử dụng Logic VIP!")
                    self.logic_type = "free"
            except Exception as exc:
                log_debug(f"CDTD VIP permission check error: {exc}")
                self.logic_type = "free"

        try:
            res = json.loads(self.home(coin))
            ki = res["data"]["issue_id"]

        except Exception as e:
            cprints("ruby", f"❌ Lỗi lấy ván hiện tại: {e}")
            return None

        analysis_time = 10

        while True:
            try:
                if _check_daily_pause_window(
                    getattr(self, "daily_pause_window", None), "CDTD"
                ):
                    return None
                timer = json.loads(self.home(coin))
                expire = int(timer.get("data", {}).get("expire_seconds", 0))
                if expire <= analysis_time:
                    break
                time.sleep(1)
            except Exception as e:
                cprints("ruby", f"⚠️ Lỗi đếm giờ: {e}")
                time.sleep(2)

        if _check_daily_pause_window(
            getattr(self, "daily_pause_window", None), "CDTD"
        ):
            return None

        if type_bet == "winner":
            if self.logic_type == "free":
                bot_chon = choose_free_cdtd(history, int(so_nv))
            elif self.logic_type == "vip":
                bot_chon = choose_vip_cdtd(history, int(so_nv))
            else:
                bot_chon = self.random_result(int(so_nv))

        elif type_bet == "not_winner":
            if self.logic_type == "free":
                bot_chon = choose_not_winner_free(history, int(so_nv))
            elif self.logic_type == "vip":
                bot_chon = choose_not_winner_vip(history, int(so_nv))
            else:
                bot_chon = self.random_result(int(so_nv))
        else:
            bot_chon = self.random_result(int(so_nv))

        ten_nv = [NV.get(i, str(i)) for i in bot_chon]

        bet_amount = bet_amount0 / len(bot_chon)

        if history and history[0].get("kq") is False:
            previous_total_bet = float(history[0].get("my_total_bet") or 0)
            if previous_total_bet > 0:
                bet_amount = (previous_total_bet * he_so) / len(bot_chon)

        total_round_bet = float((bet_amount * max(1, len(bot_chon))) or 0)
        self._telegram_current_bet = total_round_bet
        if _check_daily_pause_window(
            getattr(self, "daily_pause_window", None), "CDTD"
        ):
            return None
        _cdtd_telegram_round_bet(self, ki, total_round_bet, bot_chon, self.auto_bet)

        if self.auto_bet:

            try:
                pending_record = {
                    "issue_id": int(ki),
                    "bot_chon": list(bot_chon),
                    "selected": list(bot_chon),
                    "top1": None,
                    "my_total_bet": float(bet_amount * len(bot_chon)),
                    "my_total_award": 0.0,
                    "kq": None,
                    "status": "ĐANG CƯỢC",
                    "logic_type": self.logic_type,
                    "_pending_cdtd": True,
                }
                history[:] = [h for h in history if not (isinstance(h, dict) and h.get("_pending_cdtd") and str(h.get("issue_id")) == str(ki))]
                history.insert(0, pending_record)
            except Exception as e:
                log_debug(f"CDTD pending history error: {e}")

            while True:
                try:
                    if _check_daily_pause_window(
                        getattr(self, "daily_pause_window", None), "CDTD"
                    ):
                        history[:] = [
                            item for item in history
                            if not (
                                isinstance(item, dict)
                                and item.get("_pending_cdtd")
                            )
                        ]
                        return None
                    timer = json.loads(self.home(coin))
                    expire = int(timer.get("data", {}).get("expire_seconds", 0))
                    if expire <= 10:
                        break
                    time.sleep(1)
                except Exception as e:
                    cprints("ruby", f"⚠️ Lỗi đếm giờ: {e}")
                    time.sleep(2)

            try:
                res = json.loads(self.home(coin))
                ki = res["data"]["issue_id"]
            except Exception:
                ki = ki

            placed_count = 0
            for i in bot_chon:
                try:
                    if _check_daily_pause_window(
                        getattr(self, "daily_pause_window", None), "CDTD"
                    ):
                        history[:] = [
                            item for item in history
                            if not (
                                isinstance(item, dict)
                                and item.get("_pending_cdtd")
                            )
                        ]
                        return None
                    if self.bet(ki, i, coin, bet_amount, type_bet):
                        placed_count += 1
                except Exception as e:
                    log_debug(f"CDTD place bet error P{i}: {e}")

          
            try:
                for item in history:
                    if isinstance(item, dict) and item.get("_pending_cdtd") and str(item.get("issue_id")) == str(ki):
                        item["status"] = "ĐÃ ĐẶT CƯỢC" if placed_count > 0 else "CƯỢC THẤT BẠI"
                        item["my_total_bet"] = float(bet_amount * max(1, placed_count if placed_count else len(bot_chon)))
                        break
            except Exception as e:
                log_debug(f"CDTD pending history update error: {e}")


        return self.check_result(
            coin,
            bot_chon,
            type_bet,
        )
        
    def user_asset(self):
        while True:
            try:
                json_data = {
                    "user_id": int(self.headers["user-id"]),
                    "source": "home",
                }

                response = self.session.post(
                    "https://wallet.3games.io/api/wallet/user_asset",
                    headers=self.headers,
                    json=json_data,
                    timeout=10,
                )

                res = json.loads(response.text)

                asset = {
                    "USDT": res["data"]["user_asset"]["USDT"],
                    "WORLD": res["data"]["user_asset"]["WORLD"],
                    "BUILD": res["data"]["user_asset"]["BUILD"],
                }

                return asset

            except requests.exceptions.RequestException as e:
                cprints("ruby", f"⚠️ Lỗi kết nối lấy số dư: {e}")
                cprints("yellow", "🔄 Đang thử lấy lại số dư...")
                time.sleep(2)

            except Exception as e:
                cprints("ruby", f"❌ Lỗi khi lấy số dư: {e}")
                time.sleep(2)

def _read_float_input(prompt: str, default: float = 0.0) -> float:
    while True:
        raw = input(prompt).strip()
        if raw == "":
            return default
        try:
            return float(raw)
        except ValueError:
            cprints("ruby", "❌ Giá trị không hợp lệ, vui lòng nhập số.")

def _read_int_input(prompt: str, default: int = 0):
    while True:
        raw = input(prompt).strip()
        if raw == "":
            return default
        try:
            return int(raw)
        except ValueError:
            cprints("ruby", "❌ Giá trị không hợp lệ, vui lòng nhập số nguyên.")

def _fmt_cdtd_number(value: Any, decimals: int = 6) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "0"
    text = f"{num:.{decimals}f}".rstrip("0").rstrip(".")
    return text if text not in ("", "-0") else "0"

def _cdtd_capital_stop_reason(game, current_balance) -> tuple[bool, str]:
    try:
        current_balance = float(current_balance)
    except (TypeError, ValueError):
        return False, ""

    target_balance = float(getattr(game, "target_profit", 0.0) or 0.0)
    stop_balance = float(getattr(game, "stop_loss", 0.0) or 0.0)
    coin = str(getattr(game, "coin", "BUILD") or "BUILD")

    if target_balance > 0 and current_balance >= target_balance:
        return True, (
            f"🎉Đã đạt mục tiêu lãi ({_fmt_cdtd_number(current_balance)} {coin}). Dừng tool."
        )

    if stop_balance > 0 and current_balance <= stop_balance:
        return True, (
            f"💀Đã chạm cắt lỗ ({_fmt_cdtd_number(current_balance)} {coin}). Dừng tool."
        )

    return False, ""

def _cdtd_telegram_round_bet(game, issue_id, total_bet, bot_chon, auto_bet=True):

    coin = str(getattr(game, "coin", "BUILD") or "BUILD").upper()
    total = float(total_bet or 0)
    game._telegram_current_bet = total
    game._telegram_selected_names = ", ".join(NV.get(int(x), str(x)) for x in (bot_chon or [])) or "-"
    cprints(
        "gold",
        f"📌 VÁN #{issue_id}\n"
        f"🎯 Đánh : {game._telegram_selected_names}\n"
        f"💰 Cược: {_fmt_cdtd_number(total)} {coin}"
    )
def _cdtd_telegram_round_result(game, res, user_id, balance_before=None, balance_after=None, stats=None, run_start=None, start_balance=None):

    coin = str(getattr(game, "coin", "BUILD") or "BUILD").upper()
    res = res if isinstance(res, dict) else {}
    issue = res.get("issue_id", "-")
    top1 = res.get("top1")
    top1_name = NV.get(int(top1), str(top1)) if top1 is not None else "-"
    bet = float(res.get("my_total_bet") or 0)
    award = float(res.get("my_total_award") or 0)
    won = bool(res.get("kq"))

    if start_balance is not None and balance_after is not None:
        try:
            profit = float(balance_after) - float(start_balance)
        except (TypeError, ValueError):
            profit = 0.0
    else:

        profit = award - bet
    stats = stats or {}
    win_streak = int(stats.get("win_streak", 0) or 0)
    max_win_streak = int(stats.get("max_win_streak", 0) or 0)
    lose_streak = int(stats.get("lose_streak", 0) or 0)
    max_lose_streak = int(stats.get("max_lose_streak", 0) or 0)
    if run_start is not None:
        elapsed = max(0, int(time.time() - float(run_start)))
    else:
        elapsed = 0
    run_h, rem = divmod(elapsed, 3600)
    run_m = rem // 60

    balance_from = start_balance if start_balance is not None else (balance_before if balance_before is not None else balance_after)
    balance_to = balance_after if balance_after is not None else balance_before
    selected_names = getattr(game, "_telegram_selected_names", "-") or "-"
    result_line = "✅ WIN" if won else "❌ LOSE"
    logic_label = str(getattr(game, "logic_type", "free") or "free").upper()
    lines = [
        f"👤 User:{user_id}",
        f"🧠 LOGIC {logic_label}",
        f"📌 Ván: #{issue}",
        f"🎯 Đánh: {selected_names}",
        f"👑 Quán Quân: {top1_name}",
        f"🏆 WIN:{win_streak}(MAX:{max_win_streak}) | 💀 LOSE:{lose_streak}(MAX:{max_lose_streak})",
        result_line,
        f"📊 Lãi/lỗ {profit:+,.6f} {coin}",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"💳 SỐ DƯ: {_fmt_cdtd_number(balance_from)} -> {_fmt_cdtd_number(balance_to)}",
        f"⏳ CHẠY BOT: {run_h} Giờ {run_m} Phút",
    ]
    cprints("bright_green" if won else "ruby", "\n".join(lines))

def cauhinh_game(game):
    time.sleep(0.6)
    cprints("bright_green", "💰 CHỌN LOẠI TIỀN CƯỢC")

    while True:
        coin_key = input().strip().upper()
        coin_map = {
            "USDT": "USDT",
            "BUILD": "BUILD",
            "XWORLD": "WORLD",
        }

        if coin_key in coin_map:
            game.coin = coin_map[coin_key]
            break

        cprints(
            "ruby",
            "❌ Lựa chọn không hợp lệ, vui lòng chọn USDT, BUILD hoặc XWORLD.",
        )

    while True:
        game.bet_amount = _read_float_input(
            f"1. Nhập số {game.coin} cược mỗi ván: ",
            0.0,
        )
        if game.bet_amount > 0:
            break
        cprints("ruby", "❌ Tiền cược phải lớn hơn 0.")

    while True:
        game.he_so = _read_float_input(
            "2. Nhập hệ số cược sau thua: ",
            1.0,
        )
        if game.he_so >= 1:
            break
        cprints("ruby", "❌ Hệ số cược phải lớn hơn hoặc bằng 1.")

    cprints("bright_green", "3. Chọn loại đặt:")

    while True:
        type_bet = input().strip().casefold()

        if type_bet == "ai là quán quân":
            game.type_bet = "winner"
            print(
                "✅ Đã chọn: Ai là quán quân\n\n"
                "4. Đặt vào bao nhiêu người:",
                flush=True,
            )
            break

        if type_bet == "ai không là quán quân":
            game.type_bet = "not_winner"
            print(
                "✅ Đã chọn: Ai không là quán quân\n\n"
                "4. Đặt vào bao nhiêu người:",
                flush=True,
            )
            break

        cprints(
            "ruby",
            "❌ Vui lòng chọn: Ai là quán quân hoặc Ai không là quán quân.",
        )

    while True:
        try:
            game.so_nv = int(input().strip())
        except (ValueError, TypeError):
            cprints("ruby", "❌ Vui lòng chọn số người từ 1 đến 6")
            continue

        if 1 <= game.so_nv <= 6:
            break

        cprints("ruby", "❌ Vui lòng chọn số người từ 1 đến 6")

    cprints(
        "bright_green",
        "5. Mục tiêu lãi: Nhập số BUILD muốn đặt để dừng (Nhập 0 = tắt):",
    )

    game.target_profit = _read_float_input("", 0.0)
    game.stop_loss = _read_float_input(
        "6. Cắt lỗ: Nhập số BUILD còn lại để dừng (Nhập 0 = tắt): ",
        0.0,
    )
    game.daily_pause_window = _prompt_daily_pause_window("CDTD", "7")
    _reset_daily_pause_state("CDTD")
    game.rest_after_lose = _read_int_input(
        "8. Nghỉ sau khi thua: Nghỉ bao nhiêu ván? (0 = Tắt): ",
        0,
    )
    game.anti_soi_after = _read_int_input(
        "9. Chống soi: Nghỉ 1 ván sau bao nhiêu ván cược? (0 = Tắt): ",
        0,
    )

    game.target_profit = max(0.0, float(game.target_profit))
    game.stop_loss = max(0.0, float(game.stop_loss))
    game.rest_after_lose = max(0, int(game.rest_after_lose))
    game.anti_soi_after = max(0, int(game.anti_soi_after))

    json_config = {
        "Coin": game.coin,
        "he_so": game.he_so,
        "bet_amount0": game.bet_amount,
        "type_bet": game.type_bet,
        "So_nv": game.so_nv,
        "target_profit": game.target_profit,
        "stop_loss": game.stop_loss,
        "daily_pause_window": _format_daily_pause_window(game.daily_pause_window),
        "rest_after_lose": game.rest_after_lose,
        "anti_soi_after": game.anti_soi_after,
        "auto_bet": bool(getattr(game, "auto_bet", True)),
    }

    try:
        with open("config-cdtd.txt", "w", encoding="utf-8") as f:
            json.dump(
                json_config,
                f,
                indent=4,
                ensure_ascii=False,
            )

        cprints("bright_green", "✅ Đã lưu cấu hình.")

    except OSError as e:
        cprints("ruby", f"❌ Không thể lưu cấu hình: {e}")

    return game

def _cdtd_make_game(account, base_game):
    game = SprintGame()
    game.headers = {
        "accept": "*/*",
        "accept-language": "vi,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://sprintrun.win",
        "user-agent": (
            "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/141.0.0.0 Mobile Safari/537.36"
        ),
        "user-id": str(account.get("user-id") or account.get("userId") or ""),
        "user-login": "login_v2",
        "user-secret-key": str(account.get("user-secret-key") or account.get("secretKey") or ""),
    }
    for attr in (
        "coin", "bet_amount", "he_so", "type_bet", "so_nv", "logic_type",
        "target_profit", "stop_loss", "daily_pause_window", "rest_after_lose", "anti_soi_after", "auto_bet",
    ):
        if hasattr(base_game, attr):
            setattr(game, attr, getattr(base_game, attr))
    game._telegram_current_bet = float(getattr(game, "bet_amount", 0) or 0)
    return game


def _cdtd_run_accounts(accounts, base_game, is_vip):

    def worker(index, account):
        game = _cdtd_make_game(account, base_game)
        user_id = str(account["user-id"])
        stats = {
            "win": 0, "lose": 0, "win_streak": 0, "max_win_streak": 0,
            "lose_streak": 0, "max_lose_streak": 0, "earn": 0.0,
        }
        history = []
        skip_rounds = 0

        start_balance = None
        current_balance = 0.0
        cumulative_round_profit = 0.0
        run_start = time.time()

        if False:
            cprints("bright_green", f"✅ CDTD tài khoản {index}: userId={user_id} đã bắt đầu chạy.")
        while True:
            try:
                if _check_daily_pause_window(
                    getattr(game, "daily_pause_window", None), "CDTD"
                ):
                    time.sleep(1)
                    continue
                if not check_activation_valid(exit_on_expired=False)[0]:
                    cprints("ruby", f"⛔ Key hết hạn — dừng tài khoản {user_id}.")
                    return
                if skip_rounds > 0:
                    skip_rounds -= 1
                    time.sleep(2)
                    continue
                asset = game.user_asset()
                if not isinstance(asset, dict):
                    time.sleep(3)
                    continue
                current_balance = float(asset.get(game.coin, 0) or 0)
                if start_balance is None:
                    start_balance = current_balance
                should_stop, stop_reason = _cdtd_capital_stop_reason(game, current_balance)
                if should_stop:
                    cprints("gold", f"🛑 {user_id}: {stop_reason}")
                    return
                balance_before_round = current_balance
                res = game.vanchoi(
                    game.coin, history, game.he_so, game.bet_amount,
                    game.type_bet, game.so_nv,
                )
                if res is None:
                    time.sleep(2)
                    continue
                won = bool(res.get("kq"))
                bet = float(res.get("my_total_bet") or 0)
                award = float(res.get("my_total_award") or 0)


                try:
                    result_issue = str(res.get("issue_id", ""))
                    for item in history:
                        if isinstance(item, dict) and str(item.get("issue_id", "")) == result_issue:
                            item["kq"] = won
                            item["top1"] = res.get("top1")
                            item["my_total_bet"] = bet
                            item["my_total_award"] = award
                            item["status"] = "THẮNG" if won else "THUA"
                            item.pop("_pending_cdtd", None)
                            break

                    updated = next((item for item in history if isinstance(item, dict) and str(item.get("issue_id", "")) == result_issue), None)
                    if updated is not None:
                        history.remove(updated)
                        history.insert(0, updated)
                except Exception as e:
                    log_debug(f"CDTD result history update error: {e}")

                if won:
                    stats["win"] += 1
                    stats["win_streak"] += 1
                    stats["lose_streak"] = 0
                    stats["max_win_streak"] = max(stats["max_win_streak"], stats["win_streak"])
                else:
                    stats["lose"] += 1
                    stats["lose_streak"] += 1
                    stats["win_streak"] = 0
                    stats["max_lose_streak"] = max(stats["max_lose_streak"], stats["lose_streak"])
                    if game.rest_after_lose > 0:
                        skip_rounds = game.rest_after_lose
                balance_after_round = balance_before_round
                try:

                    post_asset = game.user_asset()
                    if isinstance(post_asset, dict):
                        live_after = post_asset.get(game.coin)
                        if live_after is not None:
                            balance_after_round = float(live_after)
                            current_balance = balance_after_round
                except Exception as e:
                    log_debug(f"CDTD post-round balance error: {e}")

                if start_balance is not None and balance_after_round is not None:
                    try:
                        cumulative_round_profit = float(balance_after_round) - float(start_balance)
                    except (TypeError, ValueError):
                        cumulative_round_profit = award - bet
                else:
                    cumulative_round_profit = award - bet
                stats["earn"] = cumulative_round_profit

                _cdtd_telegram_round_result(
                    game,
                    res,
                    user_id=user_id,
                    balance_before=balance_before_round,
                    balance_after=balance_after_round,
                    stats=stats,
                    run_start=run_start,
                    start_balance=start_balance,
                )
                if game.anti_soi_after > 0 and len(history) % game.anti_soi_after == 0:
                    cprints("gold", "🛡️ Chống soi: nghỉ 1 ván theo cấu hình.")
                    skip_rounds = max(skip_rounds, 1)
            except KeyboardInterrupt:
                return
            except Exception as exc:
                cprints("ruby", f"❌ CDTD userId={user_id} lỗi: {type(exc).__name__}: {exc}")
                time.sleep(3)


    if not accounts:
        cprints("ruby", "❌ Chưa có tài khoản CDTD để chạy.")
        return
    try:
        worker(1, accounts[0])
    except KeyboardInterrupt:
        cprints("yellow", "🛑 Đang dừng CDTD...")


def main_cdtd():
    start_key_expiry_monitor()
    if not check_activation_valid()[0]:
        cprints("ruby", "❌ Key của bạn không còn hiệu lực.")
        return

    console.clear()
    am = AccountManager()
    cprints("gold", "📋 CHẠY ĐUA TỐC ĐỘ\n")
    cprints("white", "Gửi link game CDTD có userId và secretKey để thêm tài khoản.")
    cprints("dim" if "dim" in VBTOOL_COLORS else "white", "")

    try:
        raw_link = input("🔗 Dán link game: ").strip()
        if raw_link == "0":
            cprints("yellow", "🛑 Đã hủy.")
            return

        while True:
            acc = am.parse_link_to_account(raw_link)
            if acc:
                am.accounts.append(acc)
                am.save()
                break
            cprints("ruby", "❌ Link không hợp lệ! Link phải chứa userId và secretKey.")
            raw_link = input("🔗 Dán lại link game: ").strip()
            if raw_link == "0":
                return

        cprints("bright_green", f"✅ Đã thêm tài khoản: {am.accounts[-1].get('user-id') or am.accounts[-1].get('userId')}")
        selected_accounts = [am.accounts[-1]]

        game = cauhinh_game(SprintGame())
        is_valid, msg, is_vip = check_activation_valid(exit_on_expired=False)
        if not is_valid:
            cprints(
                "ruby",
                f"🔒 {msg}. Vui lòng nhập lại Key FREE hoặc Key VIP.",
            )
            return

        cprints("gold", "9.CHỌN LOGIC")
        while True:
            choice = input().strip().upper()

            # Xác thực lại sau khi người dùng nhập lựa chọn LOGIC.
            # Key hết hạn thì không được chọn tiếp kể cả LOGIC FREE.
            is_valid, msg, is_vip = check_activation_valid(exit_on_expired=False)
            if not is_valid:
                cprints(
                    "ruby",
                    f"🔒 {msg}. Vui lòng nhập lại Key FREE hoặc Key VIP.",
                )
                return

            if choice in ("1", "🆓 LOGIC FREE", "LOGIC FREE", "FREE"):
                game.logic_type = "free"
                cprints("bright_green", "✅ Đã chọn Logic FREE")
                break
            if choice in ("2", "👑 LOGIC VIP", "LOGIC VIP", "VIP"):
                if is_vip:
                    game.logic_type = "vip"
                    cprints("bright_green", "✅ Đã chọn Logic VIP")
                    break
                cprints("ruby", "❌ Key FREE không được sử dụng Logic VIP!")
                continue
            cprints("ruby", "❌ Lựa chọn không hợp lệ!")

        acc = selected_accounts[0]
        user_id = acc.get("user-id") or acc.get("userId") or "—"
        secret = acc.get("user-secret-key") or acc.get("secretKey") or ""
        balance = None

        try:
            uid_int = int(str(user_id))
            build, world, usdt = fetch_balances_3games(
                retries=2,
                timeout=6,
                params={"userId": str(user_id)},
                uid=uid_int,
                secret=secret,
            )
            coin = str(getattr(game, "coin", "BUILD") or "BUILD").upper()
            balance = (
                build if coin == "BUILD"
                else world if coin in ("XWORLD", "WORLD")
                else usdt if coin == "USDT"
                else None
            )
        except Exception as e:
            log_debug(f"CDTD config balance error: {e}")

        coin = str(getattr(game, "coin", "BUILD") or "BUILD").upper()
        base = float(getattr(game, "bet_amount", 0) or 0)
        mult = float(getattr(game, "he_so", 1) or 1)
        target = float(getattr(game, "target_profit", 0) or 0)
        stop = float(getattr(game, "stop_loss", 0) or 0)
        anti = int(getattr(game, "anti_soi_after", 0) or 0)
        pause = int(getattr(game, "rest_after_lose", 0) or 0)

        type_label = (
            "Ai là quán quân"
            if getattr(game, "type_bet", "") == "winner"
            else "Ai không là quán quân"
        )

        balance_text = _fmt_cdtd_number(balance, 2) if balance is not None else "Không lấy được"
        target_text = f"Dừng khi số dư đạt {_fmt_cdtd_number(target, 2)} {coin}" if target > 0 else "Tắt"
        stop_text = f"Dừng khi số dư còn {_fmt_cdtd_number(stop, 2)} {coin}" if stop > 0 else "Tắt"
        schedule_text = _format_daily_pause_window(
            getattr(game, "daily_pause_window", None)
        )
        anti_text = f"{anti} ván rồi nghỉ 1 ván" if anti > 0 else "Tắt"
        pause_text = f"{pause} ván" if pause > 0 else "Tắt"

        config_lines = [
            "✅ CẤU HÌNH CDTD CỦA BẠN",
            "",
            f"👤 Tài khoản: {user_id}",
            f"💳 Số dư tài khoản: {balance_text} {coin}",
            f"🧠 LOGIC {game.logic_type.upper()}",
            f"💰 Cược mỗi ván: {_fmt_cdtd_number(base, 2)} {coin}",
            f"📈 Hệ số sau thua: x{mult:.2f}",
            f"🎯 Loại đặt: {type_label}",
            f"🛡️ Chống soi: {anti_text}",
            f"⏸️ Nghỉ sau thua: {pause_text}",
            f"🎯 Mục tiêu lãi: {target_text}",
            f"🛑 Cắt lỗ: {stop_text}",
            f"⏰ Khung giờ chạy: {schedule_text}",
        ]

        cprints("bright_green", "\n".join(config_lines))
        time.sleep(0.15)
        cprints("gold", "🚀 ĐANG KHỞI ĐỘNG CDTD...")
        _cdtd_run_accounts(selected_accounts, game, is_vip)

    except KeyboardInterrupt:
        cprints("yellow", "🛑 Đã hủy thao tác CDTD.")
    except Exception as exc:
        cprints("ruby", f"❌ Lỗi khởi tạo CDTD: {type(exc).__name__}: {exc}")

# ================== TOOL 4: CANH CODE XWORLD ==================

XW_API_DETAIL = "https://web3task.3games.io/v1/task/redcode/detail"
XW_API_EXCHANGE = "https://web3task.3games.io/v1/task/redcode/exchange"
XW_API_ASSET = "https://wallet.3games.io/api/wallet/user_asset"

XW_HEADERS = {
    "accept": "*/*",
    "accept-language": "vi,en;q=0.9",
    "content-type": "application/json",
    "country-code": "vn",
    "origin": "https://xworld.info",
    "referer": "https://xworld.info/",
    "user-agent": (
        "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 "
        "Mobile Safari/537.36"
    ),
    "language": "vi-VN",
}

XW_MAX_RETRIES = 3
XW_LOG_FILE = "history_nhap_code.log"
XW_ACCOUNT_FILE = "data_xw_confirm_code.txt"

def xw_clear_screen() -> None:
    None if True else os.system("cls" if platform.system() == "Windows" else "clear")

def xw_rgb_print(r: int, g: int, b: int, text: object, end: str = "\n") -> None:
    if True:
        print(_strip_rich_markup(text), end=end, flush=True)
        return
    print(f"\033[38;2;{r};{g};{b}m{text}\033[0m", end=end)

def xw_translate_message(message: object) -> str:

    text = str(message or "").strip()
    if not text:
        return "Không xác định"

    lower = text.lower()

    rules = [
        (("success", "successful", "ok", "成功", "succeeded"), "Thành công"),
        (("already received", "already claimed", "already", "received", "đã nhận", "领取", "已领取", "已領取"), "Mã code này đã được nhận rồi"),
        (("out of", "not exist", "no exist", "not found", "empty", "hết", "không còn", "不存在", "无效", "已失效"), "Mã code đã hết hoặc không còn khả dụng"),
        (("limit", "max", "exceed", "giới hạn", "上限", "达到上限", "已达上限"), "Đã đạt giới hạn tài khoản"),
        (("expired", "hết hạn", "过期", "已过期"), "Đã hết hạn"),
        (("invalid", "không hợp lệ", "无效"), "Không hợp lệ"),
        (("network", "connect", "timeout", "timed out", "connection", "socket", "mạng", "网络", "连接"), "Lỗi kết nối mạng"),
        (("forbidden", "unauthorized", "unauth", "permission", "权限", "被拒绝"), "Không có quyền truy cập"),
        (("error", "failed", "failure", "lỗi", "失败"), "Đã xảy ra lỗi"),
    ]

    for keywords, vn in rules:
        if any(k in lower for k in keywords):
            return vn

    if re.search(r"[A-Za-z一-鿿]", text):
        return "Thông báo không xác định từ máy chủ"

    return text

def xw_get_real_len(text: object):
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return len(ansi_escape.sub("", str(text)))

def xw_print_banner() -> None:
    
    xw_clear_screen()
def xw_print_square_table(title: str, headers: list[str], rows: list[list[object]]) -> None:
    normalized = [[str(cell) for cell in row] for row in rows]
    widths = []

    for i, header in enumerate(headers):
        values = [xw_get_real_len(row[i]) for row in normalized if i < len(row)]
        widths.append(max([xw_get_real_len(header), *values]) + 2)

    def border(left: str, middle: str, right: str) -> str:
        return left + middle.join("─" * (w + 2) for w in widths) + right

    total_width = sum(widths) + 3 * len(widths) + 1
    title_pad = max(0, total_width - xw_get_real_len(title) - 4)
    print(f"┌─ {title} {'─' * title_pad}┐")
    print(border("├", "┬", "┤"))

    header_str = "│ " + " │ ".join(
        str(h).center(widths[i]) for i, h in enumerate(headers)
    ) + " │"
    print(Fore.CYAN + header_str + Style.RESET_ALL)
    print(border("├", "┼", "┤"))

    for row in normalized:
        padded = []
        for i, cell in enumerate(row):
            pad = widths[i] - xw_get_real_len(cell)
            padded.append(cell + " " * max(0, pad))
        print("│ " + " │ ".join(padded) + " │")

    print(border("└", "┴", "┘"))


def xw_make_session(uid: str, secret: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(XW_HEADERS)
    session.headers.update({
        "user-id": str(uid),
        "user-secret-key": str(secret),
    })
    return session

def xw_parse_account_link(link: str) -> tuple[str, str]:
    uid_match = re.search(r"[?&]userId=([^&]+)", link)
    secret_match = re.search(r"[?&]secretKey=([^&]+)", link)
    if not uid_match or not secret_match:
        raise ValueError("Link thiếu userId hoặc secretKey")
    return uid_match.group(1), secret_match.group(1)

def xw_load_accounts() -> list[dict]:
    xw_rgb_print(255, 215, 0, "🔐 QUẢN LÝ TÀI KHOẢN XWORLD")
    raw_accounts: list[tuple[str, str]] = []

    if os.path.exists(XW_ACCOUNT_FILE):
        choice = input(
            f"📁 Phát hiện {XW_ACCOUNT_FILE}. Dùng lại acc cũ? (y/n): "
        ).strip().lower()

        if choice == "y":
            with open(XW_ACCOUNT_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split("|", 1)
                    if len(parts) == 2:
                        raw_accounts.append((parts[0], parts[1]))

    if not raw_accounts:
        while True:
            try:
                num = int(input("👉 Nhập số lượng acc cần chạy: ").strip())
                if num <= 0:
                    print("❌ Số lượng phải lớn hơn 0!")
                    continue
                break
            except ValueError:
                print("❌ Vui lòng nhập số hợp lệ!")

        for i in range(num):
            while True:
                xw_rgb_print(
                    255,
                    0,
                    0,
                    f"\n📌 Lấy link tài khoản thứ {i + 1} "
                    "(Vào https://xworld.info/vi-VN -> Vua thoát hiểm -> Copy link):",
                )

                link = input("👉 Dán Link vào đây: ").strip()

                try:
                    uid, secret = xw_parse_account_link(link)
                    raw_accounts.append((uid, secret))
                    print(f"✅ Thành công UID: {uid}")
                    break
                except (ValueError, IndexError):
                    print("❌ Link không hợp lệ, vui lòng nhập lại!")

        with open(XW_ACCOUNT_FILE, "w", encoding="utf-8") as f:
            for uid, secret in raw_accounts:
                f.write(f"{uid}|{secret}\n")

    accounts = []
    for uid, secret in raw_accounts:
        accounts.append({
            "uid": uid,
            "secret": secret,
            "session": xw_make_session(uid, secret),
            "status": "ACTIVE",
            "error_count": 0,
            "done_this_code": False,
        })

    return accounts

def xw_show_account_assets(accounts: list[dict]) -> None:
    xw_rgb_print(0, 255, 255, "💎 ĐANG KIỂM TRA TÀI KHOẢN...")

    def get_asset(acc: dict) -> dict:
        try:
            resp = acc["session"].post(
                XW_API_ASSET,
                json={"user_id": int(acc["uid"])},
                timeout=10,
            )
            data = resp.json()

            if data.get("code") != 0:
                return {
                    "uid": acc["uid"],
                    "BUILD": 0.0,
                    "status": "LIMIT",
                }

            asset = data.get("data", {}).get("user_asset", {})

            return {
                "uid": acc["uid"],
                "BUILD": float(asset.get("BUILD", 0.0)),
                "status": "ACTIVE",
            }

        except Exception:
            return {
                "uid": acc["uid"],
                "BUILD": 0.0,
                "status": "LIMIT",
            }

    results = []
    with ThreadPoolExecutor(max_workers=min(10, max(1, len(accounts)))) as executor:
        futures = [executor.submit(get_asset, acc) for acc in accounts]
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda x: int(x["uid"]))

    rows = []
    total_build = 0.0

    for i, item in enumerate(results, 1):
        total_build += item["BUILD"]

        status = (
            Fore.GREEN + "🟢 Hoạt động" + Style.RESET_ALL
            if item["status"] == "ACTIVE"
            else Fore.RED + "🔴 Giới hạn" + Style.RESET_ALL
        )

        rows.append([
            i,
            str(item["uid"]),      
            f"{item['BUILD']:.2f}",
            status,
        ])

    rows.append([
        "",
        "TỔNG",
        f"{total_build:.2f}",
        f"{len(results)} ACC",
    ])

    xw_print_square_table(
        "DANH SÁCH TÀI KHOẢN",
        ["STT", "UID", "BUILD", "TRẠNG THÁI"],
        rows,
    )

def xw_get_code_info(code: str, session: requests.Session) -> dict:
    payload = {"platform": "android", "channel": "h5", "app": 3, "code": code}
    for attempt in range(XW_MAX_RETRIES):
        try:
            resp = session.post(XW_API_DETAIL, json=payload, timeout=10)
            data = resp.json()
            if data.get("code") == 0:
                d = data.get("data", {})
                total = int(d.get("user_cnt", 0) or 0)
                progress = int(d.get("progress", 0) or 0)
                return {
                    "code": code,
                    "value": float(d.get("value", 0) or 0),
                    "total": total,
                    "remaining": max(0, total - progress),
                    "progress": progress,
                }
            return {"error": xw_translate_message(data.get("message", "Lỗi không xác định"))}
        except Exception:
            if attempt + 1 < XW_MAX_RETRIES:
                time.sleep(0.1)
    return {"error": "Lỗi kết nối mạng, không thể kết nối được"}

def xw_redeem_code_fast(acc: dict, code: str) -> tuple[str, str, float, str]:

    payload = {"platform": "android", "channel": "h5", "app": 3, "code": code}
    try:
        resp = acc["session"].post(XW_API_EXCHANGE, json=payload, timeout=10)
        data = resp.json()
        if data.get("code") == 0:
            value = float(data.get("data", {}).get("value", 0) or 0)
            return acc["uid"], "SUCCESS", value, "OK"

        raw_msg = str(data.get("message", "")).lower()
        msg = xw_translate_message(data.get("message", ""))
        if any(x in raw_msg for x in ("received", "already", "đã nhận", "领取", "已领取", "已領取")):
            status = "ALREADY_RECEIVED"
        elif any(x in raw_msg for x in ("out of", "not exist", "hết", "empty", "不存在", "已失效")):
            status = "CODE_EMPTY"
        elif any(x in raw_msg for x in ("limit", "max", "giới hạn", "exceed", "上限", "达到上限", "已达上限")):
            status = "ACCOUNT_LIMIT"
        else:
            status = "ERROR"
        return acc["uid"], status, 0.0, msg
    except Exception as exc:
        return acc["uid"], "ERROR", 0.0, xw_translate_message(str(exc))

def xw_log_result(uid: str, code: str, threshold: int,
               build_received: float, success: bool) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "SUCCESS" if success else "FAILED"
    with open(XW_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(
            f"{timestamp}|{uid}|{code}|{threshold}|{build_received}|{status}\n"
        )

def xw_key_still_valid_or_exit() -> bool:

    is_valid, _, _ = check_activation_valid(exit_on_expired=False)
    return is_valid

def xw_watch_code(code: str, accounts: list[dict], threshold: int) -> str:
    code = str(code).strip()
    if not code:
        xw_rgb_print(255, 0, 0, "❌ Mã code không hợp lệ.")
        return "empty"

    if not accounts:
        xw_rgb_print(255, 0, 0, "❌ Không có tài khoản để canh code.")
        return "no_accounts"

    try:
        threshold = int(threshold)
    except Exception:
        threshold = 0
    threshold = max(0, threshold)

    xw_rgb_print(0, 255, 0, f"\n🚀 ĐANG CANH CODE: {code} -- NGƯỠNG: {threshold}")
    print(Fore.WHITE + "Nhấn Ctrl+C để hủy canh code này.\n" + Style.RESET_ALL)

    if not xw_key_still_valid_or_exit():
        xw_rgb_print(255, 215, 0, "🔙 Key đã hết hạn, quay về menu chính.")
        time.sleep(1)
        return "expired"

    scan_session = requests.Session()
    scan_session.headers.update(XW_HEADERS)
    results_history: list[list[object]] = []
    bot_mode = True
    last_bot_status_ts = 0.0

    for acc in accounts:
        acc["done_this_code"] = False
        acc["error_count"] = 0

    code_empty = False
    stop_reason = "normal"

    try:
        while True:
            if not xw_key_still_valid_or_exit():
                xw_rgb_print(255, 215, 0, "🔙 Key đã hết hạn, quay về menu chính.")
                stop_reason = "expired"
                break

            try:
                info = xw_get_code_info(code, scan_session)
            except KeyboardInterrupt:
                stop_reason = "interrupt"
                break

            if "error" in info:
                if bot_mode:
                    now_ts = time.time()
                    if now_ts - last_bot_status_ts >= 5.0:
                        last_bot_status_ts = now_ts
                        xw_rgb_print(255, 80, 80, f"⚠️ Lỗi kiểm tra code: {xw_translate_message(info['error'])} — đang thử lại...")
                else:
                    sys.stdout.write(
                        f"\r{Fore.RED}⚠️ Lỗi kiểm tra code: {xw_translate_message(info['error'])} - Thử lại..."
                        f"{Style.RESET_ALL}        "
                    )
                    sys.stdout.flush()
                time.sleep(1)
                continue

            remaining = int(info.get("remaining", 0))
            current_time = datetime.now().strftime("%H:%M:%S")

            if bot_mode:
                now_ts = time.time()
                if now_ts - last_bot_status_ts >= 5.0 or remaining <= threshold:
                    last_bot_status_ts = now_ts
                    xw_rgb_print(255, 215, 0, f"🕒 [{current_time}] Lượt còn lại: {remaining} / {info.get('total', 0)} (Đang chờ mốc {threshold})")
            else:
                sys.stdout.write(
                    f"\r🕒 [{current_time}] Lượt còn lại: {remaining} / {info.get('total', 0)} "
                    f"(Đang chờ mốc {threshold})        "
                )
                sys.stdout.flush()

            if remaining > threshold:
                time.sleep(1)
                continue

            if not xw_key_still_valid_or_exit():
                stop_reason = "expired"
                break

            print(
                f"\n\n⚡ ĐÃ ĐẠT NGƯỠNG {threshold}! "
                "ĐỔI CODE LIÊN TỤC ĐẾN KHI HẾT CODE HOẶC ĐẠT GIỚI HẠN..."
            )

            active_accs = [
                acc for acc in accounts
                if acc.get("status") == "ACTIVE"
                and not acc.get("done_this_code")
                and acc.get("error_count", 0) < 3
            ]

            if not active_accs:
                print("\n⚠️ Tất cả tài khoản đã nhận, đạt giới hạn hoặc lỗi quá nhiều.")
                stop_reason = "empty"
                break

            executor = ThreadPoolExecutor(max_workers=min(10, len(active_accs)))
            try:
                futures = {
                    executor.submit(xw_redeem_code_fast, acc, code): acc
                    for acc in active_accs
                }

                pending = set(futures.keys())
                stop_now = False

                while pending:
                    if not xw_key_still_valid_or_exit():
                        stop_reason = "expired"
                        stop_now = True
                        break

                    try:
                        done, pending = wait(
                            pending,
                            timeout=0.2,
                            return_when=FIRST_COMPLETED,
                        )
                    except KeyboardInterrupt:
                        stop_reason = "interrupt"
                        stop_now = True
                        break

                    if not done:
                        continue

                    for future in done:
                        acc = futures[future]
                        try:
                            uid, status, build, msg = future.result()
                        except KeyboardInterrupt:
                            stop_reason = "interrupt"
                            stop_now = True
                            break
                        except Exception as exc:
                            uid, status, build, msg = "UNKNOWN", "ERROR", 0.0, str(exc)

                        if status == "SUCCESS":
                            acc["done_this_code"] = True
                            status_text = Fore.GREEN + "THÀNH CÔNG" + Style.RESET_ALL
                            xw_log_result(uid, code, threshold, build, True)

                        elif status == "ALREADY_RECEIVED":
                            acc["done_this_code"] = True
                            status_text = Fore.YELLOW + "ĐÃ NHẬN" + Style.RESET_ALL

                        elif status == "ACCOUNT_LIMIT":
                            acc["status"] = "LIMIT"
                            status_text = Fore.MAGENTA + "GIỚI HẠN" + Style.RESET_ALL
                            xw_log_result(uid, code, threshold, 0.0, False)

                        elif status == "CODE_EMPTY":
                            code_empty = True
                            stop_reason = "empty"
                            status_text = Fore.RED + "CODE ĐÃ HẾT" + Style.RESET_ALL
                            stop_now = True

                        else:
                            acc["error_count"] = acc.get("error_count", 0) + 1
                            status_text = Fore.RED + "LỖI" + Style.RESET_ALL

                        results_history.append([
                            str(uid),
                            status_text,
                            f"{build:.2f}",
                            msg,
                        ])

                        if stop_now:
                            break

                    if stop_now:
                        break

            finally:
                executor.shutdown(wait=False, cancel_futures=True)

            if results_history:
                xw_print_square_table(
                    "KẾT QUẢ NHẬP CODE",
                    ["UID", "Trạng thái", "BUILD", "Thông báo"],
                    results_history[-20:],
                )

            if code_empty:
                print("\n❌ Code đã hết lượt hoặc không còn khả dụng.")
                break

            if stop_reason in ("interrupt", "expired"):
                break

            time.sleep(0.1)

    finally:
        try:
            scan_session.close()
        except Exception:
            pass

    if stop_reason == "interrupt":
        sys.stdout.write("\n")
        sys.stdout.flush()
        print(Fore.YELLOW + "🛑 Đã hủy canh code." + Style.RESET_ALL)
    elif stop_reason == "expired":
        print(Fore.YELLOW + "\n🔙 Đã quay về menu." + Style.RESET_ALL)
    else:
        print(Fore.GREEN + "\n👋 Đã thoát canh code." + Style.RESET_ALL)

    return stop_reason

def xw_main() -> None:
    start_key_expiry_monitor()
    bot_mode = True
    is_valid, reason, is_vip = check_activation_valid(exit_on_expired=False)

    if not is_valid:
        if "HẾT HẠN" in str(reason).upper() or "RESET" in str(reason).upper():
            xw_rgb_print(255, 215, 0, "🔙 Key đã hết hạn, quay về menu chính.")
        else:
            xw_rgb_print(255, 0, 0, f"❌ Không thể vào XWORLD: {xw_translate_message(reason)}")
        time.sleep(1.5)
        return

    if not is_vip:
        xw_rgb_print(255, 0, 0, "❌ CANH CODE chỉ dành cho key VIP.")
        xw_rgb_print(255, 215, 0, "🔑 Hãy kích hoạt key VIP để sử dụng chức năng này.")
        time.sleep(2)
        return

    xw_print_banner()

    accounts = xw_load_accounts()
    if not accounts:
        xw_rgb_print(255, 0, 0, "❌ Không có tài khoản nào để chạy.")
        return

    if bot_mode:
        xw_rgb_print(0, 255, 0, f"✅ Đã tải {len(accounts)} tài khoản XWORLD.")

    temp_session = requests.Session()
    temp_session.headers.update(XW_HEADERS)
    xw_show_account_assets(accounts)

    while True:
        is_valid, reason, _ = check_activation_valid(exit_on_expired=False)
        if not is_valid:
            if "HẾT HẠN" in str(reason).upper() or "RESET" in str(reason).upper():
                xw_rgb_print(255, 215, 0, "🔙 Key đã hết hạn, quay về menu chính.")
            else:
                xw_rgb_print(255, 0, 0, f"❌ Không thể tiếp tục: {xw_translate_message(reason)}")
            time.sleep(1)
            return

        try:
            print()
            code = input(
                Fore.CYAN + "🎁 Nhập mã code cần canh (Nhập 0 để thoát tool): "
                + Style.RESET_ALL
            ).strip()
        except KeyboardInterrupt:
            print(Fore.RED + "🛑 Đã hủy nhập mã code." + Style.RESET_ALL)
            return

        is_valid, reason, _ = check_activation_valid(exit_on_expired=False)
        if not is_valid:
            if "HẾT HẠN" in str(reason).upper() or "RESET" in str(reason).upper():
                xw_rgb_print(255, 215, 0, "🔙 Key đã hết hạn, quay về menu chính.")
            else:
                xw_rgb_print(255, 0, 0, f"❌ Không thể tiếp tục: {xw_translate_message(reason)}")
            time.sleep(1)
            return

        if code == "0":
            xw_rgb_print(0, 255, 0, "👋 Đã dừng tool. Hẹn gặp lại!")
            return

        if not code:
            continue

        xw_rgb_print(255, 215, 0, f">> Đang kiểm tra mã code: {code} ...")
        info = xw_get_code_info(code, temp_session)

        if "error" in info:
            print(
                Fore.RED
                + f"❌ Lỗi kiểm tra mã code: {xw_translate_message(info['error'])}"
                + Style.RESET_ALL
            )
            continue

        xw_print_square_table(
            "THÔNG TIN MÃ CODE",
            ["Mã Code", "Giá trị (BUILD)", "Tổng lượt", "Lượt còn lại"],
            [[
                info.get("code", code),
                f"{info.get('value', 0):.2f}",
                info.get("total", 0),
                info.get("remaining", 0)
            ]],
        )

        try:
            choice = input(
                ">> Bạn có muốn canh mã code này không? (y/n): "
            ).strip().lower()
        except KeyboardInterrupt:
            print(Fore.RED + "🛑 Đã hủy nhập mã code." + Style.RESET_ALL)
            return

        if not check_activation_valid(exit_on_expired=False)[0]:
            xw_rgb_print(255, 215, 0, "🔙 Key đã hết hạn, quay về menu chính.")
            time.sleep(1)
            return

        if choice != "y":
            continue

        try:
            threshold = int(input(
                "🎯 Nhập ngưỡng (số lượt còn lại để tự động kích hoạt nhập): "
            ).strip())
        except KeyboardInterrupt:
            print(Fore.RED + "🛑 Đã hủy nhập ngưỡng." + Style.RESET_ALL)
            continue
        except ValueError:
            print("❌ Ngưỡng phải là số nguyên!")
            continue

        if not check_activation_valid(exit_on_expired=False)[0]:
            xw_rgb_print(255, 215, 0, "🔙 Key đã hết hạn, quay về menu chính.")
            time.sleep(1)
            return

        try:
            result = xw_watch_code(code, accounts, threshold)
        except KeyboardInterrupt:
            sys.stdout.write("\n")
            sys.stdout.flush()
            print(Fore.YELLOW + "🛑 Đã hủy canh code." + Style.RESET_ALL)
            continue

        if result == "interrupt":
            continue

        if result == "empty":
            xw_rgb_print(0, 255, 0, "👋 Mã code đã hết hoặc không còn khả dụng.")
            continue

        if result == "expired":
            return

        continue

# ================== HILO TOOL  ==================

C_GREEN = '\033[92m'
C_RED = '\033[91m'
C_YELLOW = '\033[93m'
C_CYAN = '\033[96m'
C_WHITE = '\033[97m'
C_NEON_PINK = '\033[95m'
C_RESET = '\033[0m'
C_DIM = '\033[2m'

def hilo_show_temp_error(message, seconds=3):

    if True:
        print(_strip_rich_markup(message), flush=True)
        return

    lines = str(message).splitlines() or [""]
    print("\n".join(lines), flush=True)
    time.sleep(seconds)
    for index in range(len(lines)):
        print("\033[2K\r", end="")
        if index < len(lines) - 1:
            print("\033[1A", end="")
    print("\r", end="", flush=True)
def hilo_get_device_fingerprint(identifier):
    hash_id = int(hashlib.md5(str(identifier).encode()).hexdigest(), 16)
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36"
    ]
    return uas[hash_id % len(uas)]

def hilo_build_secure_session(user_agent, proxy_url=None, token=None, apikey=None):
    session = requests.Session()
    is_mobile = "Mobile" in user_agent
    
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=utf-8",
        "User-Agent": user_agent,
        "Origin": "https://hilo.turbogg4u.online",
        "Referer": "https://hilo.turbogg4u.online/",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Sec-Ch-Ua-Mobile": "?1" if is_mobile else "?0",
        "Sec-Ch-Ua-Platform": "\"Android\"" if "Android" in user_agent else ("\"iOS\"" if "iPhone" in user_agent else "\"Windows\"")
    }
    
    if token:
        headers["authorization"] = token
    if apikey:
        headers["apikey"] = apikey
        headers["metadata"] = json.dumps({"device": "mobile" if is_mobile else "desktop", "manual": True})
        headers["subpartnerid"] = ""
        
    session.headers.update(headers)
    
    retry_strategy = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})
        
    return session

def hilo__response_json(response, endpoint="API"):
    
    response.raise_for_status()
    try:
        data = response.json()
    except ValueError as exc:
        preview = (response.text or "")[:300].replace("\n", " ")
        raise RuntimeError(f"{endpoint} trả về JSON không hợp lệ: {preview}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"{endpoint} trả về kiểu dữ liệu không hợp lệ: {type(data).__name__}")
    return data


def hilo__extract_card(value, fallback=None):

    if value is None:
        return fallback

    if isinstance(value, dict):

        for key in (
            "card", "value", "rank", "number", "point",
            "cardValue", "cardRank", "currentCard",
            "drawnCard", "newCard", "resultCard", "initialCard",
        ):
            if value.get(key) is not None:
                found = hilo__extract_card(value.get(key), None)
                if found is not None:
                    return found

        for key in ("data", "result", "game", "round", "state", "payload"):
            if value.get(key) is not None:
                found = hilo__extract_card(value.get(key), None)
                if found is not None:
                    return found

        return fallback

    if isinstance(value, list):
        for item in value:
            found = hilo__extract_card(item, None)
            if found is not None:
                return found
        return fallback

    if isinstance(value, bool):
        return fallback

    if isinstance(value, (int, float)):
        value = int(value)
        return value if 1 <= value <= 13 else fallback

    text = str(value).strip()
    if not text:
        return fallback

    upper = text.upper()
    face_to_number = {"A": 1, "J": 11, "Q": 12, "K": 13}
    if upper in face_to_number:
        return face_to_number[upper]

    try:
        number = int(text)
        return number if 1 <= number <= 13 else fallback
    except ValueError:
        return fallback


def hilo__get_game_card(payload, fallback=None):

    card = hilo__extract_card(payload, None)
    if card is not None:
        return card
    return hilo__extract_card(fallback, None)


def hilo_get_card_value(card):

    normalized = hilo__extract_card(card)
    try:
        value = int(normalized)
    except (TypeError, ValueError):
        return 7

    return value if 1 <= value <= 13 else 7


def hilo_action_label(action):

    labels = {
        "hi": "HI",
        "lo": "LO",
    }
    normalized = str(action).lower()
    return labels.get(normalized, normalized.upper())


def hilo_compare_card_result(previous_card, drawn_card):

    if previous_card is None or drawn_card is None:
        return None
    try:
        previous_value = hilo_get_card_value(previous_card)
        drawn_value = hilo_get_card_value(drawn_card)
    except Exception:
        return None

    if drawn_value > previous_value:
        return "CAO"
    if drawn_value < previous_value:
        return "NHỎ"
    return "BẰNG"


# =========================
# LOGIC FREE / VIP
# =========================

def hilo__clean_history(history, limit=60):

    if not history:
        return []

    values = []
    for card in history[-limit:]:
        try:
            value = hilo_get_card_value(card)
        except Exception:
            continue

        if not 1 <= value <= 13:
            continue

        # API/display dùng 1 cho A, nhưng A là rank cao nhất khi phân tích.
        values.append(14 if value == 1 else value)

    return values


def hilo__outcome_between(previous_value, next_value):
    """Quan hệ giữa 2 lá: hi / lo / draw."""
    if next_value > previous_value:
        return "hi"
    if next_value < previous_value:
        return "lo"
    return "draw"


def hilo__direction_counts(values):
   
    hi = lo = draw = 0
    for previous_value, next_value in zip(values, values[1:]):
        outcome = hilo__outcome_between(previous_value, next_value)
        if outcome == "hi":
            hi += 1
        elif outcome == "lo":
            lo += 1
        else:
            draw += 1
    return hi, lo, draw


def hilo__weighted_direction_score(values, decay=0.82):
   
    pairs = list(zip(values, values[1:]))
    if not pairs:
        return 0.0

    score = 0.0
    weight = 1.0
    total_weight = 0.0

    for previous_value, next_value in reversed(pairs):
        outcome = hilo__outcome_between(previous_value, next_value)
        if outcome == "hi":
            score += weight
        elif outcome == "lo":
            score -= weight
     
        total_weight += weight
        weight *= decay

    if total_weight <= 0:
        return 0.0
    return score / total_weight


def hilo__streak_score(values, max_streak=5):

    pairs = list(zip(values, values[1:]))
    if not pairs:
        return 0.0

    last_outcome = hilo__outcome_between(*pairs[-1])
    if last_outcome == "draw":
        return 0.0

    streak = 0
    for previous_value, next_value in reversed(pairs):
        outcome = hilo__outcome_between(previous_value, next_value)
        if outcome != last_outcome:
            break
        streak += 1
        if streak >= max_streak:
            break

   
    strength = min(streak / max_streak, 1.0)
    return strength if last_outcome == "hi" else -strength


def hilo__markov_score(values):
   
    pairs = list(zip(values, values[1:]))
    if len(pairs) < 5:
        return 0.0

    transitions = {
        "hi": {"hi": 1.0, "lo": 1.0},
        "lo": {"hi": 1.0, "lo": 1.0},
    }

    last_direction = None
    for previous_value, next_value in pairs:
        current_direction = hilo__outcome_between(previous_value, next_value)
        if current_direction not in ("hi", "lo"):
            continue
        if last_direction in transitions:
            transitions[last_direction][current_direction] += 1.0
        last_direction = current_direction

    if last_direction not in transitions:
        return 0.0

    row = transitions[last_direction]
    total = row["hi"] + row["lo"]
    p_hi = row["hi"] / total
    return (p_hi - 0.5) * 2.0


def hilo__current_card_edge(current_card):

    value = hilo_get_card_value(current_card)
    rank_value = 14 if value == 1 else value
    hi = 14 - rank_value
    lo = rank_value - 2
    total_non_draw = hi + lo

    if total_non_draw <= 0:
        return 0.0

    return (hi - lo) / total_non_draw


def hilo__recent_balance_score(values, window=10):

    recent = values[-window:]
    if not recent:
        return 0.0

    hi = sum(1 for value in recent if value >= 8)
    lo = sum(1 for value in recent if value <= 6)
    active = hi + lo

    if active == 0:
        return 0.0

    imbalance = (hi - lo) / active
    return -imbalance * 0.25


def hilo__resolve_direction(hi_score, lo_score, fallback="hi"):
   
    if hi_score > lo_score:
        return "hi"
    if lo_score > hi_score:
        return "lo"
    return fallback


def hilo_predict_action_basic(current_card):
   
    value = hilo_get_card_value(current_card)
    rank_value = 14 if value == 1 else value
    if rank_value >= 14:  
        return "lo"
    if rank_value <= 2:    
        return "hi"
    if rank_value <= 6:
        return "hi"
    if rank_value >= 8:
        return "lo"
   
    return "hi"


def hilo_predict_action_free(current_card, history=None):
   
    values = hilo__clean_history(history, limit=30)

    current_edge = hilo__current_card_edge(current_card)
    trend = hilo__weighted_direction_score(values, decay=0.78)
    balance = hilo__recent_balance_score(values, window=8)
    streak = hilo__streak_score(values, max_streak=4)

    total_score = (
        0.60 * current_edge
        + 0.25 * trend
        + 0.10 * balance
        + 0.05 * streak
    )

 
    if abs(current_edge) < 0.01 and abs(total_score) < 0.08:
        if values:
            hi_count, lo_count, _ = hilo__direction_counts(values[-12:])
            if hi_count != lo_count:
                return "lo" if hi_count < lo_count else "hi"
        return "hi"

    return "hi" if total_score >= 0 else "lo"


def hilo_predict_action_vip(current_card, history=None):

    values = hilo__clean_history(history, limit=60)

    current_edge = hilo__current_card_edge(current_card)
    weighted_trend = hilo__weighted_direction_score(values, decay=0.88)
    short_trend = hilo__weighted_direction_score(values[-12:], decay=0.72)
    markov = hilo__markov_score(values)
    balance = hilo__recent_balance_score(values, window=12)
    streak = hilo__streak_score(values, max_streak=5)

    medium_shift = 0.0
    if len(values) >= 12:
        first = values[-12:-6]
        second = values[-6:]
        first_score = hilo__weighted_direction_score(first, decay=0.85)
        second_score = hilo__weighted_direction_score(second, decay=0.85)
        medium_shift = max(-1.0, min(1.0, second_score - first_score))

    n = len(values)
    if n < 6:

        hi_score = max(current_edge, 0.0)
        lo_score = max(-current_edge, 0.0)
        return hilo__resolve_direction(hi_score, lo_score, hilo_predict_action_basic(current_card))

    history_factor = min(1.0, n / 20.0)

    history_score = (
        0.28 * markov
        + 0.26 * short_trend
        + 0.16 * weighted_trend
        + 0.12 * balance
        + 0.10 * streak
        + 0.08 * medium_shift
    )

    combined_score = (
        (0.40 * current_edge)
        + (0.60 * history_factor * history_score)
    )

  
    current_weight = 0.55 if abs(current_edge) >= 0.50 else 0.45
    combined_score = (
        current_weight * current_edge
        + (1.0 - current_weight) * history_factor * history_score
    )


    if abs(combined_score) < 0.035:
        return hilo_predict_action_basic(current_card)

    return "hi" if combined_score > 0 else "lo"

def hilo_select_analysis_logic():
  
    try:
        is_valid, reason, is_vip = check_activation_valid(exit_on_expired=False)
    except Exception as exc:
        is_valid, reason, is_vip = False, str(exc), False

    if not is_valid:
        console.print(f"[bold red]❌ Key không hợp lệ: {reason}[/bold red]")
        return None

    print(f"\n{C_NEON_PINK}========== CHỌN LOGIC HILO ================{C_RESET}")
    print(f"{C_GREEN}1. LOGIC FREE{C_RESET}")
    if is_vip:
        print(f"{C_RED}2. LOGIC VIP{C_RESET}")
    else:
        print(f"{C_DIM}2. LOGIC VIP (chỉ Key VIP){C_RESET}")
    print(f"{C_NEON_PINK}============================================{C_RESET}")

    while True:
        choice = input(f"{C_WHITE}👉 Chọn Logic (1/2): {C_RESET}").strip()

        # Kiểm tra lại sau lúc nhập: không cho chọn LOGIC FREE nếu key vừa hết hạn.
        try:
            is_valid, reason, is_vip = check_activation_valid(exit_on_expired=False)
        except Exception as exc:
            is_valid, reason, is_vip = False, str(exc), False
        if not is_valid:
            console.print(
                f"[bold red]🔒 {reason}. Vui lòng nhập lại Key FREE hoặc Key VIP.[/bold red]"
            )
            return None

        if choice == "1":
            print(f"{C_GREEN}✅ Đã chọn HILO LOGIC FREE{C_RESET}")
            return "free"

        if choice == "2":
            if is_vip:
                print(f"{C_RED}✅ Đã chọn HILO LOGIC VIP{C_RESET}")
                return "vip"

            hilo_show_temp_error(f"{C_RED}❌ LOGIC VIP chỉ dành cho Key VIP.{C_RESET}")
            continue

        hilo_show_temp_error(f"{C_RED}❌ Chỉ được chọn 1 hoặc 2.{C_RESET}")

def hilo_get_multiline_json():
    print(f"{C_WHITE}👉 Dán Payload JSON vào đây:{C_RESET}")

    while True:
        raw = input().strip()

        if not raw:
            hilo_show_temp_error(f"{C_YELLOW}⚠️ JSON đang trống. Vui lòng dán lại.{C_RESET}")
            continue

        try:
            payload = json.loads(raw)

            if not isinstance(payload, dict):
                hilo_show_temp_error(f"{C_RED}JSON hợp lệ nhưng phải là một object/dict.{C_RESET}")
                continue

            return payload

        except json.JSONDecodeError as e:
            hilo_show_temp_error(
                f"{C_RED}❌ JSON không hợp lệ. "
                f"Vui lòng dán lại và bấm Enter.{C_RESET}"
            )

        except Exception as e:
            hilo_show_temp_error(f"{C_RED}❌ Lỗi khi đọc JSON. Vui lòng kiểm tra lại dữ liệu.{C_RESET}")

def hilo__prompt_number(prompt, cast=float, min_value=None, allow_zero=True):

    while True:
        raw = input(prompt).strip()
        try:
            value = cast(raw)
            if isinstance(value, float) and not __import__('math').isfinite(value):
                raise ValueError("giá trị phải là số hữu hạn")
            if min_value is not None and value < min_value:
                hilo_show_temp_error(f"{C_RED}❌ Giá trị phải >= {min_value}. Vui lòng nhập lại.{C_RESET}")
                continue
            if not allow_zero and value == 0:
                hilo_show_temp_error(f"{C_RED}❌ Giá trị không được bằng 0. Vui lòng nhập lại.{C_RESET}")
                continue
            return value
        except (ValueError, TypeError):
            hilo_show_temp_error(f"{C_RED}❌ Nhập sai định dạng. Vui lòng nhập lại.{C_RESET}")


def hilo_setup_config():
    None if True else os.system('clear' if os.name == 'posix' else 'cls')

    while True:
        use_proxy = input(f"{C_WHITE}Bạn có muốn sử dụng Proxy không? (y/n): {C_RESET}").strip().lower()
        if use_proxy in ('y', 'n'):
            break
        hilo_show_temp_error(f"{C_RED}❌ Chỉ được nhập y hoặc n. Vui lòng nhập lại.{C_RESET}")

    proxy_url = None
    if use_proxy == 'y':
        while True:
            proxy_url = input(f"{C_WHITE} Nhập Proxy (VD: http://user:pass@ip:port): {C_RESET}").strip()
            if not proxy_url:
                hilo_show_temp_error(f"{C_RED}❌ Proxy đang trống. Vui lòng nhập lại.{C_RESET}")
                continue

            print(f"{C_DIM}>> Đang kiểm tra Proxy...{C_RESET}")
            try:
                test_sess = requests.Session()
                test_sess.proxies.update({"http": proxy_url, "https": proxy_url})
                test_sess.get("https://hilo.turbogg4u.online", timeout=10)
                print(f"{C_GREEN}>> Proxy hoạt động tốt.{C_RESET}")
                break
            except Exception:
                hilo_show_temp_error(f"{C_RED}❌ Proxy lỗi. Vui lòng kiểm tra lại Proxy.{C_RESET}")
                while True:
                    retry_proxy = input(f"{C_WHITE} Bạn có muốn thử lại Proxy không? (y/n): {C_RESET}").strip().lower()
                    if retry_proxy in ('y', 'n'):
                        break
                    hilo_show_temp_error(f"{C_RED}❌ Chỉ được nhập y hoặc n. Vui lòng nhập lại.{C_RESET}")
                if retry_proxy == 'n':
                    proxy_url = None
                    print(f"{C_YELLOW} Tiếp tục không sử dụng Proxy.{C_RESET}")
                    break

    login_payload = hilo_get_multiline_json()
    identifier = "termux_user"
    user_agent = hilo_get_device_fingerprint(identifier)

    print(f"{C_DIM}>> Đang xác thực tài khoản...{C_RESET}")
    auth_session = hilo_build_secure_session(user_agent, proxy_url)

    while True:
        try:
            response = auth_session.post(
                "https://hilo.turbogg4u.online/api/common/profile",
                json=login_payload,
                timeout=(10, 15)
            )
            res = hilo__response_json(response, "PROFILE")

            if 'id' in res and 'token' in res:
                apikey = res['id']
                token = res['token']
                currency = res.get('currency', 'bld')
                balance = float(res.get('balance', 0))
                name = res.get('playerName', 'Unknown')
                print(f"{C_GREEN}✅ Đăng nhập thành công!{C_RESET}")
                print(f"{C_WHITE}👤 Tài khoản: {name}{C_RESET}")
                print(f"{C_WHITE}💰 Số dư: {balance} {currency}{C_RESET}\n")
                break
            else:
                hilo_show_temp_error(f"{C_RED}❌ Không thể xác thực. Vui lòng kiểm tra lại Payload JSON.{C_RESET}")

        except Exception as e:
            hilo_show_temp_error(f"{C_RED}❌ Lỗi kết nối/xác thực. Vui lòng thử lại.{C_RESET}")

        login_payload = hilo_get_multiline_json()
        print(f"{C_DIM}>> Đang xác thực lại tài khoản...{C_RESET}")

    # Chọn LOGIC theo menu HILO cũ; KEY FREE bị khóa LOGIC VIP.
    analysis_logic = hilo_select_analysis_logic()
    if analysis_logic not in ("free", "vip"):
        raise RuntimeError("Không xác định được quyền LOGIC từ KEY hiện tại.")

    base_bet = hilo__prompt_number(f"{C_WHITE}1. Số BUILD cược mỗi ván: {C_RESET}", float, min_value=0, allow_zero=False)
    target_profit = hilo__prompt_number(f"{C_WHITE}2. Nhập số BUILD muốn lãi: {C_RESET}", float, min_value=0)
    stop_loss = hilo__prompt_number(f"{C_WHITE}3. Ngưỡng cắt lỗ: {C_RESET}", float, min_value=0)
    loss_multiplier = hilo__prompt_number(f"{C_WHITE}4. Hệ số gấp thếp khi thua: {C_RESET}", float, min_value=0, allow_zero=False)
    target_opens = hilo__prompt_number(f"{C_WHITE}5. Số lá mở an toàn tối đa 1 ván: {C_RESET}", int, min_value=1)
    delay_bet = hilo__prompt_number(f"{C_WHITE}6. Delay đánh bài (giây): {C_RESET}", float, min_value=0)
    delay_win = hilo__prompt_number(f"{C_WHITE}7. Delay sau khi thắng (giây): {C_RESET}", float, min_value=0)
    delay_loss = hilo__prompt_number(f"{C_WHITE}8. Delay sau khi thua (giây): {C_RESET}", float, min_value=0)

    return {
        'proxy_url': proxy_url,
        'login_payload': login_payload,
        'analysis_logic': analysis_logic,
        'apikey': apikey,
        'token': token,
        'currency': currency,
        'original_balance': balance,
        'current_balance': balance,
        'player_name': name,
        'base_bet': base_bet,
        'target_profit': target_profit,
        'stop_loss': stop_loss,
        'loss_multiplier': loss_multiplier,
        'target_opens': target_opens,
        'delay_bet': delay_bet,
        'delay_win': delay_win,
        'delay_loss': delay_loss,
        'user_agent': user_agent
    }

# ================== GIAO DIỆN TOOL HILO ==================

HILO_COLORS = VBTOOL_COLORS
HILO_ICONS = ICONS
HILO_LOGO = LOGO_VBTOOL


def hilo_make_vbtool_header() -> Group:
    return Group()


def hilo_make_vbtool_compact_panel(state: Dict) -> Panel:
    
    adapted = dict(state or {})
    balance = adapted.get("balance", 0.0)
    original_balance = adapted.get("original_balance", balance)
    adapted["balances"] = {"BUILD": {"balance": balance}, "XWORLD": {"balance": 0.0}}
    adapted["start_balances"] = {"BUILD": {"balance": original_balance}}
    adapted["next_prediction"] = adapted.get("next_prediction", "—")
    adapted["next_confidence"] = adapted.get("next_confidence", 0.0)
    adapted["three_number_analysis"] = adapted.get("three_number_analysis", {})
    adapted["bet_stats"] = adapted.get("bet_stats")
    return make_vbtool_compact_panel(adapted)


def hilo_make_vbtool_history_cards(history, limit=5, highlight_issue=None) -> Panel:
    """Lịch sử HILO riêng, không phụ thuộc cấu trúc lịch sử LOTTO."""
    recent = list(reversed((history or [])[-limit:]))
    if not recent:
        return Panel(Text("Chưa có lịch sử", style="dim"), border_style=VBTOOL_COLORS["history_blue"])

    cards = []
    for rec in recent:
        issue = rec.get("issue_id", "—")
        ts = str(rec.get("timestamp", ""))
        short_ts = ts[11:19] if len(ts) >= 19 else ts
        current_card = hilo__hilo_rank_label(rec.get("current_card"))
        drawn_card = hilo__hilo_rank_label(rec.get("drawn_card"))
        result = str(rec.get("result", "—"))
        profit = rec.get("profit")
        try:
            profit_text = f"{float(profit):+.2f}" if profit is not None else "—"
        except (TypeError, ValueError):
            profit_text = str(profit) if profit is not None else "—"

        result_style = f"bold {VBTOOL_COLORS['emerald']}" if result.upper() in ("THẮNG", "WIN") else (
            f"bold {VBTOOL_COLORS['ruby']}" if result.upper() in ("THUA", "LOSS") else VBTOOL_COLORS["gold"])
        line = Text()
        line.append(f"#{str(issue)[-8:]} ", style=f"bold {VBTOOL_COLORS['history_blue']}")
        if short_ts:
            line.append(f"{short_ts}  ", style="dim")
        line.append(f"{current_card} → {drawn_card}  ", style=VBTOOL_COLORS["platinum"])
        line.append(result, style=result_style)
        line.append(f"  {profit_text}", style=result_style if profit is not None else "dim")
        cards.append(Panel(line, border_style=VBTOOL_COLORS["gold"] if issue == highlight_issue else VBTOOL_COLORS["onyx"], padding=(0, 1)))

    return Panel(Columns(cards, equal=True, expand=True), title="LỊCH SỬ HILO", border_style=VBTOOL_COLORS["history_blue"], box=box.ROUNDED)


def hilo__hilo_rank_label(card):

    try:
        normalized = hilo__extract_card(card, None)
        if normalized is None:
            return "—"

        if isinstance(normalized, str):
            upper = normalized.strip().upper()
            face_to_number = {"A": 1, "J": 11, "Q": 12, "K": 13}
            if upper in face_to_number:
                return str(face_to_number[upper])
            return str(int(upper)) if upper.isdigit() else upper
        value = int(normalized)
        return str(value)
    except Exception:
        return str(card) if card is not None else "—"

def hilo_make_vbtool_layout(state: Dict) -> Layout:
    layout = Layout(name="root")
    layout.split_column(
        Layout(name="header", size=8),
        Layout(name="hilo_main", ratio=1),
        Layout(name="history", size=12),
    )
    layout["header"].update(hilo_make_vbtool_header())
    layout["hilo_main"].update(hilo_make_vbtool_compact_panel(state))
    layout["history"].update(hilo_make_vbtool_history_cards(
        state.get("history", []), limit=5, highlight_issue=state.get("last_issue")
    ))
    return layout


def hilo__history_append(state, issue, current_card, drawn_card, result, profit):
    state["history"].append({
        "issue_id": issue,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "current_card": current_card,
        "drawn_card": drawn_card,
        "result": result,
        "profit": profit,
    })
    if len(state["history"]) > 30:
        del state["history"][:-30]


def hilo_main():
    cfg = hilo_setup_config()
    session = hilo_build_secure_session(
        cfg['user_agent'],
        cfg['proxy_url'],
        cfg['token'],
        cfg['apikey']
    )

    current_bet = cfg['base_bet']
    last_result = 'none'
    analysis_history = []
    stats = {"predictions": 0, "wins": 0, "losses": 0}

    state = {
        "account_id": cfg.get("player_name") or cfg.get("login_payload", {}).get("username", "—"),
        "balance": cfg.get("current_balance", 0.0),
        "original_balance": cfg.get("original_balance", 0.0),
        "last_issue": "—",
        "last_result": "CHƯA CÓ",
        "current_card": None,
        "next_prediction": "—",
        "current_bet": current_bet,
        "history": [],
        "model_stats": stats,
        "log_text": "Sẵn sàng ✅",
    }

    stop_reason = None

    live_context = (
        _HiloBotLive(state)
        if True
        else Live(hilo_make_vbtool_layout(state), console=console, refresh_per_second=8, screen=True)
    )

    with live_context as live:
        while True:
            if last_result == 'win':
                time.sleep(cfg['delay_win'])
            elif last_result == 'loss':
                time.sleep(cfg['delay_loss'])
            else:
                time.sleep(0.5)

            state["current_bet"] = current_bet
            state["balance"] = cfg["current_balance"]
            state["log_text"] = "Đang chuẩn bị ván mới..."
            live.update(hilo_make_vbtool_layout(state))

            if current_bet > cfg['current_balance']:
                if cfg['current_balance'] >= cfg['base_bet']:
                    current_bet = cfg['base_bet']
                else:
                    state["log_text"] = "Số dư không đủ để cược. Dừng tool!"
                    live.update(hilo_make_vbtool_layout(state))
                    if True:
                        print("⚠️ Số dư không đủ để đặt cược. HILO đã dừng.", flush=True)
                    break

            initial_card = random.randint(1, 13)
            create_payload = {
                "clientSeed": str(uuid.uuid4()),
                "nonce": int(time.time() * 1000) % 100000,
                "initialCard": initial_card,
            }

            try:
                response = session.post(
                    "https://hilo.turbogg4u.online/api/games/create",
                    json=create_payload,
                    timeout=(10, 30)
                )
                res_create = hilo__response_json(response, "CREATE")
                if 'roundId' not in res_create:
                    state["log_text"] = f"Đang kết nối lại"
                    live.update(hilo_make_vbtool_layout(state))
                    time.sleep(2)
                    continue

                round_id = res_create['roundId']
                actual_card = hilo__get_game_card(
                    res_create,
                    create_payload.get('initialCard')
                )
                current_play_card = actual_card
                state["last_issue"] = round_id
                state["current_card"] = actual_card
                state["next_prediction"] = "—"
                state["last_result"] = "ĐANG CHẠY"
                state["log_text"] = f"Ván #{round_id} | Bài gốc: {hilo__hilo_rank_label(actual_card)}"
                live.update(hilo_make_vbtool_layout(state))

            except Exception as exc:
                _notify_admin_code_error("HILO CREATE", exc, chat_id=str(_WORKER_CHAT_ID or ""), tool_key="hilo")
                state["log_text"] = "Lỗi tạo ván. Vui lòng thử lại!"
                live.update(hilo_make_vbtool_layout(state))
                time.sleep(3)
                continue

            game_active = True
            opened_cards = 0
            while game_active:
                time.sleep(cfg['delay_bet'])

                # Kiểm tra key hiện tại để bảo đảm LOGIC VIP không bị dùng bởi KEY FREE.
                try:
                    key_valid, key_reason, key_is_vip = check_activation_valid(exit_on_expired=False)
                except Exception as exc:
                    key_valid, key_reason, key_is_vip = False, str(exc), False

                if not key_valid:
                    state["log_text"] = f"KEY không hợp lệ: {key_reason}"
                    live.update(hilo_make_vbtool_layout(state))
                    game_active = False
                    break

                # Giữ nguyên logic mà người dùng đã chọn trong menu HILO cũ.
                runtime_logic = str(cfg.get("analysis_logic", "free")).lower()
                if runtime_logic not in ("free", "vip"):
                    runtime_logic = "free"

                # KEY FREE tuyệt đối không được chạy LOGIC VIP.
                if runtime_logic == "vip" and not key_is_vip:
                    state["log_text"] = "❌ LOGIC VIP chỉ dành cho Key VIP."
                    live.update(hilo_make_vbtool_layout(state))
                    game_active = False
                    break

                if runtime_logic == "vip":
                    action = hilo_predict_action_vip(current_play_card, analysis_history)
                else:
                    action = hilo_predict_action_free(current_play_card, analysis_history)
                action_text = hilo_action_label(action)

                stats["predictions"] += 1
                state["model_stats"] = stats
                state["next_prediction"] = action_text
                state["current_bet"] = current_bet
                state["log_text"] = f"Phân tích: {hilo__hilo_rank_label(current_play_card)} → {action_text}"
                live.update(hilo_make_vbtool_layout(state))

                place_payload = {
                    "action": action,
                    "roundId": round_id,
                    "theme": "default",
                    "currency": cfg['currency'],
                    "amount": current_bet
                }

                try:
                    response = session.post(
                        "https://hilo.turbogg4u.online/api/bets/place",
                        json=place_payload,
                        timeout=(10, 15)
                    )
                    res_place = hilo__response_json(response, "PLACE")
                    if 'status' not in res_place:
                        state["log_text"] = "Ván kết thúc."
                        live.update(hilo_make_vbtool_layout(state))
                        game_active = False
                        continue
                except requests.exceptions.ReadTimeout:
                    state["log_text"] = "Kết nối không ổn định."
                    live.update(hilo_make_vbtool_layout(state))
                    game_active = False
                    last_result = 'none'
                    time.sleep(3)
                    continue
                except requests.exceptions.RequestException:
                    state["log_text"] = "Lỗi mạng khi đặt cược. Ván kết thúc."
                    live.update(hilo_make_vbtool_layout(state))
                    game_active = False
                    last_result = 'none'
                    time.sleep(3)
                    continue
                except Exception as exc:
                    _notify_admin_code_error("HILO PLACE", exc, chat_id=str(_WORKER_CHAT_ID or ""), tool_key="hilo")
                    state["log_text"] = "Lỗi đặt cược. Ván kết thúc."
                    live.update(hilo_make_vbtool_layout(state))
                    game_active = False
                    last_result = 'none'
                    time.sleep(3)
                    continue

                status = res_place.get('status')
                drawn_card = hilo__get_game_card(res_place)

                try:
                    if res_place.get('balance') is not None:
                        cfg['current_balance'] = float(res_place['balance'])
                        state['balance'] = cfg['current_balance']
                except (TypeError, ValueError):
                    pass

                if status == 0:

                    relation = hilo_compare_card_result(current_play_card, drawn_card)
                    state["last_result"] = f"THUA ({relation})" if relation else "THUA"
                    stats["losses"] += 1
                    stats["predictions"] = max(stats["predictions"], stats["wins"] + stats["losses"])
                    state["model_stats"] = stats
                    hilo__history_append(state, round_id, current_play_card, drawn_card, "THUA", -float(current_bet))
                    state["balance"] = cfg['current_balance']
                    drawn_label = hilo__hilo_rank_label(drawn_card) if drawn_card is not None else "—"
                    if relation:
                        state["log_text"] = f"💀 THUA | {hilo__hilo_rank_label(current_play_card)} → {drawn_label} ({relation})"
                    else:
                        state["log_text"] = f"💀 THUA | {hilo__hilo_rank_label(current_play_card)} → {drawn_label}"
                    live.update(hilo_make_vbtool_layout(state))
                    last_result = 'loss'
                    try:
                        profile_response = session.post(
                            "https://hilo.turbogg4u.online/api/common/profile",
                            json=cfg['login_payload'], timeout=(10, 10)
                        )
                        profile = hilo__response_json(profile_response, "PROFILE")
                        cfg['current_balance'] = float(profile.get('balance', cfg['current_balance']))
                        state['balance'] = cfg['current_balance']
                    except Exception:
                        pass
                    current_bet *= cfg['loss_multiplier']
                    state["current_bet"] = current_bet
                    game_active = False

                elif status == 1:
                    opened_cards += 1
                    next_coeffs = res_place.get('coefficients', {}).get('next', {})
                    previous_card = current_play_card
                    relation = hilo_compare_card_result(previous_card, drawn_card)
                    if drawn_card is not None:
                        current_play_card = drawn_card
                        state["current_card"] = drawn_card
                        analysis_history.append(drawn_card)
                        if len(analysis_history) > 100:
                            analysis_history = analysis_history[-100:]
                    state["last_result"] = f"MỞ {opened_cards}"
                    state["log_text"] = f"TIẾP TỤC | {action_text} → {hilo__hilo_rank_label(drawn_card)} ({relation or '—'})"
                    live.update(hilo_make_vbtool_layout(state))

                    if not next_coeffs or opened_cards >= cfg['target_opens']:
                        cashout_payload = {"roundId": round_id}
                        try:
                            response = session.post(
                                "https://hilo.turbogg4u.online/api/bets/cashout",
                                json=cashout_payload,
                                timeout=(10, 15)
                            )
                            res_cashout = hilo__response_json(response, "CASHOUT")
                            payout = float(res_cashout.get('payout', 0.0))

                            raw_multiplier = (
                                res_cashout.get('multiplier')
                                or res_cashout.get('coefficient')
                                or res_cashout.get('coefficients', {}).get('current')
                                if isinstance(res_cashout.get('coefficients', {}), dict)
                                else None
                            )
                            try:
                                win_multiplier = float(raw_multiplier) if raw_multiplier is not None else (payout / current_bet if current_bet else 0.0)
                            except (TypeError, ValueError, ZeroDivisionError):
                                win_multiplier = payout / current_bet if current_bet else 0.0
                            last_result = 'win'
                            stats["wins"] += 1
                            stats["predictions"] = max(stats["predictions"], stats["wins"] + stats["losses"])
                            state["model_stats"] = stats
                            try:
                                profile_response = session.post(
                                    "https://hilo.turbogg4u.online/api/common/profile",
                                    json=cfg['login_payload'], timeout=(10, 10)
                                )
                                profile = hilo__response_json(profile_response, "PROFILE")
                                cfg['current_balance'] = float(profile.get('balance', cfg['current_balance']))
                            except Exception:
                                pass
                            state['balance'] = cfg['current_balance']
                            profit_now = cfg['current_balance'] - cfg['original_balance']
                            hilo__history_append(state, round_id, actual_card, current_play_card, "THẮNG", payout - current_bet)
                            state["last_result"] = "THẮNG"
                            state["log_text"] = f"✅ CHỐT LỜI | Nhân: x{win_multiplier:.2f} | Nhận: {payout:.2f} | Lãi/Lỗ: {profit_now:+.2f}"
                            current_bet = cfg['base_bet']
                            state["current_bet"] = current_bet
                            live.update(hilo_make_vbtool_layout(state))
                            game_active = False
                        except Exception as exc:
                            _notify_admin_code_error("HILO CASHOUT", exc, chat_id=str(_WORKER_CHAT_ID or ""), tool_key="hilo")
                            state["log_text"] = "Lỗi chốt lời. Ván kết thúc."
                            live.update(hilo_make_vbtool_layout(state))
                            game_active = False
                else:
                    state["last_result"] = "KẾT THÚC"
                    state["log_text"] = f"không thể đặt cược"
                    live.update(hilo_make_vbtool_layout(state))
                    last_result = 'none'
                    game_active = False

            state["balance"] = cfg['current_balance']
            state["current_bet"] = current_bet
            current_profit = cfg['current_balance'] - cfg['original_balance']

            if cfg['target_profit'] > 0 and current_profit >= cfg['target_profit']:
                stop_reason = f"🎉 ĐÃ ĐẠT CHỈ TIÊU CHỐT LỜI (+{current_profit:.2f}). DỪNG TOOL!"
                state["log_text"] = stop_reason
                state["last_result"] = "CHỐT LỜI"
                live.update(hilo_make_vbtool_layout(state))
                break

            if cfg['stop_loss'] > 0 and current_profit <= -cfg['stop_loss']:
                stop_reason = f"💀 ĐÃ ĐẠT NGƯỠNG CẮT LỖ ({current_profit:.2f}). DỪNG TOOL AN TOÀN!"
                state["log_text"] = stop_reason
                state["last_result"] = "CẮT LỖ"
                live.update(hilo_make_vbtool_layout(state))
                break

    if stop_reason:
        if True:
            print(stop_reason, flush=True)
        else:
            console.print("\n")
            if "CHỐT LỜI" in stop_reason:
                console.print(
                    Panel(
                        Text(stop_reason, justify="center"),
                        title="[bold green]🎉 CHỐT LỜI[/bold green]",
                        border_style=VBTOOL_COLORS["emerald"],
                        box=box.DOUBLE,
                        padding=(1, 3),
                        expand=False,
                    )
                )
            else:
                console.print(
                    Panel(
                        Text(stop_reason, justify="center"),
                        title="[bold red]💀 CẮT LỖ[/bold red]",
                        border_style=VBTOOL_COLORS["ruby"],
                        box=box.DOUBLE,
                        padding=(1, 3),
                        expand=False,
                    )
                )

        time.sleep(3)
        return

# ================== PHẦN 5: MAIN ==================
def main():
    
    return bot_main()

from collections import defaultdict
from typing import Dict, List, Sequence, Any

ATHLETES = tuple(range(1, 7))

def _sequence(history: Sequence[dict]) -> List[int]:
    out: List[int] = []
    for item in history or []:
        if not isinstance(item, dict):
            continue
        value = item.get("top1")
        try:
            value = int(value)
        except (TypeError, ValueError):
            continue
        if value in ATHLETES:
            out.append(value)
    return out


def _delay(seq: List[int], athlete: int) -> int:
    for i, value in enumerate(seq):
        if value == athlete:
            return i
    return len(seq) + 3


def _streak(seq: List[int], athlete: int) -> int:
    count = 0
    for value in seq:
        if value == athlete:
            count += 1
        else:
            break
    return count


def _weighted_recent(seq: List[int], athlete: int, limit: int = 40, decay: float = 0.92) -> float:
    score = 0.0
    weight = 1.0
    for value in seq[:limit]:
        if value == athlete:
            score += weight
        weight *= decay
    return score


def _transition(seq: List[int], previous: int, athlete: int, order: int = 1) -> float:
    if len(seq) <= order:
        return 0.0
    hits = total = 0
    if order == 1:
        for i in range(len(seq) - 1):
            if seq[i] == previous:
                total += 1
                if seq[i + 1] == athlete:
                    hits += 1
    else:
        for i in range(len(seq) - 2):
            if seq[i] == previous and seq[i + 1] == seq[0]:
                total += 1
                if seq[i + 2] == athlete:
                    hits += 1
    return hits / total if total else 0.0


def _avg_gap(seq: List[int], athlete: int):
    positions = [i for i, value in enumerate(seq) if value == athlete]
    if len(positions) < 2:
        return None
    gaps = [b - a for a, b in zip(positions, positions[1:])]
    return sum(gaps) / len(gaps)


def _base_score(seq: List[int], athlete: int, type_bet: str, mode: str) -> float:
    total = len(seq)
    if not total:
        return 50.0

    rate = seq.count(athlete) / total
    delay = _delay(seq, athlete)
    streak = _streak(seq, athlete)
    recent = _weighted_recent(seq, athlete)
    count10 = sum(1 for value in seq[:10] if value == athlete)
    score = 50.0

    if type_bet == "winner":
        score += (1.0 / 6.0 - rate) * 35.0
        score += min(delay * 0.55, 14.0)
        if delay >= 12:
            score += 7.0
        if streak:
            score += min(streak * 2.5, 8.0)
        score += recent * 2.0
        if count10 == 0:
            score += 5.0
        elif count10 >= 4:
            score -= 8.0
    else:
        score += (1.0 - rate) * 24.0
        score += min(delay * 0.65, 18.0)
        if delay <= 1:
            score -= 10.0
        if streak:
            score -= min(streak * 6.0, 18.0)
        score -= recent * 2.8
        if count10 == 0:
            score += 8.0
        elif count10 >= 4:
            score -= 10.0

    if len(seq) >= 2:
        previous = seq[0]
        trans = _transition(seq, previous, athlete, 1)
        score += trans * (24.0 if type_bet == "winner" else -20.0)

    if len(seq) >= 3:
        trans2 = _transition(seq, seq[1], athlete, 2)
        score += trans2 * (14.0 if type_bet == "winner" else -10.0)

    gap = _avg_gap(seq, athlete)
    if gap:
        if delay >= gap * 1.35:
            score += 7.0
        elif delay <= gap * 0.55:
            score -= 4.0

    if mode == "vip":
        recent20 = sum(1 for value in seq[:20] if value == athlete)
        recent60 = sum(1 for value in seq[:60] if value == athlete)
        if type_bet == "winner":
            score += (recent20 * 0.8) + (recent60 * 0.25)
        else:
            score -= (recent20 * 0.55) + (recent60 * 0.18)

    return max(0.0, min(100.0, score))


def analyze_cdtd_ai(history: Sequence[dict], type_bet: str = "winner", mode: str = "free") -> Dict[str, Any]:

    type_bet = "not_winner" if str(type_bet).lower() == "not_winner" else "winner"
    mode = "vip" if str(mode).lower() == "vip" else "free"
    seq = _sequence(history)

    scores = {athlete: _base_score(seq, athlete, type_bet, mode) for athlete in ATHLETES}
    ranked = sorted(ATHLETES, key=lambda athlete: (-scores[athlete], athlete))

    return {
        "module": "CDTD-AI",
        "mode": mode,
        "type_bet": type_bet,
        "history_size": len(seq),
        "scores": scores,
        "ranking": ranked,
        "selected_1": ranked[0] if ranked else None,
    }

if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--worker":

        run_worker(str(sys.argv[2]).strip().lower(), sys.argv[3] if len(sys.argv) >= 4 else None)
    else:
        _start_render_health_server()
        bot_main()
