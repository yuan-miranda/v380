import os
import json
import logging
import struct
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import zlib

import requests
from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    render_template_string,
    request,
    send_from_directory,
)
from flask_sock import Sock
from werkzeug.utils import secure_filename

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("alerto.server")

app = Flask(__name__)
sock = Sock(app)


def load_env_file(filename=".env"):
    if not os.path.exists(filename):
        return

    with open(filename, encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file()

UPLOAD_TOKEN = os.getenv("UPLOAD_TOKEN", "")
VIEW_TOKEN = os.getenv("VIEW_TOKEN", "")
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "/root/video/uploads"))
if not UPLOAD_DIR.is_absolute():
    UPLOAD_DIR = Path(__file__).resolve().parent / UPLOAD_DIR
MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE", str(500 * 1024 * 1024)))
EVENT_TOKEN = os.getenv("EVENT_TOKEN", UPLOAD_TOKEN)
PRODUCT_NAME = os.getenv("PRODUCT_NAME", "ALERTO").upper()
PHILSMS_URL = os.getenv("PHILSMS_URL", "")
PHILSMS_TOKEN = os.getenv("PHILSMS_TOKEN", "")
SENDER_ID = os.getenv("SENDER_ID", "PhilSMS")
SEND_SMS = os.getenv("SEND_SMS", "false").lower() in {"1", "true", "yes", "on"}
TARGET_MOBILE = os.getenv("TARGET_MOBILE", "")
VIDEO_DURATION_SECONDS = int(os.getenv("VIDEO_DURATION_SECONDS", "60"))
CCTV_IP = os.getenv("CCTV_IP", "192.168.100.57")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://alerto.ddns.net")
ALERT_LOCK_EXTRA_SECONDS = int(os.getenv("ALERT_LOCK_EXTRA_SECONDS", "180"))
MANILA_TIMEZONE = ZoneInfo("Asia/Manila")
CONFIG_PATH = Path(__file__).resolve().with_name("config.json")
DEFAULT_SMS_TEMPLATE = (
    "{product_name} EMERGENCY ALERT\n\n"
    "Category: {category}\n"
    "STEM 12 Newton Room\n"
    "{timestamp}\n\n"
    "Please proceed to the indicated location immediately and assess the situation. "
    "Visual incident documentation will be transmitted for review.\n\n"
    "Video: {video_url}\n\n"
)
SMS_TEMPLATE = DEFAULT_SMS_TEMPLATE
pending_events = []
active_event = None
events_lock = threading.Lock()
# Condition lets the worker wait for an event instead of polling an empty queue.
events_condition = threading.Condition(events_lock)

# Relay commands for the ESP32. The ESP32 long-polls /relay/next, just like the phone worker does with /events/next.
relay_commands = []
relay_condition = threading.Condition()
MAX_RELAY_QUEUE = 20
RELAY_COMMAND_TTL_SECONDS = 60  # stale commands are dropped instead of firing late
MAX_RELAY_DURATION_SECONDS = 3600
MAX_RELAY_BEEPS = 50

# Each WebSocket has its own send lock so one slow client cannot serialize all clients.
clients = {}
clients_lock = threading.Lock()

activity_logs = []
activity_lock = threading.Lock()
log_state = "Ready"
log_seq = 0
MAX_LOG_ENTRIES = 80
ALERT_CATEGORIES = {"1": "Hazard", "2": "Security", "3": "Medical Concern"}


class TemplateValues(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def render_sms_message(category, video_filename=""):
    if not video_filename:
        now_str = datetime.now(MANILA_TIMEZONE).strftime("%Y-%m-%d_%H-%M-%S-%f")
        btn_map = {"Hazard": "1", "Security": "2", "Medical Concern": "3"}
        b_id = btn_map.get(category, "2")
        video_filename = f"evidence_btn{b_id}_{now_str}.mp4"

    video_url = f"{PUBLIC_BASE_URL.rstrip('/')}/videos/{video_filename}?token={VIEW_TOKEN}"
    return SMS_TEMPLATE.format_map(
        TemplateValues(
            product_name=PRODUCT_NAME,
            category=category,
            timestamp=datetime.now(MANILA_TIMEZONE).strftime("%B %d, %Y — %I:%M %p"),
            video_url=video_url,
        )
    )


def _send_sms_to_recipient(recipient, message, button_id):
    headers = {
        "Authorization": f"Bearer {PHILSMS_TOKEN}",
        "Content-Type": "application/json",
    }
    requests.post(
        PHILSMS_URL,
        json={
            "recipient": recipient,
            "sender_id": SENDER_ID,
            "type": "plain",
            "message": message,
        },
        headers=headers,
        timeout=20,
    ).raise_for_status()
    logger.info("SMS sent: recipient=%s button=%s", recipient, button_id)


def send_sms_notification(button_id, video_filename=""):
    """Send SMS independently so the alert request is never blocked by SMS or video upload."""
    category = ALERT_CATEGORIES.get(button_id, "Hazard")
    recipients = recipient_list(TARGET_MOBILE)

    if not recipients:
        logger.warning("SMS not sent: no recipients configured")
        record_activity(
            f"[SMS_ERROR] No SMS recipients are configured for {category}.",
            "error",
        )
        return False

    if not SEND_SMS:
        logger.info("SMS disabled: recipients=%s", recipients)
        record_activity(
            f"[SMS_DISABLED] SMS sending is disabled for {category}.",
            "pending",
        )
        return False

    if not PHILSMS_URL or not PHILSMS_TOKEN:
        logger.warning("SMS not sent: PHILSMS_URL or PHILSMS_TOKEN is missing")
        record_activity(
            f"[SMS_ERROR] PhilSMS is not configured for {category}.",
            "error",
        )
        return False

    try:
        message = render_sms_message(category, video_filename)
    except ValueError:
        now_str = datetime.now(MANILA_TIMEZONE).strftime("%Y-%m-%d_%H-%M-%S-%f")
        fallback_video_url = (
            f"{PUBLIC_BASE_URL.rstrip('/')}/videos/"
            f"evidence_btn{button_id}_{now_str}.mp4?token={VIEW_TOKEN}"
        )
        message = DEFAULT_SMS_TEMPLATE.format_map(
            TemplateValues(
                product_name=PRODUCT_NAME,
                category=category,
                timestamp=datetime.now(MANILA_TIMEZONE).strftime(
                    "%B %d, %Y — %I:%M %p"
                ),
                video_url=fallback_video_url,
            )
        )

    record_activity(
        f"[SMS_DISPATCH] Sending {category} emergency SMS to {len(recipients)} recipient(s).",
        "pending",
    )

    success_count = 0
    with ThreadPoolExecutor(max_workers=min(10, len(recipients))) as executor:
        futures = {
            executor.submit(
                _send_sms_to_recipient, recipient, message, button_id
            ): recipient
            for recipient in recipients
        }
        for future in as_completed(futures):
            recipient = futures[future]
            try:
                future.result()
                success_count += 1
            except requests.RequestException as error:
                logger.error("SMS failed: recipient=%s error=%s", recipient, error)

    if success_count == len(recipients):
        record_activity(
            f"[SMS_SENT] Emergency SMS sent successfully to all {success_count} configured recipient(s).",
            "success",
        )
        return True

    if success_count:
        record_activity(
            f"[SMS_PARTIAL] Emergency SMS sent to {success_count}/{len(recipients)} recipient(s).",
            "error",
        )
        return False

    record_activity(
        f"[SMS_ERROR] Emergency SMS failed for all {len(recipients)} recipient(s).",
        "error",
    )
    return False


def dispatch_sms_async(button_id, video_filename):
    threading.Thread(
        target=send_sms_notification,
        args=(button_id, video_filename),
        name=f"sms-{button_id}",
        daemon=True,
    ).start()


def _clear_active_event(event_id, state="Ready"):
    global active_event

    with events_lock:
        if not active_event or active_event.get("id") != event_id:
            return False
        active_event = None

    broadcast(
        {
            "type": "alert_state",
            "busy": False,
            "active_event": None,
            "state": state,
        }
    )
    return True


def _expire_alert_lock(event_id):
    global active_event

    expired = False
    with events_lock:
        if active_event and active_event.get("id") == event_id:
            active_event = None
            pending_events.clear()
            expired = True

    if expired:
        record_activity(
            "[ALERT_TIMEOUT] Alert lock expired before the worker reported completion; new alerts are accepted.",
            "error",
            "Ready",
        )
        broadcast(
            {
                "type": "alert_state",
                "busy": False,
                "active_event": None,
                "state": "Ready",
            }
        )


def _schedule_alert_expiry(event_id, duration):
    timer = threading.Timer(
        max(30, duration + ALERT_LOCK_EXTRA_SECONDS),
        _expire_alert_lock,
        args=(event_id,),
    )
    timer.daemon = True
    timer.start()



def load_saved_configuration():
    if not CONFIG_PATH.is_file():
        return
    try:
        settings = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        logger.warning("Could not load config.json: %s", error)
        return

    global VIDEO_DURATION_SECONDS, TARGET_MOBILE, SMS_TEMPLATE, CCTV_IP
    try:
        VIDEO_DURATION_SECONDS = max(
            1,
            min(
                300, int(settings.get("VIDEO_DURATION_SECONDS", VIDEO_DURATION_SECONDS))
            ),
        )
    except (TypeError, ValueError):
        logger.warning(
            "Invalid VIDEO_DURATION_SECONDS in config.json; using %s",
            VIDEO_DURATION_SECONDS,
        )

    saved_ip = str(settings.get("CCTV_IP", CCTV_IP) or "").strip()
    if saved_ip:
        CCTV_IP = saved_ip

    saved_recipients = recipient_list(settings.get("TARGET_MOBILE", TARGET_MOBILE))
    if saved_recipients:
        TARGET_MOBILE = ",".join(saved_recipients)
    saved_template = str(settings.get("SMS_TEMPLATE", SMS_TEMPLATE) or "").strip()
    if saved_template:
        SMS_TEMPLATE = saved_template
    logger.info(
        "Loaded config.json: duration=%ss cctv_ip=%s recipients=%s",
        VIDEO_DURATION_SECONDS,
        CCTV_IP,
        recipient_list(TARGET_MOBILE),
    )


UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_SIZE


def has_token(expected_token):
    return (
        bool(expected_token)
        and request.headers.get("Authorization") == f"Bearer {expected_token}"
    )


def has_view_token():
    return bool(VIEW_TOKEN) and (
        has_token(VIEW_TOKEN) or request.args.get("token") == VIEW_TOKEN
    )


def recipient_list(value):
    values = str(value).replace(";", ",").replace("\n", ",").split(",")
    return [normalize_recipient(item) for item in values if normalize_recipient(item)]


def normalize_recipient(value):
    digits = "".join(
        character for character in str(value).strip() if character.isdigit()
    )
    if digits.startswith("09") and len(digits) == 11:
        return "63" + digits[1:]
    if digits.startswith("9") and len(digits) == 10:
        return "63" + digits
    if digits.startswith("63") and len(digits) == 12:
        return digits
    return ""


def display_recipient(value):
    normalized = normalize_recipient(value)
    return "0" + normalized[2:] if normalized else ""


def configuration_payload():
    recipients = recipient_list(TARGET_MOBILE)
    with events_lock:
        current_event = dict(active_event) if active_event else None

    return {
        "duration": VIDEO_DURATION_SECONDS,
        "cctv_ip": CCTV_IP,
        "recipients": (
            [display_recipient(recipient) for recipient in recipients] + [""] * 10
        )[:10],
        "message": SMS_TEMPLATE,
        "busy": current_event is not None,
        "active_event": current_event,
    }



def broadcast(payload):
    data = json.dumps(payload)
    with clients_lock:
        socket_items = list(clients.items())

    for websocket, client_lock in socket_items:
        try:
            with client_lock:
                websocket.send(data)
        except Exception:
            with clients_lock:
                clients.pop(websocket, None)



def record_activity(message, level, state=None):
    global log_seq, log_state
    with activity_lock:
        log_seq += 1
        if state:
            log_state = state
        entry = {
            "id": log_seq,
            "time": datetime.now(MANILA_TIMEZONE).strftime("%I:%M:%S %p"),
            "message": message,
            "level": level,
        }
        activity_logs.insert(0, entry)
        del activity_logs[MAX_LOG_ENTRIES:]
        current_state = log_state
    broadcast({"type": "log", "entry": entry, "state": current_state})
    return entry


def clear_activity_logs():
    global log_state
    with activity_lock:
        activity_logs.clear()
        log_state = "Ready"
    broadcast({"type": "logs_cleared", "logs": [], "state": "Ready"})


def activity_snapshot():
    with activity_lock:
        return {"logs": list(activity_logs), "state": log_state}


load_saved_configuration()


WEB_PAGE = """
<!doctype html>
<html>
<head>
    <title>{{ product_name }} Reporter</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="manifest" href="/manifest.json">
    <meta name="theme-color" content="#b91c1c">
    <link rel="apple-touch-icon" href="/icon-192.png">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <meta name="mobile-web-app-capable" content="yes">
    <style>
        :root { color-scheme: light; }
        * { box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; text-align: center; margin: 0; min-height: 100dvh; background: #e2e8f0; user-select: none; -webkit-user-select: none; }
        input, textarea { user-select: text; -webkit-user-select: text; }
        .container { background: #f8fafc; min-height: 100dvh; width: 100%; padding: max(32px, env(safe-area-inset-top)) max(20px, env(safe-area-inset-right)) max(28px, env(safe-area-inset-bottom)) max(20px, env(safe-area-inset-left)); display: flex; flex-direction: column; justify-content: center; align-items: center; overflow: hidden; }
        h2 { color: #0f172a; margin: 0 0 8px; font-family: Georgia, "Times New Roman", serif; font-size: clamp(28px, 7vw, 38px); font-weight: 700; line-height: 1.1; }
        .subtitle { color: #64748b; margin: 0 0 30px; font-size: 14px; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase; }
        .button-stack { display: flex; flex-direction: column; gap: 16px; width: 100%; max-width: 34rem; margin: 0 auto; }
        button { width: 100%; height: clamp(160px, 38vw, 220px); padding: 18px; font-size: clamp(21px, 6vw, 28px); color: white; border: none; border-radius: 0; cursor: pointer; font-weight: 700; box-shadow: none; transition: transform 0.1s ease, opacity 0.2s; touch-action: manipulation; }
        button:active { transform: scale(0.98); opacity: 0.9; } button:disabled { opacity: 0.45; cursor: not-allowed; transform: none; }
        .btn-hazard { background: #d97706; } .btn-security { background: #b91c1c; } .btn-medical { background: #047857; }
        .btn-disarm { background: #1e293b; height: auto; min-height: 0; padding: 16px; font-size: clamp(16px, 4.5vw, 20px); }
        .activity-log { width: 100%; max-width: 34rem; margin: 24px auto 0; border: 1px solid #cbd5e1; background: #ffffff; text-align: left; }
        .log-header { display: flex; justify-content: space-between; align-items: center; gap: 12px; padding: 12px 14px; border-bottom: 1px solid #e2e8f0; color: #1e293b; font-size: 13px; font-weight: 700; letter-spacing: 0.04em; text-transform: uppercase; }
        .log-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
        .log-action { height: auto; width: auto; min-height: 0; padding: 5px 8px; background: #e2e8f0; color: #334155; font-size: 11px; font-weight: 700; }
        #status { min-height: 54px; max-height: 150px; overflow-y: auto; }
        .log-empty, .log-entry { padding: 11px 14px; font-size: 13px; line-height: 1.35; }
        .log-empty { color: #64748b; } .log-entry { display: flex; gap: 10px; border-bottom: 1px solid #f1f5f9; color: #334155; }
        .log-time { flex: 0 0 auto; color: #94a3b8; } .log-entry.success .log-message { color: #047857; } .log-entry.error .log-message { color: #b91c1c; } .log-entry.pending .log-message { color: #b45309; }
        .configuration { display: none; width: 100%; max-width: 34rem; margin: 10px auto 0; padding: 14px; border: 1px solid #cbd5e1; background: #ffffff; text-align: left; }
        .configuration.open { display: block; } .configuration label { display: block; margin-bottom: 5px; color: #334155; font-size: 12px; font-weight: 700; }
        .field-heading { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 5px; }
        .field-heading label { margin-bottom: 0; }
        .config-reset { height: auto; width: auto; min-height: 0; padding: 4px 8px; background: #e2e8f0; color: #334155; font-size: 11px; }
        .configuration input, .configuration textarea { width: 100%; margin-bottom: 12px; padding: 9px 10px; border: 1px solid #cbd5e1; color: #1e293b; font: inherit; }
        .configuration textarea { min-height: 180px; resize: vertical; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; line-height: 1.45; }
        .configuration .hint { margin: -4px 0 12px; color: #64748b; font-size: 11px; line-height: 1.4; }
        .recipient-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; margin-bottom: 12px; } .recipient-grid input { margin-bottom: 0; min-width: 0; }
        .config-save { height: auto; min-height: 0; width: auto; padding: 9px 12px; background: #0f766e; font-size: 12px; }
        .location { width: 100%; max-width: 34rem; margin: 18px auto 0; color: #64748b; font-size: clamp(11px, 3.2vw, 14px); line-height: 1.4; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; text-align: center; }

        @media (min-width: 700px) { .container { padding: 48px; } .button-stack { gap: 18px; } button { height: 220px; } }
    </style>
</head>
<body>
    <div class="container">
        <h2>{{ product_name }}</h2><p class="subtitle">Emergency Reporting System</p>
        <div class="button-stack"><button class="btn-hazard" onclick="triggerAlert(1)">Hazard</button><button class="btn-security" onclick="triggerAlert(2)">Security</button><button class="btn-medical" onclick="triggerAlert(3)">Medical Concern</button><button class="btn-disarm" type="button" onclick="disarmAlarm()">Disarm Alarm</button></div>
        <section class="activity-log"><div class="log-header"><span>Activity log</span><div class="log-actions"><button class="log-action" id="log-state" type="button" disabled>Ready</button><button class="log-action" type="button" onclick="clearLog()">Clear log</button><button class="log-action" type="button" onclick="document.getElementById('configuration').classList.toggle('open')">Configuration</button></div></div><div id="status"><div class="log-empty">No alerts recorded in this session.</div></div></section>
        <form class="configuration" id="configuration" onsubmit="saveConfiguration(event)">
            <label for="cctv_ip">CCTV IP Address</label>
            <input id="cctv_ip" type="text" value="{{ cctv_ip }}" required>
            <label for="duration">Video duration (seconds)</label>
            <input id="duration" type="number" min="1" max="300" value="{{ duration }}" required>
            <label>SMS recipients (up to 10 numbers)</label>
            <p class="hint">Note: PhilSMS is not supported for SMART simcard subscribers.</p>        
            <div class="recipient-grid">{% for recipient in recipient_values %}<input class="recipient-slot" type="tel" inputmode="tel" autocomplete="tel" maxlength="11" value="{{ recipient }}" placeholder="09XXXXXXXXX">{% endfor %}</div>
            <div class="field-heading"><label for="message">SMS message</label><button class="config-reset" type="button" onclick="resetSmsTemplate()">Reset</button></div>
            <textarea id="message" name="message" required>{{ sms_template }}</textarea>
            <p class="hint">Variables: {product_name}, {category}, {timestamp}, {video_url}</p>
            <button class="config-save" type="submit">Save configuration</button>
        </form>
        <div class="location">STEM Department Building &bull; STEM 12 Newton Room</div>
    </div>
<script>
if ('serviceWorker' in navigator) { window.addEventListener('load', () => navigator.serviceWorker.register('/sw.js')); }

function formatRecipient(input) { let digits = input.value.replace(/\D/g, ""); if (digits.startsWith("63")) digits = "0" + digits.slice(2); if (digits.startsWith("9")) digits = "0" + digits; input.value = digits.slice(0, 11); }
document.querySelectorAll(".recipient-slot").forEach(input => { input.addEventListener("input", () => formatRecipient(input)); input.addEventListener("blur", () => formatRecipient(input)); });
const alertConfiguration = { duration: Number(document.getElementById("duration").value), recipient: Array.from(document.querySelectorAll(".recipient-slot")).map(input => input.value).filter(Boolean).join(",") };
const defaultSmsTemplate = {{ default_sms_template | tojson }};
const seenLogIds = new Set();
let syncSocket = null;
let socketConnected = false;
let alertBusy = false;

function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, character => ({
        "&":"&amp;", "<":"&lt;", ">":"&gt;",
        [String.fromCharCode(34)]:"&quot;", "'":"&#39;"
    }[character]));
}
function setLogState(state) {
    document.getElementById("log-state").innerText = state || "Ready";
}
function setAlertBusy(busy, state) {
    alertBusy = Boolean(busy);
    document.querySelectorAll(".button-stack button").forEach(button => {
        button.disabled = alertBusy;
    });
    if (state) setLogState(state);
}
function logEntryHtml(entry) {
    return '<div class="log-entry ' + escapeHtml(entry.level || "") +
        '"><span class="log-time">' + escapeHtml(entry.time || "") +
        '</span><span class="log-message">' + escapeHtml(entry.message || "") +
        '</span></div>';
}
function renderLogs(logs) {
    seenLogIds.clear();
    const status = document.getElementById("status");
    if (!logs || !logs.length) {
        status.innerHTML = '<div class="log-empty">No alerts recorded in this session.</div>';
        return;
    }
    logs.forEach(entry => { if (entry.id) seenLogIds.add(entry.id); });
    status.innerHTML = logs.map(logEntryHtml).join("");
}
function addLogEntry(entry, state) {
    if (!entry) return;
    if (entry.id) {
        if (seenLogIds.has(entry.id)) {
            if (state) setLogState(state);
            return;
        }
        seenLogIds.add(entry.id);
    }
    const status = document.getElementById("status");
    const empty = status.querySelector(".log-empty");
    if (empty) empty.remove();
    status.insertAdjacentHTML("afterbegin", logEntryHtml(entry));
    if (state) setLogState(state);
}
function addLog(message, level) {
    addLogEntry({
        time: new Date().toLocaleTimeString([], {
            hour: "2-digit", minute: "2-digit", second: "2-digit"
        }),
        message: message,
        level: level
    });
}
function applyConfiguration(data) {
    if (!data) return;
    if (data.cctv_ip != null) document.getElementById("cctv_ip").value = data.cctv_ip;
    if (data.duration != null) {
        document.getElementById("duration").value = data.duration;
        alertConfiguration.duration = Number(data.duration);
    }
    if (Array.isArray(data.recipients)) {
        document.querySelectorAll(".recipient-slot").forEach((input, index) => {
            input.value = data.recipients[index] || "";
        });
        alertConfiguration.recipient = data.recipients.filter(Boolean).join(",");
    }
    if (data.message != null) document.getElementById("message").value = data.message;
    if (data.busy != null) {
        setAlertBusy(data.busy, data.busy ? "Working" : (data.state || "Ready"));
    }
}
window.toggleConfiguration = function() {
    document.getElementById("configuration").classList.toggle("open");
};
function resetSmsTemplate() {
    document.getElementById("message").value = defaultSmsTemplate;
}
function saveConfiguration(event) {
    event.preventDefault();
    const cctv_ip = document.getElementById("cctv_ip").value.trim();
    const duration = Number(document.getElementById("duration").value);
    const recipient = Array.from(document.querySelectorAll(".recipient-slot"))
        .map(input => input.value.trim())
        .filter(Boolean)
        .join(",");
    const message = document.getElementById("message").value;

    fetch("/configuration", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({cctv_ip, duration, recipient, message})
    }).then(response => response.json().then(data => {
        if (!response.ok) throw Error(data.error || ("Server returned HTTP " + response.status));
        return data;
    })).then(() => {
        if (!socketConnected) addLog("[CONFIG] System settings updated successfully.", "success");
    }).catch(error => {
        addLog("[CONFIG_ERROR] Failed to save configuration: " + error.message, "error");
    });
}
function clearLog() {
    renderLogs([]);
    setLogState("Ready");
    if (syncSocket && syncSocket.readyState === WebSocket.OPEN) {
        syncSocket.send(JSON.stringify({type: "clear_logs"}));
    }
}
function triggerAlert(buttonId) {
    // Client-side guard gives immediate protection against touch/click spam.
    if (alertBusy) return;

    const category = {1: "Hazard", 2: "Security", 3: "Medical concern"}[buttonId];
    setAlertBusy(true, "Working");

    // The server performs a second atomic guard, so simultaneous requests from
    // multiple tabs/devices cannot create multiple active emergency events.
    fetch("/trigger-alert", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
            button: String(buttonId),
            duration: alertConfiguration.duration
        })
    }).then(async response => {
        const data = await response.json().catch(() => ({}));
        if (response.status === 409 && data.busy) {
            setAlertBusy(true, "Working");
            return;
        }
        if (!response.ok) {
            throw Error(data.error || ("Server returned HTTP " + response.status));
        }
    }).catch(error => {
        setAlertBusy(false, "Attention");
        addLog("[ALERT_ERROR] " + category + " alert dispatch failed: " + error.message, "error");
    });
}
function disarmAlarm() {
    // Not gated on alertBusy: the alarm can be silenced any time it's sounding,
    // independent of whether SMS/recording are still in progress.
    fetch("/disarm-alarm", { method: "POST" })
        .then(async response => {
            const data = await response.json().catch(() => ({}));
            if (!response.ok) throw Error(data.error || ("Server returned HTTP " + response.status));
            addLog("[ALARM] Disarm command sent.", "success");
        })
        .catch(error => {
            addLog("[ALARM_ERROR] Failed to disarm alarm: " + error.message, "error");
        });
}
function connectSync() {
    const socket = new WebSocket(
        (location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws"
    );
    syncSocket = socket;

    socket.onopen = () => {
        if (syncSocket === socket) socketConnected = true;
    };
    socket.onmessage = event => {
        if (syncSocket !== socket) return;
        try {
            const data = JSON.parse(event.data);
            if (data.type === "sync") {
                applyConfiguration(data);
                renderLogs(data.logs || []);
                setAlertBusy(Boolean(data.busy), data.busy ? "Working" : (data.state || "Ready"));
            } else if (data.type === "configuration") {
                applyConfiguration(data);
            } else if (data.type === "log") {
                addLogEntry(data.entry, data.state);
            } else if (data.type === "alert_state") {
                setAlertBusy(Boolean(data.busy), data.state || (data.busy ? "Working" : "Ready"));
            } else if (data.type === "logs_cleared") {
                renderLogs([]);
                setLogState(data.state || "Ready");
            }
        } catch (error) {
            addLog("[SOCKET_ERROR] Invalid synchronization message received.", "error");
        }
    };
    socket.onclose = () => {
        if (syncSocket !== socket) return;
        socketConnected = false;
        setTimeout(() => {
            if (syncSocket === socket) connectSync();
        }, 1000);
    };
    socket.onerror = () => socket.close();
}
connectSync();
</script></body></html>
"""


@app.get("/")
def home():
    saved = {}
    if CONFIG_PATH.is_file():
        try:
            saved = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            saved = {}
    recipients = recipient_list(saved.get("TARGET_MOBILE", TARGET_MOBILE))
    return render_template_string(
        WEB_PAGE,
        product_name=PRODUCT_NAME,
        duration=saved.get("VIDEO_DURATION_SECONDS", VIDEO_DURATION_SECONDS),
        cctv_ip=saved.get("CCTV_IP", CCTV_IP),
        recipient_values=(
            [display_recipient(recipient) for recipient in recipients] + [""] * 10
        )[:10],
        sms_template=saved.get("SMS_TEMPLATE", SMS_TEMPLATE),
        default_sms_template=DEFAULT_SMS_TEMPLATE,
    )


@app.get("/manifest.json")
def manifest():
    return jsonify(
        {
            "name": f"{PRODUCT_NAME} Emergency Alert System",
            "short_name": PRODUCT_NAME,
            "start_url": "/",
            "display": "standalone",
            "background_color": "#f8fafc",
            "theme_color": "#b91c1c",
            "icons": [
                {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png"},
                {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png"},
            ],
        }
    )


@app.get("/icon-<int:size>.png")
def icon(size):
    if size not in (192, 512):
        return "Not found", 404

    def chunk(tag, data):
        return (
            struct.pack(">I", len(data))
            + tag
            + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    row = b"\x00" + bytes((185, 28, 28)) * size
    png = b"\x89PNG\r\n\x1a\n"
    png += chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
    png += chunk(b"IDAT", zlib.compress(row * size, 9))
    return Response(png + chunk(b"IEND", b""), mimetype="image/png")


@app.get("/sw.js")
def service_worker():
    sw_code = """
    self.addEventListener('install', event => self.skipWaiting());
    self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));
    self.addEventListener('fetch', event => {
        if (event.request.headers.get('Upgrade') === 'websocket') return;
        event.respondWith(fetch(event.request));
    });
    """
    return Response(sw_code, mimetype="application/javascript")


@app.get("/worker-config")
def worker_config():
    if not has_token(EVENT_TOKEN):
        return jsonify(error="Unauthorized"), 401
    return jsonify(cctv_ip=CCTV_IP, duration=VIDEO_DURATION_SECONDS)


@app.get("/worker-env")
def worker_env():
    if not has_token(EVENT_TOKEN):
        return jsonify(error="Unauthorized"), 401

    # Always generate the worker profile from the server's CURRENT runtime
    # configuration. Do not return a stale server-side .env file, because the
    # website may have changed CCTV_IP after that file was created.
    env_text = f"""CCTV_IP={CCTV_IP}
VIDEO_DURATION_SECONDS={VIDEO_DURATION_SECONDS}
VPS_ENDPOINT={PUBLIC_BASE_URL.rstrip('/')}/upload
VPS_TOKEN={UPLOAD_TOKEN}
VPS_EVENT_ENDPOINT={PUBLIC_BASE_URL.rstrip('/')}/events/next
VPS_EVENT_STATUS_ENDPOINT={PUBLIC_BASE_URL.rstrip('/')}/events/status
VPS_EVENT_TOKEN={EVENT_TOKEN}
VPS_CONFIG_ENDPOINT={PUBLIC_BASE_URL.rstrip('/')}/worker-config
VPS_ENV_ENDPOINT={PUBLIC_BASE_URL.rstrip('/')}/worker-env
RTSP_USER=admin
RTSP_PASS=password
POLL_INTERVAL_SECONDS=0.25
"""
    logger.info(
        "Worker configuration requested: CCTV_IP=%s duration=%ss",
        CCTV_IP,
        VIDEO_DURATION_SECONDS,
    )
    return Response(env_text, mimetype="text/plain")


def _accept_alert(button_id, duration, source):
    global active_event

    now = time.time()
    timestamp = datetime.now(MANILA_TIMEZONE).strftime("%Y-%m-%d_%H-%M-%S-%f")
    event_id = uuid.uuid4().hex
    video_filename = f"evidence_btn{button_id}_{timestamp}_{event_id[:8]}.mp4"

    event = {
        "id": event_id,
        "button": button_id,
        "duration": duration,
        "filename": video_filename,
        # Snapshot the exact CCTV configuration used for this alert.
        # The worker uses this value for FFmpeg, so a later config change
        # cannot cause the event to record from a different camera.
        "cctv_ip": CCTV_IP,
        "created_at": now,
        "expires_at": now + duration + ALERT_LOCK_EXTRA_SECONDS,
        "source": source,
    }

    with events_lock:
        # Atomic single-alert gate: the first event wins; concurrent events are rejected.
        if active_event is not None:
            return None
        active_event = event
        pending_events.append(event)
        queue_size = len(pending_events)
        events_condition.notify_all()

    category = ALERT_CATEGORIES[button_id]
    logger.info(
        "Alert accepted: source=%s event_id=%s button=%s duration=%ss filename=%s queue_size=%s",
        source,
        event_id,
        button_id,
        duration,
        video_filename,
        queue_size,
    )
    record_activity(
        f"[{source.upper()}] {category} emergency accepted. SMS dispatch started and recording worker queued.",
        "pending",
        "Working",
    )
    broadcast({
        "type": "alert_state",
        "busy": True,
        "active_event": event,
        "state": "Working",
    })

    # SMS and recording are independent: video duration/upload can never delay SMS dispatch.
    dispatch_sms_async(button_id, video_filename)
    _schedule_alert_expiry(event_id, duration)
    _queue_alarm_for_button(button_id)
    return event


def _queue_alarm_for_button(button_id):
    """Fire the ESP32 relay/buzzer according to the category of the alert.

    Hazard (1)   -> beep every 5s, for 30s total (6 short beeps, 1s on / 4s off)
    Security (2) -> relay held ON continuously for 30s, no beeping
    Medical (3)  -> no alarm
    """
    if button_id == "1":
        command = {
            "id": uuid.uuid4().hex,
            "beeps": 6,
            "on_seconds": 1.0,
            "off_seconds": 4.0,
            "created_at": time.time(),
        }
        _queue_relay_command(command, "Hazard alarm (beep every 5s for 30s)")
    elif button_id == "2":
        command = {
            "id": uuid.uuid4().hex,
            "power": True,
            "duration": 30,
            "created_at": time.time(),
        }
        _queue_relay_command(command, "Security alarm (continuous 30s)")
    # button_id == "3" (Medical Concern): no alarm.


@app.post("/events")
def create_event():
    if not has_token(EVENT_TOKEN):
        return jsonify(error="Unauthorized"), 401

    data = request.get_json(silent=True) or {}
    button_id = str(data.get("button", "")).strip()
    if button_id not in {"1", "2", "3"}:
        return jsonify(error="button must be 1, 2, or 3"), 400

    try:
        duration = max(1, min(300, int(data.get("duration", VIDEO_DURATION_SECONDS))))
    except (TypeError, ValueError):
        duration = VIDEO_DURATION_SECONDS

    event = _accept_alert(button_id, duration, "hardware_signal")
    if event is None:
        logger.warning("Concurrent hardware alert ignored because another alert is active.")
        return jsonify(message="Alert ignored; another alert is already active.", busy=True), 409

    return jsonify(
        message="Alert accepted; SMS and recording started independently.",
        event_id=event["id"],
        filename=event["filename"],
    ), 202


@app.post("/configuration")
def save_configuration():
    global VIDEO_DURATION_SECONDS, TARGET_MOBILE, SMS_TEMPLATE, CCTV_IP
    settings = request.get_json(silent=True) or {}
    try:
        duration = max(
            1, min(300, int(settings.get("duration", VIDEO_DURATION_SECONDS)))
        )
    except (TypeError, ValueError):
        return jsonify(error="Video duration must be between 1 and 300 seconds"), 400

    cctv_ip = str(settings.get("cctv_ip", CCTV_IP)).strip()
    if not cctv_ip:
        return jsonify(error="CCTV IP address is required"), 400

    recipients = recipient_list(settings.get("recipient", TARGET_MOBILE))
    if not recipients:
        return jsonify(error="At least one SMS recipient is required"), 400
    message = str(settings.get("message", SMS_TEMPLATE) or "").strip()
    if not message:
        return jsonify(error="SMS message template is required"), 400
    try:
        message.format_map(
            TemplateValues(product_name="", category="", timestamp="", video_url="")
        )
    except ValueError:
        return (
            jsonify(
                error="SMS message has invalid braces. Use {product_name}, {category}, {timestamp}, {video_url}"
            ),
            400,
        )
    VIDEO_DURATION_SECONDS = duration
    CCTV_IP = cctv_ip
    TARGET_MOBILE = ",".join(recipients)
    SMS_TEMPLATE = message
    CONFIG_PATH.write_text(
        json.dumps(
            {
                "VIDEO_DURATION_SECONDS": duration,
                "CCTV_IP": CCTV_IP,
                "TARGET_MOBILE": TARGET_MOBILE,
                "SMS_TEMPLATE": SMS_TEMPLATE,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    logger.info(
        "Configuration saved: duration=%ss cctv_ip=%s recipients=%s",
        duration,
        CCTV_IP,
        recipients,
    )
    broadcast({"type": "configuration", **configuration_payload()})
    record_activity("[CONFIG] System parameters updated and synchronized with active nodes.", "success")
    return jsonify(message="Configuration saved"), 200


@app.post("/trigger-alert")
def trigger_alert():
    data = request.get_json(silent=True) or {}
    button_id = str(data.get("button", "")).strip()
    if button_id not in {"1", "2", "3"}:
        return jsonify(error="button must be 1, 2, or 3"), 400

    try:
        duration = max(1, min(300, int(data.get("duration", VIDEO_DURATION_SECONDS))))
    except (TypeError, ValueError):
        duration = VIDEO_DURATION_SECONDS

    event = _accept_alert(button_id, duration, "web_trigger")
    if event is None:
        logger.warning("Concurrent web alert ignored because another alert is active.")
        return jsonify(error="Another alert is already active.", busy=True), 409

    return jsonify(
        message="Alert accepted; SMS and recording started independently.",
        event_id=event["id"],
        filename=event["filename"],
    ), 202


@app.get("/events/next")
def next_event():
    if not has_token(EVENT_TOKEN):
        return jsonify(error="Unauthorized"), 401

    # Long-poll the queue. The worker no longer has to repeatedly reconnect to
    # an empty endpoint, and an accepted alert wakes the request immediately.
    # A bounded timeout keeps the connection recyclable if a proxy is idle.
    wait_seconds = min(max(request.args.get("wait", default=25, type=float), 0.0), 30.0)
    with events_condition:
        if not pending_events and wait_seconds > 0:
            events_condition.wait(timeout=wait_seconds)
        if not pending_events:
            return jsonify(event=None), 200
        event = pending_events.pop(0)
        queue_size = len(pending_events)

    logger.info(
        "Event delivered to phone worker: event_id=%s queue_size=%s",
        event.get("id"),
        queue_size,
    )
    return jsonify(event=event), 200


@app.post("/events/status")
def event_status():
    if not has_token(EVENT_TOKEN):
        return jsonify(error="Unauthorized"), 401

    data = request.get_json(silent=True) or {}
    event_id = str(data.get("event_id", "")).strip()
    status = str(data.get("status", "")).strip().lower()
    error = str(data.get("error", "")).strip()

    allowed_statuses = {
        "received",
        "recording_started",
        "recording_complete",
        "upload_started",
        "completed",
        "failed",
    }
    if not event_id or status not in allowed_statuses:
        return jsonify(error="event_id and a valid status are required"), 400

    with events_lock:
        current_event = dict(active_event) if active_event else None

    if current_event is None or current_event.get("id") != event_id:
        return jsonify(message="Event is no longer active.", active=False), 200

    category = ALERT_CATEGORIES.get(current_event.get("button"), "Alert")

    if status == "received":
        record_activity(
            f"[WORKER_SYNC] {category} event received by the recording worker.",
            "pending",
        )
    elif status == "recording_started":
        record_activity(
            f"[RECORDING] {category} CCTV capture started.",
            "pending",
            "Recording",
        )
    elif status == "recording_complete":
        record_activity(
            f"[RECORDING] {category} CCTV capture completed.",
            "success",
            "Uploading",
        )
    elif status == "upload_started":
        record_activity(
            f"[MEDIA_UPLOAD] {category} video upload started.",
            "pending",
            "Uploading",
        )
    elif status == "completed":
        record_activity(
            f"[ALERT_COMPLETE] {category} alert sequence completed; video is available.",
            "success",
            "Ready",
        )
        _clear_active_event(event_id, "Ready")
    elif status == "failed":
        detail = f": {error}" if error else ""
        record_activity(
            f"[ALERT_ERROR] {category} alert sequence failed{detail}",
            "error",
            "Ready",
        )
        _clear_active_event(event_id, "Ready")

    return jsonify(message="Status synchronized.", active=True), 200


def _parse_power(value):
    """Accept true/false, 1/0, "on"/"off". Returns None if it can't be understood."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "on", "yes"}:
            return True
        if text in {"0", "false", "off", "no"}:
            return False
    return None


def _queue_relay_command(command, description):
    with relay_condition:
        relay_commands.append(command)
        del relay_commands[:-MAX_RELAY_QUEUE]
        relay_condition.notify_all()
    logger.info("Relay command queued: %s", command)
    record_activity(f"[RELAY] {description} queued for the ESP32.", "pending")
    return jsonify(message="Relay command queued.", command=command), 202


@app.post("/disarm-alarm")
def disarm_alarm():
    """Public endpoint for the web page's Disarm button: immediately cuts the
    relay, interrupting a beep pattern or a continuous-on alarm mid-way."""
    command = {
        "id": uuid.uuid4().hex,
        "power": False,
        "duration": 0,
        "created_at": time.time(),
    }
    _queue_relay_command(command, "Alarm manually disarmed from the web app")
    return jsonify(message="Disarm command queued."), 202


@app.post("/relay")
def create_relay_command():
    """Queue a relay command. Two forms:

    1) Simple on/off:   {"power": true, "duration": 10}
       power    - true = relay ON, false = relay OFF
       duration - seconds to hold that state, then the ESP32 flips back.
                  0 (or omitted) = hold until the next command.

    2) Beep pattern:    {"beeps": 3, "on": 1, "off": 1}
       beeps - how many times to ring (1-50)
       on    - seconds the relay is ON for each beep  (0.05-60, default 1)
       off   - seconds of silence between beeps       (0.05-60, default 1)
       The whole pattern is sent in ONE command and timed by the ESP32 itself,
       so the beeps are evenly spaced. Patterns queue up: the ESP32 finishes
       one before it asks for the next.
    """
    if not has_token(EVENT_TOKEN):
        return jsonify(error="Unauthorized"), 401

    data = request.get_json(silent=True) or {}

    if "beeps" in data:
        try:
            beeps = int(data.get("beeps"))
            on_seconds = float(data.get("on", 1))
            off_seconds = float(data.get("off", 1))
        except (TypeError, ValueError):
            return jsonify(error="beeps must be a whole number; on/off must be seconds"), 400
        beeps = max(1, min(MAX_RELAY_BEEPS, beeps))
        on_seconds = round(max(0.05, min(60.0, on_seconds)), 3)
        off_seconds = round(max(0.05, min(60.0, off_seconds)), 3)
        command = {
            "id": uuid.uuid4().hex,
            "beeps": beeps,
            "on_seconds": on_seconds,
            "off_seconds": off_seconds,
            "created_at": time.time(),
        }
        return _queue_relay_command(
            command, f"Beep pattern ({beeps}x, {on_seconds}s on / {off_seconds}s off)"
        )

    power = _parse_power(data.get("power"))
    if power is None:
        return jsonify(error="send either power (true/false) or beeps (number)"), 400

    try:
        duration = int(data.get("duration", 0))
    except (TypeError, ValueError):
        return jsonify(error="duration must be a number of seconds"), 400
    duration = max(0, min(MAX_RELAY_DURATION_SECONDS, duration))

    command = {
        "id": uuid.uuid4().hex,
        "power": power,
        "duration": duration,
        "created_at": time.time(),
    }
    hold = f"for {duration}s" if duration else "until the next command"
    return _queue_relay_command(command, f"Relay {'ON' if power else 'OFF'} {hold}")


@app.get("/relay/next")
def next_relay_command():
    if not has_token(EVENT_TOKEN):
        return jsonify(error="Unauthorized"), 401

    # Long-poll: wakes immediately when a command is queued, returns null after `wait` seconds.
    wait_seconds = min(max(request.args.get("wait", default=20, type=float), 0.0), 30.0)
    deadline = time.time() + wait_seconds

    with relay_condition:
        while True:
            now = time.time()
            # Drop stale commands so a delayed ESP32 never fires an old request.
            relay_commands[:] = [
                c for c in relay_commands if now - c["created_at"] <= RELAY_COMMAND_TTL_SECONDS
            ]
            if relay_commands:
                command = relay_commands.pop(0)
                break
            remaining = deadline - now
            if remaining <= 0:
                return jsonify(command=None), 200
            relay_condition.wait(timeout=remaining)

    logger.info("Relay command delivered to ESP32: id=%s", command["id"])
    return jsonify(command=command), 200


@sock.route("/ws")
def websocket(ws):
    client_lock = threading.Lock()
    with clients_lock:
        clients[ws] = client_lock

    try:
        with client_lock:
            ws.send(
                json.dumps(
                    {"type": "sync", **configuration_payload(), **activity_snapshot()}
                )
            )

        while True:
            raw = ws.receive()
            if raw is None:
                break
            try:
                data = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
            if data.get("type") == "clear_logs":
                clear_activity_logs()
    except Exception:
        pass
    finally:
        with clients_lock:
            clients.pop(ws, None)


@app.post("/upload")
def upload_video():
    if not has_token(UPLOAD_TOKEN):
        return jsonify(error="Unauthorized"), 401

    video = request.files.get("video")
    if video is None or not video.filename:
        return jsonify(error="Missing video file"), 400

    filename = secure_filename(video.filename)
    if not filename.lower().endswith(".mp4"):
        return jsonify(error="Only MP4 files are accepted"), 400

    video.save(UPLOAD_DIR / filename)
    file_size_mb = (UPLOAD_DIR / filename).stat().st_size / (1024 * 1024)
    logger.info(
        "Video uploaded: file=%s size=%.2f MB",
        filename,
        file_size_mb,
    )
    record_activity(f"[MEDIA_UPLOAD] Incident video successfully uploaded ({file_size_mb:.1f} MB): {filename}", "success")
    return jsonify(message="Upload successful", filename=filename), 201


@app.get("/videos")
def list_videos():
    if not has_view_token():
        return jsonify(error="Unauthorized"), 401

    videos = sorted(
        (path for path in UPLOAD_DIR.glob("*.mp4") if path.is_file()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    links = "".join(
        f"<li>"
        f'<a href="/videos/{path.name}?token={VIEW_TOKEN}">'
        f'{datetime.fromtimestamp(path.stat().st_mtime, tz=ZoneInfo("UTC")).astimezone(MANILA_TIMEZONE).strftime("%m/%d/%Y - %I:%M:%S %p")}'
        f"</a> ({path.stat().st_size / (1024 * 1024):.1f} MB)"
        f'{" [LATEST]" if index == 0 else ""}</li>'
        for index, path in enumerate(videos)
    )

    return f"""
    <!doctype html>
    <html>
    <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Uploaded Videos</title>
    </head>
    <body>
        <h1>Uploaded Videos</h1>
        <ul>{links or '<li>No videos uploaded yet.</li>'}</ul>
    </body>
    </html>
    """


@app.get("/videos/<path:filename>")
def watch_video(filename):
    if not has_view_token():
        return jsonify(error="Unauthorized"), 401

    safe_filename = Path(filename)
    if safe_filename.name != filename or not safe_filename.name.lower().endswith(
        ".mp4"
    ):
        abort(404)

    video_path = UPLOAD_DIR / safe_filename.name
    if not video_path.is_file():
        abort(404)

    return send_from_directory(UPLOAD_DIR, safe_filename.name, mimetype="video/mp4")


@app.errorhandler(413)
def file_too_large(error):
    return jsonify(error="Video file is too large"), 413


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)