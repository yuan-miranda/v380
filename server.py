import os
import json
import logging
import struct
import threading
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
events_lock = threading.Lock()
clients = set()
clients_lock = threading.Lock()
send_lock = threading.Lock()
activity_logs = []
activity_lock = threading.Lock()
log_state = "Ready"
log_seq = 0
MAX_LOG_ENTRIES = 80
ALERT_CATEGORIES = {"1": "Hazard", "2": "Security", "3": "Medical Concern"}

# --- Pastebin (separate feature, unrelated to ALERTO) ---
PASTE_PATH = Path(__file__).resolve().with_name("pastebin.txt")
PASTE_IMAGES_DIR = Path(__file__).resolve().with_name("pastebin_images")
PASTE_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
ALLOWED_IMAGE_TYPES = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/bmp": ".bmp",
    "image/x-ms-bmp": ".bmp",
    "image/avif": ".avif",
    "image/svg+xml": ".svg",
    "image/heic": ".heic",
    "image/heif": ".heif",
}
ALLOWED_IMAGE_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".avif", ".svg", ".heic", ".heif",
}
paste_text = ""
paste_lock = threading.Lock()
paste_clients = set()
paste_clients_lock = threading.Lock()


def load_saved_paste():
    global paste_text
    if PASTE_PATH.is_file():
        try:
            paste_text = PASTE_PATH.read_text(encoding="utf-8")
        except OSError as error:
            logger.warning("Could not load pastebin.txt: %s", error)


def broadcast_paste(payload, exclude=None):
    data = json.dumps(payload)
    with paste_clients_lock:
        sockets = list(paste_clients)
    for websocket in sockets:
        if websocket is exclude:
            continue
        try:
            with send_lock:
                websocket.send(data)
        except Exception:
            with paste_clients_lock:
                paste_clients.discard(websocket)


class TemplateValues(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def render_sms_message(category, video_filename=""):
    if not video_filename:
        now_str = datetime.now(MANILA_TIMEZONE).strftime("%Y-%m-%d_%H-%M-%S")
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


def send_sms_notification(button_id, video_filename=""):
    category = ALERT_CATEGORIES.get(button_id, "Hazard")
    recipients = recipient_list(TARGET_MOBILE)
    if SEND_SMS and PHILSMS_URL and PHILSMS_TOKEN:
        try:
            message = render_sms_message(category, video_filename)
        except ValueError:
            now_str = datetime.now(MANILA_TIMEZONE).strftime("%Y-%m-%d_%H-%M-%S")
            fallback_video_url = f"{PUBLIC_BASE_URL.rstrip('/')}/videos/evidence_btn{button_id}_{now_str}.mp4?token={VIEW_TOKEN}"
            message = DEFAULT_SMS_TEMPLATE.format_map(
                TemplateValues(
                    product_name=PRODUCT_NAME,
                    category=category,
                    timestamp=datetime.now(MANILA_TIMEZONE).strftime("%B %d, %Y — %I:%M %p"),
                    video_url=fallback_video_url
                )
            )
        headers = {"Authorization": f"Bearer {PHILSMS_TOKEN}", "Content-Type": "application/json"}
        for recipient in recipients:
            try:
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
            except requests.RequestException as error:
                logger.error("SMS failed: recipient=%s error=%s", recipient, error)
    elif not SEND_SMS:
        logger.info("SMS disabled: recipients=%s", recipients)
    else:
        logger.warning("SMS not sent: PHILSMS_URL or PHILSMS_TOKEN is missing")


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
    return {
        "duration": VIDEO_DURATION_SECONDS,
        "cctv_ip": CCTV_IP,
        "recipients": (
            [display_recipient(recipient) for recipient in recipients] + [""] * 10
        )[:10],
        "message": SMS_TEMPLATE,
    }


def broadcast(payload):
    data = json.dumps(payload)
    with clients_lock:
        sockets = list(clients)
    for websocket in sockets:
        try:
            with send_lock:
                websocket.send(data)
        except Exception:
            with clients_lock:
                clients.discard(websocket)


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
load_saved_paste()


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
        button:active { transform: scale(0.98); opacity: 0.9; }
        .btn-hazard { background: #d97706; } .btn-security { background: #b91c1c; } .btn-medical { background: #047857; }
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
        <div class="button-stack"><button class="btn-hazard" onclick="triggerAlert(1)">Hazard</button><button class="btn-security" onclick="triggerAlert(2)">Security</button><button class="btn-medical" onclick="triggerAlert(3)">Medical Concern</button></div>
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

if (sessionStorage.getItem("alerto_unlocked") !== "true") {
    let password = prompt("Enter security password:");
    if (password === "alerto") {
        sessionStorage.setItem("alerto_unlocked", "true");
    } else {
        alert("Incorrect password.");
        location.reload();
    }
}

function formatRecipient(input) { let digits = input.value.replace(/\D/g, ""); if (digits.startsWith("63")) digits = "0" + digits.slice(2); if (digits.startsWith("9")) digits = "0" + digits; input.value = digits.slice(0, 11); }
document.querySelectorAll(".recipient-slot").forEach(input => { input.addEventListener("input", () => formatRecipient(input)); input.addEventListener("blur", () => formatRecipient(input)); });
const alertConfiguration = { duration: Number(document.getElementById("duration").value), recipient: Array.from(document.querySelectorAll(".recipient-slot")).map(input => input.value).filter(Boolean).join(",") };
const defaultSmsTemplate = {{ default_sms_template | tojson }};
const seenLogIds = new Set();
let syncSocket = null;
let socketConnected = false;
function escapeHtml(value) { return String(value).replace(/[&<>"']/g, character => ({"&":"&amp;","<":"&lt;",">":"&gt;",[String.fromCharCode(34)]:"&quot;","'":"&#39;"}[character])); }
function setLogState(state) { document.getElementById("log-state").innerText = state || "Ready"; }
function logEntryHtml(entry) { return '<div class="log-entry ' + escapeHtml(entry.level || "") + '"><span class="log-time">' + escapeHtml(entry.time || "") + '</span><span class="log-message">' + escapeHtml(entry.message || "") + '</span></div>'; }
function renderLogs(logs) { seenLogIds.clear(); const status = document.getElementById("status"); if (!logs || !logs.length) { status.innerHTML = '<div class="log-empty">No alerts recorded in this session.</div>'; return; } logs.forEach(entry => { if (entry.id) seenLogIds.add(entry.id); }); status.innerHTML = logs.map(logEntryHtml).join(""); }
function addLogEntry(entry, state) { if (!entry) return; if (entry.id) { if (seenLogIds.has(entry.id)) { if (state) setLogState(state); return; } seenLogIds.add(entry.id); } const status = document.getElementById("status"); const empty = status.querySelector(".log-empty"); if (empty) empty.remove(); status.insertAdjacentHTML("afterbegin", logEntryHtml(entry)); if (state) setLogState(state); }
function addLog(message, level) { addLogEntry({ time: new Date().toLocaleTimeString([], {hour: "2-digit", minute: "2-digit", second: "2-digit"}), message: message, level: level }); }
function applyConfiguration(data) { if (!data) return; if (data.cctv_ip != null) document.getElementById("cctv_ip").value = data.cctv_ip; if (data.duration != null) { document.getElementById("duration").value = data.duration; alertConfiguration.duration = Number(data.duration); } if (Array.isArray(data.recipients)) { document.querySelectorAll(".recipient-slot").forEach((input, index) => { input.value = data.recipients[index] || ""; }); alertConfiguration.recipient = data.recipients.filter(Boolean).join(","); } if (data.message != null) document.getElementById("message").value = data.message; }
window.toggleConfiguration = function() { document.getElementById("configuration").classList.toggle("open"); };
function resetSmsTemplate() { document.getElementById("message").value = defaultSmsTemplate; }
function saveConfiguration(event) { event.preventDefault(); const cctv_ip = document.getElementById("cctv_ip").value.trim(); const duration = Number(document.getElementById("duration").value); const recipient = Array.from(document.querySelectorAll(".recipient-slot")).map(input => input.value.trim()).filter(Boolean).join(","); const message = document.getElementById("message").value; fetch("/configuration", { method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({cctv_ip: cctv_ip, duration: duration, recipient: recipient, message: message}) }).then(response => response.json().then(data => { if (!response.ok) throw Error(data.error || ("Server returned HTTP " + response.status)); return data; })).then(() => { if (!socketConnected) addLog("[CONFIG] System settings updated successfully.", "success"); }).catch(error => addLog("[CONFIG_ERROR] Failed to save configuration: " + error.message, "error")); }
function clearLog() { renderLogs([]); setLogState("Ready"); if (syncSocket && syncSocket.readyState === WebSocket.OPEN) syncSocket.send(JSON.stringify({type: "clear_logs"})); }
function triggerAlert(buttonId) { const category = {1: "Hazard", 2: "Security", 3: "Medical concern"}[buttonId]; setLogState("Working"); if (!socketConnected) addLog("[ALERT_QUEUED] " + category + " emergency triggered via web UI. Awaiting worker capture...", "pending"); fetch('/trigger-alert?button=' + buttonId + '&duration=' + alertConfiguration.duration).then(response => { if (!response.ok) throw Error("Server returned HTTP " + response.status); return response.text(); }).then(() => { if (!socketConnected) { setLogState("Ready"); addLog("[ALERT_DISPATCHED] " + category + " alert successfully registered and processed.", "success"); } }).catch(error => { setLogState("Attention"); addLog("[ALERT_ERROR] " + category + " alert dispatch failed: " + error.message, "error"); }); }
function connectSync() { const socket = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws"); syncSocket = socket; socket.onopen = () => { socketConnected = true; }; socket.onmessage = event => { const data = JSON.parse(event.data); if (data.type === "sync") { applyConfiguration(data); renderLogs(data.logs || []); setLogState(data.state || "Ready"); } else if (data.type === "configuration") applyConfiguration(data); else if (data.type === "log") addLogEntry(data.entry, data.state); else if (data.type === "logs_cleared") { renderLogs([]); setLogState(data.state || "Ready"); } }; socket.onclose = () => { socketConnected = false; setTimeout(connectSync, 2000); }; socket.onerror = () => socket.close(); }
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

    env_path = Path(__file__).resolve().with_name(".env")
    if env_path.is_file():
        return Response(env_path.read_text(encoding="utf-8"), mimetype="text/plain")

    fallback_env = f"""CCTV_IP={CCTV_IP}
VIDEO_DURATION_SECONDS={VIDEO_DURATION_SECONDS}
VPS_ENDPOINT={PUBLIC_BASE_URL.rstrip('/')}/upload
VPS_TOKEN={UPLOAD_TOKEN}
VPS_EVENT_ENDPOINT={PUBLIC_BASE_URL.rstrip('/')}/events/next
VPS_EVENT_TOKEN={EVENT_TOKEN}
VPS_CONFIG_ENDPOINT={PUBLIC_BASE_URL.rstrip('/')}/worker-config
VPS_ENV_ENDPOINT={PUBLIC_BASE_URL.rstrip('/')}/worker-env
RTSP_USER=admin
RTSP_PASS=password
POLL_INTERVAL_SECONDS=2
"""
    return Response(fallback_env, mimetype="text/plain")


@app.post("/events")
def create_event():
    if not has_token(EVENT_TOKEN):
        return jsonify(error="Unauthorized"), 401

    data = request.get_json(silent=True) or {}
    button_id = str(data.get("button", "")).strip()
    if button_id not in {"1", "2", "3"}:
        return jsonify(error="button must be 1, 2, or 3"), 400

    duration = max(1, min(300, int(data.get("duration", VIDEO_DURATION_SECONDS))))
    
    # Pre-calculate filename based on server click time
    now_str = datetime.now(MANILA_TIMEZONE).strftime("%Y-%m-%d_%H-%M-%S")
    video_filename = f"evidence_btn{button_id}_{now_str}.mp4"

    with events_lock:
        pending_events.append({"button": button_id, "duration": duration, "filename": video_filename})
        queue_size = len(pending_events)
        
    logger.info(
        "ESP32 event queued: button=%s duration=%ss filename=%s queue_size=%s",
        button_id,
        duration,
        video_filename,
        queue_size,
    )
    category_name = ALERT_CATEGORIES[button_id]
    record_activity(
        f"[HARDWARE_SIGNAL] Received {category_name} event from ESP32 device node. Added to recording pipeline queue.", "pending"
    )
    
    send_sms_notification(button_id, video_filename)
    
    record_activity(
        f"[ALERT_DISPATCHED] {category_name} hardware event processed. SMS notifications broadcasted successfully.", "success"
    )
    return jsonify(message="Event queued and SMS dispatched"), 202


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


@app.get("/trigger-alert")
def trigger_alert():
    button_id = request.args.get("button", "")
    if button_id not in {"1", "2", "3"}:
        return "Invalid alert button", 400

    duration = max(
        1, min(300, int(request.args.get("duration", VIDEO_DURATION_SECONDS)))
    )
    
    # Pre-calculate filename based on web console click time
    now_str = datetime.now(MANILA_TIMEZONE).strftime("%Y-%m-%d_%H-%M-%S")
    video_filename = f"evidence_btn{button_id}_{now_str}.mp4"

    with events_lock:
        pending_events.append({"button": button_id, "duration": duration, "filename": video_filename})
        queue_size = len(pending_events)
        
    recipients = recipient_list(TARGET_MOBILE)
    category = ALERT_CATEGORIES[button_id]
    logger.info(
        "Website alert queued: button=%s duration=%ss filename=%s queue_size=%s recipients=%s",
        button_id,
        duration,
        video_filename,
        queue_size,
        recipients,
    )
    record_activity(
        f"[WEB_TRIGGER] {category} emergency requested via web console. Target CCTV recording worker activated.",
        "pending",
        "Working",
    )

    send_sms_notification(button_id, video_filename)

    record_activity(f"[ALERT_SUCCESS] {category} alert sequence accepted. Emergency protocols initiated.", "success", "Ready")
    return "Alert queued; the phone worker will record and upload the video.", 202


@app.get("/events/next")
def next_event():
    if not has_token(EVENT_TOKEN):
        return jsonify(error="Unauthorized"), 401

    with events_lock:
        if not pending_events:
            return jsonify(event=None), 200
        event = pending_events.pop(0)
        queue_size = len(pending_events)
    logger.info(
        "Event delivered to phone worker: event=%s queue_size=%s", event, queue_size
    )
    return jsonify(event=event), 200


@sock.route("/ws")
def websocket(ws):
    with clients_lock:
        clients.add(ws)
    try:
        with send_lock:
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
            clients.discard(ws)


PASTEBIN_PAGE = r"""
<!doctype html>
<html>
<head>
    <title>ELMS</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { box-sizing: border-box; }
        html, body { margin: 0; padding: 0; height: 100%; background: #fff; }
        #paste { position: fixed; inset: 0; width: 100%; height: 100%; overflow-y: auto; border: none; outline: none; background: #fff; color: #000; font: 14px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; padding: 10px; white-space: pre-wrap; word-wrap: break-word; }
        #paste:empty::before { content: attr(data-placeholder); color: #999; pointer-events: none; }
        #paste img { max-width: 100%; height: auto; display: block; margin: 6px 0; border: 1px solid #000; }
        #paste img.img-selected { outline: 2px solid #0078d4; outline-offset: 1px; }
        .resize-handle { position: fixed; width: 12px; height: 12px; background: #0078d4; border: 1px solid #fff; box-sizing: border-box; cursor: se-resize; z-index: 2; touch-action: none; }
        .bar { position: fixed; right: 3px; bottom: 3px; display: flex; gap: 6px; z-index: 1; }
        button { padding: 2px 4px; border: 1px solid #000; background: #fff; color: #000; font: inherit; font-size: 12px; cursor: pointer; }
        #status { position: fixed; left: 3px; bottom: 3px; font: 12px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; color: #666; z-index: 1; }
        #paste.drag-over { background: #f0f0f0; }
    </style>
</head>
<body>
    <div id="paste" contenteditable="true" spellcheck="false" data-placeholder="Paste or type here... (images can be pasted or dropped too)"></div>
    <div id="status"></div>
    <div class="bar">
        <button onclick="copyText()">Copy</button>
    </div>
<script>
const area = document.getElementById("paste");
const status = document.getElementById("status");
let socket = null;
let saveTimer = null;
let savedFadeTimer = null;
let reconnectTimer = null;
let pageCaching = false;

function setStatus(text) {
    clearTimeout(savedFadeTimer);
    status.textContent = text;
}

// --- HTML sanitization -----------------------------------------------
// The editor is contenteditable, and its markup gets synced verbatim to
// every connected client and written straight into innerHTML there. To
// keep that from becoming an XSS vector (e.g. someone pasting rich HTML
// from another site, or a rogue client talking to the websocket directly),
// every piece of HTML is sanitized down to an allowlist before it is
// rendered anywhere: plain text, <br>, <div> (Chrome wraps lines in these),
// and <img> whose src points at our own uploaded-image endpoint. Anything
// else is unwrapped to its plain text content.
function sanitizeHtml(html) {
    const template = document.createElement("template");
    template.innerHTML = html;
    sanitizeChildren(template.content);
    return template.innerHTML;
}

function sanitizeChildren(node) {
    for (const child of Array.from(node.childNodes)) {
        if (child.nodeType === Node.TEXT_NODE) continue;
        if (child.nodeType !== Node.ELEMENT_NODE) { child.remove(); continue; }

        const tag = child.tagName.toLowerCase();
        if (tag === "img") {
            const src = child.getAttribute("src") || "";
            if (!/^\/pastebin-image\//.test(src)) { child.remove(); continue; }
            // Preserve a user-set width (from the resize handle) but only
            // ever as a bare "width:<number>px;" value, never raw style text,
            // so this can't be used to smuggle arbitrary CSS.
            const style = child.getAttribute("style") || "";
            const widthMatch = /^\s*width:\s*(\d+(?:\.\d+)?)px;?\s*$/i.exec(style);
            for (const attr of Array.from(child.attributes)) {
                if (attr.name !== "src" && attr.name !== "alt") child.removeAttribute(attr.name);
            }
            if (widthMatch) {
                const width = Math.max(20, Math.min(4000, parseFloat(widthMatch[1])));
                child.setAttribute("style", `width:${width}px;`);
            }
            continue;
        }
        if (tag === "br" || tag === "div") {
            for (const attr of Array.from(child.attributes)) child.removeAttribute(attr.name);
            sanitizeChildren(child);
            continue;
        }
        // Unknown/disallowed element: keep its text, drop the tag itself.
        const text = document.createTextNode(child.textContent);
        child.replaceWith(text);
    }
}

function connect() {
    setStatus("Connecting...");
    socket = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/pastebin-ws");
    socket.onopen = () => setStatus("");
    socket.onmessage = event => {
        const data = JSON.parse(event.data);
        if (data.type === "update" && document.activeElement !== area) {
            deselectImage();
            area.innerHTML = sanitizeHtml(data.text || "");
        }
        if (data.type === "saved") {
            setStatus("Saved");
            savedFadeTimer = setTimeout(() => { status.textContent = ""; }, 1200);
        }
    };
    socket.onclose = () => {
        // If the page is being frozen into the back/forward cache, don't
        // bother reconnecting - pageshow will reconnect if/when it's restored.
        if (pageCaching) return;
        setStatus("Offline, reconnecting...");
        clearTimeout(reconnectTimer);
        reconnectTimer = setTimeout(connect, 2000);
    };
    socket.onerror = () => socket.close();
}
connect();

window.addEventListener("pagehide", event => {
    if (!event.persisted) return;
    // Chrome closes any open sockets when caching the page for instant
    // back/forward navigation; close it ourselves first to avoid the
    // "entered Back-Forward Cache" console error, and skip auto-reconnect.
    pageCaching = true;
    clearTimeout(reconnectTimer);
    if (socket) socket.close();
});

window.addEventListener("pageshow", event => {
    if (!event.persisted) return;
    // Page was restored from bfcache; the old socket is dead, reconnect.
    pageCaching = false;
    connect();
});

area.addEventListener("input", () => {
    setStatus("Syncing...");
    clearTimeout(saveTimer);
    saveTimer = setTimeout(sendHtml, 300);
});

function sendHtml() {
    if (socket && socket.readyState === WebSocket.OPEN) {
        setStatus("Saving...");
        socket.send(JSON.stringify({ type: "update", text: sanitizeHtml(area.innerHTML) }));
    }
}

function copyText() {
    navigator.clipboard.writeText(area.textContent);
}

// --- Cursor-position insertion -----------------------------------------
function getEditableRange() {
    const selection = window.getSelection();
    if (selection && selection.rangeCount > 0) {
        const range = selection.getRangeAt(0);
        if (area.contains(range.commonAncestorContainer)) return range;
    }
    // No caret in the editor (e.g. a drop without a prior click): fall
    // back to the end of the content.
    const range = document.createRange();
    range.selectNodeContents(area);
    range.collapse(false);
    return range;
}

function insertNodeAtRange(node, range) {
    range.deleteContents();
    range.insertNode(node);
    range.setStartAfter(node);
    range.collapse(true);
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
}

function insertTextAtRange(text, range) {
    insertNodeAtRange(document.createTextNode(text), range);
}

function insertImageAtRange(url, range) {
    const img = document.createElement("img");
    img.src = url;
    img.alt = "pasted image";
    insertNodeAtRange(img, range);
    area.dispatchEvent(new Event("input"));
}

// --- Image resizing -----------------------------------------------------
// Click an image to select it and drag the handle at its corner to resize.
// The handle itself lives outside #paste (so it never gets synced) and is
// repositioned on scroll/resize while an image stays selected.
let selectedImg = null;
let resizeHandle = null;
let resizeState = null;

function positionHandle() {
    if (!selectedImg || !resizeHandle) return;
    const rect = selectedImg.getBoundingClientRect();
    resizeHandle.style.left = `${rect.right - 6}px`;
    resizeHandle.style.top = `${rect.bottom - 6}px`;
}

function selectImage(img) {
    if (selectedImg === img) return;
    deselectImage();
    selectedImg = img;
    selectedImg.classList.add("img-selected");
    resizeHandle = document.createElement("div");
    resizeHandle.className = "resize-handle";
    document.body.appendChild(resizeHandle);
    positionHandle();
    resizeHandle.addEventListener("pointerdown", startResize);
}

function deselectImage() {
    if (selectedImg) selectedImg.classList.remove("img-selected");
    if (resizeHandle) resizeHandle.remove();
    selectedImg = null;
    resizeHandle = null;
}

function startResize(event) {
    if (!selectedImg) return;
    event.preventDefault();
    event.stopPropagation();
    resizeState = {
        startX: event.clientX,
        startWidth: selectedImg.getBoundingClientRect().width,
    };
    resizeHandle.setPointerCapture(event.pointerId);
    resizeHandle.addEventListener("pointermove", onResizeMove);
    resizeHandle.addEventListener("pointerup", endResize);
}

function onResizeMove(event) {
    if (!resizeState || !selectedImg) return;
    const width = Math.max(20, resizeState.startWidth + (event.clientX - resizeState.startX));
    selectedImg.style.width = `${Math.round(width)}px`;
    positionHandle();
}

function endResize(event) {
    resizeHandle.releasePointerCapture(event.pointerId);
    resizeHandle.removeEventListener("pointermove", onResizeMove);
    resizeHandle.removeEventListener("pointerup", endResize);
    resizeState = null;
    area.dispatchEvent(new Event("input"));
}

area.addEventListener("click", event => {
    if (event.target.tagName === "IMG" && area.contains(event.target)) {
        selectImage(event.target);
    } else {
        deselectImage();
    }
});

document.addEventListener("click", event => {
    if (event.target === resizeHandle) return;
    if (!area.contains(event.target)) deselectImage();
});

area.addEventListener("scroll", positionHandle);
window.addEventListener("resize", positionHandle);
window.addEventListener("scroll", positionHandle, true);

const IMAGE_EXTENSION_RE = /\.(png|jpe?g|gif|webp|bmp|avif|svg|heic|heif)$/i;

function isImageFile(file) {
    if (!file) return false;
    if (file.type && file.type.startsWith("image/")) return true;
    // File managers often hand over files with an empty or generic
    // (e.g. application/octet-stream) type, so fall back to the extension.
    return IMAGE_EXTENSION_RE.test(file.name || "");
}

async function uploadImage(file, range) {
    setStatus("Uploading image...");
    const formData = new FormData();
    formData.append("image", file, file.name || "pasted-image.png");
    try {
        const response = await fetch("/pastebin-image", { method: "POST", body: formData });
        if (!response.ok) throw new Error("Upload failed");
        const data = await response.json();
        insertImageAtRange(data.url, range);
        setStatus("");
    } catch (error) {
        setStatus("Image upload failed");
        savedFadeTimer = setTimeout(() => { status.textContent = ""; }, 2000);
    }
}

area.addEventListener("paste", event => {
    const items = event.clipboardData ? event.clipboardData.items : [];
    for (const item of items) {
        if (item.kind === "file") {
            const file = item.getAsFile();
            if (isImageFile(file)) {
                event.preventDefault();
                uploadImage(file, getEditableRange());
                return;
            }
        }
    }
    // Plain text paste: insert as literal text, not rich HTML from the
    // clipboard source, so the editor's content stays within our allowlist.
    const text = event.clipboardData ? event.clipboardData.getData("text/plain") : "";
    if (text) {
        event.preventDefault();
        insertTextAtRange(text, getEditableRange());
        area.dispatchEvent(new Event("input"));
    }
});

area.addEventListener("dragenter", event => {
    if (event.dataTransfer) event.dataTransfer.dropEffect = "copy";
    event.preventDefault();
    area.classList.add("drag-over");
});

area.addEventListener("dragover", event => {
    if (event.dataTransfer) event.dataTransfer.dropEffect = "copy";
    event.preventDefault();
    area.classList.add("drag-over");
});

area.addEventListener("dragleave", () => {
    area.classList.remove("drag-over");
});

function rangeFromPoint(x, y) {
    if (document.caretRangeFromPoint) {
        return document.caretRangeFromPoint(x, y);
    }
    if (document.caretPositionFromPoint) {
        const pos = document.caretPositionFromPoint(x, y);
        if (!pos) return null;
        const range = document.createRange();
        range.setStart(pos.offsetNode, pos.offset);
        range.collapse(true);
        return range;
    }
    return null;
}

area.addEventListener("drop", event => {
    event.preventDefault();
    area.classList.remove("drag-over");
    const files = event.dataTransfer ? Array.from(event.dataTransfer.files) : [];
    const images = files.filter(isImageFile);
    if (images.length) {
        const dropRange = rangeFromPoint(event.clientX, event.clientY) || getEditableRange();
        images.forEach(file => uploadImage(file, dropRange.cloneRange()));
    } else if (files.length) {
        setStatus("Dropped file isn't a recognized image");
        savedFadeTimer = setTimeout(() => { status.textContent = ""; }, 2000);
    }
});
</script>
</body>
</html>
"""


@app.get("/pastebin")
def pastebin_page():
    return render_template_string(PASTEBIN_PAGE, paste_text=paste_text)


@app.post("/pastebin-image")
def pastebin_image_upload():
    image = request.files.get("image")
    if image is None or not image.filename:
        return jsonify(error="Missing image file"), 400

    content_type = (image.mimetype or "").lower()
    extension = ALLOWED_IMAGE_TYPES.get(content_type)
    if not extension:
        guessed = Path(secure_filename(image.filename)).suffix.lower()
        if guessed in ALLOWED_IMAGE_EXTENSIONS:
            extension = ".jpg" if guessed == ".jpeg" else guessed
        else:
            return jsonify(error="Unsupported image type"), 400

    timestamp = datetime.now(MANILA_TIMEZONE).strftime("%Y%m%d_%H%M%S_%f")
    filename = f"paste_{timestamp}{extension}"
    image.save(PASTE_IMAGES_DIR / filename)
    logger.info("Pastebin image uploaded: file=%s", filename)
    return jsonify(url=f"/pastebin-image/{filename}"), 201


@app.get("/pastebin-image/<path:filename>")
def pastebin_image_view(filename):
    safe_filename = Path(filename)
    if safe_filename.name != filename:
        abort(404)

    image_path = PASTE_IMAGES_DIR / safe_filename.name
    if not image_path.is_file():
        abort(404)

    return send_from_directory(PASTE_IMAGES_DIR, safe_filename.name)


@sock.route("/pastebin-ws")
def pastebin_websocket(ws):
    global paste_text
    with paste_clients_lock:
        paste_clients.add(ws)
    try:
        with send_lock:
            ws.send(json.dumps({"type": "update", "text": paste_text}))
        while True:
            raw = ws.receive()
            if raw is None:
                break
            try:
                data = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
            if data.get("type") == "update":
                text = str(data.get("text", ""))
                with paste_lock:
                    paste_text = text
                    try:
                        PASTE_PATH.write_text(text, encoding="utf-8")
                    except OSError as error:
                        logger.warning("Could not save pastebin.txt: %s", error)
                broadcast_paste({"type": "update", "text": text}, exclude=ws)
                try:
                    with send_lock:
                        ws.send(json.dumps({"type": "saved"}))
                except Exception:
                    pass
    except Exception:
        pass
    finally:
        with paste_clients_lock:
            paste_clients.discard(ws)


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