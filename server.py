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
from flask import Flask, Response, abort, jsonify, render_template_string, request, send_from_directory
from werkzeug.utils import secure_filename


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("alerto.server")

app = Flask(__name__)


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
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://178.128.82.49:5000")
MANILA_TIMEZONE = ZoneInfo("Asia/Manila")
CONFIG_PATH = Path(__file__).resolve().with_name("config.json")
pending_events = []
events_lock = threading.Lock()


def load_saved_configuration():
    if not CONFIG_PATH.is_file():
        return
    try:
        settings = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        logger.warning("Could not load config.json: %s", error)
        return

    global VIDEO_DURATION_SECONDS, TARGET_MOBILE
    try:
        VIDEO_DURATION_SECONDS = max(1, min(300, int(settings.get("VIDEO_DURATION_SECONDS", VIDEO_DURATION_SECONDS))))
    except (TypeError, ValueError):
        logger.warning("Invalid VIDEO_DURATION_SECONDS in config.json; using %s", VIDEO_DURATION_SECONDS)
    saved_recipients = recipient_list(settings.get("TARGET_MOBILE", TARGET_MOBILE))
    if saved_recipients:
        TARGET_MOBILE = ",".join(saved_recipients)
    logger.info("Loaded config.json: duration=%ss recipients=%s", VIDEO_DURATION_SECONDS, recipient_list(TARGET_MOBILE))


UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_SIZE


def has_token(expected_token):
    return bool(expected_token) and request.headers.get("Authorization") == f"Bearer {expected_token}"


def has_view_token():
    return bool(VIEW_TOKEN) and (
        has_token(VIEW_TOKEN) or request.args.get("token") == VIEW_TOKEN
    )


def recipient_list(value):
    values = str(value).replace(";", ",").replace("\n", ",").split(",")
    return [normalize_recipient(item) for item in values if normalize_recipient(item)]


def normalize_recipient(value):
    digits = "".join(character for character in str(value).strip() if character.isdigit())
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
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; text-align: center; margin: 0; min-height: 100dvh; background: #e2e8f0; }
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
        .configuration input { width: 100%; margin-bottom: 12px; padding: 9px 10px; border: 1px solid #cbd5e1; color: #1e293b; font: inherit; }
        .recipient-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; margin-bottom: 12px; } .recipient-grid input { margin-bottom: 0; min-width: 0; }
        .config-save { height: auto; min-height: 0; width: auto; padding: 9px 12px; background: #0f766e; font-size: 12px; }
        .location { width: 100%; max-width: 34rem; margin: 18px auto 0; color: #64748b; font-size: clamp(11px, 3.2vw, 14px); line-height: 1.4; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; text-align: center; }
        @media (min-width: 700px) { .container { padding: 48px; } .button-stack { gap: 18px; } button { height: 220px; } }
    </style>
</head>
<body><div class="container">
    <h2>{{ product_name }}</h2><p class="subtitle">Emergency Reporting System</p>
    <div class="button-stack"><button class="btn-hazard" onclick="triggerAlert(1)">Hazard</button><button class="btn-security" onclick="triggerAlert(2)">Security</button><button class="btn-medical" onclick="triggerAlert(3)">Medical Concern</button></div>
    <section class="activity-log"><div class="log-header"><span>Activity log</span><div class="log-actions"><button class="log-action" id="log-state" type="button" disabled>Ready</button><button class="log-action" type="button" onclick="clearLog()">Clear log</button><button class="log-action" type="button" onclick="toggleConfiguration()">Configuration</button></div></div><div id="status"><div class="log-empty">No alerts recorded in this session.</div></div></section>
    <form class="configuration" id="configuration" onsubmit="saveConfiguration(event)"><label for="duration">Video duration (seconds)</label><input id="duration" type="number" min="1" max="300" value="{{ duration }}" required><label>SMS recipients (up to 10 numbers)</label><div class="recipient-grid">{% for recipient in recipient_values %}<input class="recipient-slot" type="tel" inputmode="tel" autocomplete="tel" maxlength="11" value="{{ recipient }}" placeholder="09XXXXXXXXX">{% endfor %}</div><button class="config-save" type="submit">Save configuration</button></form>
    <div class="location">STEM Department Building &bull; STEM 12 Newton Room</div>
</div>
<script>
if ('serviceWorker' in navigator) { window.addEventListener('load', () => navigator.serviceWorker.register('/sw.js')); }
function formatRecipient(input) { let digits = input.value.replace(/\D/g, ""); if (digits.startsWith("63")) digits = "0" + digits.slice(2); if (digits.startsWith("9")) digits = "0" + digits; input.value = digits.slice(0, 11); }
document.querySelectorAll(".recipient-slot").forEach(input => { input.addEventListener("input", () => formatRecipient(input)); input.addEventListener("blur", () => formatRecipient(input)); });
const alertConfiguration = { duration: Number(document.getElementById("duration").value), recipient: Array.from(document.querySelectorAll(".recipient-slot")).map(input => input.value).filter(Boolean).join(",") };
function toggleConfiguration() { document.getElementById("configuration").classList.toggle("open"); }
function saveConfiguration(event) { event.preventDefault(); const duration = Number(document.getElementById("duration").value); const recipient = Array.from(document.querySelectorAll(".recipient-slot")).map(input => input.value.trim()).filter(Boolean).join(","); fetch("/configuration", { method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({duration: duration, recipient: recipient}) }).then(response => { if (!response.ok) throw Error("Server returned HTTP " + response.status); return response.json(); }).then(() => { alertConfiguration.duration = duration; alertConfiguration.recipient = recipient; addLog("Configuration saved.", "success"); }).catch(error => addLog("Configuration was not saved: " + error.message, "error")); }
function clearLog() { document.getElementById("status").innerHTML = '<div class="log-empty">No alerts recorded in this session.</div>'; document.getElementById("log-state").innerText = "Ready"; }
function addLog(message, level) { const status = document.getElementById("status"); const empty = status.querySelector(".log-empty"); if (empty) empty.remove(); const entry = document.createElement("div"); entry.className = "log-entry " + level; entry.innerHTML = '<span class="log-time">' + new Date().toLocaleTimeString([], {hour: "2-digit", minute: "2-digit"}) + '</span><span class="log-message">' + message + '</span>'; status.prepend(entry); }
function triggerAlert(buttonId) { const category = {1: "Hazard", 2: "Security", 3: "Medical concern"}[buttonId]; document.getElementById("log-state").innerText = "Working"; addLog(category + " alert queued; phone worker will record the video.", "pending"); fetch('/trigger-alert?button=' + buttonId + '&duration=' + alertConfiguration.duration).then(response => { if (!response.ok) throw Error("Server returned HTTP " + response.status); return response.text(); }).then(() => { document.getElementById("log-state").innerText = "Ready"; addLog(category + " alert accepted.", "success"); }).catch(error => { document.getElementById("log-state").innerText = "Attention"; addLog(category + " alert failed: " + error.message, "error"); }); }
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
        recipient_values=([display_recipient(recipient) for recipient in recipients] + [""] * 10)[:10],
    )


@app.get("/manifest.json")
def manifest():
    return jsonify({
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
    })


@app.get("/icon-<int:size>.png")
def icon(size):
    if size not in (192, 512):
        return "Not found", 404

    def chunk(tag, data):
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)

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
    self.addEventListener('fetch', event => event.respondWith(fetch(event.request)));
    """
    return Response(sw_code, mimetype="application/javascript")


@app.post("/events")
def create_event():
    if not has_token(EVENT_TOKEN):
        return jsonify(error="Unauthorized"), 401

    data = request.get_json(silent=True) or {}
    button_id = str(data.get("button", "")).strip()
    if button_id not in {"1", "2", "3"}:
        return jsonify(error="button must be 1, 2, or 3"), 400

    duration = max(1, min(300, int(data.get("duration", VIDEO_DURATION_SECONDS))))
    with events_lock:
        pending_events.append({"button": button_id, "duration": duration})
        queue_size = len(pending_events)
    logger.info("ESP32 event queued: button=%s duration=%ss queue_size=%s", button_id, duration, queue_size)
    return jsonify(message="Event queued"), 202


@app.post("/configuration")
def save_configuration():
    global VIDEO_DURATION_SECONDS, TARGET_MOBILE
    settings = request.get_json(silent=True) or {}
    try:
        duration = max(1, min(300, int(settings.get("duration", VIDEO_DURATION_SECONDS))))
    except (TypeError, ValueError):
        return jsonify(error="Video duration must be between 1 and 300 seconds"), 400

    recipients = recipient_list(settings.get("recipient", TARGET_MOBILE))
    if not recipients:
        return jsonify(error="At least one SMS recipient is required"), 400
    VIDEO_DURATION_SECONDS = duration
    TARGET_MOBILE = ",".join(recipients)
    CONFIG_PATH.write_text(
        json.dumps({"VIDEO_DURATION_SECONDS": duration, "TARGET_MOBILE": TARGET_MOBILE}, indent=2),
        encoding="utf-8",
    )
    logger.info("Configuration saved: duration=%ss recipients=%s", duration, recipients)
    return jsonify(message="Configuration saved"), 200


@app.get("/trigger-alert")
def trigger_alert():
    button_id = request.args.get("button", "")
    if button_id not in {"1", "2", "3"}:
        return "Invalid alert button", 400

    duration = max(1, min(300, int(request.args.get("duration", VIDEO_DURATION_SECONDS))))
    with events_lock:
        pending_events.append({"button": button_id, "duration": duration})
        queue_size = len(pending_events)
    recipients = recipient_list(TARGET_MOBILE)
    logger.info("Website alert queued: button=%s duration=%ss queue_size=%s recipients=%s", button_id, duration, queue_size, recipients)

    if SEND_SMS and PHILSMS_URL and PHILSMS_TOKEN:
        category = {"1": "Hazard", "2": "Security", "3": "Medical Concern"}[button_id]
        message = (
            f"{PRODUCT_NAME} EMERGENCY ALERT\n\n"
            f"Category: {category}\n"
            "STEM 12 Newton Room\n"
            f"{datetime.now(MANILA_TIMEZONE).strftime('%B %d, %Y — %I:%M %p')}\n\n"
            "Please proceed to the indicated location immediately and assess the situation. "
            "Visual incident documentation will be transmitted for review.\n\n"
            f"Video: {PUBLIC_BASE_URL.rstrip('/')}/videos?token={VIEW_TOKEN}\n\n"
        )
        headers = {"Authorization": f"Bearer {PHILSMS_TOKEN}", "Content-Type": "application/json"}
        for recipient in recipients:
            try:
                requests.post(PHILSMS_URL, json={"recipient": recipient, "sender_id": SENDER_ID, "type": "plain", "message": message}, headers=headers, timeout=20).raise_for_status()
                logger.info("SMS sent: recipient=%s button=%s", recipient, button_id)
            except requests.RequestException as error:
                logger.error("SMS failed: recipient=%s error=%s", recipient, error)
    elif not SEND_SMS:
        logger.info("SMS disabled: recipients=%s", recipients)
    else:
        logger.warning("SMS not sent: PHILSMS_URL or PHILSMS_TOKEN is missing")

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
    logger.info("Event delivered to phone worker: event=%s queue_size=%s", event, queue_size)
    return jsonify(event=event), 200


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
    logger.info("Video uploaded: file=%s size=%d bytes", filename, (UPLOAD_DIR / filename).stat().st_size)
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
        f'<li>'
        f'<a href="/videos/{path.name}?token={VIEW_TOKEN}">'
        f'{datetime.fromtimestamp(path.stat().st_mtime, tz=ZoneInfo("UTC")).astimezone(MANILA_TIMEZONE).strftime("%m/%d/%Y - %I:%M:%S %p")}'
        f'</a> ({path.stat().st_size / (1024 * 1024):.1f} MB)'
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
    if safe_filename.name != filename or not safe_filename.name.lower().endswith(".mp4"):
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
