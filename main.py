import cv2
import json
import shutil
import subprocess
import threading
import tempfile
import time
from flask import Flask, jsonify, request, render_template_string
from datetime import datetime
import os
import requests

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
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key.strip(), value)


load_env_file()

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")


def load_config_file():
    if not os.path.isfile(CONFIG_PATH):
        return {}
    try:
        with open(CONFIG_PATH, encoding="utf-8") as config_file:
            settings = json.load(config_file)
        return settings if isinstance(settings, dict) else {}
    except (OSError, json.JSONDecodeError) as error:
        print(f"Configuration file could not be loaded: {error}")
        return {}


FILE_CONFIG = load_config_file()

current_frame = None
stream_available = False
frame_lock = threading.Lock()
RTSP_URL = os.getenv("RTSP_URL", "")
LOCAL_CAMERA_INDEX = int(os.getenv("LOCAL_CAMERA_INDEX", "0"))
CCTV_TIMEOUT_MILLISECONDS = int(os.getenv("CCTV_TIMEOUT_MILLISECONDS", "3000"))
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
VPS_ENDPOINT = os.getenv("VPS_ENDPOINT", "")
VPS_TOKEN = os.getenv("VPS_TOKEN", "")
VIDEO_URL_BASE = os.getenv("VIDEO_URL_BASE", "")
VIDEO_URL_TOKEN = os.getenv("VIDEO_URL_TOKEN", "")
VIDEO_PLACEHOLDER_URL = os.getenv("VIDEO_PLACEHOLDER_URL", "")
VIDEO_DURATION_SECONDS = int(
    FILE_CONFIG.get("VIDEO_DURATION_SECONDS", os.getenv("VIDEO_DURATION_SECONDS", "60"))
)

# PhilSMS Configuration
PHILSMS_URL = os.getenv("PHILSMS_URL", "https://dashboard.philsms.com/api/v3/sms/send")
PHILSMS_TOKEN = os.getenv("PHILSMS_TOKEN", "")
TARGET_MOBILE = FILE_CONFIG.get("TARGET_MOBILE", os.getenv("TARGET_MOBILE", ""))
SENDER_ID = os.getenv("SENDER_ID", "PhilSMS")
PRODUCT_NAME = os.getenv("PRODUCT_NAME", "Alerto")
SEND_SMS = os.getenv("SEND_SMS", "false").lower() in {"1", "true", "yes", "on"}


def save_config_file(settings):
    with open(CONFIG_PATH, "w", encoding="utf-8") as config_file:
        json.dump(settings, config_file, indent=2)
        config_file.write("\n")


def open_video_source():
    if RTSP_URL:
        try:
            cctv = cv2.VideoCapture(
                RTSP_URL,
                cv2.CAP_FFMPEG,
                [
                    cv2.CAP_PROP_OPEN_TIMEOUT_MSEC,
                    CCTV_TIMEOUT_MILLISECONDS,
                    cv2.CAP_PROP_READ_TIMEOUT_MSEC,
                    CCTV_TIMEOUT_MILLISECONDS,
                ],
            )
        except (TypeError, cv2.error):
            cctv = cv2.VideoCapture(RTSP_URL)
        if cctv.isOpened():
            print("Using CCTV stream")
            return cctv
        cctv.release()

    device_camera = cv2.VideoCapture(LOCAL_CAMERA_INDEX)
    if device_camera.isOpened():
        print(f"CCTV unavailable; using device camera {LOCAL_CAMERA_INDEX}")
        return device_camera
    device_camera.release()
    return None


def capture_stream():
    global current_frame, stream_available
    while True:
        cap = open_video_source()
        if cap is None:
            with frame_lock:
                current_frame = None
                stream_available = False
            print("No CCTV or device camera available; video capture disabled")
            return
        with frame_lock:
            stream_available = True
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            with frame_lock:
                current_frame = frame.copy()
        cap.release()
        with frame_lock:
            current_frame = None
            stream_available = False


threading.Thread(target=capture_stream, daemon=True).start()


def record_cctv_stream(filename, duration_seconds):
    if not RTSP_URL or shutil.which(FFMPEG_PATH) is None:
        return False

    command = [
        FFMPEG_PATH,
        "-y",
        "-rtsp_transport",
        "tcp",
        "-i",
        RTSP_URL,
        "-t",
        str(duration_seconds),
        "-map",
        "0",
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        filename,
    ]

    try:
        result = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=duration_seconds + 15,
            check=False,
            text=True,
        )
        if result.returncode == 0 and os.path.getsize(filename) > 0:
            print("Recorded CCTV stream directly with FFmpeg")
            return True
        print(f"Direct CCTV recording failed: {result.stderr[-500:]}")
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"Direct CCTV recording unavailable: {error}")

    if os.path.exists(filename):
        os.remove(filename)
    return False


def upload_video_file(filename):
    if not os.path.isfile(filename) or os.path.getsize(filename) == 0:
        print(f"Skipping empty or missing video: {filename}")
        return

    if not VPS_ENDPOINT:
        print(f"Video saved locally: {filename} (VPS_ENDPOINT is not configured)")
        return

    headers = {"Authorization": f"Bearer {VPS_TOKEN}"} if VPS_TOKEN else {}
    try:
        with open(filename, "rb") as video_file:
            response = requests.post(
                VPS_ENDPOINT,
                files={"video": (os.path.basename(filename), video_file, "video/mp4")},
                headers=headers,
                timeout=120,
            )
        response.raise_for_status()
        print(f"Video uploaded: {filename} | Status: {response.status_code}")
    except Exception as error:
        print(f"Video upload error: {error}")


def record_and_upload(button_id, filename, duration_seconds):
    if record_cctv_stream(filename, duration_seconds):
        upload_video_file(filename)
        return

    recording_error = None
    snapshot_dir = None

    try:
        with frame_lock:
            first_frame = None if current_frame is None else current_frame.copy()
        if first_frame is None:
            raise RuntimeError("Could not read frames from the CCTV stream or device camera")
        height, width = first_frame.shape[:2]
        fps = 20.0
        if shutil.which(FFMPEG_PATH) is None:
            raise RuntimeError("FFmpeg is required to build a video from snapshots")
        snapshot_dir = tempfile.mkdtemp(prefix="video_frames_")
        frame_number = 0

        deadline = time.monotonic() + duration_seconds
        while time.monotonic() < deadline:
            with frame_lock:
                frame = None if current_frame is None else current_frame.copy()
            if frame is None:
                time.sleep(0.05)
                continue
            time.sleep(1 / fps)
            frame_path = os.path.join(snapshot_dir, f"frame_{frame_number:06d}.jpg")
            if not cv2.imwrite(frame_path, frame):
                raise RuntimeError("Could not save a camera snapshot")
            frame_number += 1
    except Exception as error:
        recording_error = error

    if recording_error is None:
        try:
            encode_result = subprocess.run(
                [
                    FFMPEG_PATH,
                    "-y",
                    "-framerate",
                    str(fps),
                    "-i",
                    os.path.join(snapshot_dir, "frame_%06d.jpg"),
                    "-c:v",
                    "libx264",
                    "-pix_fmt",
                    "yuv420p",
                    "-movflags",
                    "+faststart",
                    filename,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                check=False,
                text=True,
            )
            if encode_result.returncode != 0:
                recording_error = RuntimeError("FFmpeg could not assemble the camera snapshots")
        except Exception as error:
            recording_error = error

    if snapshot_dir is not None:
        shutil.rmtree(snapshot_dir, ignore_errors=True)

    if recording_error is not None:
        print(f"Video recording error: {recording_error}")
        if os.path.exists(filename):
            try:
                os.remove(filename)
            except OSError as error:
                print(f"Could not remove incomplete video: {error}")
        return

    upload_video_file(filename)


def video_url(filename, video_available):
    if not video_available:
        return VIDEO_PLACEHOLDER_URL
    token = f"?token={VIDEO_URL_TOKEN}" if VIDEO_URL_TOKEN else ""
    return f"{VIDEO_URL_BASE.rstrip('/')}/{os.path.basename(filename)}{token}"

# Modern, Professional Mobile-Centric UI Template
WEB_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <title>Alerto Emergency Alert System</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        :root { color-scheme: light; }
        * { box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; text-align: center; margin: 0; min-height: 100dvh; background: #e2e8f0; }
        .container { background: #f8fafc; min-height: 100dvh; width: 100%; padding: max(32px, env(safe-area-inset-top)) max(20px, env(safe-area-inset-right)) max(28px, env(safe-area-inset-bottom)) max(20px, env(safe-area-inset-left)); display: flex; flex-direction: column; justify-content: center; align-items: center; overflow: hidden; }
        h2 { color: #0f172a; margin: 0 0 8px; font-family: Georgia, "Times New Roman", serif; font-size: clamp(28px, 7vw, 38px); font-weight: 700; letter-spacing: 0; line-height: 1.1; }
        .subtitle { color: #64748b; margin: 0 0 30px; font-size: 14px; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase; }
        p { color: #64748b; font-size: 15px; margin: 0 auto 32px; max-width: 28rem; }
        .button-stack { display: flex; flex-direction: column; gap: 16px; width: 100%; max-width: 34rem; margin: 0 auto; }
        button { width: 100%; height: clamp(160px, 38vw, 220px); padding: 18px; font-size: clamp(21px, 6vw, 28px); color: white; border: none; border-radius: 0; cursor: pointer; font-weight: 700; box-shadow: none; transition: transform 0.1s ease, opacity 0.2s; touch-action: manipulation; }
        button:active { transform: scale(0.98); opacity: 0.9; }
        
        /* Professional, non-goofy color palette */
        .btn-hazard { background: #d97706; }    /* Amber/Orange */
        .btn-security { background: #b91c1c; }  /* Deep Crimson Red */
        .btn-medical { background: #047857; }   /* Professional Emerald Green */
        
        .activity-log { width: 100%; max-width: 34rem; margin: 24px auto 0; border: 1px solid #cbd5e1; background: #ffffff; text-align: left; }
        .log-header { display: flex; justify-content: space-between; align-items: center; gap: 12px; padding: 12px 14px; border-bottom: 1px solid #e2e8f0; color: #1e293b; font-size: 13px; font-weight: 700; letter-spacing: 0.04em; text-transform: uppercase; }
        .log-state { cursor: default; opacity: 1; }
        #status { min-height: 54px; max-height: 150px; overflow-y: auto; }
        .log-empty, .log-entry { padding: 11px 14px; font-size: 13px; line-height: 1.35; }
        .log-empty { color: #64748b; }
        .log-entry { display: flex; gap: 10px; border-bottom: 1px solid #f1f5f9; color: #334155; }
        .log-entry:last-child { border-bottom: 0; }
        .log-time { flex: 0 0 auto; color: #94a3b8; font-variant-numeric: tabular-nums; }
        .log-entry.success .log-message { color: #047857; }
        .log-entry.error .log-message { color: #b91c1c; }
        .log-entry.pending .log-message { color: #b45309; }
        .log-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
        .log-action { height: auto; width: auto; padding: 5px 8px; background: #e2e8f0; color: #334155; font-size: 11px; font-weight: 700; }
        .log-action:hover { background: #cbd5e1; }
        .configuration { display: none; width: 100%; max-width: 34rem; margin: 10px auto 0; padding: 14px; border: 1px solid #cbd5e1; background: #ffffff; text-align: left; }
        .configuration.open { display: block; }
        .configuration label { display: block; margin-bottom: 5px; color: #334155; font-size: 12px; font-weight: 700; }
        .configuration input { width: 100%; margin-bottom: 12px; padding: 9px 10px; border: 1px solid #cbd5e1; color: #1e293b; font: inherit; }
        .config-save { height: auto; width: auto; padding: 9px 12px; background: #0f766e; font-size: 12px; }
        .location { width: 100%; max-width: 34rem; margin: 18px auto 0; color: #64748b; font-size: clamp(11px, 3.2vw, 14px); line-height: 1.4; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; text-align: center; }
        @media (min-width: 700px) {
            .container { padding: 48px; }
            .button-stack { gap: 18px; }
            button { height: 220px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h2>{{ product_name }}</h2>
        <p class="subtitle">Emergency Reporting System</p>
        
        <div class="button-stack">
            <!-- Button 1: Hazard -->
            <button class="btn-hazard" onclick="triggerAlert(1)">Hazard</button>
            
            <!-- Button 2: Security -->
            <button class="btn-security" onclick="triggerAlert(2)">Security</button>
            
            <!-- Button 3: Medical Concern -->
            <button class="btn-medical" onclick="triggerAlert(3)">Medical Concern</button>
        </div>
        
        <section class="activity-log" aria-live="polite">
            <div class="log-header">
                <span>Activity log</span>
                <div class="log-actions">
                    <button class="log-action log-state" id="log-state" type="button" disabled>Ready</button>
                    <button class="log-action" type="button" onclick="clearLog()">Clear log</button>
                    <button class="log-action" type="button" onclick="toggleConfiguration()">Configuration</button>
                </div>
            </div>
            <div id="status">
                <div class="log-empty">No alerts recorded in this session.</div>
            </div>
        </section>

        <form class="configuration" id="configuration" onsubmit="saveConfiguration(event)">
            <label for="duration">Video duration (seconds)</label>
            <input id="duration" type="number" min="1" max="300" value="{{ video_duration }}" required>
            <label for="recipient">SMS recipient number</label>
            <input id="recipient" type="tel" value="{{ target_mobile }}" placeholder="639XXXXXXXXX" required>
            <button class="config-save" type="submit">Save configuration</button>
        </form>

        <div class="location">STEM Department Building &bull; STEM 12 Newton Room</div>
    </div>

    <script>
        const alertConfiguration = {
            duration: Number(document.getElementById("duration").value),
            recipient: document.getElementById("recipient").value
        };

        function toggleConfiguration() {
            document.getElementById("configuration").classList.toggle("open");
        }

        function saveConfiguration(event) {
            event.preventDefault();
            const duration = Number(document.getElementById("duration").value);
            const recipient = document.getElementById("recipient").value.trim();
            fetch("/configuration", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ duration: duration, recipient: recipient })
            })
                .then(response => {
                    if (!response.ok) throw new Error("Server returned HTTP " + response.status);
                    return response.json();
                })
                .then(() => {
                    alertConfiguration.duration = duration;
                    alertConfiguration.recipient = recipient;
                    document.getElementById("configuration").classList.remove("open");
                    addLog("Configuration saved to Flask: " + duration + " second video; recipient " + recipient + ".", "success");
                })
                .catch(error => addLog("Configuration was not saved: " + error.message, "error"));
        }

        function clearLog() {
            document.getElementById("status").innerHTML = '<div class="log-empty">No alerts recorded in this session.</div>';
            document.getElementById("log-state").innerText = "Ready";
        }

        function addLog(message, level) {
            const status = document.getElementById("status");
            const empty = status.querySelector(".log-empty");
            if (empty) empty.remove();

            const entry = document.createElement("div");
            entry.className = "log-entry " + level;
            entry.innerHTML = '<span class="log-time">' + new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) + '</span>' +
                '<span class="log-message">' + message + '</span>';
            status.prepend(entry);
        }

        function triggerAlert(buttonId) {
            const category = { 1: "Hazard", 2: "Security", 3: "Medical concern" }[buttonId] || "General emergency";
            document.getElementById("log-state").innerText = "Working";
            addLog(category + " alert queued; evidence capture started.", "pending");
            const query = new URLSearchParams({
                button: buttonId,
                duration: String(alertConfiguration.duration),
                recipient: alertConfiguration.recipient
            });
            fetch('/trigger-alert?' + query.toString())
                .then(response => {
                    if (!response.ok) throw new Error("Server returned HTTP " + response.status);
                    return response.text();
                })
                .then(() => {
                    document.getElementById("log-state").innerText = "Ready";
                    addLog(category + " alert accepted; SMS dispatch completed.", "success");
                })
                .catch(error => {
                    document.getElementById("log-state").innerText = "Attention";
                    addLog(category + " alert was not confirmed: " + error.message, "error");
                    console.error('Error:', error);
                });
        }
    </script>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(
        WEB_PAGE,
        product_name=PRODUCT_NAME,
        video_duration=VIDEO_DURATION_SECONDS,
        target_mobile=TARGET_MOBILE,
    )


@app.route("/configuration", methods=["POST"])
def save_configuration():
    global VIDEO_DURATION_SECONDS, TARGET_MOBILE

    settings = request.get_json(silent=True) or {}
    try:
        duration_seconds = max(1, min(300, int(settings.get("duration", VIDEO_DURATION_SECONDS))))
    except (TypeError, ValueError):
        return jsonify(error="Video duration must be a number from 1 to 300"), 400

    recipient = str(settings.get("recipient", TARGET_MOBILE)).strip()
    if not recipient:
        return jsonify(error="SMS recipient is required"), 400

    VIDEO_DURATION_SECONDS = duration_seconds
    TARGET_MOBILE = recipient
    save_config_file({
        "VIDEO_DURATION_SECONDS": duration_seconds,
        "TARGET_MOBILE": recipient,
    })
    return jsonify(message="Configuration saved"), 200


@app.route("/trigger-alert", methods=["GET"])
def trigger_alert():
    button_id = request.args.get("button", "unknown")
    try:
        duration_seconds = max(1, min(300, int(request.args.get("duration", VIDEO_DURATION_SECONDS))))
    except (TypeError, ValueError):
        duration_seconds = VIDEO_DURATION_SECONDS
    recipient = request.args.get("recipient", TARGET_MOBILE).strip() or TARGET_MOBILE

    categories = {"1": "Hazard", "2": "Security", "3": "Medical Concern"}
    category_name = categories.get(button_id, "General Emergency")
    current_time = datetime.now().strftime("%B %d, %Y - %I:%M %p")
    video_filename = os.path.abspath(
        f"evidence_btn{button_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.mp4"
    )
    with frame_lock:
        video_available = stream_available and current_frame is not None

    threading.Thread(
        target=record_and_upload,
        args=(button_id, video_filename, duration_seconds),
        daemon=True,
    ).start()

    sms_text = (
        f"{PRODUCT_NAME} EMERGENCY ALERT\n\n"
        f"Category: {category_name}\n"
        f"Location: STEM Department Building – STEM 12 Newton Room\n"
        f"Time: {current_time}\n\n"
        f"An emergency alert has been activated. Please proceed to the indicated location immediately and assess the situation. Visual incident documentation will be transmitted for review.\n\n"
        f"Video: {video_url(video_filename, video_available)}\n\n"
        f"— {PRODUCT_NAME} Emergency Alert System"
    )

    if SEND_SMS:
        payload = {
            "recipient": recipient,
            "sender_id": SENDER_ID,
            "type": "plain",
            "message": sms_text,
        }

        headers = {
            "Authorization": f"Bearer {PHILSMS_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            response = requests.post(PHILSMS_URL, json=payload, headers=headers)
            print(f"Alert Dispatched | Status: {response.status_code}")
            return response.text, response.status_code
        except Exception as e:
            print(f"SMS Dispatch Error: {str(e)}")
            return str(e), 500

    print("SMS disabled; video capture/upload continues")
    return "Alert processed without SMS", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
