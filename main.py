import cv2
import shutil
import subprocess
import threading
import tempfile
import time
from flask import Flask, request, render_template_string
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

current_frame = None
frame_lock = threading.Lock()
RTSP_URL = os.getenv("RTSP_URL", "")
LOCAL_CAMERA_INDEX = int(os.getenv("LOCAL_CAMERA_INDEX", "0"))
CCTV_TIMEOUT_MILLISECONDS = int(os.getenv("CCTV_TIMEOUT_MILLISECONDS", "3000"))
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
VPS_ENDPOINT = os.getenv("VPS_ENDPOINT", "")
VPS_TOKEN = os.getenv("VPS_TOKEN", "")
VIDEO_DURATION_SECONDS = int(os.getenv("VIDEO_DURATION_SECONDS", "60"))

# PhilSMS Configuration
PHILSMS_URL = os.getenv("PHILSMS_URL", "https://dashboard.philsms.com/api/v3/sms/send")
PHILSMS_TOKEN = os.getenv("PHILSMS_TOKEN", "")
TARGET_MOBILE = os.getenv("TARGET_MOBILE", "")
SENDER_ID = os.getenv("SENDER_ID", "PhilSMS")
PRODUCT_NAME = os.getenv("PRODUCT_NAME", "Alerto")
SEND_SMS = os.getenv("SEND_SMS", "false").lower() in {"1", "true", "yes", "on"}


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
    global current_frame
    while True:
        cap = open_video_source()
        if cap is None:
            time.sleep(2)
            continue
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            with frame_lock:
                current_frame = frame.copy()
        cap.release()


threading.Thread(target=capture_stream, daemon=True).start()


def record_cctv_stream(filename):
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
        str(VIDEO_DURATION_SECONDS),
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
            timeout=VIDEO_DURATION_SECONDS + 15,
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


def record_and_upload(button_id):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.abspath(f"evidence_btn{button_id}_{timestamp}.mp4")

    if record_cctv_stream(filename):
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

        deadline = time.monotonic() + VIDEO_DURATION_SECONDS
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
        h2 { color: #1e293b; margin: 0 0 8px; font-size: clamp(26px, 7vw, 36px); }
        p { color: #64748b; font-size: 15px; margin: 0 auto 32px; max-width: 28rem; }
        .button-stack { display: flex; flex-direction: column; gap: 16px; width: 100%; max-width: 34rem; margin: 0 auto; }
        button { width: 100%; height: clamp(160px, 38vw, 220px); padding: 18px; font-size: clamp(21px, 6vw, 28px); color: white; border: none; border-radius: 0; cursor: pointer; font-weight: 700; box-shadow: none; transition: transform 0.1s ease, opacity 0.2s; touch-action: manipulation; }
        button:active { transform: scale(0.98); opacity: 0.9; }
        
        /* Professional, non-goofy color palette */
        .btn-hazard { background: #d97706; }    /* Amber/Orange */
        .btn-security { background: #b91c1c; }  /* Deep Crimson Red */
        .btn-medical { background: #047857; }   /* Professional Emerald Green */
        
        #status { margin: 0; max-width: 34rem; font-weight: 500; color: #334155; font-size: 15px; line-height: 1.4; min-height: 0; }
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
        <h2>{{ product_name }} Reporter</h2>
        
        <div class="button-stack">
            <!-- Button 1: Hazard -->
            <button class="btn-hazard" onclick="triggerAlert(1)">Hazard</button>
            
            <!-- Button 2: Security -->
            <button class="btn-security" onclick="triggerAlert(2)">Security</button>
            
            <!-- Button 3: Medical Concern -->
            <button class="btn-medical" onclick="triggerAlert(3)">Medical Concern</button>
        </div>
        
        <div id="status"></div>

        <div class="location">STEM Department Building &bull; STEM 12 Newton Room</div>
    </div>

    <script>
        function triggerAlert(buttonId) {
            document.getElementById("status").innerText = "Transmitting alert and securing evidence...";
            fetch('/trigger-alert?button=' + buttonId)
                .then(response => response.text)
                .then(data => {
                    document.getElementById("status").innerText = "Alert dispatched successfully. Authorities notified.";
                })
                .catch(error => {
                    document.getElementById("status").innerText = "Transmission failed. Check network connection.";
                    console.error('Error:', error);
                });
        }
    </script>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(WEB_PAGE, product_name=PRODUCT_NAME)


@app.route("/trigger-alert", methods=["GET"])
def trigger_alert():
    button_id = request.args.get("button", "unknown")

    categories = {"1": "Hazard", "2": "Security", "3": "Medical Concern"}
    category_name = categories.get(button_id, "General Emergency")
    current_time = datetime.now().strftime("%B %d, %Y - %I:%M %p")

    threading.Thread(
        target=record_and_upload,
        args=(button_id,),
        daemon=True,
    ).start()

    sms_text = (
        f"{PRODUCT_NAME} EMERGENCY ALERT\n\n"
        f"Category: {category_name}\n"
        f"Location: STEM Department Building – STEM 12 Newton Room\n"
        f"Time: {current_time}\n\n"
        f"An emergency alert has been activated. Please proceed to the indicated location immediately and assess the situation. Visual incident documentation will be transmitted for review.\n\n"
        f"— {PRODUCT_NAME} Emergency Alert System"
    )

    if SEND_SMS:
        payload = {
            "recipient": TARGET_MOBILE,
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
