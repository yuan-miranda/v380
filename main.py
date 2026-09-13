import os
import shutil
import subprocess
import time
from datetime import datetime

import requests


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

RTSP_URL = os.getenv("RTSP_URL", "")
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
VPS_ENDPOINT = os.getenv("VPS_ENDPOINT", "")
VPS_TOKEN = os.getenv("VPS_TOKEN", "")
VPS_EVENT_ENDPOINT = os.getenv("VPS_EVENT_ENDPOINT", "")
VPS_EVENT_TOKEN = os.getenv("VPS_EVENT_TOKEN", VPS_TOKEN)
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "2"))
VIDEO_DURATION_SECONDS = int(os.getenv("VIDEO_DURATION_SECONDS", "60"))


def record_cctv_stream(filename, duration_seconds):
    if not RTSP_URL or shutil.which(FFMPEG_PATH) is None:
        print("Video recording error: RTSP_URL or FFmpeg is unavailable")
        return False

    try:
        result = subprocess.run(
            [
                FFMPEG_PATH, "-y", "-rtsp_transport", "tcp", "-i", RTSP_URL,
                "-t", str(duration_seconds), "-map", "0:v:0", "-c:v", "copy",
                "-movflags", "+faststart", filename,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=duration_seconds + 30,
            check=False,
            text=True,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"Video recording error: {error}")
        return False

    if result.returncode == 0 and os.path.isfile(filename) and os.path.getsize(filename) > 0:
        return True

    print(f"Video recording error: {result.stderr[-500:]}")
    if os.path.exists(filename):
        os.remove(filename)
    return False


def upload_video(filename):
    if not VPS_ENDPOINT or not os.path.isfile(filename):
        return
    try:
        with open(filename, "rb") as video_file:
            response = requests.post(
                VPS_ENDPOINT,
                files={"video": (os.path.basename(filename), video_file, "video/mp4")},
                headers={"Authorization": f"Bearer {VPS_TOKEN}"},
                timeout=120,
            )
        response.raise_for_status()
        print(f"Video uploaded: {filename} | Status: {response.status_code}")
    except requests.RequestException as error:
        print(f"Video upload error: {error}")


def process_event(event):
    button_id = str(event.get("button", "unknown"))
    try:
        duration = max(1, min(300, int(event.get("duration", VIDEO_DURATION_SECONDS))))
    except (TypeError, ValueError):
        duration = VIDEO_DURATION_SECONDS
    filename = os.path.abspath(
        f"evidence_btn{button_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.mp4"
    )
    if record_cctv_stream(filename, duration):
        upload_video(filename)


def poll_events():
    if not VPS_EVENT_ENDPOINT:
        print("VPS_EVENT_ENDPOINT is not configured")
        return

    headers = {"Authorization": f"Bearer {VPS_EVENT_TOKEN}"}
    while True:
        try:
            response = requests.get(VPS_EVENT_ENDPOINT, headers=headers, timeout=15)
            response.raise_for_status()
            event = response.json().get("event")
            if event:
                process_event(event)
        except (requests.RequestException, ValueError, TypeError) as error:
            print(f"VPS event polling error: {error}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    print("ALERTO video worker started")
    poll_events()
