import os
import logging
import shutil
import subprocess
import time
from datetime import datetime

import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("alerto.worker")


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

FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
VPS_ENDPOINT = os.getenv("VPS_ENDPOINT", "")
VPS_TOKEN = os.getenv("VPS_TOKEN", "")
VPS_EVENT_ENDPOINT = os.getenv("VPS_EVENT_ENDPOINT", "")
VPS_EVENT_TOKEN = os.getenv("VPS_EVENT_TOKEN", VPS_TOKEN)
VPS_CONFIG_ENDPOINT = os.getenv("VPS_CONFIG_ENDPOINT", "")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "2"))
VIDEO_DURATION_SECONDS = int(os.getenv("VIDEO_DURATION_SECONDS", "60"))
RTSP_USER = os.getenv("RTSP_USER", "admin")
RTSP_PASS = os.getenv("RTSP_PASS", "password")


def get_live_rtsp_url():
    if VPS_CONFIG_ENDPOINT:
        try:
            response = requests.get(
                VPS_CONFIG_ENDPOINT,
                headers={"Authorization": f"Bearer {VPS_EVENT_TOKEN}"},
                timeout=5,
            )
            if response.ok:
                ip = response.json().get("cctv_ip")
                if ip:
                    return f"rtsp://{RTSP_USER}:{RTSP_PASS}@{ip}:554/live/ch00_0"
        except requests.RequestException as error:
            logger.warning("Could not fetch latest CCTV IP from server, falling back: %s", error)

    fallback_ip = os.getenv("CCTV_IP", "192.168.100.57")
    return f"rtsp://{RTSP_USER}:{RTSP_PASS}@{fallback_ip}:554/live/ch00_0"


def record_cctv_stream(filename, duration_seconds):
    rtsp_url = get_live_rtsp_url()
    if not rtsp_url or shutil.which(FFMPEG_PATH) is None:
        logger.error("Recording skipped: RTSP URL or FFmpeg is unavailable")
        return False

    logger.info("Recording started: duration=%ss output=%s url=%s", duration_seconds, filename, rtsp_url)
    try:
        result = subprocess.run(
            [
                FFMPEG_PATH, "-y", "-rtsp_transport", "tcp", "-i", rtsp_url,
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
        logger.error("Recording failed: %s", error)
        return False

    if result.returncode == 0 and os.path.isfile(filename) and os.path.getsize(filename) > 0:
        logger.info("Recording complete: file=%s size=%d bytes", filename, os.path.getsize(filename))
        return True

    logger.error("FFmpeg recording failed: %s", result.stderr[-500:].strip())
    if os.path.exists(filename):
        os.remove(filename)
    return False


def upload_video(filename):
    if not VPS_ENDPOINT or not os.path.isfile(filename):
        logger.warning("Upload skipped: endpoint or video file is unavailable: %s", filename)
        return
    logger.info("Upload started: file=%s endpoint=%s", filename, VPS_ENDPOINT)
    try:
        with open(filename, "rb") as video_file:
            response = requests.post(
                VPS_ENDPOINT,
                files={"video": (os.path.basename(filename), video_file, "video/mp4")},
                headers={"Authorization": f"Bearer {VPS_TOKEN}"},
                timeout=120,
            )
        response.raise_for_status()
        logger.info("Upload complete: file=%s status=%s", filename, response.status_code)
    except requests.RequestException as error:
        logger.error("Upload failed: %s", error)


def process_event(event):
    button_id = str(event.get("button", "unknown"))
    try:
        duration = max(1, min(300, int(event.get("duration", VIDEO_DURATION_SECONDS))))
    except (TypeError, ValueError):
        duration = VIDEO_DURATION_SECONDS
    filename = os.path.abspath(
        f"evidence_btn{button_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.mp4"
    )
    logger.info("Event received: button=%s duration=%ss", button_id, duration)
    if record_cctv_stream(filename, duration):
        upload_video(filename)
    else:
        logger.error("Event failed before upload: button=%s", button_id)


def poll_events():
    if not VPS_EVENT_ENDPOINT:
        logger.error("VPS_EVENT_ENDPOINT is not configured")
        return

    logger.info("Worker polling: endpoint=%s interval=%ss", VPS_EVENT_ENDPOINT, POLL_INTERVAL_SECONDS)
    headers = {"Authorization": f"Bearer {VPS_EVENT_TOKEN}"}
    while True:
        try:
            response = requests.get(VPS_EVENT_ENDPOINT, headers=headers, timeout=15)
            response.raise_for_status()
            event = response.json().get("event")
            if event:
                process_event(event)
        except (requests.RequestException, ValueError, TypeError) as error:
            logger.error("Event polling failed: %s", error)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    logger.info("ALERTO video worker started")
    poll_events()