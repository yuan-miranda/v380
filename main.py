import json
import logging
import os
import shutil
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("alerto.worker")

CONFIG_FILE = Path("config.json")
ENV_FILE = Path(".env")

DEFAULT_VPS_ENV_ENDPOINT = "http://alerto.ddns.net/worker-env"
DEFAULT_VPS_EVENT_TOKEN = "qqqq"


def load_local_env(overwrite=False):
    if not ENV_FILE.is_file():
        return

    with open(ENV_FILE, encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if overwrite:
                os.environ[key] = value
            else:
                os.environ.setdefault(key, value)


load_local_env()

VPS_ENV_ENDPOINT = os.getenv("VPS_ENV_ENDPOINT", DEFAULT_VPS_ENV_ENDPOINT)
VPS_EVENT_TOKEN = os.getenv("VPS_EVENT_TOKEN", DEFAULT_VPS_EVENT_TOKEN)


def sync_server_env_to_config():
    """Pull the current server configuration and apply it to this worker.

    The server is authoritative for CCTV_IP.  The returned values are applied
    directly to os.environ so an old process environment cannot keep an old
    CCTV address alive after synchronization.
    """
    try:
        logger.info("Pulling configuration profile from server: %s", VPS_ENV_ENDPOINT)
        response = requests.get(
            VPS_ENV_ENDPOINT,
            headers={"Authorization": f"Bearer {VPS_EVENT_TOKEN}"},
            timeout=5,
        )
        response.raise_for_status()

        env_text = response.text
        parsed = {}
        for line in env_text.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            parsed[key.strip()] = value.strip().strip('"').strip("'")

        if not parsed.get("CCTV_IP"):
            raise ValueError("Server configuration did not contain CCTV_IP")

        ENV_FILE.write_text(env_text, encoding="utf-8")
        CONFIG_FILE.write_text(json.dumps(parsed, indent=2), encoding="utf-8")

        # IMPORTANT: replace stale values instead of os.environ.setdefault().
        os.environ.update(parsed)

        logger.info(
            "Server configuration synchronized: CCTV_IP=%s duration=%ss",
            parsed.get("CCTV_IP"),
            parsed.get("VIDEO_DURATION_SECONDS", ""),
        )
        return True
    except (requests.RequestException, ValueError, OSError) as error:
        logger.warning(
            "Could not synchronize server configuration: %s. Keeping current configuration.",
            error,
        )
        return False


sync_server_env_to_config()
load_local_env(overwrite=True)

FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
VPS_ENDPOINT = os.getenv("VPS_ENDPOINT", "")
VPS_TOKEN = os.getenv("VPS_TOKEN", "")
VPS_EVENT_ENDPOINT = os.getenv("VPS_EVENT_ENDPOINT", "")
VPS_EVENT_STATUS_ENDPOINT = os.getenv("VPS_EVENT_STATUS_ENDPOINT", "")
if not VPS_EVENT_STATUS_ENDPOINT and VPS_EVENT_ENDPOINT.endswith("/events/next"):
    VPS_EVENT_STATUS_ENDPOINT = VPS_EVENT_ENDPOINT[:-len("/next")] + "/status"
VPS_EVENT_TOKEN = os.getenv("VPS_EVENT_TOKEN", VPS_TOKEN)
POLL_INTERVAL_SECONDS = max(0.1, float(os.getenv("POLL_INTERVAL_SECONDS", "0.25")))
VIDEO_DURATION_SECONDS = int(os.getenv("VIDEO_DURATION_SECONDS", "60"))
CCTV_IP = os.getenv("CCTV_IP", "192.168.100.57")
RTSP_USER = os.getenv("RTSP_USER", "admin")
RTSP_PASS = os.getenv("RTSP_PASS", "password")


def report_event_status(event_id, status, error=""):
    """Best-effort worker status synchronization with the VPS.

    Status reporting must never stop the recording/upload pipeline. The request
    therefore uses a short timeout and catches all request-level failures.
    """
    if not event_id or not VPS_EVENT_STATUS_ENDPOINT:
        return False

    payload = {
        "event_id": event_id,
        "status": status,
    }
    if error:
        payload["error"] = error

    try:
        response = requests.post(
            VPS_EVENT_STATUS_ENDPOINT,
            json=payload,
            headers={"Authorization": f"Bearer {VPS_EVENT_TOKEN}"},
            timeout=3,
        )
        response.raise_for_status()
        logger.info(
            "Event status synchronized: id=%s status=%s",
            event_id,
            status,
        )
        return True
    except requests.RequestException as exc:
        logger.warning(
            "Event status sync failed: id=%s status=%s error=%s",
            event_id,
            status,
            exc,
        )
        return False


def get_live_rtsp_url(cctv_ip=None):
    current_ip = str(cctv_ip or os.getenv("CCTV_IP", CCTV_IP)).strip()
    if not current_ip:
        raise ValueError("CCTV_IP is empty")
    return f"rtsp://{RTSP_USER}:{RTSP_PASS}@{current_ip}:554/live/ch00_0"


def record_cctv_stream(filename, duration_seconds, cctv_ip=None):
    rtsp_url = get_live_rtsp_url(cctv_ip)
    if not rtsp_url or shutil.which(FFMPEG_PATH) is None:
        logger.error("Recording skipped: RTSP URL or FFmpeg is unavailable")
        return False

    logger.info(
        "Recording started: duration=%ss output=%s url=%s",
        duration_seconds,
        filename,
        rtsp_url,
    )
    try:
        result = subprocess.run(
            [
                FFMPEG_PATH,
                "-y",
                "-rtsp_transport",
                "tcp",
                "-i",
                rtsp_url,
                "-t",
                str(duration_seconds),
                "-map",
                "0:v:0",
                "-c:v",
                "copy",
                "-movflags",
                "+faststart",
                filename,
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

    if (
        result.returncode == 0
        and os.path.isfile(filename)
        and os.path.getsize(filename) > 0
    ):
        logger.info(
            "Recording complete: file=%s size=%d bytes",
            filename,
            os.path.getsize(filename),
        )
        return True

    logger.error("FFmpeg recording failed: %s", result.stderr[-500:].strip())
    if os.path.exists(filename):
        os.remove(filename)
    return False


def upload_video(filename):
    if not VPS_ENDPOINT or not os.path.isfile(filename):
        logger.warning(
            "Upload skipped: endpoint or video file is unavailable: %s", filename
        )
        return False

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
        logger.info(
            "Upload complete: file=%s status=%s", filename, response.status_code
        )
        return True
    except requests.RequestException as error:
        logger.error("Upload failed: %s", error)
        return False



def process_event(event):
    event_id = str(event.get("id", "")).strip()
    button_id = str(event.get("button", "unknown"))

    try:
        duration = max(1, min(300, int(event.get("duration", VIDEO_DURATION_SECONDS))))
    except (TypeError, ValueError):
        duration = VIDEO_DURATION_SECONDS

    server_filename = event.get("filename")
    if server_filename:
        filename = os.path.abspath(server_filename)
    else:
        filename = os.path.abspath(
            f"evidence_btn{button_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')}.mp4"
        )

    # The event contains the exact CCTV configuration snapshot used when the
    # alert was accepted. This avoids races if the website configuration changes
    # while an event is already being processed.
    event_cctv_ip = str(event.get("cctv_ip", "")).strip()
    if not event_cctv_ip:
        event_cctv_ip = os.getenv("CCTV_IP", CCTV_IP).strip()

    logger.info(
        "Event received: id=%s button=%s duration=%ss filename=%s server_cctv_ip=%s",
        event_id,
        button_id,
        duration,
        os.path.basename(filename),
        event_cctv_ip,
    )

    report_event_status(event_id, "recording_started")
    if not record_cctv_stream(filename, duration, event_cctv_ip):
        report_event_status(event_id, "failed", "CCTV recording failed.")
        logger.error(
            "Event failed during recording: button=%s cctv_ip=%s",
            button_id,
            event_cctv_ip,
        )
        return

    report_event_status(event_id, "recording_complete")
    report_event_status(event_id, "upload_started")

    if not upload_video(filename):
        report_event_status(event_id, "failed", "Video upload failed.")
        return

    report_event_status(event_id, "completed")


def _configuration_sync_loop():
    """Refresh configuration without blocking event polling."""
    while True:
        time.sleep(10)
        sync_server_env_to_config()


def poll_events():
    if not VPS_EVENT_ENDPOINT:
        logger.error("VPS_EVENT_ENDPOINT is not configured")
        return

    logger.info(
        "Worker polling: endpoint=%s interval=%ss",
        VPS_EVENT_ENDPOINT,
        POLL_INTERVAL_SECONDS,
    )
    headers = {"Authorization": f"Bearer {VPS_EVENT_TOKEN}"}

    # The server endpoint long-polls while its queue is empty. This avoids the
    # old fixed polling delay and, more importantly, makes an accepted alert
    # wake the worker immediately. On a network failure we reconnect immediately
    # instead of sleeping for the normal polling interval.
    consecutive_failures = 0
    while True:
        try:
            response = requests.get(
                VPS_EVENT_ENDPOINT,
                params={"wait": 25},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            consecutive_failures = 0
            event = response.json().get("event")
            if event:
                process_event(event)
            else:
                # Normal long-poll timeout; reconnect immediately.
                continue
        except (requests.RequestException, ValueError, TypeError) as error:
            consecutive_failures += 1
            logger.error(
                "Event polling failed (attempt %s): %s",
                consecutive_failures,
                error,
            )
            # Only back off after repeated failures. A single connection abort
            # should not introduce an artificial 0.25/1/5-second alert delay.
            if consecutive_failures >= 5:
                time.sleep(min(2.0, consecutive_failures * 0.1))
            else:
                time.sleep(0.1)


if __name__ == "__main__":
    threading.Thread(target=_configuration_sync_loop, name="config-sync", daemon=True).start()
    logger.info("ALERTO video worker started")
    poll_events()