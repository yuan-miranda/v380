import json
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
from urllib.parse import quote
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


def load_local_env():
    if ENV_FILE.is_file():
        with open(ENV_FILE, encoding="utf-8") as env_file:
            for line in env_file:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_local_env()

VPS_ENV_ENDPOINT = os.getenv("VPS_ENV_ENDPOINT", DEFAULT_VPS_ENV_ENDPOINT)
VPS_EVENT_TOKEN = os.getenv("VPS_EVENT_TOKEN", DEFAULT_VPS_EVENT_TOKEN)


def sync_server_env_to_config():
    try:
        logger.info("Pulling configuration profile from server: %s", VPS_ENV_ENDPOINT)
        response = requests.get(
            VPS_ENV_ENDPOINT,
            headers={"Authorization": f"Bearer {VPS_EVENT_TOKEN}"},
            timeout=10,
        )
        if response.status_code == 200:
            env_text = response.text
            ENV_FILE.write_text(env_text, encoding="utf-8")

            config_data = {}
            for line in env_text.splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                config_data[key.strip()] = value.strip().strip('"').strip("'")

            CONFIG_FILE.write_text(json.dumps(config_data, indent=2), encoding="utf-8")
            logger.info(
                "Successfully synchronized server config into local config.json."
            )
        else:
            logger.warning(
                "Server returned status %s during config sync; using local fallback.",
                response.status_code,
            )
    except requests.RequestException as error:
        logger.warning(
            "Could not reach server for environment sync: %s. Using local config.",
            error,
        )


sync_server_env_to_config()
load_local_env()

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
RTSP_USER = os.getenv("RTSP_USER", "admin")
RTSP_PASS = os.getenv("RTSP_PASS", "password")
CCTV_RTSP_URL = os.getenv("CCTV_RTSP_URL", "").strip()
CCTV_IP = os.getenv("CCTV_IP", "").strip()
CONFIG_SYNC_INTERVAL_SECONDS = max(30, int(os.getenv("CONFIG_SYNC_INTERVAL_SECONDS", "60")))


def background_config_sync():
    """Refresh server settings without blocking the event polling loop."""
    while True:
        time.sleep(CONFIG_SYNC_INTERVAL_SECONDS)
        try:
            sync_server_env_to_config()
            # Keep the manually entered CCTV setting authoritative.
            if CCTV_RTSP_URL:
                os.environ["CCTV_RTSP_URL"] = CCTV_RTSP_URL
                os.environ.pop("CCTV_IP", None)
            elif CCTV_IP:
                os.environ["CCTV_IP"] = CCTV_IP
                os.environ.pop("CCTV_RTSP_URL", None)
        except Exception as error:
            logger.warning("Background configuration sync failed: %s", error)


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


def _save_local_cctv(value):
    """Persist the manually entered CCTV value without touching server settings."""
    value = value.strip()
    if not value:
        return
    try:
        lines = ENV_FILE.read_text(encoding="utf-8").splitlines() if ENV_FILE.is_file() else []
        filtered = [line for line in lines if not line.lstrip().startswith(("CCTV_RTSP_URL=", "CCTV_IP="))]
        if value.lower().startswith("rtsp://"):
            filtered.append(f"CCTV_RTSP_URL={value}")
        else:
            filtered.append(f"CCTV_IP={value}")
        ENV_FILE.write_text("\n".join(filtered) + "\n", encoding="utf-8")
    except OSError as error:
        logger.warning("Could not persist CCTV setting locally: %s", error)


def _get_manual_cctv_setting():
    load_local_env()
    url = os.getenv("CCTV_RTSP_URL", "").strip()
    ip = os.getenv("CCTV_IP", "").strip()
    return url or ip


def get_live_rtsp_url():
    global CCTV_RTSP_URL, CCTV_IP
    setting = _get_manual_cctv_setting()

    # A full RTSP URL is preferred because it lets the user specify the camera's
    # exact path/port/credentials.
    if setting.lower().startswith("rtsp://"):
        return setting

    if setting:
        user = os.getenv("RTSP_USER", RTSP_USER)
        password = os.getenv("RTSP_PASS", RTSP_PASS)
        return f"rtsp://{quote(user, safe='')}:{quote(password, safe='')}@{setting}:554/live/ch00_0"

    return ""


def configure_cctv():
    """Ask for the CCTV address once when running interactively.

    No camera discovery is performed. The operator supplies either an IP/host
    or the complete RTSP URL. An environment value can be used for unattended
    startup.
    """
    global CCTV_RTSP_URL, CCTV_IP

    cli_value = ""
    if "--cctv" in sys.argv:
        try:
            cli_value = sys.argv[sys.argv.index("--cctv") + 1].strip()
        except (IndexError, AttributeError):
            logger.error("--cctv requires an IP address or RTSP URL")
            raise SystemExit(2)

    current = _get_manual_cctv_setting()
    if cli_value:
        current = cli_value
    elif sys.stdin.isatty():
        # Always let the operator choose the camera address. The value supplied
        # by the server is only a fallback/default and is never auto-discovered.
        prompt = "Enter CCTV IP/hostname or full RTSP URL"
        if current:
            entered = input(f"{prompt} [{current}]: ").strip()
            if entered:
                current = entered
        else:
            current = input(f"{prompt}: ").strip()

    if not current:
        logger.error(
            "No CCTV address configured. Set CCTV_RTSP_URL/CCTV_IP or run: python main.py --cctv <IP-or-RTSP-URL>"
        )
        return False

    if current.lower().startswith("rtsp://"):
        CCTV_RTSP_URL = current
        CCTV_IP = ""
    else:
        CCTV_IP = current
        CCTV_RTSP_URL = ""

    _save_local_cctv(current)
    logger.info("CCTV address configured manually: %s", current)
    return True


def record_cctv_stream(filename, duration_seconds):
    rtsp_url = get_live_rtsp_url()
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

    # The server-generated filename is used so the SMS link and uploaded video match exactly.
    server_filename = event.get("filename")
    if server_filename:
        filename = os.path.abspath(server_filename)
    else:
        filename = os.path.abspath(
            f"evidence_btn{button_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')}.mp4"
        )

    logger.info(
        "Event received: id=%s button=%s duration=%ss filename=%s",
        event_id,
        button_id,
        duration,
        os.path.basename(filename),
    )
    report_event_status(event_id, "recording_started")
    if not record_cctv_stream(filename, duration):
        report_event_status(event_id, "failed", "CCTV recording failed.")
        logger.error("Event failed during recording: button=%s", button_id)
        return

    report_event_status(event_id, "recording_complete")
    report_event_status(event_id, "upload_started")

    if not upload_video(filename):
        report_event_status(event_id, "failed", "Video upload failed.")
        return

    report_event_status(event_id, "completed")



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

    poll_counter = 0
    while True:
        try:
            poll_counter += 1
            if poll_counter >= 4:
                sync_server_env_to_config()
                poll_counter = 0

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