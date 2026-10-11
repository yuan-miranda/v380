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
import websocket

try:
    import cv2
except ImportError:
    cv2 = None

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
ENABLE_LIVE_STREAM = os.getenv("ENABLE_LIVE_STREAM", "true").lower() in {"1", "true", "yes", "on"}
LIVE_STREAM_FPS = int(os.getenv("LIVE_STREAM_FPS", "15"))
LIVE_STREAM_QUALITY = int(os.getenv("LIVE_STREAM_QUALITY", "75"))
live_stream_pause_event = threading.Event()


def get_ffmpeg_executable():
    path = shutil.which(FFMPEG_PATH)
    if path:
        return path
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return None


def resolve_server_ws_url(vps_endpoint):
    url = str(vps_endpoint or "").strip().rstrip("/")
    if url.endswith("/upload"):
        url = url[:-len("/upload")]
    if url.startswith("http://"):
        ws = "ws://" + url[len("http://"):]
    elif url.startswith("https://"):
        ws = "wss://" + url[len("https://"):]
    elif url.startswith("ws://") or url.startswith("wss://"):
        ws = url
    else:
        ws = f"ws://{url}" if url else "ws://alerto.ddns.net"
    if not ws.endswith("/ws/live-feed"):
        ws = f"{ws}/ws/live-feed"
    return ws



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
    ffmpeg_bin = get_ffmpeg_executable()
    if not rtsp_url or not ffmpeg_bin:
        logger.error("Recording skipped: RTSP URL or FFmpeg is unavailable")
        return False

    def _run_recording_proc():
        return subprocess.run(
            [
                ffmpeg_bin,
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

    logger.info(
        "Recording started: duration=%ss output=%s url=%s",
        duration_seconds,
        filename,
        rtsp_url,
    )
    result = None
    try:
        result = _run_recording_proc()
    except (OSError, subprocess.TimeoutExpired) as error:
        logger.error("Recording initial run exception: %s", error)

    # If camera only permits 1 RTSP connection, temporarily yield the live stream and retry
    if not (
        result
        and result.returncode == 0
        and os.path.isfile(filename)
        and os.path.getsize(filename) > 0
    ):
        logger.warning(
            "Initial recording failed or camera busy; pausing live stream to guarantee camera access..."
        )
        live_stream_pause_event.set()
        time.sleep(1.5)
        try:
            result = _run_recording_proc()
        except Exception as retry_err:
            logger.error("Recording retry failed: %s", retry_err)
        finally:
            live_stream_pause_event.clear()

    if (
        result
        and result.returncode == 0
        and os.path.isfile(filename)
        and os.path.getsize(filename) > 0
    ):
        logger.info(
            "Recording complete: file=%s size=%d bytes",
            filename,
            os.path.getsize(filename),
        )
        return True

    if result and result.stderr:
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


def run_live_stream_service():
    """Continuously captures CCTV frames and streams them to the server via WebSocket.

    Uses FFmpeg pipe as primary engine (Termux-friendly, zero compilation, fast hardware decoding).
    Falls back to OpenCV if FFmpeg is unavailable.
    """
    logger.info("Live streaming background service started")

    while True:
        if live_stream_pause_event.is_set():
            time.sleep(1.0)
            continue

        current_ip = os.getenv("CCTV_IP", CCTV_IP)
        server_endpoint = os.getenv("VPS_ENDPOINT", VPS_ENDPOINT)
        ws_url = resolve_server_ws_url(server_endpoint)

        try:
            rtsp_url = get_live_rtsp_url(current_ip)
        except Exception as err:
            logger.warning("Live stream waiting for CCTV configuration: %s", err)
            time.sleep(5)
            continue

        ffmpeg_bin = get_ffmpeg_executable()
        if not ffmpeg_bin and cv2 is None:
            logger.error(
                "Neither FFmpeg nor OpenCV is available. In Termux run 'pkg install ffmpeg', or install imageio-ffmpeg."
            )
            time.sleep(5)
            continue

        ws = None
        proc = None
        cap = None

        try:
            ws = websocket.create_connection(ws_url, timeout=10)
            init_payload = {
                "type": "init",
                "cctv_ip": current_ip,
                "fps": LIVE_STREAM_FPS,
                "width": 1280,
                "height": 720,
                "timestamp": datetime.now().isoformat(),
            }
            ws.send(json.dumps(init_payload))
            logger.info("Live stream: Connected to server WebSocket inlet (%s)", ws_url)
        except Exception as ws_err:
            logger.warning(
                "Live stream: Could not connect to server WebSocket (%s): %s. Retrying in 3s...",
                ws_url,
                ws_err,
            )
            time.sleep(3)
            continue

        frame_counter = 0
        last_stat_log = time.time()
        active_ip = current_ip

        try:
            if ffmpeg_bin:
                # Primary Engine: Native FFmpeg piped MJPEG (lightning fast, pure C, Termux prebuilt)
                cmd = [
                    ffmpeg_bin,
                    "-rtsp_transport",
                    "tcp",
                    "-i",
                    rtsp_url,
                    "-r",
                    str(LIVE_STREAM_FPS),
                    "-f",
                    "image2pipe",
                    "-vcodec",
                    "mjpeg",
                    "-q:v",
                    "5",
                    "-",
                ]
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                )
                logger.info(
                    "Live stream: Started FFmpeg RTSP capture pipe (FPS=%d)",
                    LIVE_STREAM_FPS,
                )

                buffer = bytearray()
                while True:
                    if live_stream_pause_event.is_set():
                        logger.info("Live stream pausing for emergency alert recording...")
                        break

                    new_ip = os.getenv("CCTV_IP", CCTV_IP)
                    if new_ip != active_ip:
                        logger.info(
                            "CCTV_IP updated (%s -> %s). Reconnecting live stream...",
                            active_ip,
                            new_ip,
                        )
                        break

                    if proc.poll() is not None:
                        logger.warning(
                            "Live stream: FFmpeg process exited (code %s)",
                            proc.returncode,
                        )
                        break

                    chunk = proc.stdout.read(4096)
                    if not chunk:
                        break
                    buffer.extend(chunk)

                    while True:
                        soi = buffer.find(b"\xff\xd8")
                        if soi == -1:
                            buffer.clear()
                            break
                        if soi > 0:
                            del buffer[:soi]
                        eoi = buffer.find(b"\xff\xd9", 2)
                        if eoi == -1:
                            if len(buffer) > 1024 * 1024:
                                buffer.clear()
                            break
                        jpeg_frame = bytes(buffer[:eoi + 2])
                        del buffer[:eoi + 2]

                        # Skip older frame if a newer complete frame is already in buffer
                        next_soi = buffer.find(b"\xff\xd8")
                        next_eoi = (
                            buffer.find(b"\xff\xd9", next_soi + 2)
                            if next_soi != -1
                            else -1
                        )
                        if next_soi != -1 and next_eoi != -1:
                            continue

                        ws.send_binary(jpeg_frame)
                        frame_counter += 1

                        now = time.time()
                        if now - last_stat_log >= 15.0:
                            fps = frame_counter / (now - last_stat_log)
                            logger.info("Live stream active: %.1f FPS to server", fps)
                            frame_counter = 0
                            last_stat_log = now

            elif cv2 is not None:
                # Fallback Engine: OpenCV VideoCapture
                logger.info("Live stream: Falling back to OpenCV capture engine")
                encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), LIVE_STREAM_QUALITY]
                frame_interval = 1.0 / max(1, LIVE_STREAM_FPS)

                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"
                cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

                if not cap.isOpened():
                    logger.warning(
                        "Live stream: CCTV RTSP not reachable via OpenCV (%s)",
                        rtsp_url,
                    )
                    time.sleep(3)
                    continue

                while True:
                    new_ip = os.getenv("CCTV_IP", CCTV_IP)
                    if new_ip != active_ip:
                        break

                    tick_start = time.time()
                    ret, frame = cap.read()
                    if not ret or frame is None:
                        time.sleep(0.02)
                        continue

                    ret, jpeg_buf = cv2.imencode(".jpg", frame, encode_params)
                    if not ret:
                        continue

                    ws.send_binary(jpeg_buf.tobytes())
                    frame_counter += 1

                    now = time.time()
                    if now - last_stat_log >= 15.0:
                        fps = frame_counter / (now - last_stat_log)
                        logger.info("Live stream active: %.1f FPS to server", fps)
                        frame_counter = 0
                        last_stat_log = now

                    elapsed = time.time() - tick_start
                    sleep_time = frame_interval - elapsed
                    if sleep_time > 0.001:
                        time.sleep(sleep_time)

        except Exception as stream_err:
            logger.warning(
                "Live stream connection interrupted: %s. Reconnecting in 3s...",
                stream_err,
            )
        finally:
            if proc:
                try:
                    proc.terminate()
                    proc.wait(timeout=2)
                except Exception:
                    pass
            if cap:
                try:
                    cap.release()
                except Exception:
                    pass
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
            time.sleep(3)


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
    if ENABLE_LIVE_STREAM:
        threading.Thread(target=run_live_stream_service, name="live-stream", daemon=True).start()
    logger.info("ALERTO video worker started (event polling + continuous live streaming)")
    poll_events()