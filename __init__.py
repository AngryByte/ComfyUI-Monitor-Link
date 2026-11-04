"""
ComfyUI Monitor Link — Multi-Socket Bridge & WebM Transcoder for ComfyUI Monitor

A lightweight background extension that mirrors all ComfyUI execution events
(executing nodes, progress, outputs, errors) to secondary WebSocket connections,
and provides on-demand WebM video transcoding for embedded WebView/Chromium environments.

Features:
- Dedicated secondary WebSocket route at /ws/monitor (non-blocking broadcast)
- HTTP status endpoint at /api/execution_status
- On-demand WebM video transcoding endpoint at /api/video_to_webm (stores in temp folder)
- Hardened path traversal protection & input validation
- CPU-throttled concurrency control & temp file LRU pruning
- Does NOT require any workflow nodes (registers zero canvas nodes)
- Never interferes with or disconnects the main ComfyUI GUI
"""

import os
os.environ["AIOHTTP_NOSENDFILE"] = "1"
try:
    import aiohttp.web_fileresponse
    aiohttp.web_fileresponse.NOSENDFILE = True
except Exception:
    pass

import sys
import time
import shutil
import hashlib
import asyncio
import logging
from aiohttp import web
from server import PromptServer
import folder_paths

# Suppress harmless Windows asyncio Proactor transport shutdown [WinError 10054] noise
if sys.platform == 'win32':
    try:
        from asyncio.proactor_events import _ProactorBasePipeTransport
        _orig_call_connection_lost = _ProactorBasePipeTransport._call_connection_lost

        def _safe_call_connection_lost(self, exc):
            try:
                _orig_call_connection_lost(self, exc)
            except (ConnectionResetError, ConnectionError, OSError):
                pass

        _ProactorBasePipeTransport._call_connection_lost = _safe_call_connection_lost
    except Exception:
        pass

# Allowed video extensions for input validation
ALLOWED_VIDEO_EXTS = {'.mp4', '.mov', '.mkv', '.avi', '.flv', '.wmv', '.m4v', '.webm'}

# Global concurrency control: at most 2 FFmpeg transcode jobs run simultaneously
_transcode_semaphore = asyncio.Semaphore(2)

# Set of active mirror/monitor WebSocket connections
_monitor_sockets = set()
_active_transcodes = {}

# Reference to the PromptServer singleton
server = PromptServer.instance

# 1. Register dedicated WebSocket endpoint: /ws/monitor
@server.routes.get('/ws/monitor')
async def monitor_ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    _monitor_sockets.add(ws)
    logging.info(f"[ComfyUIMonitorLink] Monitor client connected. Total monitors: {len(_monitor_sockets)}")

    try:
        # Send current queue status
        await ws.send_json({
            "type": "status",
            "data": { "status": server.get_queue_info() }
        })

        # Send current execution state immediately (active node if running, or node: None if idle)
        last_node_id = getattr(server, "last_node_id", None)
        last_prompt_id = getattr(server, "last_prompt_id", None)
        await ws.send_json({
            "type": "executing",
            "data": {
                "node": last_node_id,
                "prompt_id": last_prompt_id
            }
        })

        # Keep connection open and handle incoming messages/pings
        async for msg in ws:
            if msg.type in (web.WSMsgType.CLOSE, web.WSMsgType.CLOSING, web.WSMsgType.CLOSED):
                break
            elif msg.type == web.WSMsgType.ERROR:
                logging.warning(f"[ComfyUIMonitorLink] WebSocket error: {ws.exception()}")
                break
    except (ConnectionResetError, ConnectionError, OSError):
        pass
    finally:
        _monitor_sockets.discard(ws)
        logging.info(f"[ComfyUIMonitorLink] Monitor client disconnected. Total monitors: {len(_monitor_sockets)}")

    return ws


# 2. Register HTTP status endpoint: /api/execution_status and /execution_status
@server.routes.get('/api/execution_status')
@server.routes.get('/execution_status')
async def get_execution_status(request):
    return web.json_response({
        "last_node_id": getattr(server, "last_node_id", None),
        "last_prompt_id": getattr(server, "last_prompt_id", None),
        "client_id": getattr(server, "client_id", None),
        "queue": server.get_queue_info(),
        "monitor_clients": len(_monitor_sockets)
    })


# 3. Helper functions for video resolution, cache pruning & WebM transcoding
def _find_source_video(filename, subfolder="", folder_type="output"):
    if not filename or '\0' in filename:
        return None

    clean_filename = os.path.basename(filename.strip())
    ext = os.path.splitext(clean_filename)[1].lower()
    if ext not in ALLOWED_VIDEO_EXTS:
        return None

    # Strict subfolder sanitization: disallow traversal, null bytes, and absolute paths
    clean_subfolder = (subfolder or "").replace('\\', '/').strip('/')
    if '\0' in clean_subfolder or ".." in clean_subfolder or os.path.isabs(clean_subfolder):
        return None

    if folder_type == "input":
        search_dirs = [folder_paths.get_input_directory()]
    elif folder_type == "temp":
        search_dirs = [folder_paths.get_temp_directory()]
    else:
        search_dirs = [
            folder_paths.get_output_directory(),
            folder_paths.get_temp_directory(),
            folder_paths.get_input_directory()
        ]

    for base in search_dirs:
        if not base or not os.path.isdir(base):
            continue
        base_abs = os.path.abspath(base)

        if clean_subfolder:
            candidate = os.path.abspath(os.path.join(base_abs, clean_subfolder, clean_filename))
            try:
                if os.path.commonpath([base_abs, candidate]) == base_abs and os.path.isfile(candidate):
                    return candidate
            except (ValueError, OSError):
                pass

        candidate = os.path.abspath(os.path.join(base_abs, clean_filename))
        try:
            if os.path.commonpath([base_abs, candidate]) == base_abs and os.path.isfile(candidate):
                return candidate
        except (ValueError, OSError):
            pass

    return None


def _prune_temp_webm_cache(max_files=100, max_age_seconds=86400):
    """Clean up old transcoded webm files in temp directory to prevent unbounded disk growth."""
    try:
        temp_dir = folder_paths.get_temp_directory()
        if not temp_dir or not os.path.isdir(temp_dir):
            return
        now = time.time()
        webm_files = []
        for entry in os.scandir(temp_dir):
            if entry.is_file() and entry.name.endswith('.webm'):
                try:
                    st = entry.stat()
                    if now - st.st_mtime > max_age_seconds:
                        try: os.remove(entry.path)
                        except Exception: pass
                    else:
                        webm_files.append((entry.path, st.st_mtime))
                except Exception:
                    pass

        if len(webm_files) > max_files:
            webm_files.sort(key=lambda x: x[1])
            for path, _ in webm_files[:len(webm_files) - max_files]:
                try: os.remove(path)
                except Exception: pass
    except Exception:
        pass


async def _transcode_to_webm(source_file, dest_file):
    if os.path.isfile(dest_file) and os.path.getsize(dest_file) > 0:
        return True

    # Unique temporary destination file per task to prevent concurrent race collisions
    task_id = id(asyncio.current_task()) if hasattr(asyncio, 'current_task') else os.getpid()
    temp_dest = f"{dest_file}.tmp_{os.getpid()}_{task_id}.webm"
    ffmpeg_bin = shutil.which("ffmpeg") or "ffmpeg"

    # Fast VP9 encode with standard 48kHz stereo Opus audio
    cmd = [
        ffmpeg_bin,
        "-y",
        "-i", source_file,
        "-c:v", "libvpx-vp9",
        "-crf", "32",
        "-b:v", "0",
        "-cpu-used", "4",
        "-deadline", "realtime",
        "-c:a", "libopus",
        "-b:a", "96k",
        "-ar", "48000",
        "-ac", "2",
        "-shortest",
        temp_dest
    ]

    async with _transcode_semaphore:
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE
            )
            # 5 minute timeout per video transcode to prevent hung processes
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=300.0)
            if proc.returncode == 0 and os.path.isfile(temp_dest) and os.path.getsize(temp_dest) > 0:
                if os.path.exists(dest_file):
                    try: os.remove(dest_file)
                    except Exception: pass
                os.rename(temp_dest, dest_file)
                _prune_temp_webm_cache()
                return True

            # Fallback to VP8 if VP9 failed
            fallback_cmd = [
                ffmpeg_bin,
                "-y",
                "-i", source_file,
                "-c:v", "libvpx",
                "-quality", "realtime",
                "-cpu-used", "5",
                "-c:a", "libvorbis",
                "-b:a", "96k",
                "-ar", "48000",
                "-ac", "2",
                temp_dest
            ]
            proc2 = await asyncio.create_subprocess_exec(
                *fallback_cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE
            )
            _, stderr2 = await asyncio.wait_for(proc2.communicate(), timeout=300.0)
            if proc2.returncode == 0 and os.path.isfile(temp_dest) and os.path.getsize(temp_dest) > 0:
                if os.path.exists(dest_file):
                    try: os.remove(dest_file)
                    except Exception: pass
                os.rename(temp_dest, dest_file)
                _prune_temp_webm_cache()
                return True
        except asyncio.TimeoutError:
            logging.error(f"[ComfyUIMonitorLink] WebM transcode timed out: {source_file}")
            try: proc.kill()
            except Exception: pass
        except Exception as e:
            logging.error(f"[ComfyUIMonitorLink] WebM transcode error: {e}")
        finally:
            if os.path.exists(temp_dest):
                try: os.remove(temp_dest)
                except Exception: pass

    return False


# 4. Register video streaming & on-demand WebM conversion endpoints
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS, HEAD",
    "Access-Control-Allow-Headers": "*"
}

async def video_stream_direct(request, file_path, content_type="video/webm"):
    """Stream video file in chunks with HTTP Range (byte-range) support to bypass Windows aiohttp sendfile stalls."""
    if not file_path or not os.path.isfile(file_path):
        return web.Response(status=404, headers=CORS_HEADERS)

    file_size = os.path.getsize(file_path)
    if file_size == 0:
        return web.Response(status=404, headers=CORS_HEADERS)

    # Handle HTTP Range requests (essential for HTML5 video seek, scrub, and CEF playback)
    range_header = request.headers.get("Range")
    if range_header and range_header.startswith("bytes="):
        try:
            ranges = range_header.replace("bytes=", "").split("-")
            start = int(ranges[0]) if ranges[0] else 0
            end = int(ranges[1]) if len(ranges) > 1 and ranges[1] else file_size - 1
            if start >= file_size or end >= file_size or start > end:
                return web.Response(status=416, headers={
                    **CORS_HEADERS,
                    "Content-Range": f"bytes */{file_size}"
                })

            length = end - start + 1
            resp = web.StreamResponse(status=206, headers={
                **CORS_HEADERS,
                "Content-Type": content_type,
                "Content-Length": str(length),
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Cache-Control": "no-cache",
            })
            await resp.prepare(request)
            with open(file_path, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk_size = min(65536, remaining)
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    await resp.write(chunk)
                    remaining -= len(chunk)
            await resp.write_eof()
            return resp
        except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
            return resp
        except Exception:
            pass

    # Standard full file stream
    resp = web.StreamResponse(status=200, headers={
        **CORS_HEADERS,
        "Content-Type": content_type,
        "Content-Length": str(file_size),
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",
    })
    try:
        await resp.prepare(request)
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                await resp.write(chunk)
        await resp.write_eof()
    except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
        pass
    return resp


@server.routes.options('/api/video_stream')
@server.routes.options('/video_stream')
async def video_stream_options(request):
    return web.Response(status=204, headers=CORS_HEADERS)


@server.routes.head('/api/video_stream')
@server.routes.head('/video_stream')
async def video_stream_head(request):
    params = request.query
    filename = params.get('filename', '').strip()
    subfolder = (params.get('subfolder', '') or '').replace('\\', '/').strip('/')
    folder_type = params.get('type', 'output').strip()

    if not filename or '\0' in filename or ".." in filename or ".." in subfolder:
        return web.Response(status=400, headers=CORS_HEADERS)

    source_path = _find_source_video(filename, subfolder, folder_type)
    if not source_path or not os.path.isfile(source_path):
        return web.Response(status=404, headers=CORS_HEADERS)

    file_size = os.path.getsize(source_path)
    ext = os.path.splitext(filename)[1].lower()
    content_type = "video/webm" if ext == ".webm" else "video/mp4"

    return web.Response(status=200, headers={
        **CORS_HEADERS,
        "Content-Type": content_type,
        "Content-Length": str(file_size),
        "Accept-Ranges": "bytes",
    })


@server.routes.get('/api/video_stream')
@server.routes.get('/video_stream')
async def video_stream_handler(request):
    params = request.query
    filename = params.get('filename', '').strip()
    subfolder = (params.get('subfolder', '') or '').replace('\\', '/').strip('/')
    folder_type = params.get('type', 'output').strip()

    if not filename or '\0' in filename or ".." in filename or ".." in subfolder:
        return web.Response(status=400, headers=CORS_HEADERS)

    source_path = _find_source_video(filename, subfolder, folder_type)
    if not source_path or not os.path.isfile(source_path):
        return web.Response(status=404, headers=CORS_HEADERS)

    ext = os.path.splitext(filename)[1].lower()
    content_type = "video/webm" if ext == ".webm" else "video/mp4"
    return await video_stream_direct(request, source_path, content_type)


@server.routes.options('/api/video_to_webm')
@server.routes.options('/video_to_webm')
async def video_to_webm_options(request):
    return web.Response(status=204, headers=CORS_HEADERS)

@server.routes.get('/api/video_to_webm')
@server.routes.get('/video_to_webm')
@server.routes.post('/api/video_to_webm')
@server.routes.post('/video_to_webm')
async def video_to_webm_handler(request):
    params = request.query
    if request.method == 'POST':
        try:
            body = await request.json()
            if isinstance(body, dict):
                params = body
        except Exception:
            pass

    filename = params.get('filename', '').strip()
    subfolder = (params.get('subfolder', '') or '').replace('\\', '/').strip('/')
    folder_type = params.get('type', 'output').strip()
    do_redirect = params.get('redirect', '').lower() in ('1', 'true', 'yes') or params.get('stream', '').lower() in ('1', 'true', 'yes')

    if not filename or '\0' in filename:
        return web.json_response({"error": "Missing or invalid filename parameter", "success": False}, status=400, headers=CORS_HEADERS)

    # Disallow path traversal sequences in subfolder or filename
    if ".." in filename or ".." in subfolder or '\0' in subfolder:
        return web.json_response({"error": "Invalid file path", "success": False}, status=400, headers=CORS_HEADERS)

    # Validate file extension
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_VIDEO_EXTS:
        return web.json_response({"error": f"Unsupported video format: '{ext}'", "success": False}, status=400, headers=CORS_HEADERS)

    # If the file is already a .webm file, stream or return stream URL immediately
    if ext == '.webm':
        source_path = _find_source_video(filename, subfolder, folder_type)
        if not source_path:
            return web.json_response({"error": f"Video file '{filename}' not found", "success": False}, status=404, headers=CORS_HEADERS)
        if do_redirect:
            return await video_stream_direct(request, source_path, "video/webm")
        sub_param = f"&subfolder={subfolder}" if subfolder else ""
        stream_url = f"/api/video_stream?filename={filename}{sub_param}&type={folder_type}"
        return web.json_response({
            "success": True,
            "filename": filename,
            "subfolder": subfolder,
            "type": folder_type,
            "url": stream_url
        }, headers=CORS_HEADERS)

    # Resolve source video file with containment checks
    source_path = _find_source_video(filename, subfolder, folder_type)
    if not source_path:
        return web.json_response({"error": f"Video file '{filename}' not found", "success": False}, status=404, headers=CORS_HEADERS)

    stat = os.stat(source_path)
    file_hash = hashlib.md5(f"{source_path}:{stat.st_size}:{stat.st_mtime}".encode('utf-8')).hexdigest()[:8]
    stem = os.path.splitext(os.path.basename(source_path))[0]
    webm_filename = f"{stem}_{file_hash}.webm"
    temp_dir = folder_paths.get_temp_directory()
    dest_path = os.path.join(temp_dir, webm_filename)

    # Check if already cached
    if not (os.path.isfile(dest_path) and os.path.getsize(dest_path) > 0):
        # Await or initiate transcode task
        if dest_path not in _active_transcodes:
            _active_transcodes[dest_path] = asyncio.create_task(_transcode_to_webm(source_path, dest_path))

        ok = await _active_transcodes[dest_path]
        _active_transcodes.pop(dest_path, None)

        if not ok or not os.path.isfile(dest_path):
            return web.json_response({"error": "Failed to transcode video to WebM", "success": False}, status=500, headers=CORS_HEADERS)

    # Direct stream or stream URL
    if do_redirect:
        return await video_stream_direct(request, dest_path, "video/webm")

    stream_url = f"/api/video_stream?filename={webm_filename}&type=temp"
    return web.json_response({
        "success": True,
        "filename": webm_filename,
        "type": "temp",
        "url": stream_url,
        "original_filename": filename
    }, headers=CORS_HEADERS)


# 5. Non-blocking WebSocket broadcast helpers
async def _send_to_monitor_socket(ws, send_func, payload):
    try:
        if not ws.closed:
            await asyncio.wait_for(send_func(payload), timeout=2.0)
        else:
            _monitor_sockets.discard(ws)
    except Exception:
        _monitor_sockets.discard(ws)


def _broadcast_to_monitors(payload, is_bytes=False):
    if not _monitor_sockets:
        return
    active_sockets = list(_monitor_sockets)
    for ws in active_sockets:
        send_func = ws.send_bytes if is_bytes else ws.send_json
        asyncio.create_task(_send_to_monitor_socket(ws, send_func, payload))


# 6. Hook server.send_json & server.send_bytes without blocking the main ComfyUI loop
_original_send_json = server.send_json
_last_broadcast_node_id = None

async def _mirrored_send_json(event, data, sid=None):
    global _last_broadcast_node_id
    if event == "executing" and isinstance(data, dict):
        node = data.get("node")
        _last_broadcast_node_id = node if node is not None else None

    if _monitor_sockets:
        _broadcast_to_monitors({"type": event, "data": data}, is_bytes=False)
    return await _original_send_json(event, data, sid=sid)

server.send_json = _mirrored_send_json

_original_send_bytes = server.send_bytes

async def _mirrored_send_bytes(event, data, sid=None):
    if _monitor_sockets:
        message = server.encode_bytes(event, data)
        _broadcast_to_monitors(message, is_bytes=True)
    return await _original_send_bytes(event, data, sid=sid)

server.send_bytes = _mirrored_send_bytes


# 7. Hook server.queue_updated to broadcast idle execution completion when client_id is null
_original_queue_updated = server.queue_updated

def _mirrored_queue_updated():
    global _last_broadcast_node_id
    res = _original_queue_updated()
    try:
        last_node = getattr(server, "last_node_id", None)
        q_info = server.get_queue_info()
        rem = q_info.get("exec_info", {}).get("queue_remaining", 0) if isinstance(q_info, dict) else 0

        # If a node was executing and now server.last_node_id is None, prompt execution finished
        if (_last_broadcast_node_id is not None or rem == 0) and last_node is None:
            last_prompt = getattr(server, "last_prompt_id", None)
            _last_broadcast_node_id = None
            if _monitor_sockets:
                _broadcast_to_monitors({
                    "type": "executing",
                    "data": {"node": None, "prompt_id": last_prompt}
                }, is_bytes=False)
                _broadcast_to_monitors({
                    "type": "execution_success",
                    "data": {"prompt_id": last_prompt}
                }, is_bytes=False)
    except Exception:
        pass
    return res

server.queue_updated = _mirrored_queue_updated

logging.info("[ComfyUIMonitorLink] Initialized successfully with hardened security. Secondary socket active at /ws/monitor, WebM transcoder at /api/video_to_webm")

NODE_CLASS_MAPPINGS = {}
NODE_DISPLAY_NAME_MAPPINGS = {}
__all__ = ['NODE_CLASS_MAPPINGS', 'NODE_DISPLAY_NAME_MAPPINGS']
