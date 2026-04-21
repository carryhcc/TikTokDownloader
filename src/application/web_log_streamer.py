import asyncio
import collections
import logging
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

router = APIRouter()

# History buffer (last 100 logs)
log_history = collections.deque(maxlen=100)
# Active client queues
log_clients = set()


def broadcast_log(message: str):
    log_history.append(message)
    for q in log_clients:
        try:
            q.put_nowait(message)
        except asyncio.QueueFull:
            pass


class WebLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            if msg.strip():
                try:
                    loop = asyncio.get_running_loop()
                    loop.call_soon_threadsafe(broadcast_log, msg)
                except RuntimeError:
                    broadcast_log(msg)
        except Exception:
            pass

def patch_console(console):
    """
    Hooks into ColorfulConsole to broadcast the printed text. 
    It also attaches to standard Python logging (Uvicorn).
    """
    original_print = console.print

    def patched_print(*args, **kwargs):
        # Call the original method to preserve regular CLI output
        original_print(*args, **kwargs)
        
        # Best-effort conversion of arguments to string for the Web UI stream
        def _to_str(a):
            try:
                # If it's a rich Text object, get its plain text
                if hasattr(a, "plain"):
                    return a.plain
                return str(a)
            except Exception:
                return ""
                
        text = " ".join(_to_str(a) for a in args)
        if text:
            # We must schedule the broadcast using the active event loop if possible,
            # or just call it directly. Queue.put_nowait is thread-safe only from the same loop,
            # but since FastAPI uses the same loop, this works well enough for simple logs.
            # (To be absolutely robust across OS threads, we can use call_soon_threadsafe)
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(broadcast_log, text)
            except RuntimeError:
                # If not inside an event loop, just append directly
                broadcast_log(text)

    console.print = patched_print


@router.on_event("startup")
async def attach_loggers():
    # Hook into Standard Python Logging to capture Uvicorn access/error logs
    # This must run on startup so Uvicorn does not override our handlers during boot up.
    web_handler = WebLogHandler()
    
    # We strip any default formatting as we just want the raw log text.
    class StripAnsiFormatter(logging.Formatter):
        def format(self, record):
            # Attempt to avoid double formatting if Uvicorn already formats it
            return super().format(record)

    class SupressTaskManagerFilter(logging.Filter):
        def filter(self, record):
            msg = record.getMessage()
            if "GET /comment-center/api/task-manager" in msg:
                return False
            return True

    web_handler.setFormatter(StripAnsiFormatter('%(levelname)s:     %(message)s'))
    
    # Uvicorn loggers
    for logger_name in ("uvicorn.access", "uvicorn.error", "fastapi"):
        l = logging.getLogger(logger_name)
        l.addHandler(web_handler)
        if logger_name == "uvicorn.access":
            l.addFilter(SupressTaskManagerFilter())
    
    # Root logger fallback
    logging.getLogger().addHandler(web_handler)


@router.get("/comment-center/api/logs/stream", tags=["评论中心"])
async def stream_logs(request: Request):
    q = asyncio.Queue(maxsize=1000)
    log_clients.add(q)
    
    async def log_generator():
        try:
            # Send historical buffer
            for msg in list(log_history):
                # Using SSE format: data: <message>\n\n
                # Need to escape newlines in SSE
                safe_msg = msg.replace('\n', '\ndata: ')
                yield f"data: {safe_msg}\n\n"
            
            while True:
                # Exit if client disconnected
                if await request.is_disconnected():
                    break
                
                # Check for new message
                msg = await q.get()
                safe_msg = msg.replace('\n', '\ndata: ')
                yield f"data: {safe_msg}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            log_clients.discard(q)

    return StreamingResponse(log_generator(), media_type="text/event-stream")
