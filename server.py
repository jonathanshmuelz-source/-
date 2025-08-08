import threading
from fastapi import FastAPI
from macro_copilot_mvp import main as run_bot

app = FastAPI()
_started = False

def _start_bot():
    # Runs the Telegram bot + scheduler (blocking) in a background thread
    run_bot()

@app.on_event("startup")
def on_startup():
    global _started
    if not _started:
        t = threading.Thread(target=_start_bot, daemon=True)
        t.start()
        _started = True

@app.get("/")
def root():
    return {"ok": True, "service": "macro-copilot", "bot": "running"}

@app.get("/health")
def health():
    return {"ok": True}
