from __future__ import annotations

import json, os, re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from zoneinfo import ZoneInfo  # חדש: תמיכה באזור זמן מקומי

load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TE_CLIENT = os.getenv("TE_CLIENT", "guest:guest")

# הגדרות ברירת מחדל ניתנות לשינוי בזמן ריצה דרך /config
DEFAULT_CONFIG = {
    "country": os.getenv("TE_COUNTRY", "United States"),
    "high_impact_only": True,
    "poll_every_seconds": 60,
    "window_minutes": 6,
    "local_tz": os.getenv("LOCAL_TZ", "Asia/Tbilisi"),  # זמן מקומי להודעות
}
CONFIG_FILE = "config.json"

SUBSCRIBERS_FILE = "subscribers.json"
PROCESSED_FILE = "processed_events.json"

# (יוגדרו בזמן main)
SCHEDULER: Optional[BackgroundScheduler] = None
SCHED_JOB_ID = "poll-job"
APPLICATION: Optional[Application] = None

if not TELEGRAM_BOT_TOKEN:
    raise SystemExit("Missing TELEGRAM_BOT_TOKEN (set it in Render → Environment).")

@dataclass
class MacroEvent:
    id_key: str
    country: str
    name: str
    category: Optional[str]
    actual: Optional[str]
    forecast: Optional[str]
    previous: Optional[str]
    unit: Optional[str]
    importance: Optional[int]
    release_time_utc: datetime
    source: Optional[str]
    source_url: Optional[str]

def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)

def get_config() -> Dict[str, Any]:
    cfg = _load_json(CONFIG_FILE, {})
    merged = {**DEFAULT_CONFIG, **cfg}
    # שמירה כדי ליצור קובץ אם לא קיים
    _save_json(CONFIG_FILE, merged)
    return merged

class TradingEconomicsProvider:
    BASE = "https://api.tradingeconomics.com"

    def __init__(self, client: str):
        self.client = client

    def fetch_calendar(self, start: datetime, end: datetime, country: str, high_impact_only: bool = True) -> List[MacroEvent]:
        params = {
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
            "country": country,
            "c": self.client,
        }
        if high_impact_only:
            params["importance"] = 3
        r = requests.get(f"{self.BASE}/calendar", params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        events: List[MacroEvent] = []
        for item in data:
            dt_str = item.get("Date") or item.get("DateUTC")
            if not dt_str:
                date_part = item.get("Date") or item.get("date")
                time_part = item.get("Time") or item.get("time") or "00:00"
                dt_str = f"{date_part} {time_part}"
            release_time_utc = _parse_te_datetime(dt_str)
            ev = MacroEvent(
                id_key=_build_id(item),
                country=item.get("Country") or country,
                name=item.get("Event") or item.get("Category", "Unknown Event"),
                category=item.get("Category"),
                actual=item.get("Actual"),
                forecast=item.get("Forecast"),
                previous=item.get("Previous"),
                unit=item.get("Unit"),
                importance=item.get("Importance"),
                release_time_utc=release_time_utc,
                source=item.get("Source"),
                source_url=item.get("SourceURL"),
            )
            events.append(ev)
        return events

def _parse_te_datetime(dt_str: str) -> datetime:
    s = dt_str.replace("T", " ").replace("Z", "")
    for fmt in ["%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%Y-%m-%d","%m/%d/%Y %H:%M:%S","%m/%d/%Y %H:%M"]:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)

def _build_id(item: Dict[str, Any]) -> str:
    date_key = (item.get("Date") or item.get("DateUTC") or "").replace(" ", "T")
    name_key = item.get("Event") or item.get("Category") or "Unknown"
    country = item.get("Country") or "NA"
    return f"{country}|{name_key}|{date_key}"

_value_num_pat = re.compile(r"(-?\d+(?:\.\d+)?)")

def parse_number(v: Optional[str]) -> Optional[float]:
    if v is None: return None
    s = str(v).strip().replace(",", "")
    mult = 1.0
    if s.upper().endswith("K"): mult, s = 1_000.0, s[:-1]
    if s.upper().endswith("M"): mult, s = 1_000_000.0, s[:-1]
    m = _value_num_pat.search(s)
    if not m: return None
    try: return float(m.group(1)) * mult
    except ValueError: return None

def interpret_event(ev: MacroEvent, local_tz: str) -> Dict[str, Any]:
    actual = parse_number(ev.actual)
    forecast = parse_number(ev.forecast)
    previous = parse_number(ev.previous)
    surprise = None
    if actual is not None and forecast is not None:
        surprise = actual - forecast

    category = (ev.category or ev.name or "").lower()
    tags: List[str] = []
    direction = "neutral"; score = 0; nuance: List[str] = []

    def hawkish_if_positive(s: Optional[float]):
        nonlocal direction, score
        if s is None: return
        if s > 0: direction, score = "hawkish", max(score, 1)
        elif s < 0: direction, score = "dovish", min(score, -1)

    def dovish_if_positive(s: Optional[float]):
        nonlocal direction, score
        if s is None: return
        if s > 0: direction, score = "dovish", min(score, -1)
        elif s < 0: direction, score = "hawkish", max(score, 1)

    if any(k in category for k in ["cpi","core cpi","ppi","inflation"]):
        tags.append("inflation")
        if surprise is not None:
            hawkish_if_positive(surprise)
            nuance.append("inflation surprise: " + ("hotter" if surprise > 0 else "cooler"))
    elif "gdp" in category or "growth" in category:
        tags.append("growth"); hawkish_if_positive(surprise)
    elif "unemployment" in category or "jobless" in category:
        tags.append("labor"); dovish_if_positive(surprise)
    elif "non-farm" in category or "nonfarm" in category or "payroll" in category:
        tags.append("labor"); hawkish_if_positive(surprise)
    elif "rate decision" in category or "interest rate" in category or "fomc" in category:
        tags.append("rates"); hawkish_if_positive(surprise)
    else:
        tags.append("other"); hawkish_if_positive(surprise)

    if actual is not None and previous is not None:
        if actual > previous: nuance.append("rising vs previous")
        elif actual < previous: nuance.append("falling vs previous")
        else: nuance.append("unchanged vs previous")

    def fmt(v: Optional[str]) -> str:
        return v if (v is not None and str(v).strip() != "") else "—"

    try:
        tz = ZoneInfo(local_tz)
        local_time = ev.release_time_utc.astimezone(tz)
        local_line = f"Time ({local_tz}): {local_time:%Y-%m-%d %H:%M}"
    except Exception:
        local_line = f"Time (LOCAL): N/A"

    summary = (
        f"{ev.country} — {ev.name}\n"
        f"Time (UTC): {ev.release_time_utc:%Y-%m-%d %H:%M}\n"
        f"{local_line}\n"
        f"Actual: {fmt(ev.actual)}  |  Forecast: {fmt(ev.forecast)}  |  Previous: {fmt(ev.previous)}"
    )
    details: List[str] = []
    if surprise is not None:
        # גם אחוז הפתעה
        pct = None
        if forecast and forecast != 0:
            try:
                fnum = parse_number(ev.forecast)
                if fnum not in (None, 0):
                    pct = (actual - fnum) / abs(fnum) * 100.0
            except Exception:
                pct = None
        details.append(f"Surprise: {surprise:+.2f}" + (f" ({pct:+.2f}%)" if pct is not None else "") + " (actual - forecast)")
    if ev.unit: details.append(f"Unit: {ev.unit}")
    if ev.importance: details.append(f"Impact: {ev.importance}/3 (TE)")
    details.append("Interpretation: " + ( "hawkish tilt (tighter conditions likelier)" if direction=="hawkish" else "dovish tilt (easier conditions likelier)" if direction=="dovish" else "neutral/unclear"))
    if nuance: details.append("Notes: " + "; ".join(nuance))

    return {"direction": direction, "score": score, "summary": summary, "details": details, "tags": tags}

provider = TradingEconomicsProvider(TE_CLIENT)

def poll_and_notify(app: Application) -> None:
    cfg = get_config()
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=cfg["window_minutes"])
    end = now + timedelta(minutes=1)
    try:
        events = provider.fetch_calendar(start, end, cfg["country"], cfg["high_impact_only"])
    except Exception as e:
        print(f"[poll] fetch error: {e}")
        return

    processed = set(_load_json(PROCESSED_FILE, []))
    subs = _load_json(SUBSCRIBERS_FILE, [])
    if not subs: return

    for ev in events:
        if not ev.actual or str(ev.actual).strip() == "": continue
        if ev.release_time_utc > now + timedelta(minutes=1): continue
        if ev.id_key in processed: continue

        analysis = interpret_event(ev, cfg["local_tz"])
        msg = analysis["summary"] + "\n" + "\n".join("• " + d for d in analysis["details"]) + "\n"
        try:
            for chat_id in subs:
                app.create_task(app.bot.send_message(chat_id=chat_id, text=msg))
            processed.add(ev.id_key)
        except Exception as e:
            print(f"[notify] send error: {e}")

    _save_json(PROCESSED_FILE, list(processed))

# ===== פקודות טלגרם =====

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "שלום! אני בוט ניתוח מאקרו (ללא הוראות מסחר).\n"
        "אקפיץ לך פרשנות כשמתפרסם נתון חשוב (US, high-impact).\n\n"
        "פקודות:\n"
        "/מנוי – קבלת עדכונים (alias: /subscribe)\n"
        "/בטל_מנוי – הפסקת עדכונים (alias: /unsubscribe)\n"
        "/מה_נשמע – הבוט יענה תשובה מנומסת ;)\n"
        "/מצב – סטטוס (alias: /status)\n"
        "/היום – תצוגת האירועים של היום\n"
        "/config – שינוי הגדרות (לדוגמה: /config country=United States impact=all poll=90 window=6 tz=Asia/Tbilisi)\n"
        "/help – עזרה\n"
    )
    await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await context.bot.send_message(chat_id=update.effective_chat.id, text=(
        "עזרה:\n"
        "/מנוי | /subscribe – הרשמה לעדכונים\n"
        "/בטל_מנוי | /unsubscribe – ביטול הרשמה\n"
        "/מה_נשמע – עונה: לא עניינך!\n"
        "/מצב | /status – מציג סטטוס והגדרות פעילות\n"
        "/היום – אירועים של היום לפי ההגדרות\n"
        "/config country=<שם מדינה> impact=high|all poll=<שניות> window=<דקות> tz=<TZ>\n"
        "לדוגמה: /config country=United States impact=high poll=60 window=6 tz=Asia/Tbilisi"
    ))

async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    subs = _load_json(SUBSCRIBERS_FILE, [])
    if chat_id not in subs:
        subs.append(chat_id)
        _save_json(SUBSCRIBERS_FILE, subs)
        await context.bot.send_message(chat_id=chat_id, text="נרשמת לעדכוני מאקרו (US, high-impact).")
    else:
        await context.bot.send_message(chat_id=chat_id, text="כבר היית רשום.")

async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    subs = _load_json(SUBSCRIBERS_FILE, [])
    if chat_id in subs:
        subs.remove(chat_id)
        _save_json(SUBSCRIBERS_FILE, subs)
        await context.bot.send_message(chat_id=chat_id, text="הוסרנו מרשימת העדכונים.")
    else:
        await context.bot.send_message(chat_id=chat_id, text="לא היית רשום.")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = get_config()
    subs = _load_json(SUBSCRIBERS_FILE, [])
    msg = (
        f"Subscribers: {len(subs)}\n"
        f"Country: {cfg['country']}\n"
        f"High-impact only: {cfg['high_impact_only']}\n"
        f"Polling: every {cfg['poll_every_seconds']}s; window={cfg['window_minutes']}m\n"
        f"Local TZ: {cfg['local_tz']}\n"
    )
    await context.bot.send_message(chat_id=update.effective_chat.id, text=msg)

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await context.bot.send_message(chat_id=update.effective_chat.id, text="pong")

async def whatsup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await context.bot.send_message(chat_id=update.effective_chat.id, text="לא עניינך!")

async def today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = get_config()
    tz = ZoneInfo(cfg["local_tz"])
    now = datetime.now(tz)
    start_utc = datetime(now.year, now.month, now.day, 0, 0, tzinfo=tz).astimezone(timezone.utc)
    end_utc = (start_utc + timedelta(days=1)) - timedelta(seconds=1)
    try:
        events = provider.fetch_calendar(start_utc, end_utc, cfg["country"], cfg["high_impact_only"])
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"שגיאת משיכה: {e}")
        return
    if not events:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="אין אירועים ליום זה לפי ההגדרות.")
        return
    lines = []
    for ev in sorted(events, key=lambda e: e.release_time_utc):
        lt = ev.release_time_utc.astimezone(tz)
        ut = ev.release_time_utc
        name = ev.name or ev.category or "Event"
        lines.append(f"- {name} [{ev.country}] | {ut:%H:%M} UTC / {lt:%H:%M} {cfg['local_tz']} | imp={ev.importance or 'N/A'}")
    # חלק את ההודעה אם ארוכה מדי
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) + 1 > 4000:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=chunk)

def parse_config_args(args: List[str]) -> Dict[str, Any]:
    # תומך: country=..., impact=high|all, poll=<int>, window=<int>, tz=<IANA TZ>
    out: Dict[str, Any] = {}
    for a in args:
        if "=" not in a: continue
        k, v = a.split("=", 1)
        k = k.strip().lower()
        v = v.strip()
        if k == "country":
            out["country"] = v
        elif k == "impact":
            out["high_impact_only"] = (v.lower() == "high")
        elif k == "poll":
            try: out["poll_every_seconds"] = max(15, int(v))
            except: pass
        elif k == "window":
            try: out["window_minutes"] = max(1, int(v))
            except: pass
        elif k == "tz":
            out["local_tz"] = v
    return out

async def config_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global SCHEDULER
    cfg = get_config()
    new_vals = parse_config_args(context.args or [])
    if not new_vals:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=(
            "אין פרמטרים. שימוש:\n"
            "/config country=<United States> impact=high|all poll=<שניות>=60 window=<דקות>=6 tz=<Asia/Tbilisi>"
        ))
        return
    cfg.update(new_vals)
    _save_json(CONFIG_FILE, cfg)

    # עדכון תדירות הפולינג בזמן אמת
    if SCHEDULER and SCHEDULER.get_job(SCHED_JOB_ID):
        try:
            SCHEDULER.reschedule_job(SCHED_JOB_ID, trigger=IntervalTrigger(seconds=cfg["poll_every_seconds"]))
        except Exception as e:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=f"שגיאה בעדכון תדירות: {e}")

    await context.bot.send_message(chat_id=update.effective_chat.id, text=(
        "עודכן בהצלחה:\n"
        f"Country={cfg['country']}, Impact={'high' if cfg['high_impact_only'] else 'all'}, "
        f"Poll={cfg['poll_every_seconds']}s, Window={cfg['window_minutes']}m, TZ={cfg['local_tz']}"
    ))

def main() -> None:
    global SCHEDULER, APPLICATION
    APPLICATION = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # פקודות
    APPLICATION.add_handler(CommandHandler("start", start))
    APPLICATION.add_handler(CommandHandler("help", help_cmd))
    APPLICATION.add_handler(CommandHandler("subscribe", subscribe))
    APPLICATION.add_handler(CommandHandler("unsubscribe", unsubscribe))
    APPLICATION.add_handler(CommandHandler("status", status))
    APPLICATION.add_handler(CommandHandler("ping", ping))
    APPLICATION.add_handler(CommandHandler("מנוי", subscribe))
    APPLICATION.add_handler(CommandHandler("בטל_מנוי", unsubscribe))
    APPLICATION.add_handler(CommandHandler("מה_נשמע", whatsup))
    APPLICATION.add_handler(CommandHandler("מצב", status))      # alias עברית
    APPLICATION.add_handler(CommandHandler("היום", today))
    APPLICATION.add_handler(CommandHandler("config", config_cmd))
    APPLICATION.add_handler(CommandHandler("help_he", help_cmd))  # אופציונלי

    # Scheduler
    cfg = get_config()
    SCHEDULER = BackgroundScheduler(timezone=timezone.utc)
    SCHEDULER.add_job(
        poll_and_notify,
        trigger=IntervalTrigger(seconds=cfg["poll_every_seconds"]),
        args=[APPLICATION],
        id=SCHED_JOB_ID,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )
    SCHEDULER.start()

    print("Bot started. Press Ctrl+C to stop.")
    try:
        APPLICATION.run_polling(close_loop=False)
    finally:
        SCHEDULER.shutdown(wait=False)

if __name__ == "__main__":
    main()

