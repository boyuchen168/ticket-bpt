#!/usr/bin/env python3
"""
Admin UI for tennis booking configuration and scheduling.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List

import yaml
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "tennis_config.yaml"
BOT_PATH = BASE_DIR / "tennis_bot.py"
LOG_PATH = BASE_DIR / "tennis_bot.log"
CRON_MARKER = "# tennis-bot-admin"

COURT_TYPES = [
    {
        "value": "4",
        "label": "室外硬地",
        "ballcode": "1",
        "sport": "网球",
        "venue": "K场 (K1-K18)",
    },
    {
        "value": "5",
        "label": "室外草地",
        "ballcode": "1",
        "sport": "网球",
        "venue": "G场 (G3-G5)",
    },
    {
        "value": "6",
        "label": "室内硬地",
        "ballcode": "1",
        "sport": "网球",
        "venue": "A场+B场 (各6个)",
    },
    {
        "value": "7",
        "label": "室内红土",
        "ballcode": "1",
        "sport": "网球",
        "venue": "H场 (2个)",
    },
    {
        "value": "27",
        "label": "网球墙",
        "ballcode": "1",
        "sport": "网球",
        "venue": "网球墙 (4个)",
    },
    {
        "value": "9",
        "label": "羽毛球",
        "ballcode": "2",
        "sport": "羽毛球",
        "venue": "羽毛球馆 (Y1-Y5)",
    },
    {
        "value": "28",
        "label": "匹克球",
        "ballcode": "3",
        "sport": "匹克球",
        "venue": "匹克球 (P1-P3)",
    },
]

COURT_TYPE_COMMENT_BLOCK = """#Home Info / Court Types:
#ballcode=1 (网球), parktypecodes:
#3 → parktypecode=4, "室外硬地", 场馆: K场 (18个场地: K1-K18)
#4 → parktypecode=5, "室外草地", 场馆: G场 (5个场地: G3-G5)
#13 → parktypecode=6, "室内硬地", 场馆: A场+B场 (各6个)
#21 → parktypecode=7, "室内红土", 场馆: H场 (2个)
#27 → parktypecode=27, "网球墙", 场馆: 网球墙 (4个)
#ballcode=2 (羽毛球), parktypecode=9, 场馆: 羽毛球馆 (Y1-Y5)
#ballcode=3 (匹克球), parktypecode=28, 场馆: 匹克球 (P1-P3)
"""


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def save_config(cfg: Dict[str, Any]) -> None:
    with CONFIG_PATH.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh, allow_unicode=True, sort_keys=False)
        fh.write("\n")
        fh.write(COURT_TYPE_COMMENT_BLOCK)


def parse_time_str(open_time_str: str) -> time:
    text = (open_time_str or "").strip()
    if not text:
        return time(0, 0, 0)
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(text, fmt)
            return time(parsed.hour, parsed.minute, parsed.second)
        except ValueError:
            continue
    raise ValueError(f"Invalid time format: {open_time_str}")


def calculate_booking_open(
    target_date_str: str,
    booking_open_time_str: str,
    advance_days: int,
) -> Dict[str, str]:
    if not target_date_str:
        raise ValueError("target_date is required")

    target_play_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    booking_day = target_play_date - timedelta(days=advance_days)
    open_time = parse_time_str(booking_open_time_str)

    booking_open_datetime = datetime.combine(booking_day, open_time)
    display_suffix = f"{open_time.hour:02d}:{open_time.minute:02d}:{open_time.second:02d}"

    # Business rule: booking day 24:00 means next day 00:00.
    if open_time == time(0, 0, 0):
        booking_open_datetime += timedelta(days=1)
        display_suffix = "24:00:00"

    return {
        "play_date": target_play_date.strftime("%Y-%m-%d"),
        "booking_day": booking_day.strftime("%Y-%m-%d"),
        "booking_open_display": f"{booking_day.strftime('%Y-%m-%d')} {display_suffix}",
        "booking_open_datetime": booking_open_datetime.strftime("%Y-%m-%d %H:%M:%S"),
    }


def calculate_cron_timing(
    target_date_str: str,
    booking_open_time_str: str,
    advance_days: int,
    buffer_minutes: int,
) -> Dict[str, Any]:
    booking_info = calculate_booking_open(target_date_str, booking_open_time_str, advance_days)
    booking_open = datetime.strptime(booking_info["booking_open_datetime"], "%Y-%m-%d %H:%M:%S")
    cron_start = booking_open - timedelta(minutes=buffer_minutes)

    return {
        **booking_info,
        "cron_start_datetime": cron_start.strftime("%Y-%m-%d %H:%M:%S"),
        "cron_minute": cron_start.minute,
        "cron_hour": cron_start.hour,
        "cron_day": cron_start.day,
        "cron_month": cron_start.month,
    }


def run_crontab_list() -> str:
    result = subprocess.run(
        ["crontab", "-l"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return ""
    return result.stdout


def get_cron_entry() -> str:
    content = run_crontab_list()
    for line in content.splitlines():
        if CRON_MARKER in line and not line.strip().startswith("#"):
            return line.strip()
    return ""


def set_cron_entry(line: str | None) -> bool:
    existing = run_crontab_list()
    lines: List[str] = [item for item in existing.splitlines() if CRON_MARKER not in item]
    if line:
        lines.append(line)
    new_content = "\n".join(lines).rstrip("\n") + "\n"
    result = subprocess.run(["crontab", "-"], input=new_content, text=True, check=False)
    return result.returncode == 0


def safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return default


def resolve_ballcode(parktypecode: str) -> str:
    for item in COURT_TYPES:
        if item["value"] == str(parktypecode):
            return str(item["ballcode"])
    return ""


def update_cfg_section(cfg: Dict[str, Any], key: str, updates: Dict[str, Any]) -> None:
    section = cfg.setdefault(key, {})
    if not isinstance(section, dict):
        section = {}
        cfg[key] = section
    section.update(updates)


@app.get("/")
def index():
    return render_template("admin.html")


@app.get("/api/court_types")
def api_court_types():
    return jsonify(COURT_TYPES)


@app.get("/api/config")
def api_get_config():
    cfg = load_config()
    court = cfg.get("court", {}) if isinstance(cfg.get("court"), dict) else {}
    strategy = cfg.get("strategy", {}) if isinstance(cfg.get("strategy"), dict) else {}

    target_date = str(court.get("target_date", "")).strip()
    open_time = str(strategy.get("booking_open_time", "00:00:00")).strip() or "00:00:00"
    advance_days = safe_int(strategy.get("advance_days", 4), 4)

    booking_info = None
    if target_date:
        try:
            booking_info = calculate_booking_open(target_date, open_time, advance_days)
        except Exception:  # noqa: BLE001
            booking_info = None

    return jsonify({"config": cfg, "booking_info": booking_info})


@app.post("/api/config")
def api_save_config():
    data = request.get_json(silent=True) or {}
    cfg = load_config()

    incoming_court = data.get("court", {}) if isinstance(data.get("court"), dict) else {}
    incoming_strategy = data.get("strategy", {}) if isinstance(data.get("strategy"), dict) else {}
    incoming_notify = data.get("notify", {}) if isinstance(data.get("notify"), dict) else {}

    parktypecode = str(incoming_court.get("parktypecode", "")).strip()
    target_date = str(incoming_court.get("target_date", "")).strip()
    booking_open_time = str(incoming_strategy.get("booking_open_time", "00:00:00")).strip()
    advance_days = safe_int(incoming_strategy.get("advance_days", 4), 4)

    booking_info = calculate_booking_open(target_date, booking_open_time, advance_days)

    court_updates = {
        "parktypecode": parktypecode,
        "target_date": target_date,
        "target_time": str(incoming_court.get("target_time", "")).strip(),
        "target_time_end": str(incoming_court.get("target_time_end", "")).strip(),
        "duration_hours": safe_int(incoming_court.get("duration_hours", 1), 1),
        "preferred_courts": incoming_court.get("preferred_courts", []),
    }

    ballcode = resolve_ballcode(parktypecode)
    if ballcode:
        court_updates["ballcode"] = ballcode

    strategy_updates = {
        "booking_open_time": booking_open_time if len(booking_open_time) > 5 else f"{booking_open_time}:00",
        "booking_open_datetime": booking_info["booking_open_datetime"],
        "advance_days": advance_days,
        "advance_ms": safe_int(incoming_strategy.get("advance_ms", 10000), 10000),
        "prewarm_sec": safe_int(incoming_strategy.get("prewarm_sec", 30), 30),
        "max_retries": safe_int(incoming_strategy.get("max_retries", 30), 30),
        "retry_interval_ms": safe_int(incoming_strategy.get("retry_interval_ms", 200), 200),
        "threads": safe_int(incoming_strategy.get("threads", 3), 3),
        "skip_price_check": bool(incoming_strategy.get("skip_price_check", True)),
    }

    notify_updates = {
        "server_chan_key": str(incoming_notify.get("server_chan_key", "")).strip(),
        "bark_url": str(incoming_notify.get("bark_url", "")).strip(),
    }

    update_cfg_section(cfg, "court", court_updates)
    update_cfg_section(cfg, "strategy", strategy_updates)
    update_cfg_section(cfg, "notify", notify_updates)
    save_config(cfg)
    return jsonify({"ok": True, "booking_info": booking_info})


@app.get("/api/cron")
def api_get_cron():
    entry = get_cron_entry()
    return jsonify({"active": bool(entry), "entry": entry})


@app.post("/api/cron/preview")
def api_cron_preview():
    data = request.get_json(silent=True) or {}
    try:
        target_date = str(data.get("target_date", "")).strip()
        open_time = str(data.get("booking_open_time", "00:00:00")).strip()
        advance_days = safe_int(data.get("advance_days", 4), 4)
        buffer_minutes = safe_int(data.get("buffer_minutes", 10), 10)
        result = calculate_cron_timing(target_date, open_time, advance_days, buffer_minutes)
        return jsonify({"ok": True, "timing": result})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/cron/set")
def api_cron_set():
    data = request.get_json(silent=True) or {}
    cfg = load_config()
    court_cfg = cfg.get("court", {}) if isinstance(cfg.get("court"), dict) else {}
    strategy_cfg = cfg.get("strategy", {}) if isinstance(cfg.get("strategy"), dict) else {}

    target_date = str(court_cfg.get("target_date", "")).strip()
    open_time = str(strategy_cfg.get("booking_open_time", "00:00:00")).strip() or "00:00:00"
    advance_days = safe_int(strategy_cfg.get("advance_days", data.get("advance_days", 4)), 4)
    buffer_minutes = safe_int(data.get("buffer_minutes", 10), 10)

    timing = calculate_cron_timing(target_date, open_time, advance_days, buffer_minutes)

    # Keep strategy.booking_open_datetime in sync with the cron schedule.
    update_cfg_section(
        cfg,
        "strategy",
        {
            "advance_days": advance_days,
            "booking_open_datetime": timing["booking_open_datetime"],
            "booking_open_time": open_time if len(open_time) > 5 else f"{open_time}:00",
        },
    )
    save_config(cfg)

    python_path = subprocess.run(
        ["which", "python3"], capture_output=True, text=True, check=False
    ).stdout.strip() or "python3"

    cron_line = (
        f"{timing['cron_minute']} {timing['cron_hour']} {timing['cron_day']} "
        f"{timing['cron_month']} * {python_path} {BOT_PATH} -c {CONFIG_PATH} book "
        f">> {LOG_PATH} 2>&1 {CRON_MARKER}"
    )

    ok = set_cron_entry(cron_line)
    return jsonify({"ok": ok, "cron_line": cron_line, "timing": timing})


@app.delete("/api/cron/remove")
def api_cron_remove():
    return jsonify({"ok": set_cron_entry(None)})


@app.post("/api/run")
def api_run_now():
    log_file = LOG_PATH.open("a", encoding="utf-8")
    proc = subprocess.Popen(  # noqa: S603
        ["python3", str(BOT_PATH), "-c", str(CONFIG_PATH), "book"],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=str(BASE_DIR),
    )
    return jsonify({"ok": True, "pid": proc.pid})


@app.get("/api/logs")
def api_logs():
    if not LOG_PATH.exists():
        return jsonify({"lines": []})
    lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
    return jsonify({"lines": lines[-150:]})


def main() -> None:
    app.run(host="0.0.0.0", port=5001, debug=False)


if __name__ == "__main__":
    main()
