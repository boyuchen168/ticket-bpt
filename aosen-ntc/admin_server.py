#!/usr/bin/env python3
"""
Admin UI for tennis booking configuration and scheduling.
"""

from __future__ import annotations

import re
import subprocess
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, jsonify, render_template, request
from pymongo.errors import PyMongoError

from config_store import get_mongo_db, load_config, save_config
from tennis_bot import TennisClient

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent
BOT_PATH = BASE_DIR / "tennis_bot.py"
LOG_PATH = BASE_DIR / "tennis_bot.log"
CRON_MARKER = "# tennis-bot-admin"

COURT_TYPES = [
    {
        "value": "3",
        "label": "室外硬地",
        "ballcode": "1",
        "sport": "网球",
        "venue": "K场 (K1-K18)",
    },
    {
        "value": "4",
        "label": "室外草地",
        "ballcode": "1",
        "sport": "网球",
        "venue": "G场 (G3-G5)",
    },
    {
        "value": "13",
        "label": "室内硬地",
        "ballcode": "1",
        "sport": "网球",
        "venue": "A场+B场 (各6个)",
    },
    {
        "value": "21",
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

def upsert_user_cookie(payload: Dict[str, Any]) -> None:
    db = get_mongo_db()
    if db is None:
        return
    mobile = str(payload.get("mobile", "")).strip()
    if not mobile:
        return
    doc = {
        "mobile": mobile,
        "userid": str(payload.get("userid", "")).strip(),
        "bnglokbj": str(payload.get("bnglokbj", "")).strip(),
        "csc": str(payload.get("csc", "")).strip(),
        "cdc": str(payload.get("cdc", "")).strip(),
        "openId": str(payload.get("openId", "")).strip(),
        "maopenId": str(payload.get("maopenId", "")).strip(),
        "unionId": str(payload.get("unionId", "")).strip(),
        "updated_at": datetime.utcnow(),
    }
    try:
        db["user_cookies"].update_one({"mobile": mobile}, {"$set": doc}, upsert=True)
    except PyMongoError as exc:
        app.logger.warning("Save user cookie to MongoDB failed: %s", exc)


def extract_auth_payload(auth: Dict[str, Any]) -> Dict[str, str]:
    return {
        "mobile": str(auth.get("mobile", "")).strip(),
        "userid": str(auth.get("userid", "")).strip(),
        "bnglokbj": str(auth.get("bnglokbj", "")).strip(),
        "csc": str(auth.get("csc", "")).strip(),
        "cdc": str(auth.get("cdc", "")).strip(),
        "openId": str(auth.get("openId", "")).strip(),
        "maopenId": str(auth.get("maopenId", "")).strip(),
        "unionId": str(auth.get("unionId", "")).strip(),
    }


def list_user_cookies() -> List[Dict[str, Any]]:
    db = get_mongo_db()
    if db is None:
        return []
    try:
        docs = db["user_cookies"].find({}, {"_id": 0, "mobile": 1, "userid": 1, "updated_at": 1})
    except PyMongoError as exc:
        app.logger.warning("List user cookies from MongoDB failed: %s", exc)
        return []

    users: List[Dict[str, Any]] = []
    for item in docs:
        if not isinstance(item, dict):
            continue
        mobile = str(item.get("mobile", "")).strip()
        if not mobile:
            continue
        user = {
            "mobile": mobile,
            "userid": str(item.get("userid", "")).strip(),
            "updated_at": "",
        }
        updated_at = item.get("updated_at")
        if isinstance(updated_at, datetime):
            user["updated_at"] = updated_at.isoformat()
        elif updated_at:
            user["updated_at"] = str(updated_at)
        users.append(user)
    return users


def get_user_cookie_by_mobile(mobile: str) -> Dict[str, str] | None:
    db = get_mongo_db()
    if db is None:
        return None
    try:
        doc = db["user_cookies"].find_one({"mobile": mobile}, {"_id": 0})
    except PyMongoError as exc:
        app.logger.warning("Query user cookie from MongoDB failed: %s", exc)
        return None
    if not isinstance(doc, dict):
        return None
    auth = extract_auth_payload(doc)
    if not auth["mobile"]:
        return None
    return auth


def build_login_client() -> TennisClient:
    cfg = load_config()
    platform = cfg.get("platform", {}) if isinstance(cfg.get("platform"), dict) else {}
    base_url = str(platform.get("base_url", TennisClient.BASE_URL)).strip() or TennisClient.BASE_URL
    auth = cfg.get("auth", {}) if isinstance(cfg.get("auth"), dict) else {}
    return TennisClient({"platform": {"base_url": base_url}, "auth": auth})


def validate_mobile(mobile: str) -> bool:
    return bool(re.fullmatch(r"1\d{10}", mobile))


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
    try:
        result = subprocess.run(
            ["crontab", "-l"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return ""
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
    # If nothing remains, remove the crontab entirely instead of writing a blank file.
    if not any(l.strip() for l in lines):
        try:
            result = subprocess.run(["crontab", "-r"], capture_output=True, text=True, check=False)
        except FileNotFoundError:
            return False
        # exit code 1 is acceptable when there was no crontab to remove.
        return result.returncode in (0, 1)
    new_content = "\n".join(lines).rstrip("\n") + "\n"
    try:
        result = subprocess.run(["crontab", "-"], input=new_content, text=True, check=False)
    except FileNotFoundError:
        return False
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


@app.get("/login")
def login_page():
    return render_template("login.html")


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


@app.get("/api/users")
def api_users():
    cfg = load_config()
    auth = cfg.get("auth", {}) if isinstance(cfg.get("auth"), dict) else {}
    active_mobile = str(auth.get("mobile", "")).strip()

    users = list_user_cookies()
    # Fallback when MongoDB is unavailable: at least expose current config user.
    if active_mobile and not any(item.get("mobile") == active_mobile for item in users):
        users.insert(
            0,
            {
                "mobile": active_mobile,
                "userid": str(auth.get("userid", "")).strip(),
                "updated_at": "",
            },
        )
    return jsonify({"users": users, "active_mobile": active_mobile})


@app.post("/api/users/switch")
def api_switch_user():
    data = request.get_json(silent=True) or {}
    mobile = str(data.get("mobile", "")).strip()
    if not mobile:
        return jsonify({"ok": False, "error": "mobile is required"}), 400

    auth_payload = get_user_cookie_by_mobile(mobile)
    if auth_payload is None:
        return jsonify({"ok": False, "error": "未找到该用户凭证，请先登录该手机号"}), 404

    missing = [k for k in ("userid", "bnglokbj", "csc", "cdc") if not auth_payload[k]]
    if missing:
        return jsonify({"ok": False, "error": f"用户凭证缺少关键字段: {', '.join(missing)}"}), 400

    cfg = load_config()
    update_cfg_section(cfg, "auth", auth_payload)
    save_config(cfg)

    return jsonify(
        {
            "ok": True,
            "message": f"已切换到用户: {mobile}",
            "auth": {"mobile": auth_payload["mobile"], "userid": auth_payload["userid"]},
        }
    )


@app.post("/api/auth/send-code")
def api_send_code():
    data = request.get_json(silent=True) or {}
    mobile = str(data.get("mobile", "")).strip()
    if not validate_mobile(mobile):
        return jsonify({"ok": False, "error": "手机号格式不正确"}), 400
    try:
        client = build_login_client()
        rsp = client.get_phone_code(mobile)
        if client.is_success(rsp):
            return jsonify({"ok": True, "message": str(rsp.get("respMsg", "验证码已发送"))})
        return jsonify(
            {
                "ok": False,
                "error": str(rsp.get("respMsg", "获取验证码失败")),
                "raw": rsp,
            }
        ), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": f"发送验证码失败: {exc}"}), 500


@app.post("/api/auth/verify")
def api_verify_code():
    data = request.get_json(silent=True) or {}
    mobile = str(data.get("mobile", "")).strip()
    code = str(data.get("code", "")).strip()

    if not validate_mobile(mobile):
        return jsonify({"ok": False, "error": "手机号格式不正确"}), 400
    if not code:
        return jsonify({"ok": False, "error": "请输入验证码"}), 400

    try:
        client = build_login_client()
        rsp = client.phone_code_login(mobile, code)
        if not client.is_success(rsp):
            return jsonify(
                {
                    "ok": False,
                    "error": str(rsp.get("respMsg", "登录失败")),
                    "raw": rsp,
                }
            ), 400

        auth_update = rsp.get("auth_update", {}) if isinstance(rsp.get("auth_update"), dict) else {}
        auth_payload = {
            "mobile": mobile,
            "userid": str(auth_update.get("userid", "")).strip(),
            "bnglokbj": str(auth_update.get("bnglokbj", "")).strip(),
            "csc": str(auth_update.get("csc", "")).strip(),
            "cdc": str(auth_update.get("cdc", "")).strip(),
            "openId": str(auth_update.get("openId", "")).strip(),
            "maopenId": str(auth_update.get("maopenId", "")).strip(),
            "unionId": str(auth_update.get("unionId", "")).strip(),
        }
        missing = [k for k in ("userid", "bnglokbj", "csc", "cdc") if not auth_payload[k]]
        if missing:
            return jsonify(
                {
                    "ok": False,
                    "error": f"登录响应缺少关键字段: {', '.join(missing)}",
                    "raw": rsp,
                }
            ), 400

        cfg = load_config()
        update_cfg_section(cfg, "auth", auth_payload)
        save_config(cfg)
        upsert_user_cookie(auth_payload)

        return jsonify(
            {
                "ok": True,
                "message": "登录成功，凭证已保存",
                "auth": auth_payload,
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": f"验证码登录失败: {exc}"}), 500


@app.post("/api/config")
def api_save_config():
    try:
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
            "burst_count": safe_int(incoming_strategy.get("burst_count", 15), 15),
            "burst_timeout_sec": safe_int(incoming_strategy.get("burst_timeout_sec", 5), 5),
            "direct_fire": bool(incoming_strategy.get("direct_fire", True)),
            "direct_fire_threads": safe_int(incoming_strategy.get("direct_fire_threads", 2), 2),
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
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": f"save config failed: {exc}"}), 500


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
    try:
        cfg = load_config()
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": f"load config failed: {exc}"}), 500

    court_cfg = cfg.get("court", {}) if isinstance(cfg.get("court"), dict) else {}
    strategy_cfg = cfg.get("strategy", {}) if isinstance(cfg.get("strategy"), dict) else {}

    target_date = str(court_cfg.get("target_date", "")).strip()
    open_time = str(strategy_cfg.get("booking_open_time", "00:00:00")).strip() or "00:00:00"
    advance_days = safe_int(strategy_cfg.get("advance_days", data.get("advance_days", 4)), 4)
    buffer_minutes = safe_int(data.get("buffer_minutes", 10), 10)

    try:
        timing = calculate_cron_timing(target_date, open_time, advance_days, buffer_minutes)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

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
        f"{timing['cron_month']} * {python_path} {BOT_PATH} book "
        f">> {LOG_PATH} 2>&1 {CRON_MARKER}"
    )

    ok = set_cron_entry(cron_line)
    return jsonify({"ok": ok, "cron_line": cron_line, "timing": timing})


@app.delete("/api/cron/remove")
def api_cron_remove():
    return jsonify({"ok": set_cron_entry(None)})


@app.post("/api/run")
def api_run_now():
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    start_line = f"\n===== manual run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====\n"
    with LOG_PATH.open("a", encoding="utf-8") as marker_file:
        marker_file.write(start_line)
        marker_file.flush()

    with LOG_PATH.open("a", encoding="utf-8") as log_file:
        proc = subprocess.Popen(  # noqa: S603
            ["python3", "-u", str(BOT_PATH), "book"],
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
