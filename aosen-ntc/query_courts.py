#!/usr/bin/env python3
"""
Quick court availability inspector.
Queries home info, booking dates, code-open time, and all available slots.
"""
import json
import sys
import yaml
from tennis_bot import TennisClient, TennisBooker

CONFIG = "tennis_config.yaml"

with open(CONFIG, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

client = TennisClient(cfg)
booker = TennisBooker(CONFIG)

DIVIDER = "=" * 70

# ── 1. Home info: court types ────────────────────────────────────────────────
print(f"\n{DIVIDER}")
print("【1】首页信息 / 场地大类 (getHomeInfo)")
print(DIVIDER)
home_rsp = client.get_home_info()
if client.is_success(home_rsp):
    payload = client.unwrap_payload(home_rsp)
    if isinstance(payload, dict):
        first_types = payload.get("parkFirstType", [])
        for ball_group in first_types:
            ball_id = ball_group.get("id")
            ball_name = ball_group.get("name", "")
            print(f"  大类 ballcode={ball_id}  {ball_name}")
            for item in ball_group.get("parktype", []):
                print(f"    → parktypecode={item.get('id')}  {item.get('name','')}  图标={item.get('img','')}")
        # booking open time hint
        notice = payload.get("notice") or payload.get("orderNotice") or payload.get("bookNotice")
        if notice:
            print(f"\n  公告/放号提示: {notice}")
    else:
        print("  payload:", json.dumps(payload, ensure_ascii=False, indent=2))
else:
    print("  FAILED:", home_rsp.get("respMsg"), home_rsp)

# ── 2. Available booking dates ───────────────────────────────────────────────
print(f"\n{DIVIDER}")
print("【2】可预约日期 (queryBookDate)")
print(DIVIDER)
dates_rsp = client.query_dates(user_id=client.auth.userid)
dates_list = []
if client.is_success(dates_rsp):
    payload = client.unwrap_payload(dates_rsp)
    if isinstance(payload, list):
        dates_list = payload
        for d in payload:
            print(f"  date={d.get('date')}  status={d.get('status')}  bookType={d.get('bookType','')}  "
                  f"bookOpenTime={d.get('bookOpenTime','')}  desc={d.get('desc','')}")
    else:
        print("  payload:", json.dumps(payload, ensure_ascii=False, indent=2))
else:
    print("  FAILED:", dates_rsp.get("respMsg"))

# ── 3. Booking open time (queryIsCodeTime) ───────────────────────────────────
print(f"\n{DIVIDER}")
print("【3】放号时间 (queryIsCodeTime)")
print(DIVIDER)
code_time_rsp = client.query_is_code_time()
if client.is_success(code_time_rsp):
    payload = client.unwrap_payload(code_time_rsp)
    print("  ", json.dumps(payload, ensure_ascii=False, indent=2))
else:
    print("  FAILED:", code_time_rsp.get("respMsg"), code_time_rsp)

# ── 4. Query courts for each available date ──────────────────────────────────
print(f"\n{DIVIDER}")
print("【4】场地可用情况 (getParkShowByParam)")
print(DIVIDER)

target_dates = [d.get("date") for d in dates_list if d.get("date")] if dates_list else []
# Always include configured target_date
configured_date = cfg.get("court", {}).get("target_date", "")
if configured_date and configured_date not in target_dates:
    target_dates.insert(0, configured_date)
if not target_dates:
    print("  No dates to query.")
    sys.exit(0)

parktypecode = str(cfg.get("court", {}).get("parktypecode", "6"))
ballcode = booker.resolve_ballcode()
cardtypecode = str(cfg.get("court", {}).get("cardtypecode", "-1"))

print(f"  ballcode={ballcode}  parktypecode={parktypecode}  cardtypecode={cardtypecode}\n")

for date in target_dates:
    print(f"  ── 日期: {date} ──")
    rsp = client.query_courts(
        date=date,
        parktypeinfo=parktypecode,
        ballcode=ballcode,
        cardtypecode=cardtypecode,
        userid=client.auth.userid,
        parkstatus="0",
        changefieldtype="0",
    )
    if not client.is_success(rsp):
        print(f"    query_courts FAILED: {rsp.get('respMsg')}")
        # Show raw response for diagnosis
        print(f"    raw: {json.dumps(rsp, ensure_ascii=False)[:300]}")
        continue

    payload = client.unwrap_payload(rsp)
    if not isinstance(payload, dict):
        print(f"    payload type unexpected: {type(payload)} — {str(payload)[:200]}")
        continue

    ven_list = payload.get("venList", [])
    if not ven_list:
        print("    (无场馆数据)")
        # Print top-level keys for clues
        print("    payload keys:", list(payload.keys()))
        continue

    all_slots = booker._collect_available_slots(payload, target_date=date)

    for venue in ven_list:
        venue_name = venue.get("vname", "未知场馆")
        parks = venue.get("park", []) or []
        print(f"\n    【场馆】{venue_name}")
        for park in parks:
            park_name = park.get("parkname", "")
            park_id = park.get("id", "")
            reserves = park.get("reserve", []) or []
            avail = [r for r in reserves if str(r.get("bookstatus", "-1")) == "0"]
            booked = [r for r in reserves if str(r.get("bookstatus", "-1")) != "0"]
            avail_hours = sorted(int(r.get("time", 0)) for r in avail)
            booked_hours = sorted(int(r.get("time", 0)) for r in booked)
            avail_str = ", ".join(f"{h}:00" for h in avail_hours) if avail_hours else "无"
            booked_str = ", ".join(f"{h}:00" for h in booked_hours) if booked_hours else "无"
            print(f"      场地 {park_name}(id={park_id})")
            print(f"        ✅ 可预约: {avail_str}")
            print(f"        ❌ 已预约: {booked_str}")

    # Summary
    if all_slots:
        print(f"\n    📊 {date} 全部可用时段汇总 ({len(all_slots)} 个):")
        by_venue: dict = {}
        for s in all_slots:
            key = f"{s['venuename']} / {s['parkname']}"
            by_venue.setdefault(key, []).append(s["time"])
        for k, hours in sorted(by_venue.items()):
            hours_str = ", ".join(f"{h}:00" for h in sorted(hours))
            print(f"      {k}: {hours_str}")
    else:
        print(f"\n    📊 {date} 暂无可预约时段")
    print()
