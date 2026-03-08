#!/usr/bin/env python3
"""Full court availability: show all parks and all time slots for each type."""
import json
from config_store import load_config
from tennis_bot import TennisClient

cfg = load_config()

client = TennisClient(cfg)

# Derive correct type mapping from home info
home_rsp = client.get_home_info()
home_payload = client.unwrap_payload(home_rsp)

# Build type_map: id -> (ballcode, parktype_name)
type_map = {}
if isinstance(home_payload, dict):
    for ball_group in home_payload.get("parkFirstType", []):
        ballcode = ball_group.get("ballcode", "1")
        balltype = ball_group.get("balltype", "")
        for item in ball_group.get("parktype", []):
            type_map[item["id"]] = {
                "ballcode": ballcode,
                "balltype": balltype,
                "name": item.get("parktype", ""),
                "parktypecode_field": item.get("parktypecode"),
            }

TARGET_DATE = "2026-03-09"

STATUS_LABEL = {0: "✅可约", 1: "❌已订", 2: "🔒关闭", 3: "🔒不可", 4: "⏳待定"}

print(f"\n{'='*72}")
print(f"  国家网球中心 场地可用情况  — {TARGET_DATE}")
print(f"{'='*72}")

for type_id, meta in type_map.items():
    ballcode = meta["ballcode"]
    type_name = meta["name"]
    balltype = meta["balltype"]
    pc_field = meta["parktypecode_field"]
    print(f"\n{'─'*72}")
    print(f"  【{balltype}】{type_name}  (parktypeinfo={type_id}, ballcode={ballcode}, parktypecode字段={pc_field})")
    print(f"{'─'*72}")

    rsp = client.query_courts(
        date=TARGET_DATE,
        parktypeinfo=str(type_id),
        ballcode=str(ballcode),
        cardtypecode="-1",
        userid=client.auth.userid,
        parkstatus="0",
        changefieldtype="0",
    )
    if not client.is_success(rsp):
        print(f"  FAILED: {rsp.get('respMsg')}")
        continue

    payload = client.unwrap_payload(rsp)
    if not isinstance(payload, dict):
        print("  No data")
        continue

    tl = payload.get("timeLimit", {})
    rt = payload.get("reservetime", [])
    avail_hours = sorted(r["time"] for r in rt) if rt else list(range(7, 23))
    if tl:
        print(f"  今日限额: 已用{tl.get('timesToday',0)}h / 上限{tl.get('timesTodayMax',0)}h / 剩余{tl.get('timeSurplus',0)}h")
    print(f"  营业时段: {avail_hours[0]}:00 — {avail_hours[-1]+1}:00  (共{len(avail_hours)}个时段)")

    ven_list = payload.get("venList", [])
    if not ven_list:
        print("  (无场馆数据)")
        continue

    for venue in ven_list:
        venue_name = venue.get("vname", "?")
        parks = venue.get("park", []) or []
        print(f"\n  【场馆: {venue_name}】  共 {len(parks)} 个场地")

        # Header row
        header_hours = avail_hours
        col = 6
        hour_header = "".join(f"{h:>{col}}" for h in header_hours)
        print(f"  {'场地':<12}{hour_header}")
        print(f"  {'─'*12}{'─'*col*len(header_hours)}")

        avail_summary: dict = {}
        for park in parks:
            park_name = park.get("parkname", "?")
            reserves = {r["time"]: r["bookstatus"] for r in (park.get("reserve") or [])}
            row = ""
            avail_slots = []
            for h in header_hours:
                status = reserves.get(h, -1)
                if status == 0:
                    cell = "  ✅"
                    avail_slots.append(h)
                elif status == 1:
                    cell = "  ──"
                elif status == 3:
                    cell = "  🔒"
                elif status == -1:
                    cell = "  ?? "
                else:
                    cell = f"  {status:2}"
                row += f"{cell:>{col}}"
            print(f"  {park_name:<12}{row}")
            if avail_slots:
                avail_summary[park_name] = avail_slots

        if avail_summary:
            print(f"\n  可预约汇总:")
            for pname, hours in avail_summary.items():
                hours_str = "  ".join(f"{h}:00" for h in hours)
                print(f"    {pname}: {hours_str}")
        else:
            print(f"\n  该类型当日已无可预约时段")

print(f"\n{'='*72}")
print("  配置建议:")
print("  MongoDB config 中 parktypecode 字段含义说明:")
print(f"{'─'*72}")
for type_id, meta in type_map.items():
    pc = meta["parktypecode_field"]
    print(f"  {meta['balltype']} / {meta['name']:10s}  →  parktypeinfo(id)={type_id}   parktypecode字段值={pc}")
print(f"\n  ⚠️  当前配置 parktypecode=6 对应的是室内硬地的 parktypecode字段(6),")
print(f"     但接口实际需要传 id=13。建议将配置改为 parktypecode: '13'")
print(f"{'='*72}\n")
