#!/usr/bin/env python3
"""Raw diagnostic: inspect all API responses in detail."""
import json
from config_store import load_config
from tennis_bot import TennisClient, TennisBooker

cfg = load_config()

client = TennisClient(cfg)
booker = TennisBooker()

DIVIDER = "─" * 70

def pp(obj, label=""):
    if label:
        print(f"\n{'='*60}")
        print(f"  {label}")
        print('='*60)
    print(json.dumps(obj, ensure_ascii=False, indent=2))

# ── Home info raw ──
home_rsp = client.get_home_info()
pp(home_rsp, "getHomeInfo — raw response")

# ── Dates raw ──
dates_rsp = client.query_dates(user_id=client.auth.userid)
pp(dates_rsp, "queryBookDate — raw")

# ── Code time raw ──
ct_rsp = client.query_is_code_time()
pp(ct_rsp, "queryIsCodeTime — raw")

# ── Try every parktypecode from home info ──
# Extract parktypecodes
payload = client.unwrap_payload(home_rsp)
type_ids = []
if isinstance(payload, dict):
    for ball_group in payload.get("parkFirstType", []):
        for item in ball_group.get("parktype", []):
            type_ids.append((ball_group.get("id"), item.get("id"), item.get("name","")))

print(f"\n{'='*60}")
print("  Discovered type_ids:", type_ids)

# Try each type for target date
target_date = cfg.get("court", {}).get("target_date", "2026-03-09")
print(f"  Target date: {target_date}")
print('='*60)

for ballcode, parktypecode, name in type_ids:
    print(f"\n{DIVIDER}")
    print(f"  ballcode={ballcode}  parktypecode={parktypecode}  name='{name}'")
    print(DIVIDER)
    rsp = client.query_courts(
        date=target_date,
        parktypeinfo=str(parktypecode),
        ballcode=str(ballcode) if ballcode else "1",
        cardtypecode="-1",
        userid=client.auth.userid,
        parkstatus="0",
        changefieldtype="0",
    )
    data = client.unwrap_payload(rsp)
    if not client.is_success(rsp):
        print(f"  FAILED respMsg={rsp.get('respMsg')}  respCode={rsp.get('respCode')}")
        continue
    if isinstance(data, dict):
        keys = list(data.keys())
        print(f"  payload keys: {keys}")
        ven = data.get("venList", [])
        tl  = data.get("timeLimit")
        rt  = data.get("reservetime")
        print(f"  venList count: {len(ven) if ven else 0}")
        print(f"  timeLimit: {tl}   reservetime: {rt}")
        if ven:
            for v in ven[:2]:
                parks = v.get("park", [])
                print(f"    venue={v.get('vname')}  parks={len(parks)}")
                for p in parks[:3]:
                    reserves = p.get("reserve", []) or []
                    statuses = [(r.get("time"), r.get("bookstatus")) for r in reserves]
                    print(f"      park={p.get('parkname')} id={p.get('id')} reserves={statuses[:8]}")
    else:
        print(f"  data type: {type(data)}")
        print(json.dumps(data, ensure_ascii=False)[:400])
