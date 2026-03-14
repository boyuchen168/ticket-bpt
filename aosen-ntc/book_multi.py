#!/usr/bin/env python3
"""
Multi-account direct-fire orchestrator.

Strategy: Pre-build all order payloads BEFORE the booking window opens,
then at T-0 fire addParkOrder from N accounts simultaneously.
Each account targets different courts to maximize total bookings.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence, Tuple

from config_store import load_all_user_credentials, load_config
from tennis_bot import TennisBooker, TennisClient, log


def partition_park_lists(
    all_candidates: List[Tuple[int, int, str, List[Dict[str, Any]]]],
    num_accounts: int,
    skip_courts: Optional[List[str]] = None,
) -> List[List[List[Dict[str, Any]]]]:
    """Distribute pre-built park_lists across N accounts via round-robin.

    Args:
        all_candidates: [(priority, start_hour, parkname, park_list), ...]
            already sorted by priority from pre_build_all_park_lists.
        num_accounts: number of accounts to distribute across.
        skip_courts: court names to always skip (e.g., ["K17"]).

    Returns:
        List of length num_accounts, each element is a list of park_lists
        for that account to fire.
    """
    skip = {s.strip().lower() for s in (skip_courts or [])}
    filtered = [
        (pri, hour, name, pl)
        for pri, hour, name, pl in all_candidates
        if name.strip().lower() not in skip
    ]

    if not filtered:
        log.warning("No candidates left after filtering skip_courts=%s", skip_courts)

    partitions: List[List[List[Dict[str, Any]]]] = [[] for _ in range(num_accounts)]
    for idx, (_pri, _hour, _name, park_list) in enumerate(filtered):
        partitions[idx % num_accounts].append(park_list)

    for i, p in enumerate(partitions):
        log.info("Account %d assigned %d park_lists", i, len(p))

    return partitions


def _threads_per_account(num_accounts: int) -> int:
    """Determine thread count per account based on total accounts."""
    if num_accounts <= 1:
        return 3
    if num_accounts <= 2:
        return 2
    if num_accounts <= 4:
        return 2
    return 1


def account_worker(
    account_auth: Dict[str, str],
    pre_park_lists: List[List[Dict[str, Any]]],
    base_config: Dict[str, Any],
    time_offset_ms: int,
    result_queue: mp.Queue,
    booked_flag: Any,  # mp.Value('i')
    target_total: int,
    worker_id: int,
) -> None:
    """Worker process for one account. Pure direct-fire at T-0."""
    mobile = account_auth.get("mobile", "unknown")
    log.info("[W%d/%s] Worker starting with %d park_lists", worker_id, mobile, len(pre_park_lists))

    if not pre_park_lists:
        log.warning("[W%d/%s] No park_lists assigned, exiting.", worker_id, mobile)
        result_queue.put({"worker": worker_id, "mobile": mobile, "success": False, "reason": "no park_lists"})
        return

    # Build config with this account's auth
    cfg = dict(base_config)
    cfg["auth"] = dict(account_auth)

    # Build client with shared NTP offset (skip re-syncing)
    client = TennisClient(cfg, time_offset_ms=time_offset_ms)
    client._require_sign_creds()

    strategy_cfg = cfg.get("strategy", {})
    court_cfg = cfg.get("court", {})
    max_retries = int(strategy_cfg.get("max_retries", 30))
    burst_timeout = int(strategy_cfg.get("burst_timeout_sec", 5))
    paywaycode = court_cfg.get("paywaycode", 2)
    ordercode = str(account_auth.get("ordercode", "")).strip()
    captcha = str(account_auth.get("captchaVerification", "")).strip()

    # Pre-warm connections
    client.pre_warm_connections(count=3)

    # Wait for open time using shared NTP offset
    booker = TennisBooker(config=cfg)
    booker.client = client
    booker.wait_for_open_time()

    log.info("[W%d/%s] GO! Firing %d park_lists", worker_id, mobile, len(pre_park_lists))

    # Determine thread count
    success_event = threading.Event()
    success_lock = threading.Lock()
    num_threads = _threads_per_account(1)  # within this process, use 2 threads

    def fire_thread(thread_id: int) -> Optional[Dict[str, Any]]:
        for attempt in range(1, max_retries + 1):
            if success_event.is_set():
                return None
            # Check global booking count
            if booked_flag.value >= target_total:
                return None

            # Cycle through park_lists, offset by thread_id
            pl_idx = (attempt - 1 + thread_id) % len(pre_park_lists)
            park_list = pre_park_lists[pl_idx]

            t0 = time.time()
            order_rsp = client.add_park_order_raw(
                park_list,
                paywaycode=paywaycode,
                mobile=mobile,
                ordercode=ordercode,
                captcha_verification=captcha,
                add_order_type="wx",
                userid=client.auth.userid,
                timeout=burst_timeout,
            )
            elapsed_ms = (time.time() - t0) * 1000

            if not client.is_success(order_rsp):
                log.info(
                    "[W%d/%s-T%d] Attempt %d failed (%.0fms): %s",
                    worker_id, mobile, thread_id, attempt, elapsed_ms,
                    order_rsp.get("respMsg", ""),
                )
                continue

            order_payload = client.unwrap_payload(order_rsp)
            order_no = ""
            if isinstance(order_payload, dict):
                for key in ("orderNo", "orderno", "order_no"):
                    if order_payload.get(key):
                        order_no = str(order_payload[key])
                        break

            if not order_no:
                log.warning("[W%d/%s-T%d] Order succeeded but no orderNo (%.0fms)", worker_id, mobile, thread_id, elapsed_ms)
                continue

            log.info("[W%d/%s-T%d] SUCCESS! orderNo=%s (%.0fms)", worker_id, mobile, thread_id, order_no, elapsed_ms)

            with success_lock:
                if not success_event.is_set():
                    success_event.set()
                    with booked_flag.get_lock():
                        booked_flag.value += 1
                    return {
                        "worker": worker_id,
                        "mobile": mobile,
                        "orderNo": order_no,
                        "parkList": park_list,
                        "success": True,
                    }
            return None
        return None

    num_threads = min(num_threads, len(pre_park_lists))
    with ThreadPoolExecutor(max_workers=max(1, num_threads)) as pool:
        futures = [pool.submit(fire_thread, t) for t in range(num_threads)]
        for future in as_completed(futures):
            try:
                res = future.result()
                if res:
                    result_queue.put(res)
                    return
            except Exception as exc:  # noqa: BLE001
                log.error("[W%d/%s] Thread error: %s", worker_id, mobile, exc)

    result_queue.put({"worker": worker_id, "mobile": mobile, "success": False, "reason": "exhausted retries"})


class MultiFireOrchestrator:
    """Pre-build payloads for all accounts, fire simultaneously at T-0."""

    def __init__(self):
        self.cfg = load_config()
        self.court_cfg = self.cfg.get("court", {})
        self.strategy_cfg = self.cfg.get("strategy", {})
        self.notify_cfg = self.cfg.get("notify", {})
        self.multi_cfg = self.cfg.get("multi_account", {})

        self.target_total = int(self.multi_cfg.get("target_total_courts", 1))
        self.skip_courts: List[str] = self.multi_cfg.get("skip_courts", [])

    def _load_accounts(self) -> List[Dict[str, str]]:
        """Load all user accounts from MongoDB."""
        creds = load_all_user_credentials()
        valid = []
        for cred in creds:
            mobile = str(cred.get("mobile", "")).strip()
            userid = str(cred.get("userid", "")).strip()
            csc = str(cred.get("csc", "")).strip()
            cdc = str(cred.get("cdc", "")).strip()
            bnglokbj = str(cred.get("bnglokbj", "")).strip()
            if all([mobile, userid, csc, cdc, bnglokbj]):
                valid.append({
                    "mobile": mobile,
                    "userid": userid,
                    "csc": csc,
                    "cdc": cdc,
                    "bnglokbj": bnglokbj,
                    "openId": str(cred.get("openId", "")).strip(),
                    "maopenId": str(cred.get("maopenId", "")).strip(),
                    "unionId": str(cred.get("unionId", "")).strip(),
                    "ordercode": str(cred.get("ordercode", "")).strip(),
                    "captchaVerification": str(cred.get("captchaVerification", "")).strip(),
                })
            else:
                log.warning("Skipping account %s: missing credentials", mobile)
        return valid

    def _resolve_target_date(self, pilot: TennisBooker) -> str:
        target_date = str(self.court_cfg.get("target_date", "")).strip()
        if target_date:
            return target_date
        dates_rsp = pilot.client.query_dates()
        payload = pilot.client.unwrap_payload(dates_rsp)
        if isinstance(payload, list) and payload:
            target_date = str(payload[0].get("date", ""))
            log.info("target_date not set, fallback to first available: %s", target_date)
            return target_date
        raise RuntimeError("No target_date configured and queryBookDate returned no data.")

    def _notify_results(self, results: List[Dict[str, Any]]) -> None:
        """Send consolidated notification for all successful bookings."""
        successes = [r for r in results if r.get("success")]
        if not successes:
            return

        lines = [f"Multi-account booking: {len(successes)} court(s) booked!"]
        for r in successes:
            lines.append(f"  {r['mobile']}: orderNo={r.get('orderNo', 'N/A')}")
            pl = r.get("parkList", [])
            if pl:
                names = ", ".join(f"{s.get('parkname', '')} {s.get('timeStr', '')}" for s in pl)
                lines.append(f"    slots: {names}")
        msg = "\n".join(lines)

        import requests as req_lib
        server_chan_key = str(self.notify_cfg.get("server_chan_key", "")).strip()
        if server_chan_key:
            try:
                req_lib.post(
                    f"https://sctapi.ftqq.com/{server_chan_key}.send",
                    data={"title": f"Tennis: {len(successes)} courts booked!", "desp": msg},
                    timeout=5,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("ServerChan notify failed: %s", exc)

        bark_url = str(self.notify_cfg.get("bark_url", "")).strip().rstrip("/")
        if bark_url:
            try:
                summary = f"{len(successes)} courts booked"
                req_lib.get(f"{bark_url}/TennisMultiBook/{summary}", timeout=5)
            except Exception as exc:  # noqa: BLE001
                log.warning("Bark notify failed: %s", exc)

    def run(self) -> bool:
        """Main orchestration: pre-build, partition, spawn workers, collect results."""
        accounts = self._load_accounts()
        if not accounts:
            raise RuntimeError("No valid accounts found in user_cookies collection.")
        log.info("Loaded %d accounts: %s", len(accounts), [a["mobile"] for a in accounts])

        # Use first account as pilot for pre-query
        pilot_cfg = dict(self.cfg)
        pilot_cfg["auth"] = dict(accounts[0])
        pilot = TennisBooker(config=pilot_cfg)
        pilot.client._require_sign_creds()

        # NTP sync (shared across all workers)
        pilot.client.sync_time_ntp()
        time_offset_ms = pilot.client.time_offset_ms

        # Resolve target date and ballcode
        target_date = self._resolve_target_date(pilot)
        ballcode = pilot.resolve_ballcode()
        log.info("Multi-fire: date=%s, ballcode=%s, accounts=%d, target_courts=%d",
                 target_date, ballcode, len(accounts), self.target_total)

        # Pre-query courts once using pilot account
        all_candidates = pilot.pre_build_all_park_lists(target_date, ballcode)
        if not all_candidates:
            raise RuntimeError("Pre-query returned no bookable courts.")

        # Partition across accounts
        num_accounts = min(len(accounts), len(all_candidates))
        partitions = partition_park_lists(all_candidates, num_accounts, self.skip_courts)

        # Spawn one process per account
        result_queue: mp.Queue = mp.Queue()
        booked_flag = mp.Value("i", 0)
        processes: List[mp.Process] = []

        for i in range(num_accounts):
            p = mp.Process(
                target=account_worker,
                args=(
                    accounts[i],
                    partitions[i],
                    self.cfg,
                    time_offset_ms,
                    result_queue,
                    booked_flag,
                    self.target_total,
                    i,
                ),
                daemon=True,
            )
            processes.append(p)

        log.info("Spawning %d worker processes...", len(processes))
        for p in processes:
            p.start()

        # Collect results with timeout
        max_wait = int(self.strategy_cfg.get("max_retries", 30)) * int(self.strategy_cfg.get("burst_timeout_sec", 5)) + 30
        results: List[Dict[str, Any]] = []
        deadline = time.time() + max_wait

        for p in processes:
            remaining = max(1, deadline - time.time())
            p.join(timeout=remaining)

        # Drain the queue
        while not result_queue.empty():
            try:
                results.append(result_queue.get_nowait())
            except Exception:  # noqa: BLE001
                break

        # Terminate any stragglers
        for p in processes:
            if p.is_alive():
                log.warning("Terminating stalled worker process %s", p.pid)
                p.terminate()

        successes = [r for r in results if r.get("success")]
        log.info("=" * 60)
        log.info("Multi-fire results: %d/%d courts booked", len(successes), self.target_total)
        for r in results:
            if r.get("success"):
                log.info("  OK: %s -> orderNo=%s", r.get("mobile"), r.get("orderNo"))
            else:
                log.info("  FAIL: %s -> %s", r.get("mobile"), r.get("reason", "unknown"))
        log.info("=" * 60)

        self._notify_results(results)

        return len(successes) > 0


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description="Multi-account direct-fire tennis booking")


def main() -> None:
    build_parser().parse_args()
    orchestrator = MultiFireOrchestrator()
    log.info("Multi-account direct-fire orchestrator starting")
    success = orchestrator.run()
    raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
