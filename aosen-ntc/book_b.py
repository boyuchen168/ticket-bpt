#!/usr/bin/env python3
"""
Strategy B booking script.

Behavior:
- fixed target time from config (`court.target_time`)
- query a single parktype
- reverse API court order (last court first)
- assign one thread per court (capped by `strategy.threads`)
- keep retrying until any thread books successfully
"""

from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Sequence, Tuple

from tennis_bot import TennisBooker, log


class BookerB(TennisBooker):
    def ordered_parks_reversed(self, payload: Dict[str, Any]) -> List[Tuple[int, str]]:
        parks: List[Tuple[int, str]] = []
        for venue in payload.get("venList", []) or []:
            for park in venue.get("park", []) or []:
                park_id = self._parse_int(park.get("id"))
                if park_id >= 0:
                    parks.append((park_id, str(park.get("parkname", ""))))
        parks.reverse()
        return parks

    def _target_hour(self) -> int:
        target_time = str(self.court_cfg.get("target_time", "")).strip()
        if not target_time:
            raise RuntimeError("Strategy B requires court.target_time in config.")
        try:
            return int(target_time.split(":", 1)[0])
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Invalid court.target_time: {target_time}") from exc

    def _target_hours(self) -> List[int]:
        start = self._target_hour()
        end_str = str(self.court_cfg.get("target_time_end", "")).strip()
        if not end_str:
            return [start]
        try:
            end = int(end_str.split(":", 1)[0])
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Invalid court.target_time_end: {end_str}") from exc
        if end < start:
            raise RuntimeError(
                f"Invalid time window: target_time_end({end:02d}:00) is earlier than "
                f"target_time({start:02d}:00)."
            )
        return list(range(start, end + 1))

    def _fixed_slots_for_park(
        self,
        slots: Sequence[Dict[str, Any]],
        *,
        parkid: int,
        target_hour: int,
        duration: int,
    ) -> List[Dict[str, Any]]:
        by_hour: Dict[int, Dict[str, Any]] = {}
        for slot in slots:
            if self._parse_int(slot.get("parkid")) != parkid:
                continue
            hour = self._parse_int(slot.get("time"))
            if hour >= 0:
                by_hour[hour] = dict(slot)

        picked: List[Dict[str, Any]] = []
        for offset in range(duration):
            hour = target_hour + offset
            item = by_hour.get(hour)
            if item is None:
                return []
            picked.append(item)
        return picked

    def try_book_b(
        self,
        thread_id: int,
        target_date: str,
        ballcode: str,
        parkid: int,
        parkname: str,
        target_hours: Sequence[int],
    ) -> bool:
        retries_per_slot = max(1, int(self.strategy_cfg.get("retries_per_slot", 2)))
        sleep_s = int(self.strategy_cfg.get("retry_interval_ms", 200)) / 1000.0
        duration = max(1, int(self.court_cfg.get("duration_hours", 1)))

        parktypeinfo = str(self.court_cfg.get("parktypecode", "6"))
        cardtypecode = str(self.court_cfg.get("cardtypecode", "-1"))
        parkstatus = str(self.court_cfg.get("parkstatus", "0"))
        paywaycode = self.court_cfg.get("paywaycode", 2)
        mobile = str(self.auth_cfg.get("mobile", "")).strip()
        ordercode = str(self.auth_cfg.get("ordercode", "")).strip()
        captcha = str(self.auth_cfg.get("captchaVerification", "")).strip()

        for target_hour in target_hours:
            for attempt in range(1, retries_per_slot + 1):
                if self.success_event.is_set():
                    return False

                log.info(
                    "[B-T%s] Attempt %d/%d, park=%s(%s), start=%02d:00, duration=%dh",
                    thread_id,
                    attempt,
                    retries_per_slot,
                    parkname,
                    parkid,
                    target_hour,
                    duration,
                )

                courts_rsp = self.client.query_courts(
                    date=target_date,
                    parktypeinfo=parktypeinfo,
                    ballcode=ballcode,
                    cardtypecode=cardtypecode,
                    userid=self.client.auth.userid,
                    parkstatus=parkstatus,
                    changefieldtype="0",
                )
                if not self.client.is_success(courts_rsp):
                    log.warning("[B-T%s] query_courts failed: %s", thread_id, courts_rsp.get("respMsg"))
                    time.sleep(sleep_s)
                    continue

                payload = self.client.unwrap_payload(courts_rsp)
                if not isinstance(payload, dict):
                    log.warning("[B-T%s] query_courts payload empty.", thread_id)
                    time.sleep(sleep_s)
                    continue

                slots = self._collect_available_slots(payload, target_date=target_date)
                selected_slots = self._fixed_slots_for_park(
                    slots,
                    parkid=parkid,
                    target_hour=target_hour,
                    duration=duration,
                )
                if not selected_slots:
                    log.info(
                        "[B-T%s] Not available for %s at %02d:00 (attempt %d/%d).",
                        thread_id,
                        parkname,
                        target_hour,
                        attempt,
                        retries_per_slot,
                    )
                    time.sleep(sleep_s)
                    continue

                park_list = self._to_park_list(selected_slots)
                log.info("[B-T%s] Candidate slots: %s", thread_id, json.dumps(park_list, ensure_ascii=False))

                price_rsp = self.client.show_price_by_user(park_list, userid=self.client.auth.userid)
                if not self.client.is_success(price_rsp):
                    log.warning("[B-T%s] showPriceByUser failed: %s", thread_id, price_rsp.get("respMsg"))
                    time.sleep(sleep_s)
                    continue

                price_payload = self.client.unwrap_payload(price_rsp)
                wx_sum = None
                if isinstance(price_payload, dict):
                    wx_price = price_payload.get("wxPrice", {})
                    if isinstance(wx_price, dict):
                        wx_sum = wx_price.get("sum")
                if wx_sum is not None:
                    log.info("[B-T%s] wxPrice sum: %s", thread_id, wx_sum)

                order_rsp = self.client.add_park_order(
                    park_list,
                    paywaycode=paywaycode,
                    mobile=mobile,
                    ordercode=ordercode,
                    captcha_verification=captcha,
                    add_order_type="wx",
                    userid=self.client.auth.userid,
                )
                if not self.client.is_success(order_rsp):
                    log.warning("[B-T%s] addParkOrder failed: %s", thread_id, order_rsp.get("respMsg"))
                    time.sleep(sleep_s)
                    continue

                order_payload = self.client.unwrap_payload(order_rsp)
                order_no = self._extract_order_no(order_payload)
                if not order_no:
                    log.warning("[B-T%s] addParkOrder succeeded but orderNo missing.", thread_id)
                    time.sleep(sleep_s)
                    continue

                state_rsp = self.client.get_park_order_state(order_no)
                if self.client.is_success(state_rsp):
                    log.info("[B-T%s] getParkOrderState success for %s", thread_id, order_no)
                else:
                    log.warning(
                        "[B-T%s] getParkOrderState response: %s",
                        thread_id,
                        state_rsp.get("respMsg"),
                    )

                with self._lock:
                    if not self.success_event.is_set():
                        self.success_event.set()
                        self.success_order = {
                            "orderNo": order_no,
                            "parkList": park_list,
                            "priceResponse": price_payload,
                            "orderStateResponse": state_rsp,
                        }
                        self._notify_success(order_no, park_list)
                return True

            log.info(
                "[B-T%s] Move to next target hour after %d attempts at %02d:00.",
                thread_id,
                retries_per_slot,
                target_hour,
            )

        return False

    def run(self) -> bool:
        self.client._require_sign_creds()
        self.client.sync_time_ntp()

        target_date = str(self.court_cfg.get("target_date", "")).strip()
        if not target_date:
            dates_rsp = self.client.query_dates()
            payload = self.client.unwrap_payload(dates_rsp)
            if isinstance(payload, list) and payload:
                target_date = str(payload[0].get("date", ""))
                log.info("target_date not set, fallback to first available: %s", target_date)
            else:
                raise RuntimeError("No target_date configured and queryBookDate returned no data.")

        target_hours = self._target_hours()
        ballcode = self.resolve_ballcode()
        parktypeinfo = str(self.court_cfg.get("parktypecode", "6"))
        cardtypecode = str(self.court_cfg.get("cardtypecode", "-1"))
        parkstatus = str(self.court_cfg.get("parkstatus", "0"))

        pre_rsp = self.client.query_courts(
            date=target_date,
            parktypeinfo=parktypeinfo,
            ballcode=ballcode,
            cardtypecode=cardtypecode,
            userid=self.client.auth.userid,
            parkstatus=parkstatus,
            changefieldtype="0",
        )
        if not self.client.is_success(pre_rsp):
            raise RuntimeError(f"query_courts failed before start: {pre_rsp.get('respMsg')}")

        pre_payload = self.client.unwrap_payload(pre_rsp)
        if not isinstance(pre_payload, dict):
            raise RuntimeError("query_courts returned unexpected payload.")

        ordered_parks = self.ordered_parks_reversed(pre_payload)
        if not ordered_parks:
            raise RuntimeError("No parks found in query_courts response.")
        targets = ordered_parks

        order_desc = " -> ".join(f"{name}({pid})" for pid, name in targets)
        hours_desc = " -> ".join(f"{hour:02d}:00" for hour in target_hours)
        log.info("Strategy B court order (last-first, all courts): %s", order_desc)
        log.info(
            "Strategy B targets: date=%s, hours=%s, parktypeinfo=%s, ballcode=%s",
            target_date,
            hours_desc,
            parktypeinfo,
            ballcode,
        )

        self.success_event.clear()
        self.success_order = {}
        self.wait_for_open_time()
        log.info("Starting Strategy B booking threads: %d", len(targets))

        with ThreadPoolExecutor(max_workers=len(targets)) as pool:
            futures = [
                pool.submit(
                    self.try_book_b,
                    idx + 1,
                    target_date,
                    ballcode,
                    parkid,
                    parkname,
                    target_hours,
                )
                for idx, (parkid, parkname) in enumerate(targets)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:  # noqa: BLE001
                    log.error("Strategy B thread failure: %s", exc)

        if self.success_event.is_set():
            log.info("Strategy B booking succeeded: %s", json.dumps(self.success_order, ensure_ascii=False))
            return True
        log.info("Strategy B booking did not succeed within retry limits.")
        return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="tennis.bjofp.cn booking Strategy B script")
    parser.add_argument(
        "-c",
        "--config",
        default="tennis_config.yaml",
        help="Path to config yaml (default: tennis_config.yaml)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    bot = BookerB(args.config)
    bot.show_info()
    log.info("Strategy: B (fixed time, parallel per-court, reverse API order)")
    success = bot.run()
    raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
