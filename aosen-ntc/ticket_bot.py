#!/usr/bin/env python3
"""
彩翼云票务平台自动抢票脚本
仅供个人学习使用，请勿用于商业目的
"""

import time
import json
import sys
import uuid
import random
import string
import logging
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import ssl
import os

import yaml
import requests
import ntplib

os.environ.setdefault("REQUESTS_CA_BUNDLE", "/etc/ssl/cert.pem")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ticket_bot")


class CYYClient:
    """彩翼云票务平台 API 客户端"""

    def __init__(self, config: dict):
        self.cfg = config
        self.base_url = config["platform"]["base_url"]
        self.session = requests.Session()
        self.session.headers.update(self._build_headers())
        self.time_offset_ms = 0

    def _build_headers(self) -> dict:
        auth = self.cfg["auth"]
        plat = self.cfg["platform"]
        headers = {
            "access-token": auth["access_token"],
            "cookie": auth["cookie"],
            "src": plat["src"],
            "terminal-src": plat["terminal_src"],
            "merchant-id": plat["merchant_id"],
            "ver": plat["ver"],
            "utc-offset": "480",
            "xweb_xhr": "1",
            "Content-Type": "application/json",
            "Accept": "*/*",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 "
                "Safari/537.36 MicroMessenger/7.0.20.1781 NetType/WIFI "
                "MiniProgramEnv/Mac MacWechat/WMPF MacWechat/3.8.7(0x13080712) "
                "XWEB/18783"
            ),
        }
        if auth.get("angry_dog"):
            headers["Angry-Dog"] = auth["angry_dog"]
        return headers

    def _common_params(self) -> dict:
        plat = self.cfg["platform"]
        return {
            "currency": "CNY",
            "lang": "zh",
            "terminalSrc": plat["terminal_src"],
            "utcOffset": "480",
            "ver": plat["ver"],
        }

    def _gen_trace_id(self) -> str:
        chars = string.ascii_lowercase + string.digits
        return "".join(random.choices(chars, k=20))

    def _request(self, method: str, path: str, params: dict = None,
                 json_body: dict = None, extra_params: dict = None) -> dict:
        url = f"{self.base_url}{path}"
        query = {**self._common_params(), **(extra_params or {})}
        if params:
            query.update(params)

        headers = {"front-trace-id": self._gen_trace_id()}

        try:
            resp = self.session.request(
                method, url, params=query, json=json_body,
                headers=headers, timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("statusCode") != 200:
                log.warning("API error: %s -> %s", path, data.get("comments", data))
            return data
        except Exception as e:
            log.error("Request failed: %s %s -> %s", method, path, e)
            return {"statusCode": -1, "comments": str(e)}

    # ─── 时间同步 ───

    def sync_time_ntp(self):
        """通过 NTP 校准本地时间偏移"""
        try:
            client = ntplib.NTPClient()
            resp = client.request("ntp.aliyun.com", version=3)
            self.time_offset_ms = int(resp.offset * 1000)
            log.info("NTP time offset: %+d ms", self.time_offset_ms)
        except Exception as e:
            log.warning("NTP sync failed: %s, trying server time...", e)
            self._sync_time_server()

    def _sync_time_server(self):
        """通过服务器时间校准"""
        try:
            t1 = time.time()
            resp = self.session.get(
                f"{self.base_url}/get_time",
                params=self._common_params(), timeout=5
            )
            t2 = time.time()
            rtt = (t2 - t1) / 2
            server_ts = resp.json().get("data", {}).get("timestamp", 0)
            if server_ts:
                local_ts = (t1 + rtt) * 1000
                self.time_offset_ms = int(server_ts - local_ts)
                log.info("Server time offset: %+d ms", self.time_offset_ms)
        except Exception as e:
            log.warning("Server time sync failed: %s", e)

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self.time_offset_ms

    # ─── 演出信息 ───

    def get_show_static(self, show_id: str) -> dict:
        path = f"/cyy_gatewayapi/show/pub/v5/show/{show_id}/static"
        return self._request("GET", path)

    def get_show_dynamic(self, show_id: str) -> dict:
        path = f"/cyy_gatewayapi/show/pub/v5/show/{show_id}/dynamic"
        return self._request("GET", path)

    def get_sessions(self, show_id: str) -> dict:
        path = f"/cyy_gatewayapi/show/pub/v3/show/{show_id}/sessions_from_marketing_countdown"
        return self._request("GET", path)

    def get_seat_plans(self, show_id: str, session_id: str) -> dict:
        path = (
            f"/cyy_gatewayapi/show/pub/v3/show/{show_id}"
            f"/show_session/{session_id}/seat_plans_from_marketing_countdown"
        )
        return self._request("GET", path)

    # ─── 观演人 ───

    def get_audiences(self) -> dict:
        return self._request("GET", "/cyy_gatewayapi/user/buyer/v3/user_audiences")

    # ─── 预填单 (热门票模式) ───

    def submit_pre_fill(self, show_id: str, session_id: str,
                        seat_plan_id: str, ticket_qty: int,
                        limit_qty: int, audience_ids: list) -> dict:
        path = "/cyy_gatewayapi/show/buyer/v3/pre_filed_info"
        body = {
            "appId": self.cfg["platform"]["app_id"],
            "audiencePhotos": [],
            "bizSeatPlanId": seat_plan_id,
            "bizShowId": show_id,
            "bizShowSessionId": session_id,
            "limitQty": limit_qty,
            "merchantId": self.cfg["platform"]["merchant_id"],
            "src": self.cfg["platform"]["src"],
            "ticketQty": ticket_qty,
            "userAudienceIds": audience_ids,
            "ver": self.cfg["platform"]["ver"],
        }
        return self._request("POST", path, json_body=body)

    def get_pre_fill(self, show_id: str) -> dict:
        path = f"/cyy_gatewayapi/show/buyer/v3/pre_filed_info/{show_id}"
        return self._request("GET", path)

    # ─── 风控检查 ───

    def check_risk_limit(self, show_id: str, session_id: str,
                         seat_plan_ids: list) -> dict:
        path = "/cyy_gatewayapi/show/pub/risk/v3/limit"
        body = {
            "appId": self.cfg["platform"]["app_id"],
            "merchantId": self.cfg["platform"]["merchant_id"],
            "seatPlanIds": seat_plan_ids,
            "sessionId": session_id,
            "showId": show_id,
            "src": self.cfg["platform"]["src"],
            "ver": self.cfg["platform"]["ver"],
        }
        return self._request("POST", path, json_body=body)

    # ─── 创建订单（核心抢票接口） ───
    # 注意：此接口尚未抓包确认，以下为基于平台规律的推测
    # 你需要在 3月10日 18:00 开售时在 Proxyman 中抓到实际的下单接口
    # 然后更新此方法

    def create_order(self, show_id: str, session_id: str,
                     seat_plan_id: str, ticket_qty: int,
                     audience_ids: list, pre_filed_id: str = "") -> dict:
        """
        创建订单 — 开售时的核心接口

        可能的接口路径（需要抓包确认）：
        - POST /cyy_gatewayapi/show/buyer/v3/order/create
        - POST /cyy_gatewayapi/show/buyer/v3/create_order
        - POST /cyy_gatewayapi/show/buyer/v3/order

        目前使用 pre_filed_info 作为下单方式（热门票模式下预填单即排队抢票）
        """
        log.info("Submitting pre-fill order (hot mode)...")
        result = self.submit_pre_fill(
            show_id, session_id, seat_plan_id,
            ticket_qty, self.cfg["show"]["limit_qty"], audience_ids
        )
        if result.get("statusCode") == 200:
            log.info("Pre-fill submitted successfully!")
            fill_info = self.get_pre_fill(show_id)
            if fill_info.get("statusCode") == 200:
                pre_filed_id = fill_info.get("data", {}).get("preFiledId", "")
                log.info("Pre-fill ID: %s", pre_filed_id)
                return fill_info
        return result


class TicketBot:
    """抢票机器人主控"""

    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, "r", encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)
        self.client = CYYClient(self.cfg)
        self.success = False

    def show_info(self):
        """显示演出和票档信息"""
        show = self.cfg["show"]
        log.info("=" * 60)
        log.info("演出: %s", show["show_name"])
        log.info("开售时间: %s", show["sale_time"])
        log.info("目标场次: %s", show["target_session_id"])
        log.info("目标票档: %s", show["target_seat_plan_id"])
        log.info("购买数量: %s", show["ticket_qty"])
        log.info("=" * 60)

    def fetch_latest_info(self):
        """获取最新的场次和票档信息"""
        show_id = self.cfg["show"]["show_id"]

        log.info("获取演出动态信息...")
        dynamic = self.client.get_show_dynamic(show_id)
        if dynamic.get("statusCode") == 200:
            d = dynamic["data"]
            log.info("状态: %s | 按钮: %s | 开售: %s",
                     d.get("showDetailStatus"), d.get("buttonText"),
                     d.get("saleTimeDesc"))

        log.info("获取场次列表...")
        sessions = self.client.get_sessions(show_id)
        if sessions.get("statusCode") == 200:
            for s in sessions["data"].get("sessionVOs", []):
                log.info("  场次: %s | ID: %s | 限购: %s",
                         s["sessionName"], s["bizShowSessionId"], s["limitation"])

        session_id = self.cfg["show"]["target_session_id"]
        log.info("获取票档列表 (session: %s)...", session_id)
        plans = self.client.get_seat_plans(show_id, session_id)
        if plans.get("statusCode") == 200:
            for p in plans["data"].get("seatPlans", []):
                status = "停售" if p.get("isStopSale") else "可售"
                log.info("  %s ¥%s | ID: %s | %s",
                         p["seatPlanName"], p["originalPrice"],
                         p["seatPlanId"], status)

    def pre_fill(self):
        """提交预填单"""
        show = self.cfg["show"]
        audience_ids = self.cfg["audience"]["user_audience_ids"]

        log.info("提交预填单...")
        result = self.client.submit_pre_fill(
            show["show_id"],
            show["target_session_id"],
            show["target_seat_plan_id"],
            show["ticket_qty"],
            show["limit_qty"],
            audience_ids,
        )
        if result.get("statusCode") == 200 and result.get("data") is True:
            log.info("预填单提交成功!")
            fill = self.client.get_pre_fill(show["show_id"])
            if fill.get("statusCode") == 200:
                log.info("预填单ID: %s", fill["data"].get("preFiledId"))
                return True
        else:
            log.error("预填单提交失败: %s", result.get("comments"))
        return False

    def _try_grab(self, thread_id: int) -> bool:
        """单次抢票尝试"""
        show = self.cfg["show"]
        audience_ids = self.cfg["audience"]["user_audience_ids"]
        strategy = self.cfg["strategy"]

        for attempt in range(strategy["max_retries"]):
            if self.success:
                return False

            log.info("[Thread-%d] 第 %d 次尝试...", thread_id, attempt + 1)
            result = self.client.create_order(
                show["show_id"],
                show["target_session_id"],
                show["target_seat_plan_id"],
                show["ticket_qty"],
                audience_ids,
            )

            status = result.get("statusCode")
            if status == 200:
                data = result.get("data", {})
                if isinstance(data, dict) and data.get("preFiledId"):
                    self.success = True
                    log.info("[Thread-%d] 抢票成功! preFiledId=%s",
                             thread_id, data.get("preFiledId"))
                    self._notify_success(data)
                    return True
                elif data is True:
                    self.success = True
                    log.info("[Thread-%d] 预填单提交成功!", thread_id)
                    self._notify_success({"status": "pre_filled"})
                    return True

            time.sleep(strategy["retry_interval_ms"] / 1000)

        return False

    def wait_and_grab(self):
        """等待开售时间并抢票"""
        self.client.sync_time_ntp()

        sale_time_str = self.cfg["show"]["sale_time"]
        sale_dt = datetime.strptime(sale_time_str, "%Y-%m-%d %H:%M:%S")
        sale_ts_ms = int(sale_dt.timestamp() * 1000)

        strategy = self.cfg["strategy"]
        advance_ms = strategy["advance_ms"]
        target_ms = sale_ts_ms - advance_ms

        log.info("开售时间: %s (ts=%d)", sale_time_str, sale_ts_ms)
        log.info("提前 %d ms 开始, 并发 %d 线程", advance_ms, strategy["threads"])

        while True:
            now = self.client.now_ms()
            remaining = target_ms - now
            if remaining <= 0:
                break
            if remaining > 60000:
                log.info("距离开售还有 %.1f 秒...", remaining / 1000)
                time.sleep(min(remaining / 1000 - 5, 30))
            elif remaining > 1000:
                log.info("距离开售还有 %.1f 秒...", remaining / 1000)
                time.sleep(0.5)
            else:
                time.sleep(0.01)

        log.info("开始抢票!")
        with ThreadPoolExecutor(max_workers=strategy["threads"]) as executor:
            futures = [
                executor.submit(self._try_grab, i)
                for i in range(strategy["threads"])
            ]
            for f in as_completed(futures):
                try:
                    if f.result():
                        log.info("有线程抢票成功!")
                except Exception as e:
                    log.error("线程异常: %s", e)

        if self.success:
            log.info("=" * 60)
            log.info("抢票成功! 请打开微信小程序完成支付!")
            log.info("=" * 60)
        else:
            log.info("抢票未成功，可以重试或手动操作")

    def _notify_success(self, data: dict):
        """发送成功通知"""
        notify = self.cfg.get("notify", {})
        msg = f"抢票成功!\n演出: {self.cfg['show']['show_name']}\n请尽快打开小程序完成支付!"

        server_chan_key = notify.get("server_chan_key", "")
        if server_chan_key:
            try:
                requests.post(
                    f"https://sctapi.ftqq.com/{server_chan_key}.send",
                    data={"title": "抢票成功!", "desp": msg},
                    timeout=5,
                )
            except Exception as e:
                log.warning("Server酱通知失败: %s", e)

        bark_url = notify.get("bark_url", "")
        if bark_url:
            try:
                requests.get(f"{bark_url}/抢票成功/{msg}", timeout=5)
            except Exception as e:
                log.warning("Bark通知失败: %s", e)


def main():
    parser = argparse.ArgumentParser(description="彩翼云抢票脚本")
    parser.add_argument("-c", "--config", default="config.yaml", help="配置文件路径")
    parser.add_argument(
        "action",
        choices=["info", "prefill", "grab", "test", "refresh"],
        help=(
            "info=查看信息, prefill=提交预填单, grab=定时抢票, "
            "test=测试连通性, refresh=自动刷新认证"
        ),
    )
    args = parser.parse_args()

    if args.action == "refresh":
        import subprocess
        script = os.path.join(os.path.dirname(__file__), "auto_credential.py")
        subprocess.run([sys.executable, script, "-c", args.config])
        return

    bot = TicketBot(args.config)

    if args.action == "info":
        bot.show_info()
        bot.fetch_latest_info()

    elif args.action == "prefill":
        bot.show_info()
        bot.pre_fill()

    elif args.action == "grab":
        bot.show_info()
        bot.fetch_latest_info()
        log.info("预填单提交中...")
        bot.pre_fill()
        log.info("等待开售...")
        bot.wait_and_grab()

    elif args.action == "test":
        log.info("测试API连通性...")
        bot.client.sync_time_ntp()

        show_id = bot.cfg["show"]["show_id"]
        result = bot.client.get_show_dynamic(show_id)
        if result.get("statusCode") == 200:
            log.info("API连通正常! 演出状态: %s",
                     result["data"].get("showDetailStatus"))
        else:
            log.error("API连通失败: %s", result.get("comments"))

        audiences = bot.client.get_audiences()
        if audiences.get("statusCode") == 200:
            for a in audiences.get("data", []):
                log.info("观演人: %s (%s)", a.get("name"), a.get("id"))
        else:
            log.error("获取观演人失败（token可能已过期）: %s",
                     audiences.get("comments"))


if __name__ == "__main__":
    main()
