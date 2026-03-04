#!/usr/bin/env python3
"""
tennis.bjofp.cn auto-booking script.

This script reproduces the WeChat mini-program request flow:
- normal/PS signing
- login token decryption
- encrypted `datas` payload decryption
- court querying and order placement
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import random
import re
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
import yaml
from Crypto.Cipher import AES

try:
    import ntplib
except ImportError:  # pragma: no cover - optional runtime dependency
    ntplib = None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tennis_bot")


@dataclass
class AuthState:
    userid: str = ""
    bnglokbj: str = ""
    csc: str = ""
    cdc: str = ""
    open_id: str = ""
    ma_open_id: str = ""

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "AuthState":
        auth = cfg.get("auth", {})
        return cls(
            userid=str(auth.get("userid", "")).strip(),
            bnglokbj=str(auth.get("bnglokbj", "")).strip(),
            csc=str(auth.get("csc", "")).strip(),
            cdc=str(auth.get("cdc", "")).strip(),
            open_id=str(auth.get("openId", auth.get("openid", ""))).strip(),
            ma_open_id=str(auth.get("maopenId", auth.get("maopenid", ""))).strip(),
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "userid": self.userid,
            "bnglokbj": self.bnglokbj,
            "csc": self.csc,
            "cdc": self.cdc,
            "openId": self.open_id,
            "maopenId": self.ma_open_id,
        }


class TennisClient:
    BASE_URL = "https://tennis.bjofp.cn"
    LOGIN_AES_SEED = "gjwqerxxzxasdfqw"
    PS_DECRYPT_HEX = "d7e0762294db597f05d77415b0584fb0"

    def __init__(self, config: Dict[str, Any]):
        self.cfg = config
        self.platform_cfg = config.get("platform", {})
        self.base_url = self.platform_cfg.get("base_url", self.BASE_URL).rstrip("/")
        self.timeout = int(self.platform_cfg.get("timeout_sec", 12))
        self.auth = AuthState.from_config(config)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "xweb_xhr": "1",
                "Accept": "application/json, text/plain, */*",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 "
                    "Safari/537.36 MicroMessenger/7.0.20.1781 "
                    "MiniProgramEnv/Mac"
                ),
            }
        )
        self.time_offset_ms = 0

    # ------------------------------------------------------------------
    # Time sync
    # ------------------------------------------------------------------
    def sync_time_ntp(self, host: str = "ntp.aliyun.com") -> None:
        if ntplib is None:
            log.warning("ntplib is not installed, skipping NTP sync.")
            return
        try:
            client = ntplib.NTPClient()
            rsp = client.request(host, version=3)
            self.time_offset_ms = int(rsp.offset * 1000)
            log.info("NTP offset: %+d ms", self.time_offset_ms)
        except Exception as exc:  # noqa: BLE001
            log.warning("NTP sync failed: %s", exc)

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self.time_offset_ms

    # ------------------------------------------------------------------
    # Sign/decrypt internals (reverse-engineered from mini-program)
    # ------------------------------------------------------------------
    @staticmethod
    def _random_nonce(min_len: int = 10, max_len: int = 32) -> str:
        size = random.randint(min_len, max_len)
        pool = string.digits + string.ascii_lowercase + string.ascii_uppercase
        return "".join(random.choices(pool, k=size))

    @staticmethod
    def _sort_pairs(params: Dict[str, Any]) -> List[str]:
        pairs: List[str] = []
        for key, value in params.items():
            if value is None:
                continue
            pairs.append(f"{key}={value}&")
        pairs.sort(key=lambda item: item)
        return pairs

    @classmethod
    def _parameter_sort(cls, params: Dict[str, Any]) -> str:
        return "".join(cls._sort_pairs(params))

    @classmethod
    def _parameter_sort_ps(cls, params: Dict[str, Any]) -> str:
        pairs = cls._sort_pairs(params)
        if len(pairs) >= 4:
            pairs = [pairs[3], pairs[2], pairs[1], pairs[0], *pairs[4:]]
        return "".join(pairs)

    @staticmethod
    def _strip_trailing_amp(text: str) -> str:
        if "&" in text:
            return text[: text.rfind("&")]
        return text

    @staticmethod
    def _md5_upper(text: str) -> str:
        return hashlib.md5(text.encode("utf-8")).hexdigest().upper()

    def _require_sign_creds(self) -> None:
        missing = [
            name
            for name, value in (
                ("userid", self.auth.userid),
                ("bnglokbj", self.auth.bnglokbj),
                ("csc", self.auth.csc),
                ("cdc", self.auth.cdc),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                f"Missing sign credentials: {', '.join(missing)}. "
                "Run login first or populate auth in tennis_config.yaml."
            )

    def generate_sign(
        self,
        params: Dict[str, Any],
        ps_mode: bool = False,
        csc: Optional[str] = None,
        cdc: Optional[str] = None,
    ) -> str:
        csc_val = csc or self.auth.csc
        cdc_val = cdc or self.auth.cdc
        if not csc_val or not cdc_val:
            raise ValueError("Missing csc/cdc for sign generation.")

        sorted_part = (
            self._parameter_sort_ps(params) if ps_mode else self._parameter_sort(params)
        )
        sorted_part = self._strip_trailing_amp(sorted_part)
        sign_input = f"{sorted_part}&eqnrlzuh={csc_val}{cdc_val}"
        return self._md5_upper(sign_input)

    def _with_sign(
        self, params: Dict[str, Any], ps_mode: bool = False, force_userid: bool = True
    ) -> Dict[str, Any]:
        self._require_sign_creds()
        payload = {k: v for k, v in params.items() if v is not None}
        if force_userid and not payload.get("userid"):
            payload["userid"] = self.auth.userid
        payload["timestamp"] = str(self.now_ms())
        payload["nonce"] = self._random_nonce()

        sign = self.generate_sign(payload, ps_mode=ps_mode)
        payload["bnglokbj"] = self.auth.bnglokbj
        payload["sign"] = sign
        payload["noEncrypt"] = ""
        return payload

    @staticmethod
    def _unpad_pkcs7(raw: bytes) -> bytes:
        if not raw:
            raise ValueError("Empty plaintext after decrypt.")
        pad = raw[-1]
        if pad < 1 or pad > 16:
            raise ValueError(f"Invalid PKCS7 padding length: {pad}")
        return raw[:-pad]

    @classmethod
    def _aes_ecb_decrypt_b64(cls, encrypted_b64: str, key: str) -> str:
        cipher = AES.new(key.encode("utf-8"), AES.MODE_ECB)
        encrypted = base64.b64decode(encrypted_b64)
        plain = cipher.decrypt(encrypted)
        plain = cls._unpad_pkcs7(plain)
        return plain.decode("utf-8", errors="replace")

    @staticmethod
    def _secret_shuffle(seed: str) -> str:
        # Same transform as mini-program function `c(...)`.
        return f"{seed[:8]}{seed[-8:]}{seed[16:-8]}{seed[8:-16]}"

    @classmethod
    def login_aes_key(cls) -> str:
        seed = cls.LOGIN_AES_SEED
        # Same transform as mini-program `strStorPs()`.
        return f"{seed[:4]}{seed[8:-4]}{seed[-4:]}{seed[4:-8]}"

    def decrypt_login_token(self, token_b64: str, key: str, regex_remove: str = "") -> str:
        text = self._aes_ecb_decrypt_b64(token_b64, key)
        if regex_remove:
            text = re.sub(regex_remove, "", text, count=1)
        return text

    def decrypt_datas(self, datas_b64: str, ps_mode: bool = False) -> str:
        if ps_mode:
            key = self._secret_shuffle(self.PS_DECRYPT_HEX)[:16]
        else:
            if not self.auth.csc:
                raise ValueError("Missing csc; cannot decrypt datas.")
            key = self.auth.csc[:16]

        text = self._aes_ecb_decrypt_b64(datas_b64, key)
        # Same behavior as JS `replace(new RegExp(key), "")`: first occurrence.
        return text.replace(key, "", 1)

    # ------------------------------------------------------------------
    # Request helpers
    # ------------------------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        form: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            rsp = self.session.request(
                method=method,
                url=url,
                params=params,
                data=form,
                json=json_body,
                timeout=self.timeout,
            )
        except Exception as exc:  # noqa: BLE001
            return {"respCode": -1, "respMsg": f"request error: {exc}", "datas": None}

        if rsp.status_code != 200:
            return {
                "respCode": -1,
                "respMsg": f"http {rsp.status_code}: {rsp.text[:160]}",
                "datas": None,
            }
        try:
            return rsp.json()
        except Exception as exc:  # noqa: BLE001
            return {"respCode": -1, "respMsg": f"invalid json: {exc}", "datas": None}

    def _postprocess_response(
        self, response: Dict[str, Any], decrypt_datas: bool, decrypt_ps: bool = False
    ) -> Dict[str, Any]:
        if decrypt_datas and isinstance(response.get("datas"), str):
            try:
                decoded = self.decrypt_datas(response["datas"], ps_mode=decrypt_ps)
                response["decoded_datas_raw"] = decoded
                try:
                    response["decoded_datas"] = json.loads(decoded)
                except json.JSONDecodeError:
                    response["decoded_datas"] = decoded
            except Exception as exc:  # noqa: BLE001
                response["decode_error"] = str(exc)
        return response

    @staticmethod
    def is_success(response: Dict[str, Any]) -> bool:
        if not isinstance(response, dict):
            return False
        if str(response.get("respCode")) == "1001":
            return True
        if str(response.get("code")) == "1001":
            return True
        return False

    @staticmethod
    def unwrap_payload(response: Dict[str, Any]) -> Any:
        if not isinstance(response, dict):
            return None
        if "decoded_datas" in response:
            return response["decoded_datas"]
        if "datas" in response:
            return response["datas"]
        if "data" in response:
            return response["data"]
        return None

    # ------------------------------------------------------------------
    # API methods
    # ------------------------------------------------------------------
    def get_home_info(self) -> Dict[str, Any]:
        return self._request(
            "POST", "/TennisCenterInterface/ddCardType/getHomeInfo.action", json_body={}
        )

    def get_phone_code(self, loginname: str, code_type: int = 3) -> Dict[str, Any]:
        params = {"loginname": loginname, "type": code_type}
        return self._request("GET", "/TennisCenterInterface/umUser/getPhoneCode.action", params=params)

    def phone_code_login(
        self,
        mobile: str,
        phonecode: str,
        union_id: str = "",
        ma_open_id: str = "",
    ) -> Dict[str, Any]:
        params = {"mobile": mobile, "phonecode": phonecode}
        if union_id:
            params["unionId"] = union_id
        if ma_open_id:
            params["maopenId"] = ma_open_id

        response = self._request(
            "GET", "/TennisCenterInterface/umUser/phoneCodeLogin.action", params=params
        )
        if not self.is_success(response):
            return response

        payload = self.unwrap_payload(response)
        if not isinstance(payload, dict):
            return response

        ftzmzcwc = payload.get("ftzmzcwc", {}) if isinstance(payload.get("ftzmzcwc"), dict) else {}
        user = payload.get("user", {}) if isinstance(payload.get("user"), dict) else {}
        if ftzmzcwc:
            key = self.login_aes_key()
            csc = self.decrypt_login_token(ftzmzcwc.get("qlakclqf", ""), key)
            cdc = self.decrypt_login_token(ftzmzcwc.get("xqqflsoy", ""), key)
            self.auth.csc = csc.strip()
            self.auth.cdc = cdc.strip()
            self.auth.bnglokbj = str(ftzmzcwc.get("bnglokbj", "")).strip()

        if user:
            if user.get("id") is not None:
                self.auth.userid = str(user.get("id"))
            self.auth.open_id = str(
                user.get("openId", user.get("openid", self.auth.open_id))
            ).strip()
            self.auth.ma_open_id = str(
                user.get("maopenId", user.get("maopenid", self.auth.ma_open_id))
            ).strip()

        response["auth_update"] = self.auth.to_dict()
        return response

    def query_user_role(self, userid: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if userid:
            params["userid"] = userid
        signed = self._with_sign(params, ps_mode=False, force_userid=True)
        return self._request(
            "GET", "/TennisCenterInterface/compMember/queryUserRole.action", params=signed
        )

    def query_dates(self, user_id: Optional[str] = None, book_type: int = 0) -> Dict[str, Any]:
        uid = user_id or self.auth.userid
        payload = {"userId": uid, "type": str(book_type)}
        return self._request(
            "POST", "/TennisCenterInterface/pmPark/queryBookDate.action", form=payload
        )

    def query_courts(
        self,
        *,
        date: str,
        parktypeinfo: str,
        ballcode: str,
        cardtypecode: str = "-1",
        userid: Optional[str] = None,
        parkstatus: str = "0",
        changefieldtype: str = "0",
        reserve_detail_ids: str = "",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "ballcode": str(ballcode),
            "parktypeinfo": str(parktypeinfo),
            "userid": str(userid or self.auth.userid),
            "cardtypecode": str(cardtypecode),
            "date": date,
            "parkstatus": str(parkstatus),
            "changefieldtype": str(changefieldtype),
        }
        if str(changefieldtype) == "1" and reserve_detail_ids:
            payload["reserveDetailIds"] = reserve_detail_ids

        signed = self._with_sign(payload, ps_mode=True, force_userid=False)
        rsp = self._request(
            "POST",
            "/TennisCenterInterface/pmPark/v2/getParkShowByParam.action",
            form=signed,
        )
        return self._postprocess_response(rsp, decrypt_datas=True, decrypt_ps=False)

    def show_price_by_user(
        self, park_list: Sequence[Dict[str, Any]], userid: Optional[str] = None
    ) -> Dict[str, Any]:
        payload = {
            "userid": str(userid or self.auth.userid),
            "parkList": json.dumps(list(park_list), ensure_ascii=False),
        }
        return self._request(
            "POST", "/TennisCenterInterface/pmPark/showPriceByUser.action", form=payload
        )

    def add_park_order(
        self,
        park_list: Sequence[Dict[str, Any]],
        *,
        paywaycode: int | str = 2,
        mobile: str = "",
        ordercode: str = "",
        captcha_verification: str = "",
        add_order_type: str = "wx",
        userid: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "userid": str(userid or self.auth.userid),
            "parkList": json.dumps(list(park_list), ensure_ascii=False),
            "paywaycode": str(paywaycode),
            "addOrderType": add_order_type,
            "mobile": mobile,
            "ordercode": ordercode,
            "captchaVerification": captcha_verification,
        }
        signed = self._with_sign(payload, ps_mode=True, force_userid=False)
        rsp = self._request(
            "POST", "/TennisCenterInterface/pmPark/addParkOrder.action", form=signed
        )
        return self._postprocess_response(rsp, decrypt_datas=True, decrypt_ps=False)

    def change_field(
        self,
        park_list: Sequence[Dict[str, Any]],
        *,
        old_order_no: str,
        reserve_detail_ids: str,
        paywaycode: int | str = 2,
        mobile: str = "",
        ordercode: str = "",
        add_order_type: str = "wx",
        userid: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "userid": str(userid or self.auth.userid),
            "parkList": json.dumps(list(park_list), ensure_ascii=False),
            "paywaycode": str(paywaycode),
            "addOrderType": add_order_type,
            "mobile": mobile,
            "ordercode": ordercode,
            "oldorderNo": old_order_no,
            "reserveDetailIds": reserve_detail_ids,
        }
        signed = self._with_sign(payload, ps_mode=False, force_userid=False)
        rsp = self._request(
            "POST", "/TennisCenterInterface/pmPark/changefield.action", form=signed
        )
        return self._postprocess_response(rsp, decrypt_datas=True, decrypt_ps=False)

    def get_park_order_state(self, order_no: str) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/TennisCenterInterface/pmPark/getParkOrderState.action",
            form={"orderNo": order_no},
        )

    def get_card_by_user(self, userid: Optional[str] = None) -> Dict[str, Any]:
        payload = {"userid": str(userid or self.auth.userid)}
        return self._request(
            "POST", "/TennisCenterInterface/umCard/getCardByUser.action", form=payload
        )

    def query_is_code_time(self, userid: Optional[str] = None) -> Dict[str, Any]:
        payload = {"userid": str(userid or self.auth.userid)}
        return self._request(
            "POST", "/TennisCenterInterface/umUser/queryIsCodeTime.action", form=payload
        )

    def wx_js_api_pay(
        self,
        *,
        open_id: str,
        order_no: str,
        pay: str | int | float,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "openId": open_id,
            "orderNo": order_no,
            "pay": str(pay),
            "userId": str(user_id or self.auth.userid),
        }
        return self._request(
            "POST", "/TennisCenterInterface/wx/wxJsApiPay.action", form=payload
        )

    def get_info_by_order_no(self, order_no: str, userid: Optional[str] = None) -> Dict[str, Any]:
        payload = {"orderNo": order_no, "userid": str(userid or self.auth.userid)}
        signed = self._with_sign(payload, ps_mode=False, force_userid=False)
        rsp = self._request(
            "POST", "/TennisCenterInterface/omOrder/getInfoByOrderNo.action", form=signed
        )
        return self._postprocess_response(rsp, decrypt_datas=True, decrypt_ps=False)


class TennisBooker:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        with self.config_path.open("r", encoding="utf-8") as fh:
            self.cfg = yaml.safe_load(fh) or {}

        self.client = TennisClient(self.cfg)
        self.success_event = threading.Event()
        self._lock = threading.Lock()
        self.success_order: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Config helpers
    # ------------------------------------------------------------------
    @property
    def auth_cfg(self) -> Dict[str, Any]:
        return self.cfg.setdefault("auth", {})

    @property
    def court_cfg(self) -> Dict[str, Any]:
        return self.cfg.setdefault("court", {})

    @property
    def strategy_cfg(self) -> Dict[str, Any]:
        return self.cfg.setdefault("strategy", {})

    @property
    def notify_cfg(self) -> Dict[str, Any]:
        return self.cfg.setdefault("notify", {})

    def save_config(self) -> None:
        with self.config_path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(self.cfg, fh, allow_unicode=True, sort_keys=False)

    # ------------------------------------------------------------------
    # High-level operations
    # ------------------------------------------------------------------
    def show_info(self) -> None:
        log.info("=" * 60)
        log.info("Base URL: %s", self.client.base_url)
        log.info("User ID: %s", self.client.auth.userid or "<empty>")
        log.info("Target date: %s", self.court_cfg.get("target_date"))
        log.info("Target time: %s", self.court_cfg.get("target_time"))
        log.info("Court type code: %s", self.court_cfg.get("parktypecode", "6"))
        log.info("Ball code: %s", self.court_cfg.get("ballcode", "<auto>"))
        log.info("Threads: %s", self.strategy_cfg.get("threads", 3))
        log.info("=" * 60)

    def login(
        self,
        mobile: str,
        phonecode: str,
        union_id: str = "",
        ma_open_id: str = "",
        save_to_config: bool = False,
    ) -> Dict[str, Any]:
        rsp = self.client.phone_code_login(
            mobile=mobile,
            phonecode=phonecode,
            union_id=union_id,
            ma_open_id=ma_open_id,
        )
        if self.client.is_success(rsp):
            auth_update = rsp.get("auth_update", {})
            self.auth_cfg.update(auth_update)
            self.auth_cfg["mobile"] = mobile
            if union_id:
                self.auth_cfg["unionId"] = union_id
            if ma_open_id:
                self.auth_cfg["maopenId"] = ma_open_id
            if save_to_config:
                self.save_config()
                log.info("Auth saved to %s", self.config_path)
        return rsp

    def test_connectivity(self) -> bool:
        ok = True

        dates_rsp = self.client.query_dates()
        if self.client.is_success(dates_rsp):
            data = self.client.unwrap_payload(dates_rsp)
            count = len(data) if isinstance(data, list) else 0
            log.info("queryBookDate ok, available dates: %d", count)
        else:
            ok = False
            log.error("queryBookDate failed: %s", dates_rsp.get("respMsg"))

        # Signed endpoint test only when sign credentials are available.
        try:
            role_rsp = self.client.query_user_role()
            if self.client.is_success(role_rsp):
                log.info("queryUserRole ok.")
            else:
                ok = False
                log.error("queryUserRole failed: %s", role_rsp.get("respMsg"))
        except Exception as exc:  # noqa: BLE001
            ok = False
            log.error("queryUserRole skipped/failed: %s", exc)

        return ok

    def resolve_ballcode(self) -> str:
        configured = str(self.court_cfg.get("ballcode", "")).strip()
        if configured:
            return configured

        rsp = self.client.get_home_info()
        payload = self.client.unwrap_payload(rsp)
        if isinstance(payload, dict):
            first_types = payload.get("parkFirstType", [])
            target_type = str(self.court_cfg.get("parktypecode", "6"))
            for ball_group in first_types:
                ballcode = ball_group.get("id")
                for item in ball_group.get("parktype", []):
                    if str(item.get("id")) == target_type and ballcode is not None:
                        return str(ballcode)

        # Safe fallback if home info is unavailable.
        return "1"

    def _target_booking_datetime(self) -> Optional[datetime]:
        explicit = str(self.strategy_cfg.get("booking_open_datetime", "")).strip()
        if explicit:
            return datetime.strptime(explicit, "%Y-%m-%d %H:%M:%S")

        open_time = str(self.strategy_cfg.get("booking_open_time", "")).strip()
        if not open_time:
            return None

        today = datetime.now().date()
        return datetime.strptime(f"{today} {open_time}", "%Y-%m-%d %H:%M:%S")

    def wait_for_open_time(self) -> None:
        target_dt = self._target_booking_datetime()
        if target_dt is None:
            return

        self.client.sync_time_ntp()
        advance_ms = int(self.strategy_cfg.get("advance_ms", 500))
        target_ms = int(target_dt.timestamp() * 1000) - advance_ms

        now = self.client.now_ms()
        if now >= target_ms:
            log.info("Open time already reached, starting immediately.")
            return

        log.info("Booking open time: %s", target_dt.strftime("%Y-%m-%d %H:%M:%S"))
        log.info("Will start %d ms early.", advance_ms)

        while True:
            now = self.client.now_ms()
            remain = target_ms - now
            if remain <= 0:
                return
            if remain > 10000:
                log.info("Remaining %.1f s...", remain / 1000)
                time.sleep(min(5.0, remain / 1000 - 1))
            elif remain > 1000:
                time.sleep(0.2)
            else:
                time.sleep(0.01)

    # ------------------------------------------------------------------
    # Court selection
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_int(value: Any, default: int = -1) -> int:
        try:
            return int(value)
        except Exception:  # noqa: BLE001
            return default

    @staticmethod
    def _parse_price(value: Any, default: int = 0) -> int:
        try:
            return int(float(value))
        except Exception:  # noqa: BLE001
            return default

    def _collect_available_slots(self, payload: Dict[str, Any], target_date: str) -> List[Dict[str, Any]]:
        slots: List[Dict[str, Any]] = []
        ven_list = payload.get("venList", []) if isinstance(payload, dict) else []
        for venue in ven_list:
            venue_name = str(venue.get("vname", ""))
            price_map: Dict[Tuple[int, int], int] = {}
            for price_info in venue.get("price", []) or []:
                park_id = self._parse_int(price_info.get("parkid"))
                hour = self._parse_int(price_info.get("time"))
                price_val = self._parse_price(price_info.get("price", 0), default=0)
                if park_id >= 0 and hour >= 0:
                    price_map[(park_id, hour)] = price_val

            for park in venue.get("park", []) or []:
                park_id = self._parse_int(park.get("id"))
                park_name = str(park.get("parkname", ""))
                reserve_items = park.get("reserve", []) or []
                for reserve in reserve_items:
                    status = self._parse_int(reserve.get("bookstatus"))
                    if status != 0:
                        continue
                    hour = self._parse_int(reserve.get("time"))
                    if hour < 0:
                        continue
                    price = reserve.get("price")
                    if price is None:
                        price = price_map.get((park_id, hour), 0)
                    slot = {
                        "date": target_date,
                        "time": hour,
                        "parkid": park_id,
                        "parkname": park_name,
                        "venuename": venue_name,
                        "price": self._parse_price(price, default=0),
                    }
                    slots.append(slot)
        return slots

    def _select_slots(self, slots: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not slots:
            return []

        target_time = str(self.court_cfg.get("target_time", "")).strip()
        target_hour: Optional[int] = None
        if target_time:
            try:
                target_hour = int(target_time.split(":", 1)[0])
            except Exception:  # noqa: BLE001
                target_hour = None

        duration = int(self.court_cfg.get("duration_hours", 1))
        duration = max(1, duration)

        preferred = {
            str(name).strip().lower()
            for name in (self.court_cfg.get("preferred_courts") or [])
            if str(name).strip()
        }

        grouped: Dict[int, Dict[int, Dict[str, Any]]] = {}
        for item in slots:
            park_id = self._parse_int(item.get("parkid"))
            hour = self._parse_int(item.get("time"))
            if park_id < 0 or hour < 0:
                continue
            grouped.setdefault(park_id, {})[hour] = item

        candidates: List[Tuple[int, int, int, str, List[Dict[str, Any]]]] = []
        for park_id, by_hour in grouped.items():
            for start_hour in sorted(by_hour):
                sequence: List[Dict[str, Any]] = []
                ok = True
                for offset in range(duration):
                    slot = by_hour.get(start_hour + offset)
                    if slot is None:
                        ok = False
                        break
                    sequence.append(slot)
                if not ok:
                    continue

                sample = sequence[0]
                park_name = str(sample.get("parkname", ""))
                venue_name = str(sample.get("venuename", ""))
                full_name = f"{venue_name}{park_name}".strip().lower()
                preferred_score = 0
                if preferred:
                    if park_name.lower() in preferred or full_name in preferred:
                        preferred_score = 0
                    else:
                        preferred_score = 1
                time_score = abs(start_hour - target_hour) if target_hour is not None else 0
                candidates.append((preferred_score, time_score, start_hour, park_name, sequence))

        if not candidates:
            return []
        candidates.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
        return candidates[0][4]

    @staticmethod
    def _to_park_list(slots: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for slot in slots:
            hour = int(slot["time"])
            park_name = str(slot["parkname"])
            item = {
                "date": str(slot["date"]),
                "time": hour,
                "parkid": int(slot["parkid"]),
                "id": f"{park_name}{hour}",
                "parkname": park_name,
                "timeStr": f"{hour}:00-{hour + 1}:00",
            }
            if slot.get("price"):
                item["price"] = int(slot["price"])
            result.append(item)
        return result

    @staticmethod
    def _extract_order_no(payload: Any) -> str:
        if isinstance(payload, dict):
            for key in ("orderNo", "orderno", "order_no"):
                if payload.get(key):
                    return str(payload[key])
        return ""

    # ------------------------------------------------------------------
    # Booking pipeline
    # ------------------------------------------------------------------
    def _notify_success(self, order_no: str, park_list: Sequence[Dict[str, Any]]) -> None:
        msg_lines = [
            "Tennis booking succeeded.",
            f"orderNo: {order_no}",
            f"slots: {json.dumps(list(park_list), ensure_ascii=False)}",
        ]
        msg = "\n".join(msg_lines)

        server_chan_key = str(self.notify_cfg.get("server_chan_key", "")).strip()
        if server_chan_key:
            try:
                requests.post(
                    f"https://sctapi.ftqq.com/{server_chan_key}.send",
                    data={"title": "Tennis booking success", "desp": msg},
                    timeout=5,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("ServerChan notify failed: %s", exc)

        bark_url = str(self.notify_cfg.get("bark_url", "")).strip().rstrip("/")
        if bark_url:
            try:
                requests.get(f"{bark_url}/TennisBooking/{order_no}", timeout=5)
            except Exception as exc:  # noqa: BLE001
                log.warning("Bark notify failed: %s", exc)

    def _try_book(self, thread_id: int, target_date: str, ballcode: str) -> bool:
        retries = int(self.strategy_cfg.get("max_retries", 30))
        sleep_s = int(self.strategy_cfg.get("retry_interval_ms", 200)) / 1000.0

        parktypeinfo = str(self.court_cfg.get("parktypecode", "6"))
        cardtypecode = str(self.court_cfg.get("cardtypecode", "-1"))
        parkstatus = str(self.court_cfg.get("parkstatus", "0"))
        paywaycode = self.court_cfg.get("paywaycode", 2)
        mobile = str(self.auth_cfg.get("mobile", "")).strip()
        ordercode = str(self.auth_cfg.get("ordercode", "")).strip()
        captcha = str(self.auth_cfg.get("captchaVerification", "")).strip()

        for attempt in range(1, retries + 1):
            if self.success_event.is_set():
                return False

            log.info("[T%s] Attempt %d/%d", thread_id, attempt, retries)

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
                log.warning("[T%s] query_courts failed: %s", thread_id, courts_rsp.get("respMsg"))
                time.sleep(sleep_s)
                continue

            payload = self.client.unwrap_payload(courts_rsp)
            if not isinstance(payload, dict):
                log.warning("[T%s] query_courts payload empty.", thread_id)
                time.sleep(sleep_s)
                continue

            slots = self._collect_available_slots(payload, target_date=target_date)
            selected_slots = self._select_slots(slots)
            if not selected_slots:
                log.info("[T%s] No matching slots currently available.", thread_id)
                time.sleep(sleep_s)
                continue

            park_list = self._to_park_list(selected_slots)
            log.info(
                "[T%s] Candidate slots: %s",
                thread_id,
                json.dumps(park_list, ensure_ascii=False),
            )

            price_rsp = self.client.show_price_by_user(park_list, userid=self.client.auth.userid)
            if not self.client.is_success(price_rsp):
                log.warning("[T%s] showPriceByUser failed: %s", thread_id, price_rsp.get("respMsg"))
                time.sleep(sleep_s)
                continue
            price_payload = self.client.unwrap_payload(price_rsp)
            wx_sum = None
            if isinstance(price_payload, dict):
                wx_price = price_payload.get("wxPrice", {})
                if isinstance(wx_price, dict):
                    wx_sum = wx_price.get("sum")
            if wx_sum is not None:
                log.info("[T%s] wxPrice sum: %s", thread_id, wx_sum)

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
                log.warning("[T%s] addParkOrder failed: %s", thread_id, order_rsp.get("respMsg"))
                time.sleep(sleep_s)
                continue

            order_payload = self.client.unwrap_payload(order_rsp)
            order_no = self._extract_order_no(order_payload)
            if not order_no:
                log.warning("[T%s] addParkOrder succeeded but orderNo missing.", thread_id)
                time.sleep(sleep_s)
                continue

            state_rsp = self.client.get_park_order_state(order_no)
            if self.client.is_success(state_rsp):
                log.info("[T%s] getParkOrderState success for %s", thread_id, order_no)
            else:
                log.warning(
                    "[T%s] getParkOrderState response: %s",
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

        return False

    def auto_book(self) -> bool:
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

        ballcode = self.resolve_ballcode()
        log.info("Using ballcode=%s, parktypecode=%s", ballcode, self.court_cfg.get("parktypecode", "6"))

        self.wait_for_open_time()
        log.info("Starting booking threads...")

        threads = max(1, int(self.strategy_cfg.get("threads", 3)))
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [
                pool.submit(self._try_book, idx + 1, target_date, ballcode)
                for idx in range(threads)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:  # noqa: BLE001
                    log.error("Thread failure: %s", exc)

        if self.success_event.is_set():
            log.info("Booking succeeded: %s", json.dumps(self.success_order, ensure_ascii=False))
            return True
        log.info("Booking did not succeed within retry limits.")
        return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="tennis.bjofp.cn auto-booking bot")
    parser.add_argument(
        "-c",
        "--config",
        default="tennis_config.yaml",
        help="Path to config yaml (default: tennis_config.yaml)",
    )
    parser.add_argument(
        "action",
        choices=["info", "login", "book", "test"],
        help="info=show config, login=refresh auth tokens, book=auto booking, test=API checks",
    )

    parser.add_argument("--mobile", default="", help="Mobile number for login")
    parser.add_argument("--phonecode", default="", help="SMS phone code for login")
    parser.add_argument("--union-id", default="", help="unionId for login (optional)")
    parser.add_argument("--maopen-id", default="", help="maopenId for login (optional)")
    parser.add_argument(
        "--save",
        action="store_true",
        help="Persist login auth result back to config file",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    bot = TennisBooker(args.config)

    if args.action == "info":
        bot.show_info()
        return

    if args.action == "test":
        ok = bot.test_connectivity()
        raise SystemExit(0 if ok else 1)

    if args.action == "login":
        mobile = args.mobile or str(bot.auth_cfg.get("mobile", "")).strip()
        phonecode = args.phonecode or str(bot.auth_cfg.get("phonecode", "")).strip()
        union_id = args.union_id or str(bot.auth_cfg.get("unionId", "")).strip()
        ma_open_id = args.maopen_id or str(bot.auth_cfg.get("maopenId", "")).strip()
        if not mobile or not phonecode:
            raise SystemExit("login requires --mobile and --phonecode (or auth.mobile/auth.phonecode in config)")

        rsp = bot.login(
            mobile=mobile,
            phonecode=phonecode,
            union_id=union_id,
            ma_open_id=ma_open_id,
            save_to_config=args.save,
        )
        if bot.client.is_success(rsp):
            log.info("login success. updated auth: %s", json.dumps(rsp.get("auth_update", {}), ensure_ascii=False))
            raise SystemExit(0)
        log.error("login failed: %s", rsp.get("respMsg"))
        raise SystemExit(1)

    if args.action == "book":
        bot.show_info()
        success = bot.auto_book()
        raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
