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
import os
import random
import re
import socket
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from Crypto.Cipher import AES
from requests.adapters import HTTPAdapter

from config_store import load_config as load_db_config
from config_store import save_config as save_db_config

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


class FastHTTPAdapter(HTTPAdapter):
    """HTTPAdapter with TCP_NODELAY to disable Nagle buffering (~40ms saving)."""

    def init_poolmanager(self, *args, **kwargs):
        import sys
        socket_options = [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
        if sys.platform == "linux":
            TCP_QUICKACK = 12  # noqa: N806
            socket_options.append((socket.IPPROTO_TCP, TCP_QUICKACK, 1))
        kwargs.setdefault("socket_options", socket_options)
        super().init_poolmanager(*args, **kwargs)


@dataclass
class AuthState:
    userid: str = ""
    bnglokbj: str = ""
    csc: str = ""
    cdc: str = ""
    open_id: str = ""
    ma_open_id: str = ""
    union_id: str = ""

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
            union_id=str(auth.get("unionId", auth.get("unionid", ""))).strip(),
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "userid": self.userid,
            "bnglokbj": self.bnglokbj,
            "csc": self.csc,
            "cdc": self.cdc,
            "openId": self.open_id,
            "maopenId": self.ma_open_id,
            "unionId": self.union_id,
        }


class TennisClient:
    BASE_URL = "https://tennis.bjofp.cn"
    LOGIN_AES_SEED = "gjwqerxxzxasdfqw"
    PS_DECRYPT_HEX = "d7e0762294db597f05d77415b0584fb0"
    # Key for encrypting loginname in getPhoneCode.action:
    # mini-program encrypCode() uses MD5("yesixur!@#$1a2b3c").toUpperCase()[:16]
    LOGINNAME_AES_KEY = hashlib.md5(b"yesixur!@#$1a2b3c").hexdigest().upper()[:16]

    def __init__(self, config: Dict[str, Any], time_offset_ms: Optional[int] = None):
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
        adapter = FastHTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        # DNS pinning: resolve once, reuse for all requests
        self._pinned_ip: Optional[str] = None
        self._pin_dns()
        self.time_offset_ms = time_offset_ms if time_offset_ms is not None else 0

    def _pin_dns(self) -> None:
        """Resolve the API host once and pin it in the session."""
        from urllib.parse import urlparse
        try:
            host = urlparse(self.base_url).hostname
            if host:
                ip = socket.getaddrinfo(host, 443, socket.AF_INET)[0][4][0]
                self._pinned_ip = ip
                # Override host resolution via a custom adapter is complex;
                # instead, just do a pre-resolve to warm the OS DNS cache.
                log.info("DNS pinned: %s -> %s", host, ip)
        except Exception as exc:  # noqa: BLE001
            log.warning("DNS pin failed: %s", exc)

    # ------------------------------------------------------------------
    # Time sync
    # ------------------------------------------------------------------
    def sync_time_ntp(self, hosts: Optional[List[str]] = None) -> None:
        if ntplib is None:
            log.warning("ntplib is not installed, skipping NTP sync.")
            return
        if hosts is None:
            hosts = ["ntp.aliyun.com", "ntp.tencent.com", "cn.ntp.org.cn", "pool.ntp.org"]
        offsets: List[float] = []
        client = ntplib.NTPClient()
        for host in hosts:
            try:
                rsp = client.request(host, version=3, timeout=2)
                offsets.append(rsp.offset * 1000)
                log.info("NTP %s offset: %+.1f ms", host, rsp.offset * 1000)
            except Exception as exc:  # noqa: BLE001
                log.warning("NTP %s failed: %s", host, exc)
        if offsets:
            offsets.sort()
            mid = len(offsets) // 2
            median = offsets[mid] if len(offsets) % 2 else (offsets[mid - 1] + offsets[mid]) / 2
            self.time_offset_ms = int(median)
            log.info("NTP median offset: %+d ms (from %d servers)", self.time_offset_ms, len(offsets))
        else:
            log.warning("All NTP servers failed, offset remains %+d ms", self.time_offset_ms)

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self.time_offset_ms

    def pre_warm_connections(self, count: int = 5) -> None:
        """Establish multiple TCP + TLS connections before the critical window."""
        def _warm(_i: int) -> None:
            try:
                self.session.head(self.base_url, timeout=5)
            except Exception:  # noqa: BLE001
                pass
        from concurrent.futures import ThreadPoolExecutor
        try:
            with ThreadPoolExecutor(max_workers=count) as pool:
                list(pool.map(_warm, range(count)))
            log.info("Pre-warmed %d connections to %s", count, self.base_url)
        except Exception as exc:  # noqa: BLE001
            log.warning("Connection pre-warm failed: %s", exc)

    def pre_warm_connection(self) -> None:
        """Backward-compatible single connection pre-warm."""
        self.pre_warm_connections(count=1)

    # ------------------------------------------------------------------
    # Sign/decrypt internals (reverse-engineered from mini-program)
    # ------------------------------------------------------------------
    @staticmethod
    def _random_nonce(min_len: int = 10, max_len: int = 32) -> str:
        size = random.randint(min_len, max_len)
        return os.urandom(size).hex()[:size]

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
                "Run login first or populate auth in MongoDB config."
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

    @classmethod
    def _aes_ecb_encrypt_b64(cls, plaintext: str, key: str) -> str:
        """AES-ECB encrypt plaintext with PKCS7 padding, return base64 string."""
        raw = plaintext.encode("utf-8")
        pad_len = 16 - (len(raw) % 16)
        raw += bytes([pad_len] * pad_len)
        cipher = AES.new(key.encode("utf-8"), AES.MODE_ECB)
        return base64.b64encode(cipher.encrypt(raw)).decode("utf-8")

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
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            rsp = self.session.request(
                method=method,
                url=url,
                params=params,
                data=form,
                json=json_body,
                timeout=timeout if timeout is not None else self.timeout,
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

    def get_phone_code(self, mobile: str, code_type: int = 3) -> Dict[str, Any]:
        # mini-program encrypCode(): AES-ECB with key = MD5("yesixur!@#$1a2b3c").upper()[:16]
        loginname = self._aes_ecb_encrypt_b64(mobile, self.LOGINNAME_AES_KEY)
        params = {"loginname": loginname, "type": str(code_type)}
        return self._request("GET", "/TennisCenterInterface/umUser/getPhoneCode.action", params=params)

    def phone_code_login(
        self,
        mobile: str,
        phonecode: str,
        union_id: str = "",
        ma_open_id: str = "",
    ) -> Dict[str, Any]:
        effective_union_id = union_id or self.auth.union_id
        effective_ma_open_id = ma_open_id or self.auth.ma_open_id
        params = {
            "mobile": mobile,
            "phonecode": phonecode,
            "unionId": effective_union_id,
            "maopenId": effective_ma_open_id,
        }

        # HAR confirms: this endpoint is GET with query params.
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
            self.auth.union_id = str(
                user.get("unionId", user.get("unionid", self.auth.union_id))
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
        timeout: Optional[int] = None,
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
            timeout=timeout,
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
        timeout: Optional[int] = None,
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
            "POST",
            "/TennisCenterInterface/pmPark/addParkOrder.action",
            form=signed,
            timeout=timeout,
        )
        return self._postprocess_response(rsp, decrypt_datas=True, decrypt_ps=False)

    def add_park_order_raw(
        self,
        park_list: Sequence[Dict[str, Any]],
        *,
        paywaycode: int | str = 2,
        mobile: str = "",
        ordercode: str = "",
        captcha_verification: str = "",
        add_order_type: str = "wx",
        userid: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Like add_park_order but skip AES decrypt on failure (~1ms saving)."""
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
            "POST",
            "/TennisCenterInterface/pmPark/addParkOrder.action",
            form=signed,
            timeout=timeout,
        )
        # Only decrypt on success to save time on failures
        if self.is_success(rsp):
            return self._postprocess_response(rsp, decrypt_datas=True, decrypt_ps=False)
        return rsp

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
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.cfg = config if config is not None else load_db_config()

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
        save_db_config(self.cfg)

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
        log.info("Burst count: %s", self.strategy_cfg.get("burst_count", 15))
        log.info("Direct fire: %s (threads=%s)", self.strategy_cfg.get("direct_fire", True), self.strategy_cfg.get("direct_fire_threads", 2))
        log.info("Skip price check: %s", self.strategy_cfg.get("skip_price_check", False))
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
                log.info("Auth saved to MongoDB config collection.")
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
                ballcode = ball_group.get("ballcode")
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
        dt = datetime.strptime(f"{today} {open_time}", "%Y-%m-%d %H:%M:%S")
        # Mirror the admin logic: "00:00:00" means next-day midnight (24:00),
        # so advance by one day to avoid returning a past datetime.
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            dt += timedelta(days=1)
        return dt

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
            elif remain > 100:
                time.sleep(0.005)
            else:
                while self.client.now_ms() < target_ms:
                    pass
                return

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

    def _collect_available_slots(
        self, payload: Dict[str, Any], target_date: str, include_booked: bool = False,
    ) -> List[Dict[str, Any]]:
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
                    if not include_booked and status != 0:
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

    def _rank_candidates(
        self, slots: Sequence[Dict[str, Any]],
    ) -> List[Tuple[int, int, int, str, List[Dict[str, Any]]]]:
        if not slots:
            return []

        target_time = str(self.court_cfg.get("target_time", "")).strip()
        target_hour: Optional[int] = None
        if target_time:
            try:
                target_hour = int(target_time.split(":", 1)[0])
            except Exception:  # noqa: BLE001
                target_hour = None

        target_time_end = str(self.court_cfg.get("target_time_end", "")).strip()
        target_hour_end: Optional[int] = None
        if target_time_end:
            try:
                target_hour_end = int(target_time_end.split(":", 1)[0])
            except Exception:  # noqa: BLE001
                target_hour_end = None

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

                if target_hour is not None and start_hour < target_hour:
                    continue
                if target_hour_end is not None and (start_hour + duration) > target_hour_end:
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

        candidates.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
        return candidates

    def _select_slots(self, slots: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        candidates = self._rank_candidates(slots)
        return candidates[0][4] if candidates else []

    def _select_slots_ranked(
        self, slots: Sequence[Dict[str, Any]], max_results: int = 5,
    ) -> List[List[Dict[str, Any]]]:
        """Return top N candidate slot groups ranked by preference."""
        candidates = self._rank_candidates(slots)
        return [c[4] for c in candidates[:max_results]]

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
    # Pre-build & direct fire
    # ------------------------------------------------------------------
    def _pre_query_structure(
        self, target_date: str, ballcode: str,
    ) -> List[List[Dict[str, Any]]]:
        """Query courts before open time to pre-build order payloads.

        Returns up to 5 candidate park_lists ranked by preference. These can
        be fired directly via add_park_order at open time, saving the query
        round-trip entirely.
        """
        parktypeinfo = str(self.court_cfg.get("parktypecode", "6"))
        cardtypecode = str(self.court_cfg.get("cardtypecode", "-1"))
        parkstatus = str(self.court_cfg.get("parkstatus", "0"))

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
            log.warning("Pre-query structure failed: %s", courts_rsp.get("respMsg"))
            return []

        payload = self.client.unwrap_payload(courts_rsp)
        if not isinstance(payload, dict):
            return []

        all_slots = self._collect_available_slots(payload, target_date, include_booked=False)
        ranked = self._select_slots_ranked(all_slots, max_results=5)
        if not ranked:
            log.warning("Pre-query: no matching slot groups found in court structure.")
            return []

        result = [self._to_park_list(group) for group in ranked]
        for i, pl in enumerate(result):
            log.info("Pre-built candidate #%d: %s", i + 1, json.dumps(pl, ensure_ascii=False))
        return result

    def pre_build_all_park_lists(
        self, target_date: str, ballcode: str,
    ) -> List[Tuple[int, int, str, List[Dict[str, Any]]]]:
        """Query courts and return ALL ranked candidates for the orchestrator.

        Returns list of (priority_score, start_hour, parkname, park_list).
        Unlike _pre_query_structure which caps at 5, this returns everything.
        """
        parktypeinfo = str(self.court_cfg.get("parktypecode", "6"))
        cardtypecode = str(self.court_cfg.get("cardtypecode", "-1"))
        parkstatus = str(self.court_cfg.get("parkstatus", "0"))

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
            log.warning("Pre-build all park_lists failed: %s", courts_rsp.get("respMsg"))
            return []

        payload = self.client.unwrap_payload(courts_rsp)
        if not isinstance(payload, dict):
            return []

        all_slots = self._collect_available_slots(payload, target_date, include_booked=False)
        candidates = self._rank_candidates(all_slots)
        if not candidates:
            log.warning("Pre-build: no matching slot groups found.")
            return []

        result = []
        for preferred_score, time_score, start_hour, park_name, slots in candidates:
            park_list = self._to_park_list(slots)
            priority = preferred_score * 1000 + time_score
            result.append((priority, start_hour, park_name, park_list))

        log.info("Pre-built %d candidate park_lists for orchestrator.", len(result))
        for i, (pri, hour, name, pl) in enumerate(result):
            log.info("  #%d: priority=%d, %s %02d:00, slots=%s", i + 1, pri, name, hour, json.dumps(pl, ensure_ascii=False))
        return result

    def _direct_fire_worker(
        self, thread_id: int, pre_park_lists: List[List[Dict[str, Any]]],
    ) -> bool:
        """Fire pre-built add_park_order requests without querying courts first."""
        if not pre_park_lists:
            return False

        retries = int(self.strategy_cfg.get("max_retries", 30))
        burst_timeout = int(self.strategy_cfg.get("burst_timeout_sec", 5))
        paywaycode = self.court_cfg.get("paywaycode", 2)
        mobile = str(self.auth_cfg.get("mobile", "")).strip()
        ordercode = str(self.auth_cfg.get("ordercode", "")).strip()
        captcha = str(self.auth_cfg.get("captchaVerification", "")).strip()

        for attempt in range(1, retries + 1):
            if self.success_event.is_set():
                return False

            park_list = pre_park_lists[(attempt - 1) % len(pre_park_lists)]

            log.info("[DF%s] Direct fire attempt %d/%d", thread_id, attempt, retries)

            t0 = time.time()
            order_rsp = self.client.add_park_order(
                park_list,
                paywaycode=paywaycode,
                mobile=mobile,
                ordercode=ordercode,
                captcha_verification=captcha,
                add_order_type="wx",
                userid=self.client.auth.userid,
                timeout=burst_timeout,
            )
            elapsed_ms = (time.time() - t0) * 1000

            if not self.client.is_success(order_rsp):
                log.info(
                    "[DF%s] Failed (%.0fms): %s",
                    thread_id, elapsed_ms, order_rsp.get("respMsg"),
                )
                continue

            order_payload = self.client.unwrap_payload(order_rsp)
            order_no = self._extract_order_no(order_payload)
            if not order_no:
                log.warning("[DF%s] Order succeeded but no orderNo (%.0fms)", thread_id, elapsed_ms)
                continue

            log.info("[DF%s] SUCCESS! orderNo=%s (%.0fms)", thread_id, order_no, elapsed_ms)

            state_rsp = self.client.get_park_order_state(order_no)
            with self._lock:
                if not self.success_event.is_set():
                    self.success_event.set()
                    self.success_order = {
                        "orderNo": order_no,
                        "parkList": park_list,
                        "orderStateResponse": state_rsp,
                    }
                    self._notify_success(order_no, park_list)
            return True

        return False

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
        retry_interval_ms = int(self.strategy_cfg.get("retry_interval_ms", 200))
        burst_count = int(self.strategy_cfg.get("burst_count", 15))
        burst_timeout = int(self.strategy_cfg.get("burst_timeout_sec", 5))
        skip_price_check = bool(self.strategy_cfg.get("skip_price_check", False))

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

            in_burst = attempt <= burst_count
            effective_timeout = burst_timeout if in_burst else self.client.timeout

            log.info("[T%s] Attempt %d/%d%s", thread_id, attempt, retries, " [BURST]" if in_burst else "")

            t0 = time.time()
            courts_rsp = self.client.query_courts(
                date=target_date,
                parktypeinfo=parktypeinfo,
                ballcode=ballcode,
                cardtypecode=cardtypecode,
                userid=self.client.auth.userid,
                parkstatus=parkstatus,
                changefieldtype="0",
                timeout=effective_timeout,
            )
            query_ms = (time.time() - t0) * 1000

            if not self.client.is_success(courts_rsp):
                log.warning("[T%s] query_courts failed (%.0fms): %s", thread_id, query_ms, courts_rsp.get("respMsg"))
                if not in_burst:
                    time.sleep(retry_interval_ms / 1000.0)
                continue

            payload = self.client.unwrap_payload(courts_rsp)
            if not isinstance(payload, dict):
                log.warning("[T%s] query_courts payload empty (%.0fms).", thread_id, query_ms)
                if not in_burst:
                    time.sleep(retry_interval_ms / 1000.0)
                continue

            slots = self._collect_available_slots(payload, target_date=target_date)
            selected_slots = self._select_slots(slots)
            if not selected_slots:
                log.info("[T%s] No matching slots (%.0fms).", thread_id, query_ms)
                if not in_burst:
                    time.sleep(retry_interval_ms / 1000.0)
                continue

            park_list = self._to_park_list(selected_slots)
            log.info(
                "[T%s] FOUND slots (%.0fms): %s",
                thread_id,
                query_ms,
                json.dumps(park_list, ensure_ascii=False),
            )

            price_payload = None
            if not skip_price_check and not in_burst:
                price_rsp = self.client.show_price_by_user(park_list, userid=self.client.auth.userid)
                if not self.client.is_success(price_rsp):
                    log.warning("[T%s] showPriceByUser failed: %s", thread_id, price_rsp.get("respMsg"))
                    continue
                price_payload = self.client.unwrap_payload(price_rsp)
                wx_sum = None
                if isinstance(price_payload, dict):
                    wx_price = price_payload.get("wxPrice", {})
                    if isinstance(wx_price, dict):
                        wx_sum = wx_price.get("sum")
                if wx_sum is not None:
                    log.info("[T%s] wxPrice sum: %s", thread_id, wx_sum)

            t1 = time.time()
            order_rsp = self.client.add_park_order(
                park_list,
                paywaycode=paywaycode,
                mobile=mobile,
                ordercode=ordercode,
                captcha_verification=captcha,
                add_order_type="wx",
                userid=self.client.auth.userid,
                timeout=effective_timeout,
            )
            order_ms = (time.time() - t1) * 1000

            if not self.client.is_success(order_rsp):
                log.warning("[T%s] addParkOrder failed (%.0fms): %s", thread_id, order_ms, order_rsp.get("respMsg"))
                if not in_burst:
                    time.sleep(retry_interval_ms / 1000.0)
                continue

            order_payload = self.client.unwrap_payload(order_rsp)
            order_no = self._extract_order_no(order_payload)
            if not order_no:
                log.warning("[T%s] addParkOrder ok but no orderNo (%.0fms).", thread_id, order_ms)
                if not in_burst:
                    time.sleep(retry_interval_ms / 1000.0)
                continue

            log.info("[T%s] SUCCESS! orderNo=%s (query %.0fms + order %.0fms)", thread_id, order_no, query_ms, order_ms)

            state_rsp = self.client.get_park_order_state(order_no)
            if self.client.is_success(state_rsp):
                log.info("[T%s] getParkOrderState success for %s", thread_id, order_no)
            else:
                log.warning("[T%s] getParkOrderState: %s", thread_id, state_rsp.get("respMsg"))

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

        # Pre-warm TCP+TLS connection pool
        self.client.pre_warm_connections()

        # Pre-query court structure for direct-fire orders
        direct_fire = bool(self.strategy_cfg.get("direct_fire", True))
        pre_park_lists: List[List[Dict[str, Any]]] = []
        if direct_fire:
            pre_park_lists = self._pre_query_structure(target_date, ballcode)

        target_dt = self._target_booking_datetime()
        if target_dt is not None:
            prewarm_sec = int(self.strategy_cfg.get("prewarm_sec", 30))
            target_ms = int(target_dt.timestamp() * 1000) - int(self.strategy_cfg.get("advance_ms", 500))
            prewarm_ms = int(target_dt.timestamp() * 1000) - prewarm_sec * 1000
            now = self.client.now_ms()
            if now < target_ms:
                if now < prewarm_ms:
                    time.sleep((prewarm_ms - now) / 1000.0)
                log.info("Prewarm: queryBookDate once at T-%ss.", prewarm_sec)
                self.client.query_dates()
                # Re-warm the connection right before the critical window
                self.client.pre_warm_connections()

        self.wait_for_open_time()

        threads = max(1, int(self.strategy_cfg.get("threads", 3)))
        direct_fire_threads = int(self.strategy_cfg.get("direct_fire_threads", 2))
        actual_df = direct_fire_threads if pre_park_lists else 0
        total_workers = threads + actual_df

        log.info(
            "Starting %d workers: %d query + %d direct-fire (burst_count=%s)",
            total_workers,
            threads,
            actual_df,
            self.strategy_cfg.get("burst_count", 15),
        )

        with ThreadPoolExecutor(max_workers=total_workers) as pool:
            futures = []
            for idx in range(actual_df):
                futures.append(pool.submit(self._direct_fire_worker, idx + 1, pre_park_lists))
            for idx in range(threads):
                futures.append(pool.submit(self._try_book, idx + 1, target_date, ballcode))

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
        "action",
        choices=["info", "login", "auth", "book", "test"],
        help=(
            "info=show config, "
            "login=two-step interactive login (request SMS then verify), "
            "auth=inject tokens captured from proxy directly, "
            "book=auto booking, "
            "test=API checks"
        ),
    )

    parser.add_argument("--mobile", default="", help="Mobile number for login")
    parser.add_argument("--phonecode", default="", help="SMS phone code (login action only)")
    parser.add_argument("--union-id", default="", help="unionId for login (optional)")
    parser.add_argument("--maopen-id", default="", help="maopenId for login (optional)")
    parser.add_argument("--save", action="store_true", help="Persist result back to MongoDB config")

    # auth (proxy token injection) arguments
    parser.add_argument("--userid", default="", help="userid (auth action)")
    parser.add_argument("--bnglokbj", default="", help="bnglokbj token (auth action)")
    parser.add_argument("--csc", default="", help="csc signing key (auth action)")
    parser.add_argument("--cdc", default="", help="cdc signing key (auth action)")
    return parser


def _do_interactive_login(bot: "TennisBooker", args: argparse.Namespace) -> None:
    """Two-step interactive login: request SMS code, then verify."""
    mobile = args.mobile or str(bot.auth_cfg.get("mobile", "")).strip()
    if not mobile:
        raise SystemExit("login requires --mobile (or auth.mobile in config)")

    union_id = args.union_id or str(bot.auth_cfg.get("unionId", "")).strip()
    ma_open_id = args.maopen_id or str(bot.auth_cfg.get("maopenId", "")).strip()

    phonecode = args.phonecode or str(bot.auth_cfg.get("phonecode", "")).strip()

    if not phonecode:
        # Step 1: request SMS code automatically
        log.info("Requesting SMS code for %s ...", mobile)
        sms_rsp = bot.client.get_phone_code(mobile)
        if not bot.client.is_success(sms_rsp):
            log.warning("getPhoneCode failed: %s | full response: %s", sms_rsp.get("respMsg"), sms_rsp)
        else:
            log.info("SMS sent. Check your phone.")

        # Step 2: read from stdin
        try:
            phonecode = input("Enter SMS verification code: ").strip()
        except (EOFError, KeyboardInterrupt):
            raise SystemExit("\nAborted.")

    if not phonecode:
        raise SystemExit("No verification code provided.")

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


def _do_auth_inject(bot: "TennisBooker", args: argparse.Namespace) -> None:
    """Directly inject tokens captured from a proxy (Charles / mitmproxy / Proxyman).

    Usage example:
        python tennis_bot.py auth --save \\
            --userid 43592 \\
            --bnglokbj <value from proxy> \\
            --csc <decrypted csc> \\
            --cdc <decrypted cdc>

    How to capture via proxy:
        1. Configure Charles/Proxyman/mitmproxy on your phone/Mac.
        2. Open the WeChat mini-program and log in normally.
        3. Find the request to phoneCodeLogin.action in the proxy history.
        4. The response JSON contains ftzmzcwc.bnglokbj (plain) and encrypted
           ftzmzcwc.qlakclqf (csc) / ftzmzcwc.xqqflsoy (cdc).
        5. Run the bot once with --phonecode to let it decrypt and --save, OR
           decrypt manually with the AES key and pass plain values here.
    """
    userid = args.userid or str(bot.auth_cfg.get("userid", "")).strip()
    bnglokbj = args.bnglokbj or str(bot.auth_cfg.get("bnglokbj", "")).strip()
    csc = args.csc or str(bot.auth_cfg.get("csc", "")).strip()
    cdc = args.cdc or str(bot.auth_cfg.get("cdc", "")).strip()
    mobile = args.mobile or str(bot.auth_cfg.get("mobile", "")).strip()

    missing = [n for n, v in [("userid", userid), ("bnglokbj", bnglokbj), ("csc", csc), ("cdc", cdc)] if not v]
    if missing:
        raise SystemExit(
            f"auth requires: {', '.join(missing)}\n"
            "Pass them via --userid --bnglokbj --csc --cdc, or populate auth section in config."
        )

    bot.auth_cfg.update({
        "userid": userid,
        "bnglokbj": bnglokbj,
        "csc": csc,
        "cdc": cdc,
    })
    if mobile:
        bot.auth_cfg["mobile"] = mobile

    bot.client.auth.userid = userid
    bot.client.auth.bnglokbj = bnglokbj
    bot.client.auth.csc = csc
    bot.client.auth.cdc = cdc

    if args.save:
        bot.save_config()
        log.info("Tokens saved to MongoDB config collection.")

    # Quick sanity-check with a signed API call
    role_rsp = bot.client.query_user_role()
    if bot.client.is_success(role_rsp):
        log.info("Token validation OK — queryUserRole succeeded.")
        raise SystemExit(0)
    log.error("Token validation failed: %s", role_rsp.get("respMsg"))
    raise SystemExit(1)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    bot = TennisBooker()

    if args.action == "info":
        bot.show_info()
        return

    if args.action == "test":
        ok = bot.test_connectivity()
        raise SystemExit(0 if ok else 1)

    if args.action == "login":
        _do_interactive_login(bot, args)
        return

    if args.action == "auth":
        _do_auth_inject(bot, args)
        return

    if args.action == "book":
        bot.show_info()
        success = bot.auto_book()
        raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
