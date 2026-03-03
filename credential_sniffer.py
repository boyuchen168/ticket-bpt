"""
mitmproxy addon: 自动从微信小程序流量中提取认证信息并保存到 config.yaml

用法:
    mitmdump -s credential_sniffer.py -p 9090 --set config_path=config.yaml

当检测到 caiyicloud.com 的请求时自动提取:
  - access-token (JWT)
  - cookie
  - Angry-Dog (反爬 token)
  - pre_login 响应中的 accessToken
"""

import json
import time
from pathlib import Path

import yaml
from mitmproxy import ctx, http


TARGET_HOST_SUFFIX = "caiyicloud.com"
PRE_LOGIN_PATH = "/cyy_gatewayapi/user/pub/v3/wx/mini/pre_login"


class CredentialSniffer:
    def __init__(self):
        self.credentials = {
            "access_token": None,
            "cookie": None,
            "angry_dog": None,
        }
        self.captured_count = 0
        self.config_path = "config.yaml"
        self.saved = False

    def load(self, loader):
        loader.add_option(
            name="config_path",
            typespec=str,
            default="config.yaml",
            help="Path to ticket-bot config.yaml",
        )

    def configure(self, updated):
        if "config_path" in updated:
            self.config_path = ctx.options.config_path

    def request(self, flow: http.HTTPFlow):
        host = flow.request.pretty_host
        if TARGET_HOST_SUFFIX not in host:
            return

        headers = flow.request.headers
        updated = False

        token = headers.get("access-token", "")
        if token and token != self.credentials["access_token"]:
            self.credentials["access_token"] = token
            updated = True
            ctx.log.info(f"[Sniffer] Captured access-token: {token[:50]}...")

        cookie = headers.get("cookie", "")
        if cookie and cookie != self.credentials["cookie"]:
            self.credentials["cookie"] = cookie
            updated = True
            ctx.log.info(f"[Sniffer] Captured cookie: {cookie[:60]}...")

        angry_dog = headers.get("Angry-Dog", "") or headers.get("angry-dog", "")
        if angry_dog and angry_dog != self.credentials["angry_dog"]:
            self.credentials["angry_dog"] = angry_dog
            updated = True
            ctx.log.info(f"[Sniffer] Captured Angry-Dog: {angry_dog[:50]}...")

        if updated:
            self.captured_count += 1
            self._try_save()

    def response(self, flow: http.HTTPFlow):
        host = flow.request.pretty_host
        if TARGET_HOST_SUFFIX not in host:
            return

        if PRE_LOGIN_PATH in flow.request.path:
            try:
                data = json.loads(flow.response.get_text())
                if data.get("statusCode") == 200:
                    new_token = data.get("data", {}).get("accessToken", "")
                    if new_token:
                        self.credentials["access_token"] = new_token
                        ctx.log.info(
                            f"[Sniffer] Fresh token from pre_login: {new_token[:50]}..."
                        )
                        self._try_save()
            except Exception as e:
                ctx.log.warn(f"[Sniffer] Failed to parse pre_login response: {e}")

    def _try_save(self):
        if not self.credentials["access_token"]:
            return

        config_file = Path(self.config_path)
        if not config_file.exists():
            ctx.log.warn(f"[Sniffer] Config not found: {self.config_path}")
            return

        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            auth = config.setdefault("auth", {})
            changed = False

            for key in ("access_token", "cookie", "angry_dog"):
                val = self.credentials[key]
                if val and auth.get(key) != val:
                    auth[key] = val
                    changed = True

            if changed:
                with open(config_file, "w", encoding="utf-8") as f:
                    yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
                self.saved = True
                ctx.log.info(
                    f"[Sniffer] Credentials saved to {self.config_path}"
                )
                self._print_summary()
        except Exception as e:
            ctx.log.error(f"[Sniffer] Failed to save config: {e}")

    def _print_summary(self):
        ctx.log.info("=" * 50)
        ctx.log.info("[Sniffer] Credential Summary:")
        for key, val in self.credentials.items():
            if val:
                ctx.log.info(f"  {key}: {val[:40]}...")
            else:
                ctx.log.info(f"  {key}: (not captured)")
        ctx.log.info("=" * 50)
        ctx.log.info(
            "[Sniffer] You can now stop the proxy (Ctrl+C) and run ticket_bot.py"
        )


addons = [CredentialSniffer()]
