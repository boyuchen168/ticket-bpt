#!/usr/bin/env python3
"""
自动刷新认证信息

工作原理:
  1. 启动 mitmproxy 作为本地 HTTPS 代理 (端口 9090)
  2. 自动配置 macOS 系统代理
  3. 等待用户打开微信小程序（触发自动登录）
  4. 从流量中提取 access-token / cookie / Angry-Dog
  5. 保存到 config.yaml
  6. 恢复系统代理设置

前置条件:
  brew install mitmproxy
  # 首次运行需安装 mitmproxy CA 证书到系统信任:
  # 1. 运行 mitmproxy 一次生成证书
  # 2. 打开 ~/.mitmproxy/mitmproxy-ca-cert.pem
  # 3. 双击导入 Keychain，在"钥匙串访问"中设为"始终信任"
"""

import os
import sys
import signal
import subprocess
import time
import argparse
from pathlib import Path


PROXY_PORT = 9090
SCRIPT_DIR = Path(__file__).parent
SNIFFER_SCRIPT = SCRIPT_DIR / "credential_sniffer.py"
NETWORK_SERVICE = None


def get_active_network_service() -> str:
    """获取当前活跃的网络接口名称"""
    try:
        result = subprocess.run(
            ["networksetup", "-listnetworkserviceorder"],
            capture_output=True, text=True,
        )
        lines = result.stdout.strip().split("\n")
        for i, line in enumerate(lines):
            if line.startswith("(") and "Wi-Fi" in line:
                return "Wi-Fi"
            if line.startswith("(") and "Ethernet" in line:
                return "Ethernet"
    except Exception:
        pass
    return "Wi-Fi"


def set_proxy(service: str, host: str, port: int):
    """设置 macOS 系统 HTTP/HTTPS 代理"""
    for proto in ("webproxy", "securewebproxy"):
        subprocess.run(
            ["networksetup", f"-set{proto}", service, host, str(port)],
            check=True,
        )
        subprocess.run(
            ["networksetup", f"-set{proto}state", service, "on"],
            check=True,
        )
    print(f"[Proxy] System proxy set to {host}:{port} on {service}")


def unset_proxy(service: str):
    """关闭 macOS 系统代理"""
    for proto in ("webproxy", "securewebproxy"):
        subprocess.run(
            ["networksetup", f"-set{proto}state", service, "off"],
            check=False,
        )
    print(f"[Proxy] System proxy disabled on {service}")


def check_mitmproxy_installed() -> bool:
    result = subprocess.run(
        ["which", "mitmdump"], capture_output=True, text=True,
    )
    return result.returncode == 0


def check_cert_installed() -> bool:
    cert_path = Path.home() / ".mitmproxy" / "mitmproxy-ca-cert.pem"
    return cert_path.exists()


def install_cert_instructions():
    cert_path = Path.home() / ".mitmproxy" / "mitmproxy-ca-cert.pem"
    print()
    print("=" * 60)
    print("首次使用需要安装 mitmproxy CA 证书:")
    print("=" * 60)
    print()
    print("步骤 1: 生成证书（如果还没有）")
    print("  mitmdump --listen-port 19999 &")
    print("  kill %1")
    print()
    print("步骤 2: 导入证书到系统钥匙串")
    print(f"  open {cert_path}")
    print("  -> 在弹出的'钥匙串访问'中找到 mitmproxy")
    print("  -> 双击 -> 信任 -> 设为'始终信任'")
    print()
    print("步骤 3: 重新运行本脚本")
    print("=" * 60)


def run(config_path: str):
    global NETWORK_SERVICE

    if not check_mitmproxy_installed():
        print("[Error] mitmproxy 未安装，请运行: brew install mitmproxy")
        sys.exit(1)

    if not check_cert_installed():
        print("[Warning] mitmproxy CA 证书可能未生成")
        print("正在生成证书...")
        proc = subprocess.Popen(
            ["mitmdump", "--listen-port", "19999"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(2)
        proc.terminate()
        proc.wait()

        if not check_cert_installed():
            install_cert_instructions()
            sys.exit(1)

        install_cert_instructions()
        sys.exit(1)

    NETWORK_SERVICE = get_active_network_service()

    def cleanup(signum=None, frame=None):
        print("\n[Cleanup] 恢复系统代理设置...")
        unset_proxy(NETWORK_SERVICE)
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    config_abs = str(Path(config_path).resolve())

    print()
    print("=" * 60)
    print("自动抓取认证信息")
    print("=" * 60)
    print(f"  代理端口: {PROXY_PORT}")
    print(f"  网络接口: {NETWORK_SERVICE}")
    print(f"  配置文件: {config_abs}")
    print()
    print("操作步骤:")
    print("  1. 脚本会自动设置系统代理")
    print("  2. 请打开微信 -> 进入小程序 -> 随意浏览")
    print("  3. 看到 'Credentials saved' 后按 Ctrl+C 退出")
    print("=" * 60)
    print()

    set_proxy(NETWORK_SERVICE, "127.0.0.1", PROXY_PORT)

    try:
        cmd = [
            "mitmdump",
            "--listen-port", str(PROXY_PORT),
            "--ssl-insecure",
            "-s", str(SNIFFER_SCRIPT),
            "--set", f"config_path={config_abs}",
            "--quiet",
        ]
        proc = subprocess.run(cmd)
    except KeyboardInterrupt:
        pass
    finally:
        cleanup()


def main():
    parser = argparse.ArgumentParser(description="自动刷新抢票认证信息")
    parser.add_argument(
        "-c", "--config", default="config.yaml",
        help="config.yaml 路径 (default: config.yaml)",
    )
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
