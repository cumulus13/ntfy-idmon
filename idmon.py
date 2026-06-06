#!/usr/bin/env python3

# File: idmon.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-06-07
# Description: Production-ready download monitor with dynamic, un-padded chart tracking
# License: MIT

import os
import sys
import json
import time
import re
import asyncio
import argparse
from datetime import datetime
from collections import deque
from rich.live import Live
from rich.table import Table
from rich.console import Console, Group

# --- Safe External Dependency Mappings ---
try:
    import gntplib
    HAS_GNTPLIB = True
except ImportError:
    HAS_GNTPLIB = False

try:
    import asciichartpy as asciichart
    HAS_ASCIICHART = True
except ImportError:
    asciichart = None
    HAS_ASCIICHART = False

try:
    import aiohttp
except ImportError:
    print("[CRITICAL] Dependency 'aiohttp' is missing.", file=sys.stderr)
    print("Execute: pip install aiohttp", file=sys.stderr)
    sys.exit(1)

try:
    from rchf import CustomRichHelpFormatter  # type: ignore
except ImportError:
    CustomRichHelpFormatter = argparse.RawTextHelpFormatter

# --- Global Operational State ---
DEBUG_MODE = False
console = Console()
GLOBAL_CONFIG = {}
gntp_publisher = None


def global_exception_handler(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    console.print(f"\n[bold red]❌ CRITICAL RUNTIME ERROR:[/bold red] {exc_value}")
    if DEBUG_MODE:
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

sys.excepthook = global_exception_handler


# --- Configuration & Path Engine ---
def load_file_config():
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.getcwd()
    config_path = os.path.join(base_dir, "config.json")
    
    defaults = {
        "host": "127.0.0.1", 
        "port": 8888, 
        "ntfy_url": "http://localhost:8080/androcall",
        "growl_host": "127.0.0.1",
        "growl_port": 23053,
        "growl_password": ""
    }
    
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                return {**defaults, **json.load(f)}
        except Exception:
            pass
    return defaults

def resolve_config(args):
    file_config = load_file_config()
    return {
        "host": args.host or file_config.get("host"),
        "port": int(args.port or file_config.get("port")),
        "ntfy_url": args.ntfy or file_config.get("ntfy_url"),
        "growl_host": args.growl_host or file_config.get("growl_host"),
        "growl_port": int(args.growl_port or file_config.get("growl_port")),
        "growl_password": args.growl_password or file_config.get("growl_password")
    }


# --- GNTP Delivery Workers ---
def init_gntp_publisher():
    global gntp_publisher
    if not HAS_GNTPLIB:
        return
    try:
        gntp_publisher = gntplib.Publisher(
            "NTFY-IDM Monitor",
            ["Download Update", "System Error"],
            hostname=GLOBAL_CONFIG["growl_host"],
            port=GLOBAL_CONFIG["growl_port"],
            password=GLOBAL_CONFIG["growl_password"] if GLOBAL_CONFIG["growl_password"] else None
        )
        gntp_publisher.register()
    except Exception as e:
        if DEBUG_MODE:
            print(f"[DEBUG-GROWL-INIT-ERR] Failed to register gntplib client: {e}", file=sys.stderr)

def send_growl_notification(title, message, notification_type="Download Update"):
    global gntp_publisher
    if not HAS_GNTPLIB or not gntp_publisher:
        return
    try:
        gntp_publisher.publish(notification_type, title, message)
    except Exception as e:
        if DEBUG_MODE:
            print(f"[DEBUG-GROWL-ERR] gntplib delivery fault: {e}", file=sys.stderr)


# --- Helper to Normalize Speeds to Numeric KB/s Values ---
def parse_speed_to_kb(speed_str) -> float:
    """Extracts raw numeric speed value as KB/s float for math or charts calculation."""
    try:
        clean_str = speed_str.strip().upper()
        match = re.search(r"([0-9.]+)\s*([KMG]?B/S)", clean_str)
        if not match:
            return 0.0
        val = float(match.group(1))
        unit = match.group(2)
        if "GB/S" in unit:
            return val * 1024 * 1024
        if "MB/S" in unit:
            return val * 1024
        return val
    except Exception:
        return 0.0


# --- State Management & Parsing Engine ---
class DownloadMonitor:
    def __init__(self):
        self.current_state = None
        self.previous_state = None
        self.locked_complete = False
        self.last_notified_asset = None
        # Max buffer size allowed to scale inside standard wide terminal viewports
        self.speed_history = deque(maxlen=500)

    def parse_raw_data(self, raw_str) -> bool:
        if not raw_str or not raw_str.strip():
            return False

        try:
            data = json.loads(raw_str.strip())
            if data.get("event") not in (None, "message"):
                return False

            msg_obj = data
            while isinstance(msg_obj, dict) and "message" in msg_obj:
                inner = msg_obj["message"]
                if isinstance(inner, str):
                    try:
                        parsed_inner = json.loads(inner.strip())
                        if isinstance(parsed_inner, (dict, list)):
                            msg_obj = parsed_inner
                        else:
                            break
                    except json.JSONDecodeError:
                        break
                elif isinstance(inner, dict):
                    msg_obj = inner
                else:
                    break

            if isinstance(msg_obj, dict) and "title" in msg_obj:
                title = msg_obj.get("title", "")
                clean_title = title.replace("📱", "").strip()
                if not clean_title.startswith("idm.internet."):
                    return False  
                
                inner_msg = msg_obj.get("message", "")
                lines = [line.strip() for line in inner_msg.split("\n") if line.strip()]
                
                name = lines[0] if len(lines) > 0 else "Unknown Asset"
                metrics = lines[1].split("|") if len(lines) > 1 else []
                
                percent = metrics[0].strip() if len(metrics) > 0 else "0%"
                size = metrics[1].strip() if len(metrics) > 1 else "Unknown Size"
                speed = metrics[2].strip() if len(metrics) > 2 else "0KB/s"
                
                time_raw = metrics[3].strip() if len(metrics) > 3 else "--/--"
                if "/" in time_raw:
                    eta, total_duration = time_raw.split("/", 1)
                else:
                    eta, total_duration = time_raw, "--"
            else:
                val_str = msg_obj.get("message", "") if isinstance(msg_obj, dict) else str(msg_obj)
                if not val_str.startswith("idm.internet."):
                    return False
                name, percent, size, speed, eta, total_duration = val_str, "0%", "Unknown", "0KB/s", "--", "--"

            # Reset logic for new asset item arrivals
            if self.current_state and name != self.current_state.get("name"):
                self.locked_complete = False
                self.speed_history.clear()

            timestamp = datetime.fromtimestamp(data.get("time", time.time())).strftime('%Y-%m-%d %H:%M:%S')

            self.previous_state = self.current_state
            self.current_state = {
                "name": name,
                "percent": percent,
                "size": size,
                "speed": speed,
                "eta": eta.strip(),
                "total_duration": total_duration.strip(),
                "timestamp": timestamp
            }

            numeric_speed = parse_speed_to_kb(speed)
            self.speed_history.append(numeric_speed)

            if percent == "100%" and self.last_notified_asset != name:
                self.locked_complete = True
                self.last_notified_asset = name
                asyncio.to_thread(
                    send_growl_notification, 
                    "📥 Download Complete!", 
                    f"Asset: {name}\nSize: {size}",
                    "Download Update"
                )
            return True
        except Exception as err:
            if DEBUG_MODE:
                console.print(f"[DEBUG-PARSE-ERR] {err}")
            return False

    def build_layout(self):
        """Builds a secure flat layout where the chart grows naturally without zero-padding chaos."""
        table = Table(title="📡 Live NTFY-IDM Monitor", title_style="bold magenta", expand=True)
        table.add_column("Progress", justify="right")
        table.add_column("Name", style="white", ratio=2)
        table.add_column("Size", justify="center", style="green")
        table.add_column("Speed", justify="center", style="yellow")
        table.add_column("ETA", justify="center", style="magenta")
        table.add_column("Total Duration", justify="center", style="blue")
        table.add_column("Status", justify="center")
        table.add_column("Timestamp", justify="center", style="dim white")

        if not self.current_state:
            table.add_row("-", "Awaiting target stream transmission connection...", "-", "-", "-", "-", "[dim]idle[/dim]", "-")
            return table

        curr, prev = self.current_state, self.previous_state
        if self.locked_complete or curr['percent'] == "100%":
            status = "✅ [green]done[/green]"
            progress_style = "[bold green]100%[/bold green]"
        else:
            progress_style = f"[cyan]{curr['percent']}[/cyan]"
            if prev and curr["speed"] == prev["speed"] and curr["percent"] == prev["percent"]:
                status = "🛑 [red]stop[/red]"
            else:
                status = "⚡ [green]run[/green]"

        table.add_row(
            progress_style, curr["name"], curr["size"], curr["speed"],
            curr["eta"], curr["total_duration"], status, curr["timestamp"]
        )

        if HAS_ASCIICHART and len(self.speed_history) > 1:
            try:
                # Read dynamic terminal width to safeguard layout edge wraps
                term_width = console.size.width
                safe_width = max(10, term_width - 16)
                
                # Extract historical data without forcing structural artificial zero metrics
                data_points = list(self.speed_history)[-safe_width:]
                
                if max(data_points) == 0:
                    data_points[-1] = 0.01
                
                chart_output = asciichart.plot(data_points, {'height': 6, 'format': '{:8.1f} KB/s'})
                return Group(table, "", "📈 [cyan]Real-Time Speed Telemetry[/cyan]", chart_output)
            except Exception:
                return Group(table, "", "[yellow]Telemetry rendering synchronization offset...[/yellow]")
        else:
            return Group(table, "", "[dim white]Collecting metrics tracking telemetry sequences...[/dim white]")
    

# --- Global Instance Initialization ---
monitor = DownloadMonitor()


# --- Network Context Operators ---
async def handle_client(reader, writer):
    buffer = b""
    while True:
        try:
            data = await reader.read(4096)
            if not data:
                break
            buffer += data
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                if line.strip():
                    monitor.parse_raw_data(line.decode('utf-8', errors='ignore'))
        except Exception:
            break
    writer.close()
    await writer.wait_closed()

async def run_server(host, port):
    server = await asyncio.start_server(handle_client, host, port)
    while True:
        await asyncio.sleep(1)

async def run_ntfy_subscriber(ntfy_url):
    if not ntfy_url.endswith("/json"):
        ntfy_url = ntfy_url.rstrip("/") + "/json"
    
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(ntfy_url) as response:
                    if response.status != 200:
                        await asyncio.sleep(5)
                        continue
                    async for line in response.content:
                        if line.strip():
                            monitor.parse_raw_data(line.decode('utf-8', errors='ignore'))
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(2)
        except Exception:
            await asyncio.sleep(2)


# --- Core UI Wrapper Loop ---
async def main_application_loop(is_ntfy_active, target_ntfy_url):
    host = GLOBAL_CONFIG["host"]
    port = GLOBAL_CONFIG["port"]

    if HAS_GNTPLIB:
        await asyncio.to_thread(init_gntp_publisher)

    if is_ntfy_active:
        asyncio.create_task(run_ntfy_subscriber(target_ntfy_url))
    else:
        asyncio.create_task(run_server(host, port))

    if DEBUG_MODE:
        if is_ntfy_active:
            await run_ntfy_subscriber(target_ntfy_url)
        else:
            await run_server(host, port)
    else:
        with Live(get_renderable=monitor.build_layout, refresh_per_second=2, console=console):
            while True:
                await asyncio.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="IDM Terminal Dynamic Display Stream Engine Infrastructure",
        formatter_class=CustomRichHelpFormatter,
        prog="idmon"
    )
    parser.add_argument("-H", "--host", type=str, help="Bind address string destination target")
    parser.add_argument("-p", "--port", type=int, help="Target connection networking port integer")
    parser.add_argument("-n", "--ntfy", type=str, nargs='?', const="http://localhost:8080/androcall", help="Activate stream extraction mode via subscription URL target")
    parser.add_argument("-d", "--debug", action="store_true", help="Launch low-level terminal telemetry debugging outputs")
    
    parser.add_argument("--growl-host", type=str, help="Growl server destination network address target")
    parser.add_argument("--growl-port", type=int, help="Growl connection network communication port")
    parser.add_argument("--growl-password", type=str, help="Growl verification access credential target")
    
    args = parser.parse_args()
    GLOBAL_CONFIG = resolve_config(args)
    
    if args.debug:
        DEBUG_MODE = True

    is_ntfy_active = args.ntfy is not None or (len(sys.argv) > 1 and any(arg in sys.argv for arg in ['-n', '--ntfy']))
    target_ntfy_url = args.ntfy if isinstance(args.ntfy, str) else GLOBAL_CONFIG["ntfy_url"]

    try:
        if not is_ntfy_active:
            console.print("[bold #00FFFF]use[/] [bold #FFFF00]'-h/--help'[/] [bold #00FFFF]for help[/]")
        asyncio.run(main_application_loop(is_ntfy_active, target_ntfy_url))
    except KeyboardInterrupt:
        console.print("\n[bold red]Termination requested. Safe exit operations finalized.[/bold red]")