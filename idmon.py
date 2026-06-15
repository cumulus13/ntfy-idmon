#!/usr/bin/env python3

# File: idmon.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-06-07
# Description: Production-ready download monitor using gntplib and optional telemetry speed graphs
# License: MIT

import os
import sys
import json
import time
import re
import asyncio
import argparse
import threading
from datetime import datetime
from collections import deque, OrderedDict
from rich.live import Live
from rich.table import Table
from rich.console import Console, Group
from rich.panel import Panel

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


# --- Speed scale / formatting helpers (mirrors aria-cli.py) ---
def _choose_scale(max_val: float):
    if max_val >= 1 << 30:
        return (1 << 30, "GB")
    if max_val >= 1 << 20:
        return (1 << 20, "MB")
    if max_val >= 1 << 10:
        return (1 << 10, "KB")
    return (1, "B")

def format_size(bytes_size: float) -> str:
    if bytes_size == 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} PB"

def parse_speed_to_bytes(speed_str: str) -> int:
    """Convert speed string like '242.00KB/s' or '1.5 MB/s' -> bytes/s int."""
    try:
        clean = speed_str.strip().upper()
        m = re.search(r"([0-9.]+)\s*([KMG]?B(?:/S)?)", clean)
        if not m:
            return 0
        val = float(m.group(1))
        unit = m.group(2)
        if "GB" in unit:
            return int(val * 1024 ** 3)
        if "MB" in unit:
            return int(val * 1024 ** 2)
        if "KB" in unit:
            return int(val * 1024)
        return int(val)
    except Exception:
        return 0


# --- State Management & Parsing Engine ---
class DownloadMonitor:
    """
    Tracks an arbitrary number of *concurrent* downloads, each identified by
    its file name. Each download keeps its own state snapshot + its own
    speed-history ring buffer, so simultaneous transfers (e.g. several IDM
    items downloading at once) are displayed/charted independently — and a
    100%-complete download no longer freezes the whole monitor.
    """

    # how many speed samples to retain per series — set large enough that even
    # a 300-column terminal never runs out of historical points to plot
    MAX_SAMPLES = 400
    # how many finished downloads to keep visible in the table before pruning
    MAX_COMPLETED_VISIBLE = 5
    # how many per-download mini-charts to render (busiest first)
    TOP_K_CHARTS = 6

    def __init__(self, chart_height: int = 6):
        self.chart_height = chart_height
        self._lock = threading.Lock()

        # name -> state dict (insertion-ordered == display order)
        self.downloads: "OrderedDict[str, dict]" = OrderedDict()

        # name -> deque of bytes/s samples
        self.speed_history: dict = {}

        # global combined throughput series (sum of all *active* downloads)
        self.global_speeds: deque = deque(maxlen=self.MAX_SAMPLES)

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------
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

                percent  = metrics[0].strip() if len(metrics) > 0 else "0%"
                size     = metrics[1].strip() if len(metrics) > 1 else "Unknown Size"
                speed    = metrics[2].strip() if len(metrics) > 2 else "0KB/s"

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

            timestamp = datetime.fromtimestamp(data.get("time", time.time())).strftime('%Y-%m-%d %H:%M:%S')
            with self._lock:
                self._update_download(name, percent, size, speed, eta.strip(), total_duration.strip(), timestamp)
            return True
        except Exception as err:
            if DEBUG_MODE:
                console.print(f"[DEBUG-PARSE-ERR] {err}")
            return False

    def _update_download(self, name, percent, size, speed, eta, total_duration, timestamp):
        prev = self.downloads.get(name)
        was_complete = bool(prev and prev.get("completed"))

        # Determine run/stop status by comparing to previous snapshot
        if prev and not was_complete:
            if prev["speed"] == speed and prev["percent"] == percent:
                run_status = "🛑 [red]stop[/red]"
            else:
                run_status = "⚡ [green]run[/green]"
        else:
            run_status = "⚡ [green]run[/green]"

        is_complete_now = (percent == "100%")
        if is_complete_now:
            run_status = "✅ [green]done[/green]"

        entry = {
            "name": name,
            "percent": percent,
            "size": size,
            "speed": speed,
            "eta": eta,
            "total_duration": total_duration,
            "timestamp": timestamp,
            "status": run_status,
            "completed": is_complete_now,
        }
        self.downloads[name] = entry
        # Move to end so most-recently-updated downloads float to the bottom
        # of insertion order (keeps active items grouped together over time)
        self.downloads.move_to_end(name)

        # --- speed telemetry ---
        speed_bytes = parse_speed_to_bytes(speed)
        dq = self.speed_history.setdefault(name, deque(maxlen=self.MAX_SAMPLES))
        dq.append(speed_bytes)

        # Global throughput = sum of latest sample of every *active* download
        total_active_speed = 0
        for dl_name, dl_entry in self.downloads.items():
            if dl_entry.get("completed"):
                continue
            hist = self.speed_history.get(dl_name)
            if hist:
                total_active_speed += hist[-1]
        self.global_speeds.append(total_active_speed)

        # Notify + prune on completion
        if is_complete_now and not was_complete:
            try:
                loop = asyncio.get_event_loop()
                loop.run_in_executor(
                    None,
                    send_growl_notification,
                    "📥 Download Complete!",
                    f"Asset: {name}\nSize: {size}",
                    "Download Update"
                )
            except RuntimeError:
                # No running event loop (e.g. during tests) — call directly
                send_growl_notification("📥 Download Complete!", f"Asset: {name}\nSize: {size}")

        self._prune_completed()

    def _prune_completed(self):
        """Keep only the N most-recently-completed downloads visible; drop the rest."""
        completed_names = [n for n, e in self.downloads.items() if e.get("completed")]
        if len(completed_names) > self.MAX_COMPLETED_VISIBLE:
            for n in completed_names[:-self.MAX_COMPLETED_VISIBLE]:
                self.downloads.pop(n, None)
                self.speed_history.pop(n, None)

    # ------------------------------------------------------------------
    # Rendering — mirrors aria-cli.py monitor_downloads() layout
    # ------------------------------------------------------------------
    def build_layout(self):
        # Take a consistent snapshot under the lock so the Rich refresh
        # thread never races with the asyncio writer thread.
        with self._lock:
            downloads_snapshot   = list(self.downloads.values())
            active_count         = sum(1 for e in downloads_snapshot if not e.get("completed"))
            speed_history_snap   = {k: list(v) for k, v in self.speed_history.items()}
            global_speeds_snap   = list(self.global_speeds)

        # ── 1. Status table (one row per download, active + recently done) ──
        table = Table(
            title=f"📡 Live NTFY-IDM Monitor  ({active_count} active)",
            title_style="bold magenta",
            expand=True,
            show_header=True,
            header_style="bold cyan"
        )
        table.add_column("📋 #",           justify="right", style="cyan", no_wrap=True)
        table.add_column("Progress",       justify="right")
        table.add_column("Name",           style="white",    ratio=2)
        table.add_column("Size",           justify="center", style="green")
        table.add_column("Speed",          justify="center", style="yellow")
        table.add_column("ETA",            justify="center", style="magenta")
        table.add_column("Total Duration", justify="center", style="blue")
        table.add_column("Status",         justify="center")
        table.add_column("Timestamp",      justify="center", style="dim white")

        if not downloads_snapshot:
            table.add_row("-", "-", "Awaiting target stream transmission connection...",
                          "-", "-", "-", "-", "[dim]idle[/dim]", "-")
            return table

        for i, entry in enumerate(downloads_snapshot, 1):
            progress_style = (
                "[bold green]100%[/bold green]" if entry["completed"]
                else f"[cyan]{entry['percent']}[/cyan]"
            )
            table.add_row(
                str(i),
                progress_style,
                entry["name"],
                entry["size"],
                entry["speed"],
                entry["eta"],
                entry["total_duration"],
                entry["status"],
                entry["timestamp"],
            )

        # ── 2. Speed graphs — full-width panel ──
        graphs_panel = self._build_graphs_panel(
            downloads_snapshot, speed_history_snap, global_speeds_snap
        )
        return Group(table, graphs_panel)

    @staticmethod
    def _term_cols() -> int:
        try:
            return os.get_terminal_size().columns
        except OSError:
            return 120

    @staticmethod
    def _chart_width(format_str: str = "{:8.1f}", panel_overhead: int = 6) -> int:
        """
        Return data-point count so asciichart fills terminal width exactly.
        axis_prefix = len(format_str rendered) + len(" ┤") = format width + 2.
        panel_overhead covers Rich Panel borders + padding + safety (default 6).
        """
        try:
            term_cols = os.get_terminal_size().columns
        except OSError:
            term_cols = 120
        # extract the numeric width from the format string e.g. "{:8.1f}" -> 8
        m = re.search(r'\{:(\d+)', format_str)
        fmt_width  = int(m.group(1)) if m else 8
        axis_prefix = fmt_width + 2   # number + " ┤"
        return max(10, term_cols - axis_prefix - panel_overhead)

    def _build_graphs_panel(self, downloads_snapshot, speed_history_snap, global_speeds_snap) -> Panel:
        max_global  = max(global_speeds_snap) if global_speeds_snap else 0
        scale, unit = _choose_scale(max_global)
        unit_label  = f"{unit}/s"

        # Width for each chart type — format strings differ so axis prefix differs
        global_fmt     = "{:8.1f}"
        per_file_fmt   = "{:6.1f}"
        global_cols    = self._chart_width(global_fmt)
        per_file_cols  = self._chart_width(per_file_fmt)

        graphs_parts = []

        # --- Global combined throughput chart ---
        all_global    = global_speeds_snap
        scaled_global = [v / scale for v in all_global[-global_cols:]]
        if HAS_ASCIICHART and len(scaled_global) >= 2:
            try:
                cfg          = {"height": self.chart_height, "format": global_fmt}
                global_chart = asciichart.plot(scaled_global, cfg)
                max_label    = format_size(max_global) + "/s"
                graphs_parts.append(f"[bold]Global throughput ({unit_label})  max {max_label}[/]")
                graphs_parts.append(global_chart)
            except Exception:
                samples = all_global[-8:]
                graphs_parts.append(
                    f"[bold]{unit_label}[/] " + ", ".join(format_size(s) + "/s" for s in samples)
                )
        elif self.global_speeds:
            samples = all_global[-8:]
            graphs_parts.append(
                f"[bold]{unit_label}[/] " +
                ", ".join(f"{v/scale:.1f}" for v in samples) +
                f"  (max {format_size(max_global)}/s)"
            )
        else:
            graphs_parts.append("[dim]Collecting metrics tracking telemetry sequences...[/dim]")

        if not HAS_ASCIICHART:
            graphs_parts.append("[dim red]ℹ pip install asciichartpy to enable dynamic tracking telemetry charts[/dim red]")

        # --- Per-download mini charts (busiest active downloads first) ---
        active_with_speed = [
            (entry["name"], speed_history_snap[entry["name"]][-1])
            for entry in downloads_snapshot
            if not entry.get("completed")
            and entry["name"] in speed_history_snap
            and speed_history_snap[entry["name"]]
        ]
        active_with_speed.sort(key=lambda x: x[1], reverse=True)

        if active_with_speed:
            individual_height = max(2, self.chart_height - 1)
            per_name_lines = []
            for name, _ in active_with_speed[:self.TOP_K_CHARTS]:
                series        = speed_history_snap.get(name, [])
                # Slice per-file series to per_file_cols width
                scaled_series = [s / scale for s in series[-per_file_cols:]]
                latest_val    = series[-1] if series else 0
                latest_label  = format_size(latest_val) + "/s"
                short_name    = (name[:30] + "...") if len(name) > 33 else name

                if HAS_ASCIICHART and len(scaled_series) >= 2:
                    try:
                        g = asciichart.plot(scaled_series, {"height": individual_height, "format": per_file_fmt})
                        per_name_lines.append(f"{short_name} ({latest_label})\n{g}")
                    except Exception:
                        per_name_lines.append(
                            f"{short_name} ({latest_label}): " +
                            ", ".join(format_size(s) + "/s" for s in series[-6:])
                        )
                else:
                    per_name_lines.append(
                        f"{short_name} ({latest_label}): " +
                        ", ".join(f"{s/scale:.1f}" for s in series[-6:])
                    )

            if per_name_lines:
                graphs_parts.append(f"\n[bold]Top file speeds ({unit_label})[/]")
                graphs_parts.extend(per_name_lines)

        graphs_text = "\n\n".join(graphs_parts)

        return Panel(
            graphs_text,
            title="📈 Real-Time Speed Telemetry",
            border_style="cyan",
            padding=(1, 1),
            expand=True,   # <- ensures full terminal width, matching aria-cli.py
        )


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
    parser.add_argument("-H", "--host",     type=str, help="Bind address string destination target")
    parser.add_argument("-p", "--port",     type=int, help="Target connection networking port integer")
    parser.add_argument("-n", "--ntfy",     type=str, nargs='?', const="http://localhost:8080/androcall",
                        help="Activate stream extraction mode via subscription URL target")
    parser.add_argument("-d", "--debug",    action="store_true", help="Launch low-level terminal telemetry debugging outputs")
    parser.add_argument("--chart-height",   type=int, default=6, help="Height of speed charts (default: 6)")

    parser.add_argument("--growl-host",     type=str, help="Growl server destination network address target")
    parser.add_argument("--growl-port",     type=int, help="Growl connection network communication port")
    parser.add_argument("--growl-password", type=str, help="Growl verification access credential target")

    args = parser.parse_args()
    GLOBAL_CONFIG = resolve_config(args)

    if args.debug:
        DEBUG_MODE = True

    # rebuild global monitor with requested chart height
    monitor = DownloadMonitor(chart_height=args.chart_height)

    is_ntfy_active  = args.ntfy is not None or any(arg in sys.argv for arg in ['-n', '--ntfy'])
    target_ntfy_url = args.ntfy if isinstance(args.ntfy, str) else GLOBAL_CONFIG["ntfy_url"]

    try:
        if not is_ntfy_active:
            console.print("[bold #00FFFF]use[/] [bold #FFFF00]'-h/--help'[/] [bold #00FFFF]for help[/]")
        asyncio.run(main_application_loop(is_ntfy_active, target_ntfy_url))
    except KeyboardInterrupt:
        console.print("\n[bold red]Termination requested. Safe exit operations finalized.[/bold red]")
