#!/usr/bin/env python3
"""
MunchMusings Automotive-style TUI Dashboard
Thematic dashboard for pipeline management.
"""

import csv
import json
import os
import sys
import time
import select
import tty
import termios
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table
from rich.text import Text
from rich import box

# Constants
REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / 'artifacts/collection/collection-run-manifest.csv'
ACCOUNTING_PATH = REPO_ROOT / 'plans/recent_accounting.csv'
ANOMALY_REPORT_PATH = REPO_ROOT / 'plans/regional_anomaly_report.csv'
CYCLE_ROOT = REPO_ROOT / 'artifacts/operating-cycles'

console = Console()

def is_data():
    return select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])

def load_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline='', encoding='utf-8') as f:
            return list(csv.DictReader(f))
    except Exception:
        return []

def get_latest_log() -> Tuple[Path, List[str]]:
    if not CYCLE_ROOT.exists():
        return Path(), []
    
    cycles = sorted(CYCLE_ROOT.glob('*/'), reverse=True)
    if not cycles:
        return Path(), []
    
    latest_cycle = cycles[0]
    log_file = latest_cycle / 'run.log'
    if not log_file.exists():
        return latest_cycle, []
    
    with log_file.open(encoding='utf-8') as f:
        lines = f.readlines()
    return latest_cycle, lines[-15:]  # Last 15 lines

def get_engine_metrics() -> Dict[str, Any]:
    manifest = load_csv(MANIFEST_PATH)
    counts = {
        'Scheduled': 0,
        'Staged': 0,
        'Completed': 0,
        'Failed': 0
    }
    for row in manifest:
        status = row.get('status', '').lower()
        if status == 'ready':
            counts['Scheduled'] += 1
        elif 'staged' in status:
            counts['Staged'] += 1
        elif status == 'completed':
            counts['Completed'] += 1
        elif status == 'failed':
            counts['Failed'] += 1
    
    # Engine status logic
    if counts['Failed'] > 0:
        status = 'CHECK ENGINE'
        color = 'bold red'
    elif counts['Scheduled'] > 0 or counts['Staged'] > 0:
        status = 'RUNNING'
        color = 'bold yellow'
    else:
        status = 'IDLE'
        color = 'bold green'
        
    return {
        'counts': counts,
        'status': status,
        'color': color,
        'total': len(manifest)
    }

def get_fuel_metrics() -> Dict[str, Any]:
    accounting = load_csv(ACCOUNTING_PATH)
    tier1 = [row for row in accounting if row.get('priority_tier') == 'tier1']
    if not tier1:
        return {'percent': 0.0, 'current': 0, 'total': 0}
    
    current = sum(1 for row in tier1 if row.get('recency_status') == 'current')
    percent = (current / len(tier1)) * 100
    return {
        'percent': percent,
        'current': current,
        'total': len(tier1)
    }

def get_anomaly_metrics() -> List[Dict[str, str]]:
    return load_csv(ANOMALY_REPORT_PATH)

def make_layout() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="main"),
        Layout(name="footer", size=10)
    )
    layout["main"].split_row(
        Layout(name="left"),
        Layout(name="right"),
    )
    return layout

class Header:
    def __init__(self, title="MunchMusings DASHBOARD // TUI-v2"):
        self.title = title

    def __rich__(self) -> Panel:
        grid = Table.grid(expand=True)
        grid.add_column(justify="left", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="right", ratio=1)
        grid.add_row(
            Text(self.title, style="bold cyan"),
            Text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), style="bold white"),
            Text("MISSION CONTROL", style="bold red"),
        )
        return Panel(grid, style="white on blue", box=box.HEAVY)

def make_engine_panel(metrics: Dict[str, Any]) -> Panel:
    table = Table.grid(padding=1)
    table.add_column(style="bold cyan", width=16)
    table.add_column(style="bold white")
    
    for key, value in metrics['counts'].items():
        # Status Light (LED)
        led = "●"
        style = "white"
        if key == "Failed":
            style = "bold red" if value > 0 else "dim red"
        elif key == "Completed":
            style = "bold green" if value > 0 else "dim green"
        elif key == "Scheduled":
            style = "bold yellow" if value > 0 else "dim yellow"
        elif key == "Staged":
            style = "bold cyan" if value > 0 else "dim cyan"
            
        table.add_row(f"[{style}]{led}[/{style}] {key.upper()}", Text(str(value), style=style))
    
    status_text = Text(f"\nENGINE STATUS: {metrics['status']}", style=metrics['color'])
    
    return Panel(
        Group(table, status_text),
        title="[bold yellow]PIPELINE ENGINE[/bold yellow]",
        border_style="yellow",
        box=box.DOUBLE,
        subtitle="[dim]Telemetry-Ready[/dim]"
    )

def make_fuel_panel(metrics: Dict[str, Any]) -> Panel:
    # Custom Gauge-like progress
    color = "green"
    if metrics['percent'] < 25:
        color = "bold red"
    elif metrics['percent'] < 50:
        color = "yellow"
    
    # ASCII Gauge with some 'dial' feel
    gauge_width = 24
    filled = int(metrics['percent'] / 100 * gauge_width)
    gauge_text = "[" + ("=" * filled) + ("-" * (gauge_width - filled)) + "]"
    
    gauge = Text(f" E {gauge_text} F ", style=f"bold {color}")
    
    percent_text = Text(f"\n{metrics['percent']:>3.1f}% SOURCE FRESHNESS", style=f"bold {color}")
    detail = Text(f"\n{metrics['current']}/{metrics['total']} Tier 1 Sources (Current)", style="dim white")
    
    return Panel(
        Group(gauge, percent_text, detail),
        title="[bold green]SOURCE FRESHNESS FUEL[/bold green]",
        border_style="green",
        box=box.DOUBLE,
        subtitle="[dim]Tier 1 Status[/dim]"
    )

def make_maritime_radar() -> Panel:
    grid = Table.grid(expand=True)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    
    def make_dial(name, status, color):
        return Group(
            Text(name, style="bold white"),
            Text("╭───╮", style=color),
            Text(f"│ {status} │", style=f"bold {color}"),
            Text("╰───╯", style=color),
        )

    grid.add_row(
        make_dial("SUEZ", "OK ", "green"),
        make_dial("ASHDOD", "ACT", "green"),
        make_dial("HAIFA", "ACT", "green")
    )
    
    return Panel(grid, title="[bold cyan]MARITIME RADAR[/bold cyan]", border_style="cyan", box=box.ROUNDED)

def make_price_shock_panel(anomaly_data: List[Dict[str, str]]) -> Panel:
    table = Table.grid(padding=1)
    table.add_column(style="bold white", width=12)
    table.add_column(width=20)
    
    # Extract price shock items from anomaly data if present
    items = []
    for row in anomaly_data:
        if "Price" in row.get('Anomaly_Type', '') or "Demand" in row.get('Anomaly_Type', ''):
            impact = float(row.get('Impact_Score', 0.5)) * 20  # Scale to 0-20
            color = "green"
            if impact > 15:
                color = "bold red"
            elif impact > 10:
                color = "orange3"
            elif impact > 5:
                color = "yellow"
            
            food = row.get('Migration_Indicator_Food', 'STAPLE').split('(')[0].strip().upper()
            items.append((food[:10], impact, color))
    
    # Fallback items if CSV is empty or lacking shocks
    if not items:
        items = [
            ("BREAD", 5, "yellow"),
            ("OIL", 12, "bold red"),
            ("SUGAR", 8, "orange3"),
            ("GRAINS", 3, "green")
        ]
    
    for name, val, color in items[:4]:
        bar_width = 15
        filled = int(val / 20 * bar_width)
        bar = "[" + ("█" * filled) + (" " * (bar_width - filled)) + "]"
        table.add_row(name, Text(f"{bar} +{int(val)}%", style=color))
        
    return Panel(table, title="[bold red]PRICE SHOCK GAUGES[/bold red]", border_style="red", box=box.DOUBLE)

def make_migration_heatmap(anomaly_data: List[Dict[str, str]]) -> Panel:
    table = Table(box=box.SIMPLE, expand=True)
    table.add_column("REGION", style="cyan")
    table.add_column("GROUP", style="yellow")
    table.add_column("HABIT SHIFT", style="magenta")
    table.add_column("HEAT", justify="right")
    
    if not anomaly_data:
        table.add_row("Cairo (Sudanese)", "Sudanese", "High Bakery Density", "[bold red]🔥🔥🔥[/bold red]")
        table.add_row("Alexandria", "Mixed", "Stable", "[blue]❄️[/blue]")
        table.add_row("Giza", "Sudanese/Syrian", "Lexicon Drift", "[bold yellow]🔥[/bold yellow]")
    else:
        for row in anomaly_data:
            impact = float(row.get('Impact_Score', 0))
            if impact > 0.8:
                heat = "[bold red]🔥🔥🔥[/bold red]"
            elif impact > 0.6:
                heat = "[bold orange3]🔥🔥[/bold orange3]"
            elif impact > 0.4:
                heat = "[bold yellow]🔥[/bold yellow]"
            else:
                heat = "[blue]❄️[/blue]"
            
            table.add_row(
                row.get('Region', 'Unknown'),
                row.get('Associated_Group', 'N/A')[:15],
                row.get('Migration_Indicator_Food', 'None')[:25],
                heat
            )
    
    return Panel(table, title="[bold magenta]MIGRATION HEATMAP[/bold magenta]", border_style="magenta", box=box.ROUNDED)

def make_telemetry_panel(log_lines: List[str], cycle_id: str, instructions: str = "") -> Panel:
    lines = [Text(line.strip(), style="dim green", overflow="ellipsis") for line in log_lines]
    if instructions:
        lines.append(Text("\n" + instructions, style="bold white on blue", justify="center"))
    content = Group(*lines)
    return Panel(
        content,
        title=f"[bold cyan]REAL-TIME TELEMETRY[/bold cyan] - {cycle_id}",
        border_style="cyan",
        box=box.SQUARE
    )

def main():
    layout = make_layout()
    header = Header()
    
    # Input handling setup
    old_settings = termios.tcgetattr(sys.stdin)
    tty.setcbreak(sys.stdin.fileno())
    
    current_view = "engine"
    
    try:
        with Live(layout, refresh_per_second=2, screen=True):
            while True:
                if is_data():
                    char = sys.stdin.read(1)
                    if char == 'q':
                        break
                    elif char == '1':
                        current_view = "engine"
                    elif char == '2':
                        current_view = "cross_border"

                cycle_path, log_lines = get_latest_log()
                anomaly_data = get_anomaly_metrics()
                instr = "[1] PIPELINE ENGINE   [2] CROSS-BORDER   [Q] QUIT"

                if current_view == "engine":
                    engine = get_engine_metrics()
                    fuel = get_fuel_metrics()
                    header.title = "MunchMusings DASHBOARD // ENGINE"
                    
                    layout["left"].update(make_engine_panel(engine))
                    layout["right"].update(make_fuel_panel(fuel))
                else:
                    header.title = "MunchMusings DASHBOARD // CROSS-BORDER"
                    layout["left"].update(make_maritime_radar())
                    # Split right into two for Cross-Border
                    right_group = Group(make_price_shock_panel(anomaly_data), make_migration_heatmap(anomaly_data))
                    layout["right"].update(right_group)

                layout["header"].update(header)
                layout["footer"].update(make_telemetry_panel(log_lines, cycle_path.name, instr))
                
                time.sleep(0.1)
    finally:
        termios.tcsetattr(sys.stdin, old_settings)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
