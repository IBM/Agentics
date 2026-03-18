#!/usr/bin/env python3
"""
Cleanup Orphaned Listener Processes
====================================

This script identifies and cleans up orphaned listener processes that are:
1. Running but not tracked in the AGStream Manager UI (orphaned processes)
2. Tracked in UI but the process is no longer running (zombie entries)

Usage:
    python cleanup_orphaned_listeners.py [--dry-run] [--kill-orphans] [--clean-zombies]

Options:
    --dry-run         Show what would be done without making changes
    --kill-orphans    Kill orphaned processes not tracked in UI
    --clean-zombies   Remove zombie entries from UI
    --auto            Automatically clean both orphans and zombies (non-interactive)
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

import psutil

# Configuration
AGSTREAM_BACKENDS = os.getenv("AGSTREAM_BACKENDS")
if AGSTREAM_BACKENDS:
    PERSISTENCE_DIR = Path(AGSTREAM_BACKENDS) / "agstream-manager"
else:
    SCRIPT_DIR = Path(__file__).parent.parent
    PERSISTENCE_DIR = SCRIPT_DIR

LISTENERS_STORE_FILE = PERSISTENCE_DIR / "agstream_listeners.json"


def load_tracked_listeners() -> Dict:
    """Load listener configurations tracked by the UI"""
    try:
        if LISTENERS_STORE_FILE.exists():
            with open(LISTENERS_STORE_FILE, "r") as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"✗ Error loading listeners config: {e}")
        return {}


def save_tracked_listeners(listeners: Dict):
    """Save updated listener configurations"""
    try:
        with open(LISTENERS_STORE_FILE, "w") as f:
            json.dump(listeners, f, indent=2)
        print(f"✓ Saved updated listeners config to {LISTENERS_STORE_FILE}")
    except Exception as e:
        print(f"✗ Error saving listeners config: {e}")


def find_listener_processes() -> List[Tuple[int, str, str]]:
    """
    Find all Python processes running listener scripts from /tmp/

    Returns:
        List of (pid, script_path, cmdline) tuples
    """
    listener_processes = []

    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            if proc.info["name"] == "python" or proc.info["name"].startswith("python"):
                cmdline = proc.info["cmdline"]
                if cmdline and len(cmdline) >= 2:
                    script_path = cmdline[1]
                    # Check if it's a listener script in /tmp/
                    if script_path.startswith("/tmp/") and script_path.endswith(".py"):
                        listener_processes.append(
                            (proc.info["pid"], script_path, " ".join(cmdline))
                        )
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    return listener_processes


def get_tracked_pids(tracked_listeners: Dict) -> Set[int]:
    """Extract PIDs from tracked listeners"""
    pids = set()
    for listener_data in tracked_listeners.values():
        process = listener_data.get("process")
        if process and isinstance(process, dict):
            pid = process.get("pid")
            if pid:
                pids.add(pid)
    return pids


def get_tracked_temp_files(tracked_listeners: Dict) -> Set[str]:
    """Extract temp file paths from tracked listeners"""
    temp_files = set()
    for listener_data in tracked_listeners.values():
        temp_file = listener_data.get("temp_file")
        if temp_file:
            temp_files.add(temp_file)
    return temp_files


def identify_orphans_and_zombies(
    running_processes: List[Tuple[int, str, str]], tracked_listeners: Dict
) -> Tuple[List[Tuple[int, str]], List[str]]:
    """
    Identify orphaned processes and zombie entries

    Returns:
        (orphaned_processes, zombie_listener_ids)
    """
    tracked_pids = get_tracked_pids(tracked_listeners)
    tracked_temp_files = get_tracked_temp_files(tracked_listeners)

    # Find orphaned processes (running but not tracked)
    orphaned = []
    for pid, script_path, cmdline in running_processes:
        if pid not in tracked_pids and script_path not in tracked_temp_files:
            orphaned.append((pid, script_path))

    # Find zombie entries (tracked but not running)
    zombies = []
    for listener_id, listener_data in tracked_listeners.items():
        process = listener_data.get("process")
        if process and isinstance(process, dict):
            pid = process.get("pid")
            if pid:
                # Check if process is still running
                try:
                    proc = psutil.Process(pid)
                    if not proc.is_running():
                        zombies.append(listener_id)
                except psutil.NoSuchProcess:
                    zombies.append(listener_id)
        elif listener_data.get("status") == "running":
            # Marked as running but no process info
            zombies.append(listener_id)

    return orphaned, zombies


def kill_process(pid: int, script_path: str) -> bool:
    """Kill a process by PID"""
    try:
        proc = psutil.Process(pid)
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except psutil.TimeoutExpired:
            proc.kill()
        print(f"  ✓ Killed process {pid}: {script_path}")
        return True
    except Exception as e:
        print(f"  ✗ Failed to kill process {pid}: {e}")
        return False


def clean_zombie_entry(listener_id: str, tracked_listeners: Dict) -> bool:
    """Mark a zombie entry as stopped"""
    try:
        if listener_id in tracked_listeners:
            tracked_listeners[listener_id]["status"] = "stopped"
            tracked_listeners[listener_id]["process"] = None
            print(f"  ✓ Marked listener '{listener_id}' as stopped")
            return True
        return False
    except Exception as e:
        print(f"  ✗ Failed to clean zombie entry '{listener_id}': {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Cleanup orphaned listener processes and zombie entries"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--kill-orphans",
        action="store_true",
        help="Kill orphaned processes not tracked in UI",
    )
    parser.add_argument(
        "--clean-zombies", action="store_true", help="Remove zombie entries from UI"
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Automatically clean both orphans and zombies (non-interactive)",
    )

    args = parser.parse_args()

    print("=" * 70)
    print("🔍 AGStream Listener Cleanup Tool")
    print("=" * 70)
    print()

    # Load tracked listeners
    print("📂 Loading tracked listeners from UI...")
    tracked_listeners = load_tracked_listeners()
    print(f"   Found {len(tracked_listeners)} tracked listeners")
    print()

    # Find running processes
    print("🔎 Scanning for running listener processes...")
    running_processes = find_listener_processes()
    print(f"   Found {len(running_processes)} running listener processes")
    print()

    # Identify orphans and zombies
    print("🔍 Analyzing processes...")
    orphaned, zombies = identify_orphans_and_zombies(
        running_processes, tracked_listeners
    )
    print()

    # Report findings
    print("=" * 70)
    print("📊 ANALYSIS RESULTS")
    print("=" * 70)
    print()

    if orphaned:
        print(f"⚠️  Found {len(orphaned)} ORPHANED PROCESSES (running but not tracked):")
        for pid, script_path in orphaned:
            print(f"   • PID {pid}: {script_path}")
        print()
    else:
        print("✓ No orphaned processes found")
        print()

    if zombies:
        print(f"⚠️  Found {len(zombies)} ZOMBIE ENTRIES (tracked but not running):")
        for listener_id in zombies:
            listener_data = tracked_listeners[listener_id]
            print(
                f"   • {listener_id}: {listener_data.get('listener_name', 'Unknown')}"
            )
        print()
    else:
        print("✓ No zombie entries found")
        print()

    # Take action
    if not orphaned and not zombies:
        print("✓ All listeners are properly tracked. No cleanup needed.")
        return 0

    if args.dry_run:
        print("🔍 DRY RUN MODE - No changes will be made")
        print()
        if orphaned:
            print("Would kill orphaned processes:")
            for pid, script_path in orphaned:
                print(f"   • PID {pid}: {script_path}")
        if zombies:
            print("Would clean zombie entries:")
            for listener_id in zombies:
                print(f"   • {listener_id}")
        return 0

    # Determine what to do
    kill_orphans = args.kill_orphans or args.auto
    clean_zombies_flag = args.clean_zombies or args.auto

    if not kill_orphans and not clean_zombies_flag and not args.auto:
        print("ℹ️  No action specified. Use --kill-orphans, --clean-zombies, or --auto")
        print("   Run with --help for usage information")
        return 1

    # Kill orphaned processes
    if kill_orphans and orphaned:
        print("=" * 70)
        print("🔪 KILLING ORPHANED PROCESSES")
        print("=" * 70)
        print()
        for pid, script_path in orphaned:
            kill_process(pid, script_path)
        print()

    # Clean zombie entries
    if clean_zombies_flag and zombies:
        print("=" * 70)
        print("🧹 CLEANING ZOMBIE ENTRIES")
        print("=" * 70)
        print()
        for listener_id in zombies:
            clean_zombie_entry(listener_id, tracked_listeners)

        # Save updated config
        save_tracked_listeners(tracked_listeners)
        print()

    print("=" * 70)
    print("✓ Cleanup complete!")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())

# Made with Bob
