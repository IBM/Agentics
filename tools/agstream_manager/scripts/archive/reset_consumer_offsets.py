#!/usr/bin/env python3
"""
reset_consumer_offsets.py
=========================
Reset Kafka consumer group offsets to fix listener consumption issues.

This script addresses the common problem where listeners don't consume messages
because the consumer group offset is already at the end of the topic.

Typical causes:
1. Consumer group already at end of topic
2. Consumer group committed offset after messages were produced
3. Listener started before messages were produced

Usage:
    # Reset specific consumer group for a topic to earliest
    python reset_consumer_offsets.py -g flink-listener-1 -t movies

    # Reset all topics for a consumer group
    python reset_consumer_offsets.py -g my-consumer-group

    # Dry run to see what would be reset
    python reset_consumer_offsets.py -g flink-listener-1 -t movies --dry-run
"""

import argparse
import subprocess
import sys
from typing import Optional


class Colors:
    """ANSI color codes for terminal output."""

    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    BLUE = "\033[0;34m"
    NC = "\033[0m"  # No Color


def print_colored(message: str, color: str = Colors.NC):
    """Print colored message to stdout."""
    print(f"{color}{message}{Colors.NC}")


def run_kafka_command(cmd: list, check: bool = True) -> subprocess.CompletedProcess:
    """Run a Kafka command and return the result."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=check)
        return result
    except subprocess.CalledProcessError as e:
        print_colored(f"Error running command: {' '.join(cmd)}", Colors.RED)
        print_colored(f"Error: {e.stderr}", Colors.RED)
        raise
    except FileNotFoundError:
        print_colored("Error: kafka-consumer-groups command not found", Colors.RED)
        print_colored(
            "Please ensure Kafka is installed and kafka-consumer-groups is in your PATH",
            Colors.RED,
        )
        sys.exit(1)


def describe_consumer_group(broker: str, group: str) -> bool:
    """Describe the current state of a consumer group."""
    cmd = [
        "kafka-consumer-groups",
        "--bootstrap-server",
        broker,
        "--group",
        group,
        "--describe",
    ]

    print_colored("Current consumer group state:", Colors.YELLOW)
    result = run_kafka_command(cmd, check=False)

    if result.returncode != 0:
        print_colored(
            "Warning: Could not describe consumer group (it may not exist yet)",
            Colors.YELLOW,
        )
        return False

    print(result.stdout)
    return True


def reset_offsets(
    broker: str, group: str, topic: Optional[str], reset_to: str, dry_run: bool
) -> bool:
    """Reset consumer group offsets."""
    cmd = [
        "kafka-consumer-groups",
        "--bootstrap-server",
        broker,
        "--group",
        group,
        "--reset-offsets",
        f"--to-{reset_to}",
    ]

    if topic:
        cmd.extend(["--topic", topic])
    else:
        cmd.append("--all-topics")

    if dry_run:
        cmd.append("--dry-run")
        print_colored("DRY RUN - Showing what would be reset:", Colors.YELLOW)
    else:
        cmd.append("--execute")
        print_colored("Resetting offsets...", Colors.YELLOW)

    result = run_kafka_command(cmd)
    print(result.stdout)

    if not dry_run:
        print_colored("✓ Offsets reset successfully!", Colors.GREEN)
        print()
        print_colored("New consumer group state:", Colors.YELLOW)
        describe_consumer_group(broker, group)
    else:
        print()
        print_colored("To actually reset, run without --dry-run flag", Colors.GREEN)

    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Reset Kafka consumer group offsets to fix listener consumption issues.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Reset specific consumer group for a topic to earliest
  %(prog)s -g flink-listener-1 -t movies

  # Reset all topics for a consumer group
  %(prog)s -g my-consumer-group

  # Dry run to see what would be reset
  %(prog)s -g flink-listener-1 -t movies --dry-run

  # Reset to latest instead of earliest
  %(prog)s -g flink-listener-1 -t movies --reset-to latest

Common Use Cases:
  1. Listener not consuming messages (offset at end):
     %(prog)s -g <consumer-group> -t <topic>

  2. Reset all listeners for a topic:
     %(prog)s -g <consumer-group>

  3. Check current offsets before resetting:
     %(prog)s -g <consumer-group> -t <topic> --dry-run
        """,
    )

    parser.add_argument("-g", "--group", required=True, help="Consumer group ID")
    parser.add_argument(
        "-t",
        "--topic",
        help="Topic name (optional, resets all topics if not specified)",
    )
    parser.add_argument(
        "-b",
        "--broker",
        default="localhost:9092",
        help="Kafka broker address (default: localhost:9092)",
    )
    parser.add_argument(
        "-r",
        "--reset-to",
        default="earliest",
        choices=["earliest", "latest"],
        help="Reset position (default: earliest)",
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        help="Show what would be reset without actually resetting",
    )

    args = parser.parse_args()

    # Print header
    print_colored("=== Kafka Consumer Offset Reset Tool ===", Colors.GREEN)
    print(f"Broker: {args.broker}")
    print(f"Consumer Group: {args.group}")
    print(f"Topic: {args.topic or 'all topics'}")
    print(f"Reset To: {args.reset_to}")
    print(f"Dry Run: {args.dry_run}")
    print()

    # Describe current state
    describe_consumer_group(args.broker, args.group)
    print()

    # Reset offsets
    try:
        reset_offsets(args.broker, args.group, args.topic, args.reset_to, args.dry_run)
    except Exception as e:
        print_colored(f"Error: {e}", Colors.RED)
        sys.exit(1)

    # Print footer
    print()
    print_colored("=== Reset Complete ===", Colors.GREEN)
    print()
    print("Next steps:")
    print("1. Restart your listener/consumer")
    print(f"2. It should now consume messages from the {args.reset_to} position")
    print()
    print("To verify consumption, check:")
    print(
        f"  - Consumer lag: kafka-consumer-groups --bootstrap-server {args.broker} --group {args.group} --describe"
    )
    if args.topic:
        print(
            f"  - Topic messages: kafka-console-consumer --bootstrap-server {args.broker} --topic {args.topic} --from-beginning --max-messages 5"
        )


if __name__ == "__main__":
    main()

# Made with Bob
