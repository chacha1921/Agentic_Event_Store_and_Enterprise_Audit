import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ledger.event_store import EventStore


load_dotenv(PROJECT_ROOT / ".env")


async def generate_cost_report(db_url: str):
    print("Scanning Event Store for Agent LLM usage...")
    store = EventStore(db_url)
    try:
        await store.connect()
    except OSError as exc:
        raise SystemExit(
            "Could not connect to PostgreSQL. "
            "Start the database first and verify DATABASE_URL points to a running server. "
            f"Current db url: {db_url}\nOriginal error: {exc}"
        ) from exc

    total_cost = 0.0
    total_tokens = 0
    app_costs = {}

    try:
        async for event in store.load_all():
            if event.event_type != "AgentSessionCompleted":
                continue

            payload = event.payload
            app_id = payload.get("application_id", "UNKNOWN")
            cost = float(payload.get("total_cost_usd", 0.0))
            tokens = int(payload.get("total_tokens_used", 0))

            total_cost += cost
            total_tokens += tokens
            app_costs[app_id] = app_costs.get(app_id, 0.0) + cost
    finally:
        await store.close()

    num_apps = len(app_costs)
    avg_cost = total_cost / num_apps if num_apps > 0 else 0.0

    artifacts_dir = PROJECT_ROOT / "artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    report_path = artifacts_dir / "api_cost_report.txt"

    with report_path.open("w", encoding="utf-8") as file_handle:
        file_handle.write("=== Apex Ledger: API Cost Report ===\n")
        file_handle.write(f"Total Applications Processed: {num_apps}\n")
        file_handle.write(f"Total Tokens Used: {total_tokens:,}\n")
        file_handle.write(f"Total Cost (USD): ${total_cost:.4f}\n")
        file_handle.write(f"Average Cost Per App: ${avg_cost:.4f}\n\n")
        file_handle.write("--- Breakdown by Application ---\n")
        for app_id, cost in sorted(app_costs.items(), key=lambda item: item[1], reverse=True):
            file_handle.write(f"{app_id}: ${cost:.4f}\n")

    print(f"\n✅ Report successfully generated at: {report_path}")
    print(f"Total Cost: ${total_cost:.4f} | Avg Cost: ${avg_cost:.4f}/app")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an API cost report from AgentSessionCompleted events.")
    parser.add_argument(
        "--db-url",
        default=os.environ.get("DATABASE_URL", "postgresql://localhost/apex_ledger"),
        help="PostgreSQL connection string for the event store.",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    asyncio.run(generate_cost_report(args.db_url))