"""
scripts/run_pipeline.py — Process one application through all agents.
Usage: python scripts/run_pipeline.py --application APEX-0007 [--phase all|document|credit|fraud|compliance|decision]
"""
import argparse, asyncio, os, sys
from pathlib import Path; sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv; load_dotenv()

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--application", required=True)
    p.add_argument("--phase", default="all")
    p.add_argument("--db-url", default=os.environ.get("DATABASE_URL","postgresql://localhost/apex_ledger"))
    args = p.parse_args()

    # TODO: Initialize EventStore, ApplicantRegistryClient, AsyncAnthropic
    # TODO: Route to appropriate agent(s) based on --phase
    print(f"Processing {args.application} through phase: {args.phase}")
    print("TODO: Implement after EventStore and agents are complete.")
    print("See ledger/event_store.py and ledger/agents/base_agent.py")

if __name__ == "__main__":
    asyncio.run(main())
