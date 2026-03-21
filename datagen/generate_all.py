"""
datagen/generate_all.py — Main entry point for the Apex data generator.
Orchestrates: company generation → document generation → event simulation → DB write → validation.

Run: python datagen/generate_all.py [--help]
"""
from __future__ import annotations
import argparse, asyncio, csv, json, os, random, sys
from pathlib import Path
from faker import Faker

sys.path.insert(0, str(Path(__file__).parent.parent))
from datagen.company_generator import generate_companies, TRAJECTORIES
from datagen.pdf_generator import (generate_income_statement_pdf, generate_balance_sheet_pdf,
                                    generate_application_proposal_pdf)
from datagen.excel_generator import generate_financial_excel
from datagen.event_simulator import EventSimulator
from datagen.schema_validator import SchemaValidator

fake = Faker()

SEED_SCENARIOS = [
    ("SUBMITTED",             6),   # Docs requested, nothing uploaded yet
    ("DOCUMENTS_UPLOADED",    5),   # Docs uploaded, agent not run yet
    ("DOCUMENTS_PROCESSED",   4),   # Week 3 pipeline done, credit not started
    ("CREDIT_COMPLETE",       3),   # Credit done, fraud not started
    ("FRAUD_COMPLETE",        2),   # Fraud done, compliance not started
    ("APPROVED",              5),   # Full happy path
    ("DECLINED",              2),   # Agent-driven decline
    ("DECLINED_COMPLIANCE",   1),   # REG-003 (Montana) hard block
    ("REFERRED",              1),   # Low-confidence orchestrator → human review
]

EVENT_STORE_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS events (
    event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id        TEXT NOT NULL,
    stream_position  BIGINT NOT NULL,
    global_position  BIGINT GENERATED ALWAYS AS IDENTITY,
    event_type       TEXT NOT NULL,
    event_version    SMALLINT NOT NULL DEFAULT 1,
    payload          JSONB NOT NULL,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);
CREATE INDEX IF NOT EXISTS idx_events_stream    ON events (stream_id, stream_position);
CREATE INDEX IF NOT EXISTS idx_events_global    ON events (global_position);
CREATE INDEX IF NOT EXISTS idx_events_type      ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_recorded  ON events (recorded_at);
CREATE TABLE IF NOT EXISTS event_streams (
    stream_id        TEXT PRIMARY KEY,
    aggregate_type   TEXT NOT NULL,
    current_version  BIGINT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at      TIMESTAMPTZ,
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_streams_type ON event_streams (aggregate_type);
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name  TEXT PRIMARY KEY,
    last_position    BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS outbox (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         UUID NOT NULL REFERENCES events(event_id),
    destination      TEXT NOT NULL,
    payload          JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at     TIMESTAMPTZ,
    attempts         SMALLINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id        TEXT NOT NULL REFERENCES event_streams(stream_id),
    stream_position  BIGINT NOT NULL,
    aggregate_type   TEXT NOT NULL,
    snapshot_version INT NOT NULL,
    state            JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

REGISTRY_SQL = """
CREATE SCHEMA IF NOT EXISTS applicant_registry;
CREATE TABLE IF NOT EXISTS applicant_registry.companies (
    company_id TEXT PRIMARY KEY, name TEXT NOT NULL, industry TEXT NOT NULL,
    naics TEXT NOT NULL, jurisdiction TEXT NOT NULL, legal_type TEXT NOT NULL,
    founded_year INT NOT NULL, employee_count INT NOT NULL, ein TEXT NOT NULL UNIQUE,
    address_city TEXT NOT NULL, address_state TEXT NOT NULL,
    relationship_start DATE NOT NULL, account_manager TEXT NOT NULL,
    risk_segment TEXT NOT NULL CHECK (risk_segment IN ('LOW','MEDIUM','HIGH')),
    trajectory TEXT NOT NULL, submission_channel TEXT NOT NULL, ip_region TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS applicant_registry.financial_history (
    id SERIAL PRIMARY KEY, company_id TEXT NOT NULL REFERENCES applicant_registry.companies(company_id),
    fiscal_year INT NOT NULL, total_revenue NUMERIC(15,2) NOT NULL, gross_profit NUMERIC(15,2) NOT NULL,
    operating_expenses NUMERIC(15,2) NOT NULL, operating_income NUMERIC(15,2) NOT NULL,
    ebitda NUMERIC(15,2) NOT NULL, depreciation_amortization NUMERIC(15,2) NOT NULL,
    interest_expense NUMERIC(15,2) NOT NULL, income_before_tax NUMERIC(15,2) NOT NULL,
    tax_expense NUMERIC(15,2) NOT NULL, net_income NUMERIC(15,2) NOT NULL,
    total_assets NUMERIC(15,2) NOT NULL, current_assets NUMERIC(15,2) NOT NULL,
    cash_and_equivalents NUMERIC(15,2) NOT NULL, accounts_receivable NUMERIC(15,2) NOT NULL,
    inventory NUMERIC(15,2) NOT NULL, total_liabilities NUMERIC(15,2) NOT NULL,
    current_liabilities NUMERIC(15,2) NOT NULL, long_term_debt NUMERIC(15,2) NOT NULL,
    total_equity NUMERIC(15,2) NOT NULL, operating_cash_flow NUMERIC(15,2) NOT NULL,
    investing_cash_flow NUMERIC(15,2) NOT NULL, financing_cash_flow NUMERIC(15,2) NOT NULL,
    free_cash_flow NUMERIC(15,2) NOT NULL, debt_to_equity NUMERIC(8,4),
    current_ratio NUMERIC(8,4), debt_to_ebitda NUMERIC(8,4),
    interest_coverage_ratio NUMERIC(8,4), gross_margin NUMERIC(8,4),
    ebitda_margin NUMERIC(8,4), net_margin NUMERIC(8,4),
    balance_sheet_check BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (company_id, fiscal_year)
);
CREATE TABLE IF NOT EXISTS applicant_registry.compliance_flags (
    id SERIAL PRIMARY KEY, company_id TEXT NOT NULL REFERENCES applicant_registry.companies(company_id),
    flag_type TEXT NOT NULL CHECK (flag_type IN ('AML_WATCH','SANCTIONS_REVIEW','PEP_LINK')),
    severity TEXT NOT NULL CHECK (severity IN ('LOW','MEDIUM','HIGH')),
    is_active BOOLEAN NOT NULL, added_date DATE NOT NULL, note TEXT
);
CREATE TABLE IF NOT EXISTS applicant_registry.loan_relationships (
    id SERIAL PRIMARY KEY, company_id TEXT NOT NULL REFERENCES applicant_registry.companies(company_id),
    loan_amount NUMERIC(15,2) NOT NULL, loan_year INT NOT NULL,
    was_repaid BOOLEAN NOT NULL, default_occurred BOOLEAN NOT NULL, note TEXT
);
"""

async def write_to_db(db_url: str, companies, all_events):
    import asyncpg, uuid as _uuid
    from datetime import date
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(REGISTRY_SQL)
        await conn.execute(EVENT_STORE_SQL)
        for c in companies:
            await conn.execute("""INSERT INTO applicant_registry.companies
                (company_id,name,industry,naics,jurisdiction,legal_type,founded_year,employee_count,
                 ein,address_city,address_state,relationship_start,account_manager,risk_segment,
                 trajectory,submission_channel,ip_region)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
                ON CONFLICT(company_id) DO NOTHING""",
                c.company_id,c.name,c.industry,c.naics,c.jurisdiction,c.legal_type,
                c.founded_year,c.employee_count,c.ein,c.address_city,c.address_state,
                date.fromisoformat(c.relationship_start),c.account_manager,
                c.risk_segment,c.trajectory,c.submission_channel,c.ip_region)
            for f in c.financials:
                await conn.execute("""INSERT INTO applicant_registry.financial_history
                    (company_id,fiscal_year,total_revenue,gross_profit,operating_expenses,
                     operating_income,ebitda,depreciation_amortization,interest_expense,
                     income_before_tax,tax_expense,net_income,total_assets,current_assets,
                     cash_and_equivalents,accounts_receivable,inventory,total_liabilities,
                     current_liabilities,long_term_debt,total_equity,operating_cash_flow,
                     investing_cash_flow,financing_cash_flow,free_cash_flow,debt_to_equity,
                     current_ratio,debt_to_ebitda,interest_coverage_ratio,gross_margin,
                     ebitda_margin,net_margin,balance_sheet_check)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
                           $19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33)
                    ON CONFLICT(company_id,fiscal_year) DO NOTHING""",
                    c.company_id,f["fiscal_year"],f["total_revenue"],f["gross_profit"],
                    f["operating_expenses"],f["operating_income"],f["ebitda"],
                    f["depreciation_amortization"],f["interest_expense"],f["income_before_tax"],
                    f["tax_expense"],f["net_income"],f["total_assets"],f["current_assets"],
                    f["cash_and_equivalents"],f["accounts_receivable"],f["inventory"],
                    f["total_liabilities"],f["current_liabilities"],f["long_term_debt"],
                    f["total_equity"],f["operating_cash_flow"],f["investing_cash_flow"],
                    f["financing_cash_flow"],f["free_cash_flow"],f["debt_to_equity"],
                    f["current_ratio"],f["debt_to_ebitda"],f["interest_coverage_ratio"],
                    f["gross_margin"],f["ebitda_margin"],f["net_margin"],f["balance_sheet_check"])
            for fl in c.compliance_flags:
                await conn.execute("""INSERT INTO applicant_registry.compliance_flags
                    (company_id,flag_type,severity,is_active,added_date,note)
                    VALUES($1,$2,$3,$4,$5,$6)""",
                    c.company_id,fl["flag_type"],fl["severity"],fl["is_active"],
                    date.fromisoformat(fl["added_date"]),fl["note"])
        pos_map: dict[str, int] = {}
        for stream_id, event_dict, recorded_at in all_events:
            pos = pos_map.get(stream_id, 0) + 1; pos_map[stream_id] = pos
            agg = stream_id.split("-")[0]
            await conn.execute("""INSERT INTO event_streams(stream_id,aggregate_type,current_version)
                VALUES($1,$2,$3) ON CONFLICT(stream_id) DO UPDATE SET current_version=$3""",
                stream_id, agg, pos)
            eid = _uuid.uuid4()
            await conn.execute("""INSERT INTO events
                (event_id,stream_id,stream_position,event_type,event_version,payload,metadata,recorded_at)
                VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8) ON CONFLICT DO NOTHING""",
                eid, stream_id, pos, event_dict["event_type"], event_dict["event_version"],
                json.dumps(event_dict["payload"]),
                json.dumps({"seed":True,"generated_by":"datagen/generate_all.py"}),
                __import__('datetime').datetime.fromisoformat(recorded_at))
        for name in ["application_summary","agent_performance","compliance_audit"]:
            await conn.execute("""INSERT INTO projection_checkpoints(projection_name,last_position)
                VALUES($1,0) ON CONFLICT DO NOTHING""", name)
        print(f"  [DB] {len(companies)} companies, {len(all_events)} events written")
    finally:
        await conn.close()

def main():
    p = argparse.ArgumentParser(description="Apex Financial Services Data Generator")
    p.add_argument("--applicants", type=int, default=80)
    p.add_argument("--output-dir", default="./data")
    p.add_argument("--docs-dir", default="./documents")
    p.add_argument("--db-url", default="postgresql://localhost/apex_ledger")
    p.add_argument("--random-seed", type=int, default=42)
    p.add_argument("--skip-db", action="store_true")
    p.add_argument("--skip-docs", action="store_true")
    p.add_argument("--validate-only", action="store_true")
    args = p.parse_args()

    random.seed(args.random_seed); Faker.seed(args.random_seed)
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.docs_dir, exist_ok=True)

    print("="*60)
    print("APEX FINANCIAL SERVICES — DATA GENERATOR"); print("="*60)

    # ── Step 1: Companies ──────────────────────────────────────────────────────
    print(f"\n[1/5] Generating {args.applicants} company profiles...")
    companies = generate_companies(args.applicants)
    traj_dist = {t: sum(1 for c in companies if c.trajectory==t) for t in TRAJECTORIES}
    risk_dist = {r: sum(1 for c in companies if c.risk_segment==r) for r in ["LOW","MEDIUM","HIGH"]}
    print(f"  [OK] {len(companies)} companies")
    print(f"       Trajectory: {traj_dist}")
    print(f"       Risk: {risk_dist}")
    print(f"       Compliance flags: {sum(1 for c in companies if c.compliance_flags)}")
    mt = next((c for c in companies if c.jurisdiction=="MT"), None)
    print(f"       Montana (REG-003): {mt.company_id if mt else 'NONE — regenerate!'}")
    with open(f"{args.output_dir}/applicant_profiles.json","w") as f:
        json.dump([{"company_id":c.company_id,"name":c.name,"industry":c.industry,
                    "jurisdiction":c.jurisdiction,"legal_type":c.legal_type,
                    "trajectory":c.trajectory,"risk_segment":c.risk_segment,
                    "compliance_flags":c.compliance_flags} for c in companies], f, indent=2)

    # ── Step 2: Documents ──────────────────────────────────────────────────────
    if not args.skip_docs:
        print(f"\n[2/5] Generating financial documents (PDF + Excel + CSV)...")
        count = 0
        for i, c in enumerate(companies):
            d = Path(args.docs_dir)/c.company_id; d.mkdir(exist_ok=True)
            y = 2024
            # Distribute PDF variants for realism
            inc_variant = "missing_ebitda" if i%20==0 else "dense" if i%15==0 else "scanned" if i%12==0 else "clean"
            bs_variant = "scanned" if i%10==0 else "clean"
            generate_income_statement_pdf(c, y, str(d/f"income_statement_{y}.pdf"), inc_variant)
            generate_balance_sheet_pdf(c, y, str(d/f"balance_sheet_{y}.pdf"), bs_variant)
            generate_application_proposal_pdf(c, f"APEX-PROP-{c.company_id}",
                c.financials[-1]["total_revenue"]*random.uniform(0.10,0.35),
                random.choice(c.loan_purposes), str(d/"application_proposal.pdf"))
            generate_financial_excel(c, str(d/"financial_statements.xlsx"))
            # CSV summary
            with open(str(d/"financial_summary.csv"),"w",newline="") as f:
                w = csv.writer(f); w.writerow(["field","value","fiscal_year","currency"])
                for k,v in c.financials[-1].items():
                    if isinstance(v,(int,float)) and k!="fiscal_year":
                        w.writerow([k,v,2024,"USD"])
            count += 5
            if (i+1)%20==0: print(f"  ... {i+1}/{len(companies)} done")
        print(f"  [OK] {count} files in {args.docs_dir}/")

    # ── Step 3: Simulate Events ────────────────────────────────────────────────
    print(f"\n[3/5] Simulating seed event history (all 5 agent pipelines)...")
    all_events = []; validator = SchemaValidator()
    mt_company = next((c for c in companies if c.jurisdiction=="MT"), companies[-1])
    app_num = 1
    for target_state, count in SEED_SCENARIOS:
        for _ in range(count):
            company = mt_company if target_state=="DECLINED_COMPLIANCE" else \
                      random.choice([c for c in companies if c.jurisdiction!="MT"])
            app_id = f"APEX-{app_num:04d}"
            req = round(random.uniform(company.financials[-1]["total_revenue"]*0.08,
                                       company.financials[-1]["total_revenue"]*0.38), -3)
            purpose = random.choice(company.loan_purposes)
            sim = EventSimulator(company=company, application_id=app_id,
                                 requested_amount=req, loan_purpose=purpose)
            events = sim.run(target_state)
            for stream_id, event_dict, _ in events:
                validator.validate(stream_id, event_dict)
            all_events.extend(events); app_num += 1
    print(f"  [OK] {len(all_events)} events across {app_num-1} applications")

    # ── Step 4: Schema validation ──────────────────────────────────────────────
    print(f"\n[4/5] Schema validation...")
    print(validator.report(all_events))
    validator.assert_valid()
    print(f"  [OK] All events validated against EVENT_REGISTRY")

    if args.validate_only:
        print("\n  Validation-only mode. No writes."); return

    # Save JSONL
    out_file = f"{args.output_dir}/seed_events.jsonl"
    with open(out_file,"w") as f:
        for sid, ed, ts in all_events:
            f.write(json.dumps({"stream_id":sid,"event_type":ed["event_type"],"event_version":ed["event_version"],"payload":ed["payload"],"recorded_at":ts})+"\n")

    # ── Step 5: Database ───────────────────────────────────────────────────────
    if not args.skip_db:
        print(f"\n[5/5] Writing to: {args.db_url}")
        try:
            asyncio.run(write_to_db(args.db_url, companies, all_events))
        except ImportError:
            print("  [SKIP] asyncpg not installed — pip install asyncpg")
        except Exception as e:
            print(f"  [WARN] DB write failed: {e}\n         Seed events saved to {out_file}")

    from collections import Counter
    type_dist = Counter(e[1]["event_type"] for e in all_events)
    print(f"\n{'='*60}\nGENERATION COMPLETE")
    print(f"  Companies:       {len(companies)}")
    print(f"  Applications:    {app_num-1}")
    print(f"  Total events:    {len(all_events)}")
    print(f"  Avg per app:     ~{len(all_events)//(app_num-1)}")
    print(f"  Top event types:")
    for et, cnt in sorted(type_dist.items(), key=lambda x:-x[1])[:8]:
        print(f"    {et}: {cnt}")
    print(f"\nNext: python datagen/verify.py --db-url {args.db_url}")

if __name__ == "__main__":
    main()
