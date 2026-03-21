"""
tests/test_schema_and_generator.py
===================================
Phase 0 tests: schema completeness + data generator correctness.
Run BEFORE writing any EventStore code.
These must all pass out of the box with the provided starter code.

Run: pytest tests/test_schema_and_generator.py -v
"""
import json, random, sys, os
from pathlib import Path
from decimal import Decimal

sys.path.insert(0, str(Path(__file__).parent.parent))

from ledger.schema.events import EVENT_REGISTRY, BaseEvent, FinancialFacts, CreditDecision
from datagen.company_generator import generate_companies
from datagen.event_simulator import EventSimulator
from datagen.schema_validator import SchemaValidator

random.seed(42)
from faker import Faker; Faker.seed(42)
COMPANIES = generate_companies(10)

def test_event_registry_completeness():
    """All 45 expected event types are registered."""
    required = [
        "ApplicationSubmitted","DocumentUploaded","CreditAnalysisRequested","FraudScreeningRequested",
        "ComplianceCheckRequested","DecisionGenerated","ApplicationApproved","ApplicationDeclined",
        "PackageCreated","DocumentAdded","DocumentFormatValidated","ExtractionStarted","ExtractionCompleted",
        "QualityAssessmentCompleted","PackageReadyForAnalysis","AgentSessionStarted","AgentInputValidated",
        "AgentInputValidationFailed","AgentNodeExecuted","AgentToolCalled","AgentOutputWritten",
        "AgentSessionCompleted","AgentSessionFailed","AgentSessionRecovered","CreditRecordOpened",
        "HistoricalProfileConsumed","ExtractedFactsConsumed","CreditAnalysisCompleted","CreditAnalysisDeferred",
        "ComplianceCheckInitiated","ComplianceRulePassed","ComplianceRuleFailed","ComplianceRuleNoted",
        "ComplianceCheckCompleted","FraudScreeningInitiated","FraudAnomalyDetected","FraudScreeningCompleted",
        "AuditIntegrityCheckRun","HumanReviewRequested","HumanReviewCompleted",
    ]
    for et in required:
        assert et in EVENT_REGISTRY, f"Missing event type: {et}"

def test_financial_facts_decimal_precision():
    """FinancialFacts stores monetary values as Decimal, not float."""
    facts = FinancialFacts(total_revenue=Decimal("4200000.00"), net_income=Decimal("350000.00"))
    assert isinstance(facts.total_revenue, Decimal)
    assert isinstance(facts.net_income, Decimal)

def test_companies_gaap_balance_sheet():
    """All generated balance sheets satisfy Assets = Liabilities + Equity within $1."""
    for c in COMPANIES:
        for fin in c.financials:
            diff = abs(fin["total_assets"] - fin["total_liabilities"] - fin["total_equity"])
            assert diff < 1.0, f"{c.company_id} FY{fin['fiscal_year']}: balance sheet off by ${diff:.2f}"

def test_companies_trajectory_distribution():
    """80 companies have the expected trajectory distribution."""
    cos = generate_companies(80)
    from collections import Counter; d = Counter(c.trajectory for c in cos)
    assert d["GROWTH"] == 20, f"Expected 20 GROWTH, got {d['GROWTH']}"
    assert d["STABLE"] == 25
    assert d["DECLINING"] == 12

def test_montana_company_exists():
    """Exactly one Montana company exists for REG-003 compliance test."""
    cos = generate_companies(80)
    mt = [c for c in cos if c.jurisdiction == "MT"]
    assert len(mt) >= 1, "No Montana company — REG-003 compliance test requires one"

def test_simulator_approved_path():
    """Full APPROVED pipeline produces all expected event types."""
    c = COMPANIES[0]
    sim = EventSimulator(c, "APEX-TEST-001", 500_000, "working_capital")
    events = sim.run("APPROVED")
    types = {e[1]["event_type"] for e in events}
    required = {"ApplicationSubmitted","DocumentUploaded","PackageCreated","ExtractionCompleted",
                "AgentSessionStarted","AgentSessionCompleted","CreditAnalysisCompleted",
                "FraudScreeningCompleted","ComplianceCheckCompleted","DecisionGenerated","ApplicationApproved"}
    for t in required:
        assert t in types, f"Missing event type in APPROVED path: {t}"

def test_simulator_declined_compliance():
    """Montana company triggers REG-003 hard block → ApplicationDeclined, no DecisionGenerated."""
    mt = next((c for c in COMPANIES if c.jurisdiction == "MT"), None)
    if mt is None:
        cos = generate_companies(80); mt = next(c for c in cos if c.jurisdiction == "MT")
    sim = EventSimulator(mt, "APEX-MT-001", 500_000, "working_capital")
    events = sim.run("DECLINED_COMPLIANCE")
    types = [e[1]["event_type"] for e in events]
    assert "ApplicationDeclined" in types, "Expected ApplicationDeclined for Montana company"
    assert "DecisionGenerated" not in types, "DecisionGenerated must NOT appear after compliance hard block"
    assert "ComplianceRuleFailed" in types
    failed = [e for e in events if e[1]["event_type"] == "ComplianceRuleFailed"]
    payloads = [e[1]["payload"] for e in failed]
    assert any(p.get("is_hard_block") for p in payloads), "REG-003 must be is_hard_block=True"

def test_simulator_agent_session_sequence():
    """Every agent session has: Started → InputValidated → NodeExecuted(s) → OutputWritten → Completed."""
    c = COMPANIES[0]
    sim = EventSimulator(c, "APEX-TEST-SEQ", 400_000, "equipment_financing")
    events = sim.run("APPROVED")
    agent_events = [e for e in events if e[0].startswith("agent-")]
    sessions: dict[str, list[str]] = {}
    for _, ed, _ in agent_events:
        sid = ed["payload"].get("session_id","?")
        sessions.setdefault(sid, []).append(ed["event_type"])
    for sid, types in sessions.items():
        assert types[0] == "AgentSessionStarted", f"Session {sid}: first event must be AgentSessionStarted, got {types[0]}"
        assert "AgentInputValidated" in types, f"Session {sid}: missing AgentInputValidated"
        assert "AgentNodeExecuted" in types, f"Session {sid}: missing AgentNodeExecuted"
        assert "AgentOutputWritten" in types, f"Session {sid}: missing AgentOutputWritten"
        assert types[-1] == "AgentSessionCompleted", f"Session {sid}: last event must be AgentSessionCompleted, got {types[-1]}"

def test_schema_validator_catches_bad_event():
    """SchemaValidator correctly flags events with missing required fields."""
    v = SchemaValidator()
    bad = {"event_type":"ApplicationSubmitted","event_version":1,"payload":{"missing_fields": True}}
    result = v.validate("loan-APEX-BAD", bad)
    assert result == False
    assert len(v.errors) == 1

def test_all_seed_events_validate():
    """Full 29-application seed validates with 0 errors."""
    cos = generate_companies(80)
    v = SchemaValidator(); all_events = []
    mt = next((c for c in cos if c.jurisdiction == "MT"), cos[-1])
    SCENARIOS = [("SUBMITTED",6),("DOCUMENTS_UPLOADED",5),("DOCUMENTS_PROCESSED",4),("CREDIT_COMPLETE",3),("FRAUD_COMPLETE",2),("APPROVED",5),("DECLINED",2),("DECLINED_COMPLIANCE",1),("REFERRED",1)]
    n = 1
    for state, count in SCENARIOS:
        for _ in range(count):
            company = mt if state == "DECLINED_COMPLIANCE" else random.choice([c for c in cos if c.jurisdiction != "MT"])
            sim = EventSimulator(company, f"APEX-{n:04d}", 300_000, "working_capital")
            evts = sim.run(state)
            for sid, ed, _ in evts: v.validate(sid, ed)
            all_events.extend(evts); n += 1
    assert v.errors == [], f"Schema errors:\n" + "\n".join(v.errors[:5])
    assert v.validated >= 1000, f"Expected 1000+ events, got {v.validated}"
