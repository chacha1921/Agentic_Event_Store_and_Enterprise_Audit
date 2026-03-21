"""
ledger/agents/stub_agents.py
============================
STUB IMPLEMENTATIONS for DocumentProcessingAgent, FraudDetectionAgent,
ComplianceAgent, and DecisionOrchestratorAgent.

Each stub contains:
  - The State TypedDict
  - build_graph() with the correct node sequence
  - All node method stubs with TODO instructions
  - The exact events each node must write
  - WHEN IT WORKS criteria for each agent

Pattern: follow CreditAnalysisAgent exactly. Same build_graph() structure,
same _record_node_execution() calls, same _append_with_retry() for domain writes.
"""
from __future__ import annotations
import time, json
from datetime import datetime
from decimal import Decimal
from typing import TypedDict
from uuid import uuid4

from langgraph.graph import StateGraph, END

from ledger.agents.base_agent import BaseApexAgent


# ─── DOCUMENT PROCESSING AGENT ───────────────────────────────────────────────

class DocProcState(TypedDict):
    application_id: str
    session_id: str
    document_ids: list[str] | None
    document_paths: list[str] | None
    extraction_results: list[dict] | None  # one per document
    quality_assessment: dict | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class DocumentProcessingAgent(BaseApexAgent):
    """
    Wraps the Week 3 Document Intelligence pipeline.
    Processes uploaded PDFs and appends extraction events.

    LangGraph nodes:
        validate_inputs → validate_document_formats → extract_income_statement →
        extract_balance_sheet → assess_quality → write_output

    Output events:
        docpkg-{id}:  DocumentFormatValidated (x per doc), ExtractionStarted (x per doc),
                      ExtractionCompleted (x per doc), QualityAssessmentCompleted,
                      PackageReadyForAnalysis
        loan-{id}:    CreditAnalysisRequested

    WEEK 3 INTEGRATION:
        In _node_extract_document(), call your Week 3 pipeline:
            from document_refinery.pipeline import extract_financial_facts
            facts = await extract_financial_facts(file_path, document_type)
        Wrap in try/except — append ExtractionFailed if pipeline raises.

    LLM in _node_assess_quality():
        System: "You are a financial document quality analyst.
                 Check internal consistency. Do NOT make credit decisions.
                 Return DocumentQualityAssessment JSON."
        The LLM checks: Assets = Liabilities + Equity, margins plausible, etc.

    WHEN THIS WORKS:
        pytest tests/phase2/test_document_agent.py  # all pass
        python scripts/run_pipeline.py --app APEX-0001 --phase document
          → ExtractionCompleted event in docpkg stream with non-null total_revenue
          → QualityAssessmentCompleted event present
          → PackageReadyForAnalysis event present
          → CreditAnalysisRequested on loan stream
    """

    def build_graph(self):
        g = StateGraph(DocProcState)
        g.add_node("validate_inputs",            self._node_validate_inputs)
        g.add_node("validate_document_formats",  self._node_validate_formats)
        g.add_node("extract_income_statement",   self._node_extract_is)
        g.add_node("extract_balance_sheet",      self._node_extract_bs)
        g.add_node("assess_quality",             self._node_assess_quality)
        g.add_node("write_output",               self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",           "validate_document_formats")
        g.add_edge("validate_document_formats", "extract_income_statement")
        g.add_edge("extract_income_statement",  "extract_balance_sheet")
        g.add_edge("extract_balance_sheet",     "assess_quality")
        g.add_edge("assess_quality",            "write_output")
        g.add_edge("write_output",              END)
        return g.compile()

    def _initial_state(self, application_id: str) -> DocProcState:
        return DocProcState(
            application_id=application_id, session_id=self.session_id,
            document_ids=None, document_paths=None,
            extraction_results=None, quality_assessment=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state):
        t = time.time()
        # TODO:
        # 1. Load DocumentUploaded events from "loan-{app_id}" stream
        # 2. Extract document_ids and file_paths for each uploaded document
        # 3. Verify at least APPLICATION_PROPOSAL + INCOME_STATEMENT + BALANCE_SHEET uploaded
        # 4. If any required doc missing: await self._record_input_failed([...], [...]) then raise
        # 5. await self._record_input_validated(["application_id","document_ids","file_paths"], ms)
        raise NotImplementedError("Implement _node_validate_inputs")

    async def _node_validate_formats(self, state):
        t = time.time()
        # TODO:
        # For each document:
        #   1. Check file exists on disk, is not corrupt
        #   2. Detect actual format (PyPDF2, python-magic, etc.)
        #   3. Append DocumentFormatValidated(package_id, doc_id, page_count, detected_format)
        #      to "docpkg-{app_id}" stream
        #   4. If corrupt: append DocumentFormatRejected and remove from processing list
        # 5. await self._record_node_execution("validate_document_formats", ...)
        raise NotImplementedError("Implement _node_validate_formats")

    async def _node_extract_is(self, state):
        t = time.time()
        # TODO:
        # 1. Find income statement document from state["document_paths"]
        # 2. Append ExtractionStarted(package_id, doc_id, pipeline_version, "mineru-1.0")
        #    to "docpkg-{app_id}" stream
        # 3. Call Week 3 pipeline:
        #    from document_refinery.pipeline import extract_financial_facts
        #    facts = await extract_financial_facts(file_path, "income_statement")
        # 4. On success: append ExtractionCompleted(facts=FinancialFacts(**facts), ...)
        # 5. On failure: append ExtractionFailed(error_type, error_message, partial_facts)
        # 6. await self._record_tool_call("week3_extraction_pipeline", ..., ms)
        # 7. await self._record_node_execution("extract_income_statement", ...)
        raise NotImplementedError("Implement _node_extract_is")

    async def _node_extract_bs(self, state):
        t = time.time()
        # TODO: Same pattern as _node_extract_is but for balance sheet
        # Key difference: ExtractionCompleted for balance sheet should populate
        # total_assets, total_liabilities, total_equity, current_assets, etc.
        # The QualityAssessmentCompleted LLM will check Assets = Liabilities + Equity
        raise NotImplementedError("Implement _node_extract_bs")

    async def _node_assess_quality(self, state):
        t = time.time()
        # TODO:
        # 1. Merge extraction results from IS + BS into a combined FinancialFacts
        # 2. Build LLM prompt asking for quality assessment (consistency check)
        # 3. content, ti, to, cost = await self._call_llm(SYSTEM, USER, 512)
        # 4. Parse DocumentQualityAssessment from JSON response
        # 5. Append QualityAssessmentCompleted to "docpkg-{app_id}" stream
        # 6. If critical_missing_fields: add to state["quality_flags"]
        # 7. await self._record_node_execution("assess_quality", ..., ms, ti, to, cost)
        raise NotImplementedError("Implement _node_assess_quality")

    async def _node_write_output(self, state):
        t = time.time()
        # TODO:
        # 1. Append PackageReadyForAnalysis to "docpkg-{app_id}" stream
        # 2. Append CreditAnalysisRequested to "loan-{app_id}" stream
        # 3. await self._record_output_written([...], summary)
        # 4. await self._record_node_execution("write_output", ...)
        # 5. return {**state, "next_agent": "credit_analysis"}
        raise NotImplementedError("Implement _node_write_output")


# ─── FRAUD DETECTION AGENT ───────────────────────────────────────────────────

class FraudState(TypedDict):
    application_id: str
    session_id: str
    extracted_facts: dict | None
    registry_profile: dict | None
    historical_financials: list[dict] | None
    fraud_signals: list[dict] | None
    fraud_score: float | None
    anomalies: list[dict] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class FraudDetectionAgent(BaseApexAgent):
    """
    Cross-references extracted document facts against historical registry data.
    Detects anomalous discrepancies that suggest fraud or document manipulation.

    LangGraph nodes:
        validate_inputs → load_document_facts → cross_reference_registry →
        analyze_fraud_patterns → write_output

    Output events:
        fraud-{id}: FraudScreeningInitiated, FraudAnomalyDetected (0..N),
                    FraudScreeningCompleted
        loan-{id}:  ComplianceCheckRequested

    KEY SCORING LOGIC:
        fraud_score = base(0.05)
            + revenue_discrepancy_factor   (doc revenue vs prior year registry)
            + submission_pattern_factor    (channel, timing, IP region)
            + balance_sheet_consistency    (assets = liabilities + equity within tolerance)

        revenue_discrepancy_factor:
            gap = abs(doc_revenue - registry_prior_revenue) / registry_prior_revenue
            if gap > 0.40 and trajectory not in (GROWTH, RECOVERING): += 0.25

        FraudAnomalyDetected is appended for each anomaly where severity >= MEDIUM.
        fraud_score > 0.60 → recommendation = "DECLINE"
        fraud_score 0.30..0.60 → "FLAG_FOR_REVIEW"
        fraud_score < 0.30 → "PROCEED"

    LLM in _node_analyze():
        System: "You are a financial fraud analyst.
                 Given the cross-reference results, identify specific named anomalies.
                 For each anomaly: type, severity, evidence, affected_fields.
                 Compute a final fraud_score 0-1. Return FraudAssessment JSON."

    WHEN THIS WORKS:
        pytest tests/phase2/test_fraud_agent.py
          → FraudScreeningCompleted event in fraud stream
          → fraud_score between 0.0 and 1.0
          → ComplianceCheckRequested on loan stream
          → NARR-03 (crash recovery) test passes
    """

    def build_graph(self):
        g = StateGraph(FraudState)
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("load_document_facts",     self._node_load_facts)
        g.add_node("cross_reference_registry",self._node_cross_reference)
        g.add_node("analyze_fraud_patterns",  self._node_analyze)
        g.add_node("write_output",            self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",          "load_document_facts")
        g.add_edge("load_document_facts",      "cross_reference_registry")
        g.add_edge("cross_reference_registry", "analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns",   "write_output")
        g.add_edge("write_output",             END)
        return g.compile()

    def _initial_state(self, application_id: str) -> FraudState:
        return FraudState(
            application_id=application_id, session_id=self.session_id,
            extracted_facts=None, registry_profile=None, historical_financials=None,
            fraud_signals=None, fraud_score=None, anomalies=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state): raise NotImplementedError
    async def _node_load_facts(self, state):      raise NotImplementedError
    async def _node_cross_reference(self, state): raise NotImplementedError
    async def _node_analyze(self, state):         raise NotImplementedError
    async def _node_write_output(self, state):    raise NotImplementedError


# ─── COMPLIANCE AGENT ─────────────────────────────────────────────────────────

class ComplianceState(TypedDict):
    application_id: str
    session_id: str
    company_profile: dict | None
    rule_results: list[dict] | None
    has_hard_block: bool
    block_rule_id: str | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


# Regulation definitions — deterministic, no LLM in decision path
REGULATIONS = {
    "REG-001": {
        "name": "Bank Secrecy Act (BSA) Check",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not any(
            f.get("flag_type") == "AML_WATCH" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active AML Watch flag present. Remediation required.",
        "remediation": "Provide enhanced due diligence documentation within 10 business days.",
    },
    "REG-002": {
        "name": "OFAC Sanctions Screening",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: not any(
            f.get("flag_type") == "SANCTIONS_REVIEW" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active OFAC Sanctions Review. Application blocked.",
        "remediation": None,
    },
    "REG-003": {
        "name": "Jurisdiction Lending Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: co.get("jurisdiction") != "MT",
        "failure_reason": "Jurisdiction MT not approved for commercial lending at this time.",
        "remediation": None,
    },
    "REG-004": {
        "name": "Legal Entity Type Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not (
            co.get("legal_type") == "Sole Proprietor"
            and (co.get("requested_amount_usd", 0) or 0) > 250_000
        ),
        "failure_reason": "Sole Proprietor loans >$250K require additional documentation.",
        "remediation": "Submit SBA Form 912 and personal financial statement.",
    },
    "REG-005": {
        "name": "Minimum Operating History",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: (2024 - (co.get("founded_year") or 2024)) >= 2,
        "failure_reason": "Business must have at least 2 years of operating history.",
        "remediation": None,
    },
    "REG-006": {
        "name": "CRA Community Reinvestment",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: True,   # Always noted, never fails
        "note_type": "CRA_CONSIDERATION",
        "note_text": "Jurisdiction qualifies for Community Reinvestment Act consideration.",
    },
}


class ComplianceAgent(BaseApexAgent):
    """
    Evaluates 6 deterministic regulatory rules in sequence.
    Stops at first hard block (is_hard_block=True).
    LLM not used in rule evaluation — only for human-readable evidence summaries.

    LangGraph nodes:
        validate_inputs → load_company_profile → evaluate_reg001 → evaluate_reg002 →
        evaluate_reg003 → evaluate_reg004 → evaluate_reg005 → evaluate_reg006 → write_output

    Note: Use conditional edges after each rule so hard blocks skip remaining rules.
    See add_conditional_edges() in LangGraph docs.

    Output events:
        compliance-{id}: ComplianceCheckInitiated,
                         ComplianceRulePassed/Failed/Noted (one per rule evaluated),
                         ComplianceCheckCompleted
        loan-{id}:       DecisionRequested (if no hard block)
                         ApplicationDeclined (if hard block)

    RULE EVALUATION PATTERN (each _node_evaluate_regXXX):
        1. co = state["company_profile"]
        2. passes = REGULATIONS[rule_id]["check"](co)
        3. eh = self._sha(f"{rule_id}-{co['company_id']}")
        4. If passes: append ComplianceRulePassed or ComplianceRuleNoted
        5. If fails: append ComplianceRuleFailed; if is_hard_block: set state["has_hard_block"]=True
        6. await self._record_node_execution(...)

    ROUTING:
        After each rule node, use conditional edge:
            g.add_conditional_edges(
                "evaluate_reg001",
                lambda s: "write_output" if s["has_hard_block"] else "evaluate_reg002",
            )

    WHEN THIS WORKS:
        pytest tests/phase2/test_compliance_agent.py
          → ComplianceCheckCompleted with correct verdict
          → NARR-04 (Montana REG-003 hard block): no DecisionRequested event,
            ApplicationDeclined present, adverse_action_notice_required=True
    """

    def build_graph(self):
        g = StateGraph(ComplianceState)
        g.add_node("validate_inputs",     self._node_validate_inputs)
        g.add_node("load_company_profile",self._node_load_profile)
        g.add_node("evaluate_reg001",     lambda s: self._evaluate_rule(s, "REG-001"))
        g.add_node("evaluate_reg002",     lambda s: self._evaluate_rule(s, "REG-002"))
        g.add_node("evaluate_reg003",     lambda s: self._evaluate_rule(s, "REG-003"))
        g.add_node("evaluate_reg004",     lambda s: self._evaluate_rule(s, "REG-004"))
        g.add_node("evaluate_reg005",     lambda s: self._evaluate_rule(s, "REG-005"))
        g.add_node("evaluate_reg006",     lambda s: self._evaluate_rule(s, "REG-006"))
        g.add_node("write_output",        self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",      "load_company_profile")
        g.add_edge("load_company_profile", "evaluate_reg001")

        # Conditional edges: stop at hard block, proceed otherwise
        for src, nxt in [
            ("evaluate_reg001", "evaluate_reg002"),
            ("evaluate_reg002", "evaluate_reg003"),
            ("evaluate_reg003", "evaluate_reg004"),
            ("evaluate_reg004", "evaluate_reg005"),
            ("evaluate_reg005", "evaluate_reg006"),
            ("evaluate_reg006", "write_output"),
        ]:
            g.add_conditional_edges(
                src,
                lambda s, _nxt=nxt: "write_output" if s["has_hard_block"] else _nxt,
            )
        g.add_edge("write_output", END)
        return g.compile()

    def _initial_state(self, application_id: str) -> ComplianceState:
        return ComplianceState(
            application_id=application_id, session_id=self.session_id,
            company_profile=None, rule_results=[], has_hard_block=False,
            block_rule_id=None, errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state): raise NotImplementedError
    async def _node_load_profile(self, state):    raise NotImplementedError

    async def _evaluate_rule(self, state: ComplianceState, rule_id: str) -> ComplianceState:
        """
        TODO:
        1. reg = REGULATIONS[rule_id]
        2. co = state["company_profile"] — add "requested_amount_usd" from app
        3. passes = reg["check"](co)
        4. evidence_hash = self._sha(f"{rule_id}-{co['company_id']}-{passes}")
        5. If REG-006 (always noted):
               append ComplianceRuleNoted to "compliance-{app_id}" stream
        6. Elif passes:
               append ComplianceRulePassed
        7. Else:
               append ComplianceRuleFailed
               if reg["is_hard_block"]: state["has_hard_block"]=True, state["block_rule_id"]=rule_id
        8. await self._record_node_execution(f"evaluate_{rule_id.lower().replace('-','_')}", ...)
        """
        raise NotImplementedError(f"Implement _evaluate_rule for {rule_id}")

    async def _node_write_output(self, state): raise NotImplementedError


# ─── DECISION ORCHESTRATOR ────────────────────────────────────────────────────

class OrchestratorState(TypedDict):
    application_id: str
    session_id: str
    credit_result: dict | None
    fraud_result: dict | None
    compliance_result: dict | None
    recommendation: str | None
    confidence: float | None
    approved_amount: float | None
    executive_summary: str | None
    conditions: list[str] | None
    hard_constraints_applied: list[str] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class DecisionOrchestratorAgent(BaseApexAgent):
    """
    Synthesises all prior agent outputs into a final recommendation.
    The only agent that reads from multiple aggregate streams before deciding.

    LangGraph nodes:
        validate_inputs → load_credit_result → load_fraud_result →
        load_compliance_result → synthesize_decision → apply_hard_constraints →
        write_output

    Input streams read (load_* nodes):
        credit-{id}:     CreditAnalysisCompleted (last event of this type)
        fraud-{id}:      FraudScreeningCompleted
        compliance-{id}: ComplianceCheckCompleted

    Output events:
        loan-{id}:  DecisionGenerated
                    ApplicationApproved (if APPROVE)
                    ApplicationDeclined (if DECLINE)
                    HumanReviewRequested (if REFER)

    HARD CONSTRAINTS (Python, not LLM — applied in apply_hard_constraints node):
        1. compliance BLOCKED → recommendation = DECLINE (cannot override)
        2. confidence < 0.60 → recommendation = REFER
        3. fraud_score > 0.60 → recommendation = REFER
        4. risk_tier == HIGH and confidence < 0.70 → recommendation = REFER

    LLM in synthesize_decision:
        System: "You are a senior loan officer synthesising multi-agent analysis.
                 Produce a recommendation (APPROVE/DECLINE/REFER),
                 approved_amount_usd, executive_summary (3-5 sentences),
                 and key_risks list. Return OrchestratorDecision JSON."
        NOTE: The LLM recommendation may be overridden by apply_hard_constraints.
              Log this override in DecisionGenerated.policy_overrides_applied.

    WHEN THIS WORKS:
        pytest tests/phase2/test_orchestrator_agent.py
          → DecisionGenerated event on loan stream
          → NARR-05 (human override): DecisionGenerated.recommendation="DECLINE",
            followed by HumanReviewCompleted.override=True,
            followed by ApplicationApproved with correct override fields
    """

    def build_graph(self):
        g = StateGraph(OrchestratorState)
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("load_credit_result",      self._node_load_credit)
        g.add_node("load_fraud_result",       self._node_load_fraud)
        g.add_node("load_compliance_result",  self._node_load_compliance)
        g.add_node("synthesize_decision",     self._node_synthesize)
        g.add_node("apply_hard_constraints",  self._node_constraints)
        g.add_node("write_output",            self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",        "load_credit_result")
        g.add_edge("load_credit_result",     "load_fraud_result")
        g.add_edge("load_fraud_result",      "load_compliance_result")
        g.add_edge("load_compliance_result", "synthesize_decision")
        g.add_edge("synthesize_decision",    "apply_hard_constraints")
        g.add_edge("apply_hard_constraints", "write_output")
        g.add_edge("write_output",           END)
        return g.compile()

    def _initial_state(self, application_id: str) -> OrchestratorState:
        return OrchestratorState(
            application_id=application_id, session_id=self.session_id,
            credit_result=None, fraud_result=None, compliance_result=None,
            recommendation=None, confidence=None, approved_amount=None,
            executive_summary=None, conditions=None, hard_constraints_applied=[],
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state):  raise NotImplementedError
    async def _node_load_credit(self, state):      raise NotImplementedError
    async def _node_load_fraud(self, state):       raise NotImplementedError
    async def _node_load_compliance(self, state):  raise NotImplementedError
    async def _node_synthesize(self, state):       raise NotImplementedError
    async def _node_constraints(self, state):      raise NotImplementedError
    async def _node_write_output(self, state):     raise NotImplementedError
