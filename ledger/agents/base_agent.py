"""
ledger/agents/base_agent.py
===========================
BASE LANGGRAPH AGENT + all 5 agent class stubs.
CreditAnalysisAgent is the reference implementation with full LangGraph pattern.
The other 4 agents are stubs with complete docstrings for implementation.
"""
from __future__ import annotations
import asyncio, hashlib, json, time
from abc import ABC, abstractmethod
from datetime import datetime
from uuid import uuid4
from anthropic import AsyncAnthropic
from langgraph.graph import StateGraph, END

LANGGRAPH_VERSION = "1.0.0"
MAX_OCC_RETRIES = 5

class BaseApexAgent(ABC):
    """
    Base for all 5 Apex agents. Provides Gas Town session management,
    per-node event recording, tool call recording, OCC retry scaffolding.

    AGENT NODE SEQUENCE (all agents follow this):
        start_session → validate_inputs → load_context → [domain nodes] → write_output → end_session

    Each node must call self._record_node_execution() at its end.
    Each tool/registry call must call self._record_tool_call().
    The write_output node must call self._record_output_written() then self._record_node_execution().
    """
    def __init__(self, agent_id: str, agent_type: str, store, registry, client: AsyncAnthropic, model="claude-sonnet-4-20250514"):
        self.agent_id = agent_id; self.agent_type = agent_type
        self.store = store; self.registry = registry; self.client = client; self.model = model
        self.session_id = None; self.application_id = None
        self._session_stream = None; self._t0 = None
        self._seq = 0; self._llm_calls = 0; self._tokens = 0; self._cost = 0.0
        self._graph = None

    @abstractmethod
    def build_graph(self): raise NotImplementedError

    async def process_application(self, application_id: str) -> None:
        if not self._graph: self._graph = self.build_graph()
        self.application_id = application_id
        self.session_id = f"sess-{self.agent_type[:3]}-{uuid4().hex[:8]}"
        self._session_stream = f"agent-{self.agent_type}-{self.session_id}"
        self._t0 = time.time(); self._seq = 0; self._llm_calls = 0; self._tokens = 0; self._cost = 0.0
        await self._start_session(application_id)
        try:
            result = await self._graph.ainvoke(self._initial_state(application_id))
            await self._complete_session(result)
        except Exception as e:
            await self._fail_session(type(e).__name__, str(e)); raise

    def _initial_state(self, app_id):
        return {"application_id": app_id, "session_id": self.session_id,
                "agent_id": self.agent_id, "errors": [], "output_events_written": [], "next_agent_triggered": None}

    async def _start_session(self, app_id):
        await self._append_session({"event_type":"AgentSessionStarted","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"agent_id":self.agent_id,
            "application_id":app_id,"model_version":self.model,"langgraph_graph_version":LANGGRAPH_VERSION,
            "context_source":"fresh","context_token_count":1000,"started_at":datetime.now().isoformat()}})

    async def _record_node_execution(self, name, in_keys, out_keys, ms, tok_in=None, tok_out=None, cost=None):
        self._seq += 1
        if tok_in: self._tokens += tok_in + (tok_out or 0); self._llm_calls += 1
        if cost: self._cost += cost
        await self._append_session({"event_type":"AgentNodeExecuted","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"node_name":name,
            "node_sequence":self._seq,"input_keys":in_keys,"output_keys":out_keys,
            "llm_called":tok_in is not None,"llm_tokens_input":tok_in,"llm_tokens_output":tok_out,
            "llm_cost_usd":cost,"duration_ms":ms,"executed_at":datetime.now().isoformat()}})

    async def _record_tool_call(self, tool, inp, out, ms):
        await self._append_session({"event_type":"AgentToolCalled","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"tool_name":tool,
            "tool_input_summary":inp,"tool_output_summary":out,"tool_duration_ms":ms,
            "called_at":datetime.now().isoformat()}})

    async def _record_output_written(self, events_written, summary):
        await self._append_session({"event_type":"AgentOutputWritten","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"application_id":self.application_id,
            "events_written":events_written,"output_summary":summary,"written_at":datetime.now().isoformat()}})

    async def _complete_session(self, result):
        ms = int((time.time()-self._t0)*1000)
        await self._append_session({"event_type":"AgentSessionCompleted","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"application_id":self.application_id,
            "total_nodes_executed":self._seq,"total_llm_calls":self._llm_calls,"total_tokens_used":self._tokens,
            "total_cost_usd":round(self._cost,6),"total_duration_ms":ms,
            "next_agent_triggered":result.get("next_agent_triggered"),"completed_at":datetime.now().isoformat()}})

    async def _fail_session(self, etype, emsg):
        await self._append_session({"event_type":"AgentSessionFailed","event_version":1,"payload":{
            "session_id":self.session_id,"agent_type":self.agent_type,"application_id":self.application_id,
            "error_type":etype,"error_message":emsg[:500],"last_successful_node":f"node_{self._seq}",
            "recoverable":etype in ("llm_timeout","RateLimitError"),"failed_at":datetime.now().isoformat()}})

    async def _append_session(self, event: dict):
        """TODO: replace print with actual EventStore.append() call"""
        print(f"  [{self.agent_type[:8]}:{self.session_id}] {event['event_type']}")

    async def _append_stream(self, stream_id: str, event_dict: dict, causation_id: str = None):
        """Append to any aggregate stream with OCC retry."""
        for attempt in range(MAX_OCC_RETRIES):
            try:
                ver = await self.store.stream_version(stream_id)
                await self.store.append(stream_id=stream_id, events=[event_dict],
                    expected_version=ver, causation_id=causation_id)
                return
            except Exception as e:
                if "OptimisticConcurrencyError" in type(e).__name__ and attempt < MAX_OCC_RETRIES-1:
                    await asyncio.sleep(0.1 * (2**attempt)); continue
                raise

    async def _call_llm(self, system, user, max_tokens=1024):
        resp = await self.client.messages.create(model=self.model, max_tokens=max_tokens,
            system=system, messages=[{"role":"user","content":user}])
        t = resp.content[0].text; i = resp.usage.input_tokens; o = resp.usage.output_tokens
        return t, i, o, round(i/1e6*3.0 + o/1e6*15.0, 6)

    @staticmethod
    def _sha(d): return hashlib.sha256(json.dumps(str(d),sort_keys=True).encode()).hexdigest()[:16]


class CreditAnalysisAgent(BaseApexAgent):
    """
    Reference implementation. LangGraph nodes:
      validate_inputs → open_credit_record → load_applicant_registry
      → load_extracted_facts → analyze_credit_risk → apply_policy_constraints → write_output

    Output streams:
      credit-{id}: CreditRecordOpened, HistoricalProfileConsumed, ExtractedFactsConsumed, CreditAnalysisCompleted
      loan-{id}: FraudScreeningRequested  (triggers next agent)
    """
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            applicant_id: str | None; requested_amount_usd: float | None; loan_purpose: str | None
            historical_financials: list | None; company_profile: dict | None
            compliance_flags: list | None; loan_history: list | None
            extracted_facts: dict | None; quality_flags: list | None
            credit_decision: dict | None; policy_violations: list | None
            errors: list; output_events_written: list; next_agent_triggered: str | None

        g = StateGraph(S)
        for name, fn in [
            ("validate_inputs",          self._node_validate_inputs),
            ("open_credit_record",       self._node_open_credit_record),
            ("load_applicant_registry",  self._node_load_registry),
            ("load_extracted_facts",     self._node_load_facts),
            ("analyze_credit_risk",      self._node_analyze),
            ("apply_policy_constraints", self._node_policy),
            ("write_output",             self._node_write),
        ]: g.add_node(name, fn)
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs","open_credit_record")
        g.add_edge("open_credit_record","load_applicant_registry")
        g.add_edge("load_applicant_registry","load_extracted_facts")
        g.add_edge("load_extracted_facts","analyze_credit_risk")
        g.add_edge("analyze_credit_risk","apply_policy_constraints")
        g.add_edge("apply_policy_constraints","write_output")
        g.add_edge("write_output", END)
        return g.compile()

    async def _node_validate_inputs(self, state):
        t = time.time()
        # TODO: Load LoanApplicationAggregate, verify state == DOCUMENTS_PROCESSED
        # TODO: Load applicant_id, requested_amount, loan_purpose from ApplicationSubmitted event
        # TODO: Verify PackageReadyForAnalysis event exists in docpkg stream
        # PLACEHOLDER:
        state = {**state, "applicant_id": f"COMP-001", "requested_amount_usd": 500_000.0, "loan_purpose": "working_capital"}
        await self._record_node_execution("validate_inputs",["application_id"],["applicant_id","requested_amount_usd","loan_purpose"],int((time.time()-t)*1000))
        return state

    async def _node_open_credit_record(self, state):
        t = time.time()
        # TODO: await self._append_stream(f"credit-{state['application_id']}", CreditRecordOpened(...).to_store_dict(), expected_version=-1)
        await self._record_node_execution("open_credit_record",["applicant_id"],["credit_stream_opened"],int((time.time()-t)*1000))
        return state

    async def _node_load_registry(self, state):
        t = time.time()
        # TODO: profile = await self.registry.get_company(state["applicant_id"])
        # TODO: hist = await self.registry.get_financial_history(state["applicant_id"], years=[2022,2023,2024])
        # TODO: flags = await self.registry.get_compliance_flags(state["applicant_id"])
        # TODO: loans = await self.registry.get_loan_relationships(state["applicant_id"])
        ms = int((time.time()-t)*1000)
        await self._record_tool_call("query_applicant_registry", f"company_id={state['applicant_id']}", "3yr financials loaded", ms)
        # TODO: await self._append_stream(f"credit-{state['application_id']}", HistoricalProfileConsumed(...).to_store_dict())
        await self._record_node_execution("load_applicant_registry",["applicant_id"],["historical_financials","compliance_flags","loan_history"],ms)
        return {**state,"company_profile":{},"historical_financials":[],"compliance_flags":[],"loan_history":[]}

    async def _node_load_facts(self, state):
        t = time.time()
        # TODO: load ExtractionCompleted events from f"docpkg-{state['application_id']}"
        # TODO: merge FinancialFacts from income_statement + balance_sheet documents
        ms = int((time.time()-t)*1000)
        await self._record_tool_call("load_event_store_stream", f"docpkg-{state['application_id']}", "ExtractionCompleted events loaded", ms)
        # TODO: await self._append_stream(f"credit-{state['application_id']}", ExtractedFactsConsumed(...).to_store_dict())
        await self._record_node_execution("load_extracted_facts",["document_package_events"],["extracted_facts","quality_flags"],ms)
        return {**state,"extracted_facts":{},"quality_flags":[]}

    async def _node_analyze(self, state):
        t = time.time()
        hist = state.get("historical_financials") or []
        fin_table = "\n".join([f"FY{f['fiscal_year'] if isinstance(f,dict) else ''}: (historical data)" for f in hist]) if hist else "No historical data loaded — TODO: implement load_applicant_registry"
        system = """You are a commercial credit analyst at Apex Financial Services.
Evaluate the loan application and return ONLY a JSON object with these fields:
{"risk_tier":"LOW"|"MEDIUM"|"HIGH","recommended_limit_usd":<int>,"confidence":<float 0-1>,
 "rationale":"<3-5 sentences>","key_concerns":[],"data_quality_caveats":[],"policy_overrides_applied":[]}
Hard policy rules you must enforce:
1. recommended_limit_usd <= annual_revenue * 0.35
2. Any prior default → risk_tier must be HIGH
3. Active HIGH compliance flag → confidence must be <= 0.50"""
        user = f"""Applicant: {state.get('company_profile',{}).get('name','Unknown')}
Requested: ${state.get('requested_amount_usd',0):,.0f} for {state.get('loan_purpose','unknown')}
Historical financials:\n{fin_table}
Current year extracted facts: {json.dumps(state.get('extracted_facts',{}),default=str)[:1000]}
Quality flags: {state.get('quality_flags',[])}
Compliance flags: {state.get('compliance_flags',[])}
Prior loans: {state.get('loan_history',[])}"""
        try:
            content, tok_in, tok_out, cost = await self._call_llm(system, user, max_tokens=800)
            import re; m = re.search(r'\{.*\}', content, re.DOTALL)
            decision = json.loads(m.group()) if m else {}
        except Exception as e:
            decision = {"risk_tier":"MEDIUM","recommended_limit_usd":int(state.get("requested_amount_usd",0)*0.8),"confidence":0.45,"rationale":f"Analysis deferred: {e}","key_concerns":["LLM analysis failed — human review required"],"data_quality_caveats":[],"policy_overrides_applied":[]}
            tok_in=tok_out=0; cost=0.0
        ms = int((time.time()-t)*1000)
        await self._record_node_execution("analyze_credit_risk",["historical_financials","extracted_facts"],["credit_decision"],ms,tok_in,tok_out,cost)
        return {**state,"credit_decision":decision}

    async def _node_policy(self, state):
        t = time.time()
        d = state.get("credit_decision") or {}; violations = []
        hist = state.get("historical_financials") or []
        if hist:
            rev = hist[-1].get("total_revenue",0) if isinstance(hist[-1],dict) else 0
            if rev > 0 and d.get("recommended_limit_usd",0) > rev*0.35:
                d["recommended_limit_usd"] = int(rev*0.35); violations.append("REV_CAP")
        if any(l.get("default_occurred") for l in (state.get("loan_history") or [])):
            d["risk_tier"] = "HIGH"; violations.append("PRIOR_DEFAULT")
        if any(f.get("severity")=="HIGH" and f.get("is_active") for f in (state.get("compliance_flags") or [])):
            d["confidence"] = min(d.get("confidence",1.0), 0.50); violations.append("COMPLIANCE_FLAG")
        if violations: d["policy_overrides_applied"] = d.get("policy_overrides_applied",[]) + violations
        await self._record_node_execution("apply_policy_constraints",["credit_decision"],["credit_decision"],int((time.time()-t)*1000))
        return {**state,"credit_decision":d,"policy_violations":violations}

    async def _node_write(self, state):
        t = time.time()
        app_id = state["application_id"]; d = state["credit_decision"]
        # TODO: append CreditAnalysisCompleted to f"credit-{app_id}"
        # TODO: append FraudScreeningRequested to f"loan-{app_id}"
        # Use OCC retry via self._append_stream()
        events_written = [
            {"stream_id":f"credit-{app_id}","event_type":"CreditAnalysisCompleted","stream_position":"TODO"},
            {"stream_id":f"loan-{app_id}","event_type":"FraudScreeningRequested","stream_position":"TODO"},
        ]
        await self._record_output_written(events_written, f"Credit: {d.get('risk_tier')} risk, ${d.get('recommended_limit_usd',0):,.0f} limit, {d.get('confidence',0):.0%} confidence. Fraud screening triggered.")
        await self._record_node_execution("write_output",["credit_decision"],["events_written"],int((time.time()-t)*1000))
        return {**state,"output_events_written":events_written,"next_agent_triggered":"fraud_detection"}


class DocumentProcessingAgent(BaseApexAgent):
    """
    Wraps the Week 3 Document Intelligence pipeline as a LangGraph agent.

    NODES TO IMPLEMENT:
        validate_inputs → validate_document_format → run_week3_extraction
        → assess_quality (LLM) → write_output

    WEEK 3 INTEGRATION — in _node_run_week3_extraction:
        from document_refinery.pipeline import extract_financial_facts
        for each doc in package:
            append ExtractionStarted to docpkg stream
            facts = await extract_financial_facts(file_path, document_type)
            append ExtractionCompleted(facts=facts) to docpkg stream

    LLM ROLE — in _node_assess_quality:
        System prompt: "You are a financial document quality analyst.
        Check extracted facts for internal consistency. Do NOT make credit decisions.
        Return DocumentQualityAssessment JSON."
        Specifically check: balance_sheet_balances, EBITDA plausibility,
        margin ranges for industry, critical missing fields.

    OUTPUT STREAMS:
        docpkg-{id}: DocumentFormatValidated, ExtractionStarted, ExtractionCompleted,
                     QualityAssessmentCompleted, PackageReadyForAnalysis
        loan-{id}: CreditAnalysisRequested
    """
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            document_ids: list | None; extracted_facts_by_doc: dict | None
            quality_assessment: dict | None; has_critical_issues: bool | None
            errors: list; output_events_written: list; next_agent_triggered: str | None
        g = StateGraph(S)
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("validate_document_format",self._node_validate_format)
        g.add_node("run_week3_extraction",     self._node_extract)
        g.add_node("assess_quality",           self._node_assess_quality)
        g.add_node("write_output",             self._node_write_output)
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs","validate_document_format")
        g.add_edge("validate_document_format","run_week3_extraction")
        g.add_edge("run_week3_extraction","assess_quality")
        g.add_edge("assess_quality","write_output")
        g.add_edge("write_output", END)
        return g.compile()

    async def _node_validate_inputs(self, state):
        raise NotImplementedError("Implement _node_validate_inputs: verify DocumentUploaded events exist on loan stream")
    async def _node_validate_format(self, state):
        raise NotImplementedError("Implement _node_validate_format: check PDF/XLSX format, append DocumentFormatValidated or Rejected")
    async def _node_extract(self, state):
        raise NotImplementedError("Implement _node_extract: call Week 3 pipeline per document, append ExtractionStarted + ExtractionCompleted")
    async def _node_assess_quality(self, state):
        raise NotImplementedError("Implement _node_assess_quality: LLM coherence check, append QualityAssessmentCompleted")
    async def _node_write_output(self, state):
        raise NotImplementedError("Implement _node_write_output: append PackageReadyForAnalysis, trigger CreditAnalysisRequested")


class FraudDetectionAgent(BaseApexAgent):
    """
    Detects inconsistencies between submitted documents and registry history.

    NODES TO IMPLEMENT:
        validate_inputs → load_document_facts → cross_reference_registry
        → analyze_fraud_patterns (LLM) → write_output

    LLM ROLE — in _node_analyze_fraud_patterns:
        Compare extracted current-year facts against historical_financials from registry.
        Flag: revenue_discrepancy (> 50% unexplained gap year-on-year),
              balance_sheet_inconsistency, unusual_submission_pattern.
        Compute fraud_score as weighted sum of anomaly severities.
        Return FraudAssessment JSON with named anomalies.
        RULE: fraud_score > 0.3 → must include at least one named anomaly with evidence.

    OUTPUT STREAMS:
        fraud-{id}: FraudScreeningInitiated, FraudAnomalyDetected (0+), FraudScreeningCompleted
        loan-{id}: ComplianceCheckRequested
    """
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            extracted_facts: dict | None; historical_financials: list | None
            company_profile: dict | None; fraud_assessment: dict | None
            errors: list; output_events_written: list; next_agent_triggered: str | None
        g = StateGraph(S)
        for name in ["validate_inputs","load_document_facts","cross_reference_registry","analyze_fraud_patterns","write_output"]:
            g.add_node(name, getattr(self, f"_node_{name}"))
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs","load_document_facts")
        g.add_edge("load_document_facts","cross_reference_registry")
        g.add_edge("cross_reference_registry","analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns","write_output")
        g.add_edge("write_output",END)
        return g.compile()

    async def _node_validate_inputs(self, state): raise NotImplementedError("verify FraudScreeningRequested event exists on loan stream")
    async def _node_load_document_facts(self, state): raise NotImplementedError("load ExtractionCompleted events from docpkg stream")
    async def _node_cross_reference_registry(self, state): raise NotImplementedError("query registry: get_company + get_financial_history")
    async def _node_analyze_fraud_patterns(self, state): raise NotImplementedError("LLM: compare extracted facts vs registry history, compute fraud_score")
    async def _node_write_output(self, state): raise NotImplementedError("append FraudScreeningCompleted, trigger ComplianceCheckRequested")


class ComplianceAgent(BaseApexAgent):
    """
    Evaluates 6 deterministic regulatory rules. No LLM in decision path.

    NODES (6 rule nodes + bookend nodes):
        validate_inputs → check_reg001 → check_reg002 → check_reg003
        → check_reg004 → check_reg005 → check_reg006 → write_output

    Use conditional edges after each hard-block rule:
        graph.add_conditional_edges("check_reg002", self._should_continue,
                                     {"continue":"check_reg003","hard_block":"write_output"})

    RULE IMPLEMENTATIONS (deterministic — no LLM):
        REG-001: not any AML_WATCH flag is_active  → ComplianceRulePassed/Failed
        REG-002: not any SANCTIONS_REVIEW is_active → hard_block=True if failed
        REG-003: jurisdiction != "MT"               → hard_block=True if failed
        REG-004: not (Sole Proprietor AND >$250K)   → remediation_available=True if failed
        REG-005: founded_year <= 2022               → hard_block=True if failed
        REG-006: Always passes → ComplianceRuleNoted(CRA_CONSIDERATION)

    OUTPUT STREAMS:
        compliance-{id}: ComplianceCheckInitiated, ComplianceRulePassed/Failed/Noted (6x), ComplianceCheckCompleted
        loan-{id}: DecisionRequested (if CLEAR/CONDITIONAL) OR ApplicationDeclined (if BLOCKED)
    """
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            company_profile: dict | None; rules_results: list | None
            hard_block: bool | None; overall_verdict: str | None
            errors: list; output_events_written: list; next_agent_triggered: str | None
        g = StateGraph(S)
        for name in ["validate_inputs","check_reg001","check_reg002","check_reg003","check_reg004","check_reg005","check_reg006","write_output"]:
            g.add_node(name, getattr(self, f"_node_{name}"))
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs","check_reg001")
        g.add_edge("check_reg001","check_reg002")
        # REG-002 and REG-003 are hard blocks: conditional edge to write_output if failed
        g.add_conditional_edges("check_reg002", lambda s: "write_output" if s.get("hard_block") else "check_reg003")
        g.add_conditional_edges("check_reg003", lambda s: "write_output" if s.get("hard_block") else "check_reg004")
        g.add_edge("check_reg004","check_reg005")
        g.add_conditional_edges("check_reg005", lambda s: "write_output" if s.get("hard_block") else "check_reg006")
        g.add_edge("check_reg006","write_output")
        g.add_edge("write_output",END)
        return g.compile()

    async def _node_validate_inputs(self, state): raise NotImplementedError("load company profile from registry, verify ComplianceCheckRequested event")
    async def _node_check_reg001(self, state): raise NotImplementedError("BSA: check AML_WATCH flags, append ComplianceRulePassed/Failed")
    async def _node_check_reg002(self, state): raise NotImplementedError("OFAC: check SANCTIONS_REVIEW flags, hard_block=True if failed")
    async def _node_check_reg003(self, state): raise NotImplementedError("Jurisdiction: jurisdiction != 'MT', hard_block=True if failed")
    async def _node_check_reg004(self, state): raise NotImplementedError("Legal type: Sole Proprietor + >250K → failed, remediation_available=True")
    async def _node_check_reg005(self, state): raise NotImplementedError("Operating history: founded_year <= 2022, hard_block=True if failed")
    async def _node_check_reg006(self, state): raise NotImplementedError("CRA: always passes, append ComplianceRuleNoted")
    async def _node_write_output(self, state): raise NotImplementedError("append ComplianceCheckCompleted, then DecisionRequested or ApplicationDeclined")


class DecisionOrchestratorAgent(BaseApexAgent):
    """
    Synthesises all prior agent outputs. Reads from ALL prior agent streams.

    NODES:
        validate_inputs → load_all_analyses → synthesize_decision (LLM)
        → apply_hard_constraints → write_output

    READS FROM:
        credit-{id}: CreditAnalysisCompleted (risk_tier, confidence, limit)
        fraud-{id}: FraudScreeningCompleted (fraud_score, anomalies)
        compliance-{id}: ComplianceCheckCompleted (overall_verdict)

    HARD CONSTRAINTS (Python — not LLM, in apply_hard_constraints):
        1. compliance BLOCKED → must DECLINE regardless of LLM
        2. confidence < 0.60 → must REFER
        3. fraud_score > 0.60 → must REFER
        4. risk_tier HIGH AND confidence >= 0.70 → DECLINE eligible

    LLM ROLE (synthesize_decision):
        Given all 3 analyses, produce executive_summary and key_risks.
        Initial recommendation (may be overridden by hard constraints).
        Return OrchestratorDecision JSON.

    OUTPUT STREAMS:
        loan-{id}: DecisionGenerated
        loan-{id}: ApplicationApproved (if APPROVE) OR ApplicationDeclined (if DECLINE)
                   OR HumanReviewRequested (if REFER)
    """
    def build_graph(self):
        from typing import TypedDict
        class S(TypedDict):
            application_id: str; session_id: str; agent_id: str
            credit_analysis: dict | None; fraud_screening: dict | None
            compliance_record: dict | None; orchestrator_decision: dict | None
            errors: list; output_events_written: list; next_agent_triggered: str | None
        g = StateGraph(S)
        for name in ["validate_inputs","load_all_analyses","synthesize_decision","apply_hard_constraints","write_output"]:
            g.add_node(name, getattr(self, f"_node_{name}"))
        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs","load_all_analyses")
        g.add_edge("load_all_analyses","synthesize_decision")
        g.add_edge("synthesize_decision","apply_hard_constraints")
        g.add_edge("apply_hard_constraints","write_output")
        g.add_edge("write_output",END)
        return g.compile()

    async def _node_validate_inputs(self, state): raise NotImplementedError("verify DecisionRequested event, all 3 analysis streams complete")
    async def _node_load_all_analyses(self, state): raise NotImplementedError("load credit, fraud, compliance streams; extract latest completed events")
    async def _node_synthesize_decision(self, state): raise NotImplementedError("LLM: synthesize all 3 inputs into recommendation + executive_summary")
    async def _node_apply_hard_constraints(self, state): raise NotImplementedError("Python rules: compliance BLOCKED→DECLINE, confidence<0.6→REFER, fraud>0.6→REFER")
    async def _node_write_output(self, state): raise NotImplementedError("append DecisionGenerated + ApplicationApproved/Declined/HumanReviewRequested")
