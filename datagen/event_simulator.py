"""datagen/event_simulator.py — Full agent event history simulator for seeding"""
from __future__ import annotations
import hashlib, json, random
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4
from faker import Faker
fake = Faker()

# Import all event classes from canonical schema
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from ledger.schema.events import *

LG_VERSION = "1.0.0-sim"
MODEL = "claude-sonnet-4-20250514"

def _sha(d): return hashlib.sha256(json.dumps(str(d),sort_keys=True).encode()).hexdigest()[:16]
def _sid(t): return f"sess-{t[:3]}-{uuid4().hex[:8]}"

class EventSimulator:
    """
    Deterministically simulates complete agent event history for one application.
    Used to seed the event store with realistic data BEFORE real agents run.
    Every event written is validated against EVENT_REGISTRY.
    The pattern here mirrors exactly what real LangGraph agents produce.
    """
    def __init__(self, company, application_id: str, requested_amount: float, loan_purpose: str):
        self.company = company
        self.application_id = application_id
        self.requested_amount = requested_amount
        self.loan_purpose = loan_purpose
        self.events: list[tuple[str, dict, str]] = []  # (stream_id, event_dict, iso_timestamp)
        self.t = datetime.now() - timedelta(days=random.randint(2, 30))

    def _tick(self, **kw) -> datetime:
        self.t += timedelta(**kw); return self.t

    def _emit(self, stream_id: str, event: BaseEvent) -> None:
        cls = EVENT_REGISTRY.get(event.event_type)
        assert cls, f"Unknown event_type: {event.event_type}"
        cls(event_type=event.event_type, **event.to_payload())  # validate
        self.events.append((stream_id, event.to_store_dict(), self.t.isoformat()))

    def _node(self, stream: str, session_id: str, agent_type: AgentType,
              name: str, seq: int, in_keys: list, out_keys: list,
              llm: bool = False, cost: float = None) -> None:
        tok_in = random.randint(800,3500) if llm else None
        tok_out = random.randint(200,900) if llm else None
        c = cost if cost else (round((tok_in/1e6*3.0 + tok_out/1e6*15.0), 6) if llm else None)
        self._emit(stream, AgentNodeExecuted(
            session_id=session_id, agent_type=agent_type, node_name=name,
            node_sequence=seq, input_keys=in_keys, output_keys=out_keys,
            llm_called=llm, llm_tokens_input=tok_in, llm_tokens_output=tok_out,
            llm_cost_usd=c, duration_ms=random.randint(3000,12000) if llm else random.randint(80,500),
            executed_at=self._tick(seconds=random.randint(8,40) if llm else 2),
        ))

    def run(self, target_state: str) -> list[tuple]:
        STATES = ["SUBMITTED","DOCUMENTS_UPLOADED","DOCUMENTS_PROCESSED",
                  "CREDIT_COMPLETE","FRAUD_COMPLETE","COMPLIANCE_COMPLETE",
                  "APPROVED","DECLINED","DECLINED_COMPLIANCE","REFERRED"]
        idx = STATES.index(target_state)
        self._step_submit()
        if idx == 0: return self.events
        self._step_upload()
        if idx == 1: return self.events
        self._step_doc_processing()
        if idx == 2: return self.events
        self._step_credit()
        if idx == 3: return self.events
        self._step_fraud()
        if idx == 4: return self.events
        comply_blocked = self._step_compliance()
        if idx <= 5 or target_state == "DECLINED_COMPLIANCE": return self.events
        if not comply_blocked:
            self._step_decision(target_state)
        return self.events

    def _step_submit(self):
        app, pkg = f"loan-{self.application_id}", f"docpkg-{self.application_id}"
        self._emit(app, ApplicationSubmitted(
            application_id=self.application_id, applicant_id=self.company.company_id,
            requested_amount_usd=Decimal(str(round(self.requested_amount,-3))),
            loan_purpose=LoanPurpose(self.loan_purpose),
            loan_term_months=random.choice([24,36,48,60]),
            submission_channel=self.company.submission_channel,
            contact_email=fake.email(), contact_name=fake.name(),
            submitted_at=self._tick(minutes=random.randint(1,5)),
            application_reference=self.application_id,
        ))
        self._emit(app, DocumentUploadRequested(
            application_id=self.application_id,
            required_document_types=[DocumentType.APPLICATION_PROPOSAL, DocumentType.INCOME_STATEMENT, DocumentType.BALANCE_SHEET],
            deadline=self.t + timedelta(days=7), requested_by="system",
        ))
        self._emit(pkg, PackageCreated(
            package_id=self.application_id, application_id=self.application_id,
            required_documents=[DocumentType.APPLICATION_PROPOSAL, DocumentType.INCOME_STATEMENT, DocumentType.BALANCE_SHEET],
            created_at=self._tick(seconds=20),
        ))

    def _step_upload(self):
        app, pkg = f"loan-{self.application_id}", f"docpkg-{self.application_id}"
        self._tick(hours=random.randint(2,48))
        for doc_type, fmt, suffix in [
            (DocumentType.APPLICATION_PROPOSAL, DocumentFormat.PDF, "proposal.pdf"),
            (DocumentType.INCOME_STATEMENT, DocumentFormat.PDF, "income_stmt_2024.pdf"),
            (DocumentType.BALANCE_SHEET, DocumentFormat.PDF, "balance_sheet_2024.pdf"),
            (DocumentType.INCOME_STATEMENT, DocumentFormat.XLSX, "financials.xlsx"),
        ]:
            doc_id = f"doc-{uuid4().hex[:8]}"
            fh = _sha(suffix)
            self._emit(app, DocumentUploaded(
                application_id=self.application_id, document_id=doc_id,
                document_type=doc_type, document_format=fmt,
                filename=f"{self.application_id}_{suffix}",
                file_path=f"documents/{self.company.company_id}/{self.application_id}_{suffix}",
                file_size_bytes=random.randint(45_000,380_000),
                file_hash=fh, fiscal_year=2024 if doc_type != DocumentType.APPLICATION_PROPOSAL else None,
                uploaded_at=self._tick(minutes=random.randint(1,10)), uploaded_by="applicant",
            ))
            self._emit(pkg, DocumentAdded(
                package_id=self.application_id, document_id=doc_id,
                document_type=doc_type, document_format=fmt, file_hash=fh, added_at=self.t,
            ))

    def _step_doc_processing(self):
        pkg = f"docpkg-{self.application_id}"
        app = f"loan-{self.application_id}"
        sid = _sid("document_processing")
        astream = f"agent-document_processing-{sid}"
        self._tick(minutes=random.randint(5,30))
        fin = self.company.financials[-1]

        self._emit(astream, AgentSessionStarted(
            session_id=sid, agent_type=AgentType.DOCUMENT_PROCESSING, agent_id="doc-agent-1",
            application_id=self.application_id, model_version=MODEL, langgraph_graph_version=LG_VERSION,
            context_source="fresh", context_token_count=random.randint(800,1800), started_at=self._tick(seconds=3),
        ))
        self._emit(astream, AgentInputValidated(
            session_id=sid, agent_type=AgentType.DOCUMENT_PROCESSING, application_id=self.application_id,
            inputs_validated=["application_id","document_ids","applicant_registry_access"],
            validation_duration_ms=random.randint(50,200), validated_at=self._tick(seconds=2),
        ))
        self._node(astream, sid, AgentType.DOCUMENT_PROCESSING, "validate_inputs", 1, ["application_id","documents"],["validated_inputs","document_paths"])

        for doc_type in [DocumentType.INCOME_STATEMENT, DocumentType.BALANCE_SHEET]:
            doc_id = f"doc-{uuid4().hex[:8]}"
            self._emit(pkg, DocumentFormatValidated(
                package_id=self.application_id, document_id=doc_id, document_type=doc_type,
                page_count=random.randint(2,5), detected_format="pdf", validated_at=self._tick(seconds=5),
            ))
            self._emit(pkg, ExtractionStarted(
                package_id=self.application_id, document_id=doc_id, document_type=doc_type,
                pipeline_version="week3-v1.0", extraction_model="mineru-1.0", started_at=self.t,
            ))
            self._emit(astream, AgentToolCalled(
                session_id=sid, agent_type=AgentType.DOCUMENT_PROCESSING,
                tool_name="week3_extraction_pipeline",
                tool_input_summary=f"PDF extraction: {doc_type.value}",
                tool_output_summary=f"Extracted {random.randint(8,14)} financial line items",
                tool_duration_ms=random.randint(2000,8000), called_at=self._tick(seconds=random.randint(20,60)),
            ))
            facts = FinancialFacts(
                total_revenue=Decimal(str(fin["total_revenue"])),
                gross_profit=Decimal(str(fin["gross_profit"])),
                operating_income=Decimal(str(fin["operating_income"])),
                net_income=Decimal(str(fin["net_income"])),
                ebitda=Decimal(str(fin["ebitda"])) if doc_type==DocumentType.INCOME_STATEMENT else None,
                interest_expense=Decimal(str(fin["interest_expense"])),
                depreciation_amortization=Decimal(str(fin["depreciation_amortization"])),
                total_assets=Decimal(str(fin["total_assets"])),
                total_liabilities=Decimal(str(fin["total_liabilities"])),
                total_equity=Decimal(str(fin["total_equity"])),
                cash_and_equivalents=Decimal(str(fin["cash_and_equivalents"])),
                current_assets=Decimal(str(fin["current_assets"])),
                current_liabilities=Decimal(str(fin["current_liabilities"])),
                long_term_debt=Decimal(str(fin["long_term_debt"])),
                accounts_receivable=Decimal(str(fin["accounts_receivable"])),
                inventory=Decimal(str(fin["inventory"])),
                operating_cash_flow=Decimal(str(fin["operating_cash_flow"])),
                debt_to_equity=fin["debt_to_equity"], current_ratio=fin["current_ratio"],
                debt_to_ebitda=fin["debt_to_ebitda"], interest_coverage=fin["interest_coverage_ratio"],
                gross_margin=fin["gross_margin"], net_margin=fin["net_margin"],
                fiscal_year_end=f"2024-12-31",
                field_confidence={"total_revenue":random.uniform(0.88,0.98),"net_income":random.uniform(0.82,0.95),"total_assets":random.uniform(0.88,0.97)},
                page_references={"total_revenue":"page 1, table 1, row 1","net_income":"page 1, table 1, row 9"},
                balance_sheet_balances=fin["balance_sheet_check"],
            )
            self._emit(pkg, ExtractionCompleted(
                package_id=self.application_id, document_id=doc_id, document_type=doc_type,
                facts=facts, raw_text_length=random.randint(2000,6000),
                tables_extracted=random.randint(2,5), processing_ms=random.randint(2000,8000),
                completed_at=self._tick(seconds=random.randint(15,40)),
            ))

        self._node(astream, sid, AgentType.DOCUMENT_PROCESSING, "run_extraction", 2, ["document_paths"],["raw_facts","page_refs"], llm=False)
        self._node(astream, sid, AgentType.DOCUMENT_PROCESSING, "assess_quality", 3, ["raw_facts","company_profile"],["quality_assessment"], llm=True)

        self._emit(pkg, QualityAssessmentCompleted(
            package_id=self.application_id, document_id=f"doc-{uuid4().hex[:8]}",
            overall_confidence=random.uniform(0.80,0.96), is_coherent=True,
            anomalies=[], critical_missing_fields=[], reextraction_recommended=False,
            auditor_notes="Financial statements appear internally consistent. Balance sheet balances. Revenue consistent with prior year trajectory.",
            assessed_at=self._tick(seconds=15),
        ))
        self._emit(pkg, PackageReadyForAnalysis(
            package_id=self.application_id, application_id=self.application_id,
            documents_processed=3, has_quality_flags=False, quality_flag_count=0, ready_at=self.t,
        ))
        cost_total = round(random.uniform(0.008,0.025),5)
        self._emit(astream, AgentOutputWritten(
            session_id=sid, agent_type=AgentType.DOCUMENT_PROCESSING,
            application_id=self.application_id,
            events_written=[{"stream_id":pkg,"event_type":"PackageReadyForAnalysis","stream_position":len([e for e in self.events if e[0]==pkg])}],
            output_summary=f"3 documents processed, high confidence. Package ready for credit analysis.",
            written_at=self.t,
        ))
        self._emit(astream, AgentSessionCompleted(
            session_id=sid, agent_type=AgentType.DOCUMENT_PROCESSING,
            application_id=self.application_id, total_nodes_executed=3, total_llm_calls=1,
            total_tokens_used=random.randint(1000,2200), total_cost_usd=cost_total,
            total_duration_ms=random.randint(8000,25000), next_agent_triggered="credit_analysis",
            completed_at=self._tick(seconds=8),
        ))
        self._emit(app, CreditAnalysisRequested(
            application_id=self.application_id, requested_at=self._tick(seconds=5),
            requested_by=f"system:session-{sid}", priority="NORMAL",
        ))

    def _step_credit(self):
        app = f"loan-{self.application_id}"
        credit = f"credit-{self.application_id}"
        sid = _sid("credit_analysis")
        astream = f"agent-credit_analysis-{sid}"
        self._tick(minutes=random.randint(2,15))
        fin = self.company.financials[-1]
        score = ((1-min(fin["debt_to_ebitda"]/6,1))*0.3 + min(fin["current_ratio"]/2,1)*0.25 +
                 (1-min(fin["debt_to_equity"]/3,1))*0.25 +
                 (0.3 if self.company.trajectory in ("GROWTH","STABLE") else 0.1 if self.company.trajectory=="DECLINING" else 0.2)*0.2)
        risk_tier = RiskTier.LOW if score>0.70 else RiskTier.MEDIUM if score>0.45 else RiskTier.HIGH
        confidence = round(0.55 + score*0.38, 2)
        limit = round(min(self.requested_amount, fin["total_revenue"]*0.35)*random.uniform(0.80,1.0), -3)

        self._emit(astream, AgentSessionStarted(
            session_id=sid, agent_type=AgentType.CREDIT_ANALYSIS, agent_id="credit-agent-1",
            application_id=self.application_id, model_version=MODEL, langgraph_graph_version=LG_VERSION,
            context_source="fresh", context_token_count=random.randint(1200,2400), started_at=self._tick(seconds=3),
        ))
        self._emit(astream, AgentInputValidated(
            session_id=sid, agent_type=AgentType.CREDIT_ANALYSIS, application_id=self.application_id,
            inputs_validated=["application_id","document_package_id","applicant_id"],
            validation_duration_ms=random.randint(80,250), validated_at=self._tick(seconds=2),
        ))
        self._emit(credit, CreditRecordOpened(
            application_id=self.application_id, applicant_id=self.company.company_id, opened_at=self.t,
        ))
        for seq, (name, in_k, out_k, llm) in enumerate([
            ("validate_inputs",["application_id"],["validated"],False),
            ("open_credit_record",["applicant_id"],["credit_stream_opened"],False),
            ("load_applicant_registry",["applicant_id"],["historical_financials","compliance_flags","loan_history"],False),
            ("load_extracted_facts",["document_package_id"],["current_year_facts","quality_flags"],False),
            ("analyze_credit_risk",["historical_financials","current_year_facts","loan_request"],["credit_decision"],True),
            ("apply_policy_constraints",["credit_decision","loan_history"],["credit_decision","policy_violations"],False),
            ("write_output",["credit_decision"],["events_written"],False),
        ], 1):
            if name == "load_applicant_registry":
                self._emit(astream, AgentToolCalled(
                    session_id=sid, agent_type=AgentType.CREDIT_ANALYSIS,
                    tool_name="query_applicant_registry",
                    tool_input_summary=f"company_id={self.company.company_id}, years=[2022,2023,2024]",
                    tool_output_summary=f"3yr financials, {len(self.company.compliance_flags)} flags",
                    tool_duration_ms=random.randint(50,300), called_at=self._tick(seconds=1),
                ))
                self._emit(credit, HistoricalProfileConsumed(
                    application_id=self.application_id, session_id=sid,
                    fiscal_years_loaded=[2022,2023,2024], has_prior_loans=True, has_defaults=False,
                    revenue_trajectory=self.company.trajectory, data_hash=_sha(self.company.financials),
                    consumed_at=self.t,
                ))
            if name == "load_extracted_facts":
                self._emit(credit, ExtractedFactsConsumed(
                    application_id=self.application_id, session_id=sid,
                    document_ids_consumed=[f"doc-{uuid4().hex[:8]}","doc-{uuid4().hex[:8]}"],
                    facts_summary=f"Revenue ${fin['total_revenue']:,.0f}, EBITDA ${fin['ebitda']:,.0f}, Net Income ${fin['net_income']:,.0f}",
                    quality_flags_present=False, consumed_at=self.t,
                ))
            self._node(astream, sid, AgentType.CREDIT_ANALYSIS, name, seq, in_k, out_k, llm)

        self._emit(credit, CreditAnalysisCompleted(
            application_id=self.application_id, session_id=sid,
            decision=CreditDecision(
                risk_tier=risk_tier, recommended_limit_usd=Decimal(str(limit)),
                confidence=confidence,
                rationale=f"Based on {self.company.trajectory.lower()} revenue trajectory and Debt/EBITDA of {fin['debt_to_ebitda']:.1f}x, the company demonstrates {'strong' if risk_tier==RiskTier.LOW else 'adequate' if risk_tier==RiskTier.MEDIUM else 'stressed'} debt service capacity.",
                key_concerns=[] if risk_tier==RiskTier.LOW else [f"Debt/EBITDA {fin['debt_to_ebitda']:.1f}x above optimal"],
                data_quality_caveats=[],
            ),
            model_version=MODEL, model_deployment_id=f"dep-{uuid4().hex[:8]}",
            input_data_hash=_sha({"fin":self.company.financials,"req":self.requested_amount}),
            analysis_duration_ms=random.randint(15000,45000), completed_at=self._tick(seconds=10),
        ))
        total_tok = random.randint(2000,5000)
        self._emit(astream, AgentOutputWritten(
            session_id=sid, agent_type=AgentType.CREDIT_ANALYSIS, application_id=self.application_id,
            events_written=[{"stream_id":credit,"event_type":"CreditAnalysisCompleted","stream_position":4}],
            output_summary=f"Credit complete. Risk: {risk_tier.value}. Limit: ${limit:,.0f}. Confidence: {confidence:.0%}.",
            written_at=self.t,
        ))
        self._emit(astream, AgentSessionCompleted(
            session_id=sid, agent_type=AgentType.CREDIT_ANALYSIS, application_id=self.application_id,
            total_nodes_executed=7, total_llm_calls=1, total_tokens_used=total_tok,
            total_cost_usd=round(total_tok/1e6*9.0,5), total_duration_ms=random.randint(20000,55000),
            next_agent_triggered="fraud_detection", completed_at=self._tick(seconds=5),
        ))
        self._emit(app, FraudScreeningRequested(
            application_id=self.application_id, requested_at=self._tick(seconds=5),
            triggered_by_event_id=_sha(sid),
        ))

    def _step_fraud(self):
        app = f"loan-{self.application_id}"
        fraud = f"fraud-{self.application_id}"
        sid = _sid("fraud_detection")
        astream = f"agent-fraud_detection-{sid}"
        self._tick(minutes=random.randint(1,8))
        fraud_score = round(min(0.05 + random.uniform(0, 0.15), 1.0), 2)
        risk_level = "LOW" if fraud_score<0.2 else "MEDIUM" if fraud_score<0.5 else "HIGH"
        rec = "PROCEED" if fraud_score<0.3 else "FLAG_FOR_REVIEW" if fraud_score<0.6 else "DECLINE"

        self._emit(astream, AgentSessionStarted(
            session_id=sid, agent_type=AgentType.FRAUD_DETECTION, agent_id="fraud-agent-1",
            application_id=self.application_id, model_version=MODEL, langgraph_graph_version=LG_VERSION,
            context_source="fresh", context_token_count=random.randint(900,1800), started_at=self._tick(seconds=3),
        ))
        self._emit(astream, AgentInputValidated(
            session_id=sid, agent_type=AgentType.FRAUD_DETECTION, application_id=self.application_id,
            inputs_validated=["application_id","extracted_facts_events","registry_access"],
            validation_duration_ms=random.randint(50,150), validated_at=self._tick(seconds=1),
        ))
        self._emit(fraud, FraudScreeningInitiated(
            application_id=self.application_id, session_id=sid,
            screening_model_version=MODEL, initiated_at=self.t,
        ))
        for seq, (name, llm) in enumerate([
            ("validate_inputs",False),("load_document_facts",False),
            ("cross_reference_registry",False),("analyze_fraud_patterns",True),("write_output",False),
        ], 1):
            self._node(astream, sid, AgentType.FRAUD_DETECTION, name, seq, ["application_id"],["fraud_assessment" if name=="analyze_fraud_patterns" else "data"], llm)

        self._emit(fraud, FraudScreeningCompleted(
            application_id=self.application_id, session_id=sid, fraud_score=fraud_score,
            risk_level=risk_level, anomalies_found=0, recommendation=rec,
            screening_model_version=MODEL, input_data_hash=_sha(self.company.company_id),
            completed_at=self._tick(seconds=5),
        ))
        total_tok = random.randint(1200,3100)
        self._emit(astream, AgentOutputWritten(
            session_id=sid, agent_type=AgentType.FRAUD_DETECTION, application_id=self.application_id,
            events_written=[{"stream_id":fraud,"event_type":"FraudScreeningCompleted","stream_position":2}],
            output_summary=f"Fraud screening complete. Score: {fraud_score:.2f}. {rec}.",
            written_at=self.t,
        ))
        self._emit(astream, AgentSessionCompleted(
            session_id=sid, agent_type=AgentType.FRAUD_DETECTION, application_id=self.application_id,
            total_nodes_executed=5, total_llm_calls=1, total_tokens_used=total_tok,
            total_cost_usd=round(total_tok/1e6*9.0,5), total_duration_ms=random.randint(12000,35000),
            next_agent_triggered="compliance", completed_at=self.t,
        ))
        self._emit(app, ComplianceCheckRequested(
            application_id=self.application_id, requested_at=self._tick(seconds=3),
            triggered_by_event_id=_sha(sid), regulation_set_version="2026-Q1",
            rules_to_evaluate=["REG-001","REG-002","REG-003","REG-004","REG-005","REG-006"],
        ))

    def _step_compliance(self) -> bool:
        """Returns True if hard block occurred."""
        app = f"loan-{self.application_id}"
        comp = f"compliance-{self.application_id}"
        sid = _sid("compliance")
        astream = f"agent-compliance-{sid}"
        self._tick(minutes=random.randint(1,6))
        RULES = {
            "REG-001":("Bank Secrecy Act (BSA)",not any(f.get("flag_type")=="AML_WATCH" and f.get("is_active") for f in self.company.compliance_flags),False),
            "REG-002":("OFAC Sanctions",not any(f.get("flag_type")=="SANCTIONS_REVIEW" and f.get("is_active") for f in self.company.compliance_flags),True),
            "REG-003":("Jurisdiction Eligibility",self.company.jurisdiction!="MT",True),
            "REG-004":("Legal Entity Eligibility",not(self.company.legal_type=="Sole Proprietor" and self.requested_amount>250_000),False),
            "REG-005":("Operating History",(2024-self.company.founded_year)>=2,True),
            "REG-006":("CRA Consideration",True,False),
        }
        self._emit(astream, AgentSessionStarted(
            session_id=sid, agent_type=AgentType.COMPLIANCE, agent_id="compliance-agent-1",
            application_id=self.application_id, model_version=MODEL, langgraph_graph_version=LG_VERSION,
            context_source="fresh", context_token_count=random.randint(600,1400), started_at=self._tick(seconds=2),
        ))
        self._emit(astream, AgentInputValidated(
            session_id=sid, agent_type=AgentType.COMPLIANCE, application_id=self.application_id,
            inputs_validated=["application_id","company_profile","regulation_set_version"],
            validation_duration_ms=random.randint(40,150), validated_at=self._tick(seconds=1),
        ))
        self._emit(comp, ComplianceCheckInitiated(
            application_id=self.application_id, session_id=sid,
            regulation_set_version="2026-Q1", rules_to_evaluate=list(RULES.keys()), initiated_at=self.t,
        ))
        passed=failed=noted=0; hard_block=False
        for seq, (rule_id,(name,passes,is_hard)) in enumerate(RULES.items(),1):
            self._node(astream,sid,AgentType.COMPLIANCE,f"evaluate_{rule_id.lower().replace('-','_')}",seq,["company_profile"],[f"{rule_id}_result"])
            eh = _sha(f"{rule_id}-{self.company.company_id}")
            if passes:
                if rule_id=="REG-006":
                    self._emit(comp,ComplianceRuleNoted(application_id=self.application_id,session_id=sid,rule_id=rule_id,rule_name=name,note_type="CRA_CONSIDERATION",note_text="Applicant jurisdiction qualifies for CRA consideration.",evaluated_at=self.t)); noted+=1
                else:
                    self._emit(comp,ComplianceRulePassed(application_id=self.application_id,session_id=sid,rule_id=rule_id,rule_name=name,rule_version="2026-Q1-v1",evidence_hash=eh,evaluation_notes=f"{name}: Clear.",evaluated_at=self.t)); passed+=1
            else:
                self._emit(comp,ComplianceRuleFailed(application_id=self.application_id,session_id=sid,rule_id=rule_id,rule_name=name,rule_version="2026-Q1-v1",failure_reason=f"{name} check failed.",is_hard_block=is_hard,remediation_available=not is_hard,evidence_hash=eh,evaluated_at=self.t)); failed+=1
                if is_hard: hard_block=True; break
        verdict="BLOCKED" if hard_block else "CLEAR" if failed==0 else "CONDITIONAL"
        self._emit(comp,ComplianceCheckCompleted(application_id=self.application_id,session_id=sid,rules_evaluated=passed+failed+noted,rules_passed=passed,rules_failed=failed,rules_noted=noted,has_hard_block=hard_block,overall_verdict=verdict,completed_at=self._tick(seconds=5)))
        total_tok=random.randint(500,1200)
        self._emit(astream,AgentOutputWritten(session_id=sid,agent_type=AgentType.COMPLIANCE,application_id=self.application_id,events_written=[{"stream_id":comp,"event_type":"ComplianceCheckCompleted","stream_position":passed+failed+noted+1}],output_summary=f"Compliance: {verdict}. {passed} passed, {failed} failed.",written_at=self.t))
        self._emit(astream,AgentSessionCompleted(session_id=sid,agent_type=AgentType.COMPLIANCE,application_id=self.application_id,total_nodes_executed=len(RULES),total_llm_calls=0,total_tokens_used=total_tok,total_cost_usd=0.0,total_duration_ms=random.randint(2000,8000),next_agent_triggered=None if hard_block else "decision_orchestrator",completed_at=self.t))
        if hard_block:
            self._emit(app,ApplicationDeclined(application_id=self.application_id,decline_reasons=[f"Compliance hard block: {next(r for r,(n,p,h) in RULES.items() if not p and h)}"],declined_by="compliance-system",adverse_action_notice_required=True,adverse_action_codes=["COMPLIANCE_BLOCK"],declined_at=self._tick(seconds=5)))
        else:
            self._emit(app,DecisionRequested(application_id=self.application_id,requested_at=self._tick(seconds=3),all_analyses_complete=True,triggered_by_event_id=_sha(sid)))
        return hard_block

    def _step_decision(self, target_state: str):
        app = f"loan-{self.application_id}"
        sid = _sid("decision_orchestrator")
        astream = f"agent-decision_orchestrator-{sid}"
        self._tick(minutes=random.randint(2,10))
        rec = "APPROVE" if target_state=="APPROVED" else "DECLINE" if target_state=="DECLINED" else "REFER"
        conf = round(random.uniform(0.70,0.92) if rec=="APPROVE" else random.uniform(0.68,0.88) if rec=="DECLINE" else random.uniform(0.45,0.65),2)
        amt = Decimal(str(round(self.requested_amount*random.uniform(0.80,1.0),-3))) if rec=="APPROVE" else None

        self._emit(astream,AgentSessionStarted(session_id=sid,agent_type=AgentType.DECISION_ORCHESTRATOR,agent_id="orchestrator-1",application_id=self.application_id,model_version=MODEL,langgraph_graph_version=LG_VERSION,context_source="fresh",context_token_count=random.randint(2000,4000),started_at=self._tick(seconds=3)))
        self._emit(astream,AgentInputValidated(session_id=sid,agent_type=AgentType.DECISION_ORCHESTRATOR,application_id=self.application_id,inputs_validated=["application_id","credit_stream","fraud_stream","compliance_stream"],validation_duration_ms=random.randint(100,300),validated_at=self._tick(seconds=2)))
        for seq,(name,llm) in enumerate([("validate_inputs",False),("load_all_analyses",False),("synthesize_decision",True),("apply_hard_constraints",False),("write_output",False)],1):
            self._node(astream,sid,AgentType.DECISION_ORCHESTRATOR,name,seq,["all_agent_outputs"],["final_decision"],llm)
        self._emit(app,DecisionGenerated(
            application_id=self.application_id,orchestrator_session_id=sid,recommendation=rec,confidence=conf,
            approved_amount_usd=amt,conditions=["Monthly financial reporting required","Personal guarantee from principal"] if rec=="APPROVE" else [],
            executive_summary=f"{'Approval' if rec=='APPROVE' else 'Decline' if rec=='DECLINE' else 'Human review'} recommended. {self.company.trajectory} trajectory. Compliance clear. Fraud screening low risk.",
            key_risks=[] if rec=="APPROVE" else ["Revenue trend requires monitoring"],contributing_sessions=[],model_versions={"orchestrator":MODEL},generated_at=self.t,
        ))
        if rec=="APPROVE":
            self._emit(app,ApplicationApproved(application_id=self.application_id,approved_amount_usd=amt,interest_rate_pct=round(random.uniform(5.5,9.5),2),term_months=random.choice([24,36,48,60]),conditions=["Monthly financial reporting required"],approved_by="auto",effective_date=(datetime.now()+timedelta(days=3)).strftime("%Y-%m-%d"),approved_at=self._tick(seconds=10)))
        elif rec=="DECLINE":
            self._emit(app,ApplicationDeclined(application_id=self.application_id,decline_reasons=["Insufficient debt service coverage","High leverage"],declined_by="auto",adverse_action_notice_required=True,adverse_action_codes=["HIGH_DEBT_BURDEN"],declined_at=self._tick(seconds=10)))
        total_tok=random.randint(3000,7000)
        self._emit(astream,AgentOutputWritten(session_id=sid,agent_type=AgentType.DECISION_ORCHESTRATOR,application_id=self.application_id,events_written=[{"stream_id":app,"event_type":"DecisionGenerated","stream_position":10}],output_summary=f"Decision: {rec}. Confidence: {conf:.0%}.",written_at=self.t))
        self._emit(astream,AgentSessionCompleted(session_id=sid,agent_type=AgentType.DECISION_ORCHESTRATOR,application_id=self.application_id,total_nodes_executed=5,total_llm_calls=1,total_tokens_used=total_tok,total_cost_usd=round(total_tok/1e6*9.0,5),total_duration_ms=random.randint(20000,55000),next_agent_triggered=None,completed_at=self._tick(seconds=5)))
