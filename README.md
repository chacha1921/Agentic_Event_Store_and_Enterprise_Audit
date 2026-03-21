# The Ledger — Weeks 9-10 Starter Code

## Quick Start

### Python Version
- Use Python 3.11 or 3.12 for this starter.
- Do not use Python 3.13: `asyncpg>=0.29.0,<0.30` does not build cleanly there.

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start PostgreSQL
docker run -d -e POSTGRES_PASSWORD=apex -e POSTGRES_DB=apex_ledger -p 5432:5432 postgres:16

# 3. Set environment
cp .env.example .env
# Edit .env — add your ANTHROPIC_API_KEY

# 4. Generate all data (companies + documents + seed events → DB)
python datagen/generate_all.py --db-url postgresql://postgres:apex@localhost/apex_ledger

# 5. Validate schema (no DB needed)
python datagen/generate_all.py --skip-db --skip-docs --validate-only

# 6. Run Phase 0 tests (must pass before starting Phase 1)
pytest tests/test_schema_and_generator.py -v

# 7. Begin Phase 1: implement EventStore
# Edit: ledger/event_store.py
# Test: pytest tests/test_event_store.py -v
```

## What Works Out of the Box
- Full event schema (45 event types) — `ledger/schema/events.py`
- Complete data generator (GAAP PDFs, Excel, CSV, 1,200+ seed events)
- Event simulator (all 5 agent pipelines, deterministic)
- Schema validator (validates all events against EVENT_REGISTRY)
- Phase 0 tests: 10/10 passing

## What You Implement
| Component | File | Phase |
|-----------|------|-------|
| EventStore | `ledger/event_store.py` | 1 |
| ApplicantRegistryClient | `ledger/registry/client.py` | 1 |
| Domain aggregates | `ledger/domain/aggregates/` | 2 |
| DocumentProcessingAgent | `ledger/agents/base_agent.py` | 2 |
| CreditAnalysisAgent | `ledger/agents/base_agent.py` | 2 (reference given) |
| FraudDetectionAgent | `ledger/agents/base_agent.py` | 3 |
| ComplianceAgent | `ledger/agents/base_agent.py` | 3 |
| DecisionOrchestratorAgent | `ledger/agents/base_agent.py` | 3 |
| Projections + daemon | `ledger/projections/` | 4 |
| Upcasters | `ledger/upcasters.py` | 4 |
| MCP server | `ledger/mcp_server.py` | 5 |

## Gate Tests by Phase
```bash
pytest tests/test_schema_and_generator.py -v  # Phase 0: all must pass before Phase 1
pytest tests/test_event_store.py -v           # Phase 1
pytest tests/test_domain.py -v               # Phase 2
pytest tests/test_narratives.py -v           # Phase 3: all 5 must pass
pytest tests/test_projections.py -v          # Phase 4
pytest tests/test_mcp.py -v                  # Phase 5
```
