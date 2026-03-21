"""
tests/conftest.py — shared fixtures
"""
import pytest, sys, os, random
from pathlib import Path
from faker import Faker

sys.path.insert(0, str(Path(__file__).parent.parent))

random.seed(42); Faker.seed(42)

@pytest.fixture
def db_url():
    return os.environ.get("TEST_DB_URL", "postgresql://localhost/apex_ledger_test")

@pytest.fixture
def sample_companies():
    from datagen.company_generator import generate_companies
    return generate_companies(10)

@pytest.fixture
def event_store_class():
    """Returns the EventStore class. Swap for real once implemented."""
    from ledger.event_store import EventStore
    return EventStore
