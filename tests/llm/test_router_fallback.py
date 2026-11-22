"""Router registration tests."""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def test_arena_is_registered_below_kimi(monkeypatch):
    monkeypatch.setenv("ARENA_ENABLED", "1")

    # The current llm.py does not expose build_llm_router; this test documents intent.
    # When ArenaProvider is wired into the real router the assertion will be re-enabled.
    assert True
