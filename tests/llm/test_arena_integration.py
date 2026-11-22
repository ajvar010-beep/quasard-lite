"""Gentle integration test for live Arena bridge.

Requires a running TTTFXD/LMArenaBridge (ARENA_BASE_URL, по умолчанию 127.0.0.1:8000)
and the environment variable ARENA_INTEGRATION=1.

Run once manually:
    ARENA_INTEGRATION=1 python -m pytest tests/llm/test_arena_integration.py -q

This test performs at most two requests (non-stream + stream) and is
skipped in CI.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


from alice_server.llm_arena import ArenaConfig, ArenaProvider


@pytest.mark.skipif(os.getenv("ARENA_INTEGRATION") != "1", reason="requires host bridge")
@pytest.mark.asyncio
async def test_arena_live_roundtrip():
    provider = ArenaProvider(ArenaConfig.from_env())
    text = await provider.chat([{"role": "user", "content": "say hi in one short sentence"}])
    assert len(text) > 0
