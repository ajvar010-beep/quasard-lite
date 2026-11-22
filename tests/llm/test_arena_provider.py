"""Tests for ArenaProvider configuration and interface."""

import sys
from pathlib import Path

import pytest

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from alice_server.llm_arena import ArenaConfig



def test_arena_config_loads_from_env(monkeypatch):
    monkeypatch.setenv("ARENA_ENABLED", "1")
    cfg = ArenaConfig.from_env()
    assert cfg.base_url == "http://127.0.0.1:8000"
    assert cfg.enabled is True


@pytest.mark.asyncio
async def test_arena_chat_returns_text(monkeypatch):
    from alice_server.llm_arena import ArenaProvider

    provider = ArenaProvider(ArenaConfig(base_url="http://127.0.0.1:12345", enabled=True))

    class FakeResponse:
        status_code = 200

        def json(self):
            return {"choices": [{"message": {"content": "hello from arena"}}]}

        def raise_for_status(self):
            pass

    async def fake_post(*args, **kwargs):
        return FakeResponse()

    monkeypatch.setattr(provider.client, "post", fake_post)

    result = await provider.chat([{"role": "user", "content": "hi"}])
    assert "hello" in result.lower()


@pytest.mark.asyncio
async def test_arena_stream_yields_chunks(monkeypatch):
    from alice_server.llm_arena import ArenaProvider

    provider = ArenaProvider(ArenaConfig(base_url="http://127.0.0.1:12345", enabled=True))

    async def fake_stream(*args, **kwargs):
        yield b'data: {"choices":[{"delta":{"content":"hel"}}]}\n'
        yield b'data: {"choices":[{"delta":{"content":"lo"}}]}\n'
        yield b'data: [DONE]\n'

    monkeypatch.setattr(provider.client, "stream", lambda *a, **k: fake_stream())

    chunks = [c async for c in provider.chat_stream([{"role": "user", "content": "hi"}])]
    assert chunks == ["hel", "lo"]
