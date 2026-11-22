"""Error mapping tests for ArenaProvider."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


class _Resp:
    def __init__(self, code):
        self.status_code = code


def test_maps_401_to_auth_error():
    from alice_server.llm_arena import ArenaProvider, ArenaAuthError

    resp = _Resp(401)
    with pytest.raises(ArenaAuthError):
        ArenaProvider._handle_http_error(resp)
