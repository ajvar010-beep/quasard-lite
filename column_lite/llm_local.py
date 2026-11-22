"""LLM для ответов на вопросы в автономном режиме (без ПК).

Работает с самих колонок: Pollinations отдаёт ответ за 0,8–6 с без ключа и
без регистрации, доступен из РФ (проверено с колонки 16.11: «Кто написал
Войну и мир?» -> верный ответ за 6 с).

Зачем свой клиент, а не общий alice_server/llm.py: тот тянет fastapi,
инструменты (Tavily), историю в файлах — на колонке с 238 МБ RAM это лишнее.
Здесь минимум: httpx + короткая история в памяти.

Ключи (необязательно, через lite.toml [llm] api_key или env): Orca/SambaNova
дают лучшие модели, но требуют ключ; Pollinations — последний рубеж и
работает всегда.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time

import httpx

log = logging.getLogger("llm")

# Провайдеры по приоритету. Провайдер с ключом (env или lite.toml [llm])
# включается; без ключа пропускается. Опрос — ПАРАЛЛЕЛЬНЫЙ, побеждает первый
# ответивший (см. ask), поэтому добавлять сюда медленные провайдеры бессмысленно.
# Доступность проверена С СЕТИ КОЛОНКИ 20.11.2026: mistral/z.ai/sambanova
# отвечают; groq, cerebras, NVIDIA NIM, together, gemini, openrouter — геоблок
# России (403/451), не подключаем.
PROVIDERS = [
    {"name": "orca", "url": "https://api.orcarouter.ai/v1/chat/completions",
     "env": "ALICE_ORCA_API_KEY", "cfg": "llm_orca_key",
     "model": "z-ai/glm-5.3-flash-free", "max_tokens": 500},
    {"name": "sambanova", "url": "https://api.sambanova.ai/v1/chat/completions",
     "env": "ALICE_SAMBANOVA_API_KEY", "cfg": "llm_sambanova_key",
     "model": "gemma-4-31B-it", "max_tokens": 400},
    {"name": "mistral", "url": "https://api.mistral.ai/v1/chat/completions",
     "env": "ALICE_MISTRAL_API_KEY", "cfg": "llm_mistral_key",
     "model": "mistral-small-latest", "max_tokens": 300, "timeout": 9.0},
    {"name": "zai", "url": "https://api.z.ai/api/paas/v4/chat/completions",
     "env": "ALICE_ZAI_API_KEY", "cfg": "llm_zai_key",
     # glm-4.5-flash по умолчанию «размышляет» (reasoning_content съедает
     # весь лимит токенов, content пустой). С thinking=disabled — ответ за
     # 0.9 с вместо 6.7 (проверено с колонки 20.11).
     "model": "glm-4.5-flash", "max_tokens": 300, "timeout": 9.0,
     "extra": {"thinking": {"type": "disabled"}}},
    # llm7.io: без ключа и регистрации, из РФ (проверено С КОЛОНКИ 17.11:
    # ответ за 0.3–1.8 с, верный русский). Лимит — ~1 запрос/с на IP, при
    # параллельных запросах 429 с «retry after N s»: для голосового ассистента
    # этого хватает. Модель «default» — агрегатор сам выбирает живую.
    {"name": "llm7", "url": "https://api.llm7.io/v1/chat/completions",
     "env": None, "model": "default", "max_tokens": 200, "timeout": 9.0},
    {"name": "pollinations", "url": "https://text.pollinations.ai/openai",
     "env": None, "model": "openai-fast", "max_tokens": 200, "timeout": 12.0},
]

SYSTEM = ("Ты голосовой ассистент Алиса, живёшь в умной колонке. "
          "Ответ — ОДНО-ДВА коротких предложения, до 200 символов: это озвучат "
          "вслух, длинный ответ заставляет ждать. По-русски, без списков, "
          "без markdown, без смайликов. Не знаешь — скажи честно.")

HISTORY_TURNS = 4       # сколько пар «вопрос-ответ» помнить
HISTORY_TTL_S = 600.0   # история забывается через 10 минут тишины


def _retry_after(resp: httpx.Response) -> float:
    """429: сколько секунд просит подождать (заголовок или текст ошибки).

    llm7 честно пишет «retry after 698 seconds» — уважаем, но не больше
    15 минут: дольше проще считать сервис недоступным и жить на pollinations.
    """
    try:
        ra = float(resp.headers.get("retry-after") or 0)
        if 0 < ra <= 900:
            return ra
    except (TypeError, ValueError):
        pass
    try:
        msg = (resp.json().get("error") or {}).get("message", "")
        m = re.search(r"retry after (\d+)", msg, re.I)
        if m:
            return min(float(m.group(1)), 900.0)
    except Exception:  # noqa: BLE001
        pass
    return 30.0


class LocalLLM:
    """Короткий диалог + цепочка провайдеров. Один на процесс lite."""

    def __init__(self, cfg=None):
        self.history: list[dict] = []
        self._last_ts = 0.0
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=8.0))
        self.stats = {"requests": 0, "ok": 0, "failed": 0, "provider": "", "last_error": ""}
        # 429 от провайдера: пропускаем его до истечения паузы. Без этого llm7
        # (лимит ~1 запрос/с) получал 429 почти каждый раз и ответ уходил
        # на медленный pollinations (или не уходил вовсе).
        self._cooldown: dict[str, float] = {}
        # ключи: сначала env, потом lite.toml [llm] (по провайдерам)
        self._cfg = cfg
        self._cfg_key = getattr(cfg, "llm_api_key", "") if cfg is not None else ""
        self._model_override = getattr(cfg, "llm_model", "") if cfg is not None else ""

    def _providers(self) -> list[dict]:
        out = []
        for p in PROVIDERS:
            key = ""
            if p["env"]:
                key = os.environ.get(p["env"], "")
                if not key and p.get("cfg"):
                    # ключ из lite.toml [llm] *_key — удобнее, чем env:
                    # переживает рестарт lite без правки скриптов запуска
                    key = str(getattr(self._cfg, p["cfg"], "") or "")
                if not key:
                    continue  # провайдер без ключа пропускаем
            out.append({**p, "key": key})
        return out

    async def _ask_one(self, p: dict, messages: list[dict]) -> str:
        """Один провайдер. '' при неудаче. 429 -> пауза провайдера."""
        body = {"model": (self._model_override or p["model"]),
                "messages": messages,
                "max_tokens": p["max_tokens"],
                "temperature": 0.6}
        if p.get("extra"):
            body.update(p["extra"])   # напр. thinking=disabled у GLM
        headers = {"Authorization": f"Bearer {p['key']}"} if p["key"] else {}
        t0 = time.monotonic()
        r = await asyncio.wait_for(
            self._client.post(p["url"], json=body, headers=headers),
            timeout=p.get("timeout", 12.0))
        if r.status_code == 429:
            wait = _retry_after(r)
            self._cooldown[p["name"]] = time.monotonic() + wait
            self.stats["last_error"] = f"{p['name']} 429"
            log.info("llm %s: 429, пауза %.0f с", p["name"], wait)
            return ""
        if r.status_code != 200:
            self.stats["last_error"] = f"{p['name']} {r.status_code}"
            log.info("llm %s: HTTP %d", p["name"], r.status_code)
            return ""
        data = r.json()
        answer = ((data.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        answer = answer.strip()
        if answer:
            self.stats["provider"] = p["name"]
            log.info("llm %s: ответ за %.1f с (%d симв)", p["name"],
                     time.monotonic() - t0, len(answer))
        return answer

    async def ask(self, text: str, timeout: float = 20.0) -> str:
        """Вопрос -> ответ. '' если все провайдеры не ответили.

        Провайдеры опрашиваются ПАРАЛЛЕЛЬНО, берём первый ответ: раньше они
        шли по очереди, и при 429 у llm7 каждый вопрос ждал pollinations
        целиком (5–11 с). Теперь время = самый быстрый живой, а не сумма.
        """
        text = (text or "").strip()
        if not text:
            return ""
        now = time.monotonic()
        if now - self._last_ts > HISTORY_TTL_S:
            self.history.clear()
        messages = [{"role": "system", "content": SYSTEM}]
        messages += self.history[-HISTORY_TURNS * 2:]
        messages.append({"role": "user", "content": text})
        self.stats["requests"] += 1
        provs = [p for p in self._providers()
                 if self._cooldown.get(p["name"], 0) <= time.monotonic()]
        if not provs:
            # все в паузе (429): один «дешёвый» шанс лучше молчания —
            # пауза может быть ложной (квота сбросилась раньше)
            provs = [p for p in self._providers() if p["name"] == "pollinations"] \
                    or self._providers()
        tasks = [asyncio.ensure_future(self._ask_one(p, messages)) for p in provs]
        try:
            for fut in asyncio.as_completed(tasks, timeout=timeout):
                try:
                    answer = await fut
                except Exception as e:  # noqa: BLE001
                    log.warning("llm: %r", e)
                    continue
                if not answer:
                    continue
                for t in tasks:
                    t.cancel()
                self.stats["ok"] += 1
                self._last_ts = time.monotonic()
                self.history += [{"role": "user", "content": text},
                                 {"role": "assistant", "content": answer}]
                self.history = self.history[-HISTORY_TURNS * 2:]
                return answer
        except asyncio.TimeoutError:
            pass
        finally:
            for t in tasks:
                t.cancel()
        self.stats["failed"] += 1
        return ""

    def active_model(self) -> tuple[str, str]:
        """(провайдер, модель), по которой последний раз ответили.

        Нужен для честного ответа на «на какой модели ты работаешь?»: раньше
        matcher вопроса был, но нигде не вызывался, и вопрос уходил в LLM —
        та врала про своё имя (аудит 22.11).
        """
        prov = self.stats.get("provider") or ""
        model = self._model_override or ""
        for p in PROVIDERS:
            if p["name"] == prov:
                model = model or p["model"]
                break
        return prov or "резервный", model or "неизвестная"

    def reset(self) -> None:
        self.history.clear()
        self._last_ts = 0.0
