"""MiniMax M2.5 client via Anthropic-compatible API."""

from __future__ import annotations

import os
import anthropic


MINIMAX_BASE_URL = "https://api.minimaxi.com/anthropic"
DEFAULT_MODEL = "MiniMax-M2.5"


class MiniMaxClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ["MINIMAX_API_KEY"]
        self._client = anthropic.Anthropic(
            api_key=self.api_key,
            base_url=MINIMAX_BASE_URL,
        )

    def chat(
        self,
        prompt: str,
        system: str = "",
        temperature: float = 0.3,
        max_tokens: int = 2048,
    ) -> str:
        kwargs = {
            "model": DEFAULT_MODEL,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            kwargs["system"] = system

        message = self._client.messages.create(**kwargs)

        # Extract text from response blocks
        parts = []
        for block in message.content:
            if block.type == "text":
                parts.append(block.text)
        return "\n".join(parts).strip()

    def batch_process(
        self,
        items: list[str],
        prompt_template: str,
        system: str = "",
    ) -> list[str]:
        results = []
        for item in items:
            prompt = prompt_template.format(content=item)
            results.append(self.chat(prompt, system=system))
        return results

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
