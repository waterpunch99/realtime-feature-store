from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class SendResult:
    accepted: bool
    status_code: int
    response: dict[str, Any] | None
    error: str | None = None


class EventSender:
    def __init__(
        self,
        *,
        collector_url: str,
        timeout_seconds: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.collector_url = collector_url
        self.timeout_seconds = timeout_seconds
        self.transport = transport
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "EventSender":
        self._client = httpx.AsyncClient(
            timeout=self.timeout_seconds,
            transport=self.transport,
        )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send(self, event: dict[str, Any]) -> SendResult:
        client = self._client
        close_client = False
        if client is None:
            client = httpx.AsyncClient(
                timeout=self.timeout_seconds,
                transport=self.transport,
            )
            close_client = True

        try:
            response = await client.post(self.collector_url, json=event)
        except httpx.HTTPError as exc:
            return SendResult(
                accepted=False,
                status_code=0,
                response=None,
                error=str(exc),
            )
        finally:
            if close_client:
                await client.aclose()

        try:
            response_payload = response.json()
        except ValueError:
            response_payload = None

        return SendResult(
            accepted=200 <= response.status_code < 300,
            status_code=response.status_code,
            response=response_payload,
            error=None if 200 <= response.status_code < 300 else response.text,
        )
