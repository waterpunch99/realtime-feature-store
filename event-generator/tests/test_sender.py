import httpx
import pytest

from generator.sender import EventSender


@pytest.mark.asyncio
async def test_sender_posts_event_to_collector_url() -> None:
    requests = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"accepted": True, "event_id": "e_1"})

    transport = httpx.MockTransport(handler)
    sender = EventSender(collector_url="http://collector.test/events", transport=transport)
    result = await sender.send({"event_id": "e_1"})

    assert result.accepted is True
    assert requests[0].url == "http://collector.test/events"
