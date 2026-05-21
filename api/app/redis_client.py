from redis.asyncio import Redis

from app.config import Settings


class RedisClient:
    def __init__(self, settings: Settings):
        self._client: Redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
        )

    @property
    def client(self) -> Redis:
        return self._client

    async def close(self) -> None:
        await self._client.aclose()

    async def healthcheck(self) -> bool:
        try:
            return bool(await self._client.ping())
        except Exception:
            return False

