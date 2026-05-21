from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import Settings


class Base(DeclarativeBase):
    pass


class Database:
    def __init__(self, settings: Settings):
        self._engine: AsyncEngine = create_async_engine(
            settings.database_url,
            pool_pre_ping=True,
        )
        self.session_factory = async_sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    async def close(self) -> None:
        await self._engine.dispose()

    async def healthcheck(self) -> bool:
        try:
            async with self._engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


async def get_session() -> AsyncIterator[AsyncSession]:
    from app.main import app

    database: Database = app.state.database
    async with database.session_factory() as session:
        yield session

