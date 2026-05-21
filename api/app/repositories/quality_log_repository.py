from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.event import EventQualityLog


class QualityLogRepository:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def add_event_quality_log(
        self,
        *,
        event_id: str | None,
        event_type: str | None,
        user_id: str | None,
        reason_code: str,
        reason_detail: str | None,
        raw_event: dict[str, Any],
    ) -> None:
        self._session.add(
            EventQualityLog(
                event_id=event_id,
                event_type=event_type,
                user_id=user_id,
                reason_code=reason_code,
                reason_detail=reason_detail,
                raw_event=raw_event,
            )
        )
        await self._session.commit()
