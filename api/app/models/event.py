from datetime import datetime

from sqlalchemy import DateTime, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class EventQualityLog(Base):
    __tablename__ = "event_quality_log"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str | None] = mapped_column(Text)
    event_type: Mapped[str | None] = mapped_column(Text)
    user_id: Mapped[str | None] = mapped_column(Text)
    reason_code: Mapped[str] = mapped_column(Text)
    reason_detail: Mapped[str | None] = mapped_column(Text)
    raw_event: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class LateEventLog(Base):
    __tablename__ = "late_event_log"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(Text)
    event_type: Mapped[str | None] = mapped_column(Text)
    user_id: Mapped[str | None] = mapped_column(Text)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ingest_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    lateness_seconds: Mapped[int] = mapped_column(Integer)
    reason_code: Mapped[str] = mapped_column(Text)
    raw_event: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

