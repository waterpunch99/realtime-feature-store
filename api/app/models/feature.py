from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Numeric, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class FeatureUserLatest(Base):
    __tablename__ = "feature_user_latest"

    user_id: Mapped[str] = mapped_column(Text, primary_key=True)
    user_recent_click_categories_10m: Mapped[list] = mapped_column(JSONB, default=list)
    user_recent_click_categories_1h: Mapped[list] = mapped_column(JSONB, default=list)
    user_click_count_10m: Mapped[int] = mapped_column(default=0)
    user_click_count_1h: Mapped[int] = mapped_column(default=0)
    user_view_count_10m: Mapped[int] = mapped_column(default=0)
    user_add_to_cart_count_1h: Mapped[int] = mapped_column(default=0)
    user_purchase_count_1h: Mapped[int] = mapped_column(default=0)
    user_purchase_amount_1h: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    user_avg_purchase_amount_24h: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    user_last_event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    user_last_product_id: Mapped[str | None] = mapped_column(Text)
    features: Mapped[dict] = mapped_column(JSONB, default=dict)
    window_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class FeatureProductLatest(Base):
    __tablename__ = "feature_product_latest"

    product_id: Mapped[str] = mapped_column(Text, primary_key=True)
    category_id: Mapped[str | None] = mapped_column(Text)
    product_view_count_10m: Mapped[int] = mapped_column(default=0)
    product_click_count_10m: Mapped[int] = mapped_column(default=0)
    product_add_to_cart_count_10m: Mapped[int] = mapped_column(default=0)
    product_purchase_count_1h: Mapped[int] = mapped_column(default=0)
    product_ctr_10m: Mapped[Decimal] = mapped_column(Numeric(18, 6), default=0)
    product_conversion_rate_1h: Mapped[Decimal] = mapped_column(Numeric(18, 6), default=0)
    product_popularity_score_10m: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    features: Mapped[dict] = mapped_column(JSONB, default=dict)
    window_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class FeatureCategoryLatest(Base):
    __tablename__ = "feature_category_latest"

    category_id: Mapped[str] = mapped_column(Text, primary_key=True)
    category_view_count_10m: Mapped[int] = mapped_column(default=0)
    category_click_count_10m: Mapped[int] = mapped_column(default=0)
    category_add_to_cart_count_1h: Mapped[int] = mapped_column(default=0)
    category_purchase_count_1h: Mapped[int] = mapped_column(default=0)
    category_purchase_amount_1h: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    category_popularity_score_10m: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    features: Mapped[dict] = mapped_column(JSONB, default=dict)
    window_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

