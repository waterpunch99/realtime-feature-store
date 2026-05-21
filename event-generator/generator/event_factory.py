from datetime import UTC, datetime, timedelta
from random import Random
from uuid import uuid4

from faker import Faker

from generator.catalog import Catalog, Product
from generator.models import EventProperties, EventType, SyntheticEvent


class EventFactory:
    def __init__(self, *, seed: int | None = None, catalog: Catalog | None = None) -> None:
        self.random = Random(seed)
        self.fake = Faker()
        if seed is not None:
            Faker.seed(seed)
        self.catalog = catalog or Catalog()

    def normal_event(
        self,
        *,
        event_type: EventType | None = None,
        user_id: str | None = None,
        category_id: str | None = None,
    ) -> SyntheticEvent:
        selected_type = event_type or self._weighted_event_type()
        product = self._product_for_type(selected_type, category_id)
        event_user_id = user_id or f"u_{self.random.randint(1, 20000):05d}"

        return SyntheticEvent(
            event_id=str(uuid4()),
            event_type=selected_type,
            user_id=event_user_id,
            session_id=f"s_{self.random.randint(1, 100000):05d}",
            product_id=None if selected_type == "search" else product.product_id,
            category_id=None if selected_type == "search" else product.category_id,
            event_time=datetime.now(UTC),
            device_type=self.random.choice(["mobile", "desktop", "tablet"]),
            country=self.random.choice(["KR", "US", "JP", "SG"]),
            properties=self._properties(selected_type, product),
        )

    def invalid_event(self) -> dict:
        event = self.normal_event(event_type=self.random.choice(["click", "purchase", "search"]))
        payload = event.to_payload()
        invalid_case = self.random.choice(["missing_user", "missing_product", "bad_quantity"])

        if invalid_case == "missing_user":
            payload["user_id"] = ""
        elif invalid_case == "missing_product":
            payload.pop("product_id", None)
        else:
            payload.setdefault("properties", {})["quantity"] = 0

        return payload

    def duplicate_pair(self) -> tuple[dict, dict]:
        payload = self.normal_event().to_payload()
        return payload, dict(payload)

    def late_event(self, *, minutes_late: int = 5) -> dict:
        event = self.normal_event()
        event.event_time = datetime.now(UTC) - timedelta(minutes=minutes_late)
        return event.to_payload()

    def scenario_event(self, *, user_id: str, category_id: str) -> SyntheticEvent:
        event_type: EventType = self.random.choice(["view", "click", "add_to_cart", "purchase"])
        return self.normal_event(event_type=event_type, user_id=user_id, category_id=category_id)

    def _weighted_event_type(self) -> EventType:
        return self.random.choices(
            ["view", "click", "add_to_cart", "purchase", "search"],
            weights=[45, 30, 10, 5, 10],
            k=1,
        )[0]

    def _product_for_type(self, event_type: EventType, category_id: str | None) -> Product:
        if event_type == "search":
            return self.catalog.random_product(self.random)
        if category_id:
            return self.catalog.random_product_in_category(self.random, category_id)
        return self.catalog.random_product(self.random)

    def _properties(self, event_type: EventType, product: Product) -> EventProperties:
        page = self.random.choice(["home", "category", "product_detail", "search_result"])
        referrer = self.random.choice(["home", "search", "recommendation", "ad"])

        if event_type in {"add_to_cart", "purchase"}:
            return EventProperties(
                price=float(product.base_price),
                quantity=self.random.randint(1, 3),
                page=page,
                referrer=referrer,
            )

        if event_type == "search":
            return EventProperties(search_query=self.fake.word(), page="search", referrer=referrer)

        return EventProperties(page=page, referrer=referrer)

