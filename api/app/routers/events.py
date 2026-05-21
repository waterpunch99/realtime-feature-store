from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request, status

from app.repositories.quality_log_repository import QualityLogRepository
from app.schemas.event import BulkEventIngestResponse, BulkEventResult, EventIngestResponse
from app.services.event_collector import EventCollectorFailure, EventCollectorService
from app.services.event_validation import EventValidationService

router = APIRouter(prefix="/events", tags=["events"])


@router.post("", response_model=EventIngestResponse)
async def collect_event(
    request: Request,
    payload: dict[str, Any] = Body(...),
) -> EventIngestResponse:
    request.app.state.metrics.increment("api_requests_total")
    collector = _build_collector(request)

    try:
        return await collector.collect(payload)
    except EventCollectorFailure as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={
                "event_id": exc.event_id,
                "error_code": exc.reason_code,
                "error_message": exc.reason_detail,
            },
        ) from exc


@router.post("/bulk", response_model=BulkEventIngestResponse, status_code=status.HTTP_200_OK)
async def collect_events_bulk(
    request: Request,
    payload: list[dict[str, Any]] = Body(...),
) -> BulkEventIngestResponse:
    request.app.state.metrics.increment("api_requests_total")

    results: list[BulkEventResult] = []
    collector = _build_collector(request)

    for index, raw_event in enumerate(payload):
        try:
            result = await collector.collect(raw_event)
            results.append(BulkEventResult(index=index, **result.model_dump()))
        except EventCollectorFailure as exc:
            results.append(
                BulkEventResult(
                    index=index,
                    accepted=False,
                    event_id=exc.event_id,
                    error_code=exc.reason_code,
                    error_message=exc.reason_detail,
                )
            )

    accepted = sum(1 for result in results if result.accepted)
    return BulkEventIngestResponse(
        total=len(results),
        accepted=accepted,
        failed=len(results) - accepted,
        results=results,
    )


def _build_collector(request: Request) -> EventCollectorService:
    session = request.state.db_session
    return EventCollectorService(
        validation_service=EventValidationService(),
        quality_log_repository=QualityLogRepository(session),
        kafka_client=request.app.state.kafka,
        metrics_service=request.app.state.metrics,
        raw_topic=request.app.state.settings.kafka_raw_user_events_topic,
    )
