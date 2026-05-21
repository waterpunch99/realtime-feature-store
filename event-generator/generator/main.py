import argparse
import asyncio
import logging
import time
from collections.abc import Iterable

from generator.config import GeneratorConfig
from generator.event_factory import EventFactory
from generator.scenarios import burst_events, mixed_events, normal_events, scenario_events
from generator.sender import EventSender

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic HTTP event generator")
    parser.add_argument("--mode", choices=["normal", "mixed", "burst", "scenario"], default="normal")
    parser.add_argument("--collector-url", default=GeneratorConfig.collector_url)
    parser.add_argument("--rate", type=int, default=GeneratorConfig.default_rate)
    parser.add_argument("--duration", type=int, default=GeneratorConfig.default_duration_seconds)
    parser.add_argument("--duplicate-ratio", type=float, default=0.0)
    parser.add_argument("--invalid-ratio", type=float, default=0.0)
    parser.add_argument("--late-ratio", type=float, default=0.0)
    parser.add_argument("--user-id", default="u_10001")
    parser.add_argument("--category-id", default="c_electronics")
    parser.add_argument("--seed", type=int, default=None)
    return parser.parse_args()


async def run(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    total = max(args.rate, 0) * max(args.duration, 0)
    factory = EventFactory(seed=args.seed)
    sender = EventSender(collector_url=args.collector_url)
    events = build_events(args, factory, total)

    sent = 0
    accepted = 0
    failed = 0
    interval = 1 / args.rate if args.rate > 0 else 0

    for event in events:
        started_at = time.monotonic()
        result = await sender.send(event)
        sent += 1
        accepted += int(result.accepted)
        failed += int(not result.accepted)

        if sent % max(args.rate, 1) == 0:
            logger.info("sent=%s accepted=%s failed=%s", sent, accepted, failed)

        elapsed = time.monotonic() - started_at
        if interval > elapsed:
            await asyncio.sleep(interval - elapsed)

    logger.info("completed sent=%s accepted=%s failed=%s", sent, accepted, failed)


def build_events(args: argparse.Namespace, factory: EventFactory, total: int) -> Iterable[dict]:
    if args.mode == "normal":
        return normal_events(factory, total)
    if args.mode == "mixed":
        return mixed_events(
            factory,
            total,
            duplicate_ratio=args.duplicate_ratio,
            invalid_ratio=args.invalid_ratio,
            late_ratio=args.late_ratio,
            seed=args.seed,
        )
    if args.mode == "burst":
        return burst_events(factory, total)
    return scenario_events(
        factory,
        total,
        user_id=args.user_id,
        category_id=args.category_id,
    )


def main() -> None:
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()

