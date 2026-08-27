"""Seeded synthetic e-commerce order events, including poison rows for quarantine.

This is an event stream (placed / paid / shipped / …), not a warehouse star schema.
Re-running with the same seed produces identical rows.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

DEFAULT_SEED = 42
N_GOOD_EVENTS = 40
N_POISON_ROWS = 4
# One extra bronze row that shares event_id with the first good event (later ts wins in silver).
N_BRONZE_DUPLICATES = 1

EVENT_TYPES = ("placed", "paid", "shipped", "delivered", "cancelled", "refunded")

# New event_ids used by the MERGE replay so current silver has more rows than VERSION AS OF 0.
MERGE_NEW_EVENT_START = 100
MERGE_NEW_EVENT_COUNT = 6


def _iso_ts(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_good_events(seed: int = DEFAULT_SEED, n: int = N_GOOD_EVENTS) -> list[dict[str, str]]:
    rng = random.Random(seed)
    start = datetime(2024, 1, 15, tzinfo=UTC)
    events: list[dict[str, str]] = []
    for i in range(1, n + 1):
        amount = round(rng.uniform(5.0, 250.0), 2)
        events.append(
            {
                "event_id": f"evt-{i:04d}",
                "order_id": str(202401150000 + ((i - 1) // 2) + 1),
                "customer_id": str(((i - 1) % 100) + 1),
                "event_type": EVENT_TYPES[(i - 1) % len(EVENT_TYPES)],
                "amount": f"{amount:.2f}",
                "event_ts": _iso_ts(start + timedelta(hours=i)),
            }
        )
    return events


def generate_poison_rows() -> list[dict[str, str | None]]:
    """Rows that must land in silver_quarantine, never in silver."""
    ts = "2024-01-15T12:00:00Z"
    return [
        {
            "event_id": None,
            "order_id": "9990001",
            "customer_id": "1",
            "event_type": "placed",
            "amount": "10.00",
            "event_ts": ts,
        },
        {
            "event_id": "evt-poison-neg",
            "order_id": "9990002",
            "customer_id": "2",
            "event_type": "refunded",
            "amount": "-15.50",
            "event_ts": ts,
        },
        {
            "event_id": "evt-poison-nan",
            "order_id": "9990003",
            "customer_id": "3",
            "event_type": "paid",
            "amount": "not-a-number",
            "event_ts": ts,
        },
        {
            "event_id": "",
            "order_id": "9990004",
            "customer_id": "4",
            "event_type": "placed",
            "amount": "20.00",
            "event_ts": ts,
        },
    ]


def generate_events(seed: int = DEFAULT_SEED) -> list[dict[str, str | None]]:
    """Bronze payload: good events + one duplicate event_id + poison rows."""
    good = generate_good_events(seed)
    duplicate = dict(good[0])
    duplicate["amount"] = "99.00"
    duplicate["event_ts"] = "2024-01-16T00:00:00Z"
    return [*good, duplicate, *generate_poison_rows()]


def expected_bronze_count(seed: int = DEFAULT_SEED) -> int:
    return len(generate_events(seed))


def generate_merge_batch(seed: int = DEFAULT_SEED) -> list[dict[str, str]]:
    """Replay for MERGE: one in-place update of evt-0001 plus new event_ids."""
    first = generate_good_events(seed)[0]
    updated = dict(first)
    updated["amount"] = "111.11"
    updated["event_type"] = "delivered"
    updated["event_ts"] = "2024-01-20T08:00:00Z"

    rng = random.Random(seed + 99)
    start = datetime(2024, 1, 21, tzinfo=UTC)
    new_events: list[dict[str, str]] = []
    for offset in range(MERGE_NEW_EVENT_COUNT):
        i = MERGE_NEW_EVENT_START + offset
        amount = round(rng.uniform(5.0, 250.0), 2)
        new_events.append(
            {
                "event_id": f"evt-{i:04d}",
                "order_id": str(202401210000 + offset + 1),
                "customer_id": str((offset % 100) + 1),
                "event_type": EVENT_TYPES[offset % len(EVENT_TYPES)],
                "amount": f"{amount:.2f}",
                "event_ts": _iso_ts(start + timedelta(hours=offset + 1)),
            }
        )
    return [updated, *new_events]
