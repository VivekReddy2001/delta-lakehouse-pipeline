"""Deterministic generator of zero-trust access telemetry.

Each record is one access event: a user reached a private application on a
server/port/protocol and moved some bytes. The generator reproduces the three
properties of real telemetry that break naive pipelines:

* **late arrival** - a fraction of events shows up in a batch several days
  after the day it happened;
* **redelivery** - at-least-once transport re-sends some events in a later
  batch (same ``event_id``);
* **skew** - a few applications and users dominate traffic.

Output is one JSON-lines file per arrival batch:
``<landing>/<batch_id>/events.jsonl``, with ``batch_id = YYYY-MM-DD``.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PROTOCOLS = ["TCP", "UDP"]


@dataclass(frozen=True)
class GenConfig:
    start: date = date(2026, 1, 1)
    days: int = 10
    users: int = 200
    apps: int = 40
    events_per_day: int = 5_000
    late_fraction: float = 0.05  # events that arrive 1..max_lag days late
    max_lag_days: int = 4
    redelivery_fraction: float = 0.02  # events re-sent in the next batch
    seed: int = 7


def _event_id(seed: int, day: date, i: int) -> str:
    return hashlib.sha1(f"{seed}:{day.isoformat()}:{i}".encode()).hexdigest()[:20]


class _World:
    """Static topology: which servers/ports serve each app, which apps each user uses."""

    def __init__(self, cfg: GenConfig, rng: random.Random):
        self.apps = [f"app-{a:03d}" for a in range(cfg.apps)]
        self.app_weight = [1.0 / (a + 1) ** 0.8 for a in range(cfg.apps)]  # Zipf-like popularity
        self.app_servers = {
            app: [f"10.{a // 250}.{a % 250}.{s + 1}" for s in range(rng.randint(1, 4))]
            for a, app in enumerate(self.apps)
        }
        self.app_ports = {
            app: [
                (rng.choice([22, 443, 3306, 5432, 6379, 8080, 8443, 9092]), rng.choice(PROTOCOLS))
                for _ in range(rng.randint(1, 2))
            ]
            for app in self.apps
        }
        self.users = [f"user-{u:04d}" for u in range(cfg.users)]
        self.user_weight = [rng.paretovariate(1.5) for _ in self.users]
        self.user_apps = {}
        for u in self.users:
            k = rng.randint(2, 8)
            self.user_apps[u] = rng.choices(self.apps, weights=self.app_weight, k=k)


def _make_event(world: _World, rng: random.Random, cfg: GenConfig, day: date, i: int) -> dict:
    user = rng.choices(world.users, weights=world.user_weight, k=1)[0]
    app = rng.choice(world.user_apps[user])
    port, proto = rng.choice(world.app_ports[app])
    ts = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(
        seconds=int(rng.triangular(0, 86_399, 50_000))
    )
    return {
        "event_id": _event_id(cfg.seed, day, i),
        "event_time": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id": user,
        "app_id": app,
        "server_ip": rng.choice(world.app_servers[app]),
        "port": port,
        "protocol": proto,
        "bytes_sent": int(rng.lognormvariate(8, 1.2)),
        "bytes_recv": int(rng.lognormvariate(9, 1.5)),
        "action": "block" if rng.random() < 0.03 else "allow",
    }


def generate(landing: Path, cfg: GenConfig | None = None) -> list[str]:
    """Write one batch per arrival day; return the batch ids in arrival order."""
    cfg = cfg or GenConfig()
    rng = random.Random(cfg.seed)
    world = _World(cfg, rng)
    batches: dict[date, list[dict]] = {cfg.start + timedelta(d): [] for d in range(cfg.days)}
    last_day = cfg.start + timedelta(cfg.days - 1)

    for d in range(cfg.days):
        day = cfg.start + timedelta(d)
        for i in range(cfg.events_per_day):
            ev = _make_event(world, rng, cfg, day, i)
            arrival = day
            if rng.random() < cfg.late_fraction:
                arrival = min(day + timedelta(rng.randint(1, cfg.max_lag_days)), last_day)
            batches[arrival].append(ev)
            if rng.random() < cfg.redelivery_fraction:
                batches[min(arrival + timedelta(1), last_day)].append(dict(ev))

    ids = []
    for arrival, events in batches.items():
        batch_id = arrival.isoformat()
        out = Path(landing) / batch_id
        out.mkdir(parents=True, exist_ok=True)
        rng.shuffle(events)
        with open(out / "events.jsonl", "w") as f:
            for ev in events:
                f.write(json.dumps(ev, separators=(",", ":")) + "\n")
        ids.append(batch_id)
    return ids


def fingerprint(batch_dir: Path) -> str:
    """Content hash of a landed batch; a re-landed batch must hash the same."""
    h = hashlib.sha256()
    for p in sorted(Path(batch_dir).glob("*.jsonl")):
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()
