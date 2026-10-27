"""Exponential backoff with jitter for WebSocket reconnects."""

from __future__ import annotations

import random
from collections.abc import Callable


class Backoff:
    """Delay doubles after each failure up to `cap`, then a random 50 to 100 percent is used.

    Call `reset()` after a connection that stayed up for a while, so a long healthy session is
    followed by a short first retry rather than the capped delay.
    """

    def __init__(
        self,
        base: float = 1.0,
        cap: float = 60.0,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self._base = base
        self._cap = cap
        self._rng = rng
        self._attempt = 0

    def next(self) -> float:
        ceiling = min(self._cap, self._base * 2**self._attempt)
        self._attempt += 1
        return ceiling * (0.5 + 0.5 * self._rng())

    def reset(self) -> None:
        self._attempt = 0
