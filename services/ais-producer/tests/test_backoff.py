from __future__ import annotations

from ais_producer.backoff import Backoff


def test_delay_doubles_up_to_the_cap():
    backoff = Backoff(base=1.0, cap=10.0, rng=lambda: 1.0)  # rng=1.0 means no jitter reduction

    assert [backoff.next() for _ in range(6)] == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0]


def test_jitter_keeps_the_delay_between_half_and_full():
    low = Backoff(base=4.0, cap=60.0, rng=lambda: 0.0)
    high = Backoff(base=4.0, cap=60.0, rng=lambda: 1.0)

    assert low.next() == 2.0
    assert high.next() == 4.0


def test_reset_starts_over():
    backoff = Backoff(base=1.0, cap=60.0, rng=lambda: 1.0)
    backoff.next()
    backoff.next()

    backoff.reset()

    assert backoff.next() == 1.0
