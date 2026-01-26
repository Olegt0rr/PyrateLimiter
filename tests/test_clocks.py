import asyncio
import contextlib
import os
import time
import pytest

from pyrate_limiter.clocks import MonotonicClock, MonotonicAsyncClock, PostgresClock


def test_monotonic_clock_now_returns_int_and_increasing():
    clk = MonotonicClock()
    t1 = clk.now()
    time.sleep(0.001)
    t2 = clk.now()

    assert isinstance(t1, int)
    assert isinstance(t2, int)
    assert t2 >= t1


@pytest.mark.asyncio
async def test_monotonic_async_clock_now_is_awaitable_and_increasing():
    clk = MonotonicAsyncClock()
    t1 = await clk.now()
    await asyncio.sleep(0.001)
    t2 = await clk.now()

    assert isinstance(t1, int)
    assert isinstance(t2, int)
    assert t2 >= t1


@pytest.fixture(scope="module")
def pg_pool():
    """Create a real psycopg_pool.ConnectionPool from DATABASE_URL or skip tests.

    This fixture yields a ConnectionPool instance and ensures it's closed after tests.
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        pytest.skip("Postgres DATABASE_URL not configured for integration tests")

    try:
        from psycopg_pool import ConnectionPool
        from psycopg.errors import Error
    except Exception:
        pytest.skip("psycopg_pool is not installed in the test environment")
        raise

    with ConnectionPool(db_url) as pool:
        yield pool


@pytest.mark.postgres
def test_postgres_clock_now_reads_ms_from_pool_and_casts_to_int(pg_pool):
    clk = PostgresClock(pg_pool)
    got = clk.now()
    assert isinstance(got, int)
    assert got > 0
