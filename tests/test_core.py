import asyncio
import contextvars

import pytest

from cancelstorm import (
    Fault,
    InjectedError,
    Scenario,
    Storm,
    at_checkpoint,
    checkpoint,
    matrix,
)
from examples.payments import CHECKPOINTS, broken, corrected, operation


@pytest.mark.parametrize(
    "fault,kind",
    [
        (Fault.CANCEL, asyncio.CancelledError),
        (Fault.TIMEOUT, TimeoutError),
        (Fault.ERROR, InjectedError),
    ],
)
def test_fault_types_and_occurrence(fault, kind):
    seen = []

    async def op(scope):
        await checkpoint("repeat")
        try:
            await checkpoint("repeat")
        except kind:
            seen.append(kind)
            raise

    result = Storm([Scenario("repeat", fault, 2)]).run(op).results[0]
    assert result.passed and result.injected
    assert result.trace == ("repeat", "repeat")
    assert seen == [kind]


def test_reproducible_matrix_and_fixed_payment():
    scenarios = matrix(CHECKPOINTS)
    first = Storm(scenarios).run(operation(corrected))
    second = Storm(scenarios).run(operation(corrected))
    assert len(first.results) == 10
    assert first == second
    first.assert_passed()


def test_broken_payment_exact_failures():
    report = Storm(matrix(CHECKPOINTS)).run(operation(broken))
    assert {r.scenario.id for r in report.results if not r.passed} == {
        f"{name}#1:cancel" for name in CHECKPOINTS
    }
    after_debit = next(r for r in report.results if r.scenario.id == "payment.after_debit#1:cancel")
    assert any("money conserved" in issue for issue in after_debit.issues)
    assert any("connection closed" in issue for issue in after_debit.issues)
    with pytest.raises(AssertionError, match="payment.after_debit#1:cancel"):
        report.assert_passed()


def test_noop_and_decorator():
    @at_checkpoint("decorated")
    async def fn(value):
        return value

    assert asyncio.run(fn(9)) == 9

    async def op(scope):
        await fn(9)

    report = Storm([Scenario("decorated")]).run(op)
    report.assert_passed()
    assert report.results[0].trace == ("decorated",)


def test_unreached_is_failure():
    async def op(scope):
        await checkpoint("actual")

    report = Storm([Scenario("typo")]).run(op)
    assert not report.passed
    assert "was not reached" in str(report)


def test_swallowed_fault_still_runs_checks():
    async def op(scope):
        scope.invariant("false", lambda: False)
        try:
            await checkpoint("x")
        except asyncio.CancelledError:
            pass

    result = Storm([Scenario("x")]).run(op).results[0]
    assert result.injected and result.outcome == "returned" and not result.passed


def test_task_leak_detected_before_cleanup_and_drained():
    closed = []

    async def worker():
        try:
            await asyncio.Event().wait()
        finally:
            closed.append(True)

    async def op(scope):
        task = asyncio.create_task(worker(), name="forgotten-worker")
        await asyncio.sleep(0)
        scope.cleanup("worker already closed", lambda: task.done())

    report = Storm([Scenario()]).run(op)
    result = report.results[0]
    assert result.leaked_tasks == ("forgotten-worker",)
    assert not result.passed and closed == [True]
    assert "worker already closed" in str(report)


def test_background_exception_and_nested_tasks():
    async def fail():
        raise ValueError("child failed")

    async def parent():
        asyncio.create_task(fail())
        await asyncio.sleep(0)

    async def op(scope):
        await asyncio.create_task(parent())

    result = Storm([Scenario()]).run(op).results[0]
    assert not result.passed
    assert any("child failed" in s for s in result.issues)


def test_taskgroup_children_tracked_and_clean():
    async def op(scope):
        async with asyncio.TaskGroup() as group:
            group.create_task(asyncio.sleep(0))
            group.create_task(asyncio.sleep(0))

    Storm([Scenario()]).run(op).assert_passed()


def test_async_checks_and_multiple_failures():
    async def op(scope):
        async def check():
            await asyncio.sleep(0)
            raise AssertionError("resource open")

        scope.cleanup("async close", check)
        scope.invariant("state", lambda: False)
        raise ValueError("oops")

    result = Storm([Scenario()]).run(op).results[0]
    assert len(result.issues) == 3
    assert "resource open" in str(Storm([Scenario()]).run(op))


def test_unexpected_cancellation_is_failure():
    async def op(scope):
        raise asyncio.CancelledError("not injected")

    assert not Storm([Scenario()]).run(op).passed


def test_watchdog():
    async def op(scope):
        await asyncio.Event().wait()

    report = Storm([Scenario()], timeout=0.01).run(op)
    assert not report.passed and "watchdog" in str(report)


def test_factory_restored_and_explicit_context_supported():
    async def test():
        loop = asyncio.get_running_loop()
        original = loop.get_task_factory()
        calls = []

        def factory(loop, coro, **kwargs):
            calls.append(True)
            return asyncio.Task(coro, loop=loop, **kwargs)

        loop.set_task_factory(factory)
        try:

            async def op(scope):
                await asyncio.create_task(asyncio.sleep(0), context=contextvars.copy_context())
                detached = asyncio.create_task(asyncio.sleep(0), context=contextvars.Context())
                scope.track(detached)
                await detached

            (await Storm([Scenario()]).arun(op)).assert_passed()
            assert loop.get_task_factory() is factory and len(calls) >= 3
        finally:
            loop.set_task_factory(original)

    asyncio.run(test())


def test_external_cancellation_propagates_and_restores_factory():
    async def test():
        ready = asyncio.Event()
        closed = []
        loop = asyncio.get_running_loop()
        previous = loop.get_task_factory()

        async def op(scope):
            ready.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.append(True)

        runner = asyncio.create_task(Storm([Scenario()]).arun(op))
        await ready.wait()
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner
        assert closed == [True]
        assert loop.get_task_factory() is previous

    asyncio.run(test())


def test_overlapping_storm_rejected():
    async def op(scope):
        with pytest.raises(RuntimeError, match="overlapping"):
            await Storm([Scenario()]).arun(op)
        with pytest.raises(RuntimeError, match="use await"):
            Storm([Scenario()]).run(op)

    Storm([Scenario()]).run(op).assert_passed()


def test_custom_exception_factory():
    async def op(scope):
        with pytest.raises(OSError, match="mock service offline"):
            await checkpoint("service")

    Storm(
        [Scenario("service", Fault.ERROR)], error_factory=lambda: OSError("mock service offline")
    ).run(op).assert_passed()


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_invalid_timeout(timeout):
    with pytest.raises(ValueError):
        Storm([Scenario()], timeout=timeout)


def test_invalid_matrix():
    with pytest.raises(ValueError):
        matrix(["x", "x"])
    with pytest.raises(ValueError):
        Scenario("x", occurrence=0)
    with pytest.raises(ValueError):
        Scenario("")
    with pytest.raises(ValueError):
        Storm([])


def test_pytest_fixture(cancelstorm):
    cancelstorm(matrix(CHECKPOINTS)).run(operation(corrected)).assert_passed()


def test_eager_factory_preserved():
    async def test():
        loop = asyncio.get_running_loop()
        previous = loop.get_task_factory()
        loop.set_task_factory(asyncio.eager_task_factory)
        try:

            async def op(scope):
                await asyncio.create_task(asyncio.sleep(0))
                await checkpoint("eager")

            (await Storm([Scenario("eager")]).arun(op)).assert_passed()
            assert loop.get_task_factory() is asyncio.eager_task_factory
        finally:
            loop.set_task_factory(previous)

    asyncio.run(test())


def test_unrelated_task_is_not_adopted():
    async def test():
        unrelated = asyncio.create_task(asyncio.Event().wait(), name="outside")
        try:

            async def op(scope):
                await asyncio.sleep(0)

            (await Storm([Scenario()]).arun(op)).assert_passed()
            assert not unrelated.done()
        finally:
            unrelated.cancel()
            await asyncio.gather(unrelated, return_exceptions=True)

    asyncio.run(test())


def test_descendants_created_during_teardown_are_drained():
    children = []

    async def worker():
        try:
            await asyncio.Event().wait()
        finally:
            children.append(asyncio.create_task(asyncio.Event().wait(), name="cleanup-child"))

    async def op(scope):
        asyncio.create_task(worker(), name="leaker")
        await asyncio.sleep(0)

    assert not Storm([Scenario()]).run(op).passed
    assert children and children[0].cancelled()


def test_containment_stops_matrix_and_restores_factory():
    from cancelstorm import ContainmentError

    async def test():
        release = asyncio.Event()
        stubborn = []
        loop = asyncio.get_running_loop()
        previous = loop.get_task_factory()
        visits = []

        async def worker():
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    pass

        async def op(scope):
            visits.append(scope.scenario.id)
            stubborn.append(asyncio.create_task(worker()))
            await asyncio.sleep(0)

        try:
            with pytest.raises(ContainmentError, match="baseline"):
                await Storm([Scenario(), Scenario("never")], cleanup_timeout=0.01).arun(op)
            assert visits == ["baseline"]
            assert loop.get_task_factory() is previous
        finally:
            release.set()
            await asyncio.gather(*stubborn)

    asyncio.run(test())


def test_check_timeout_reports_failure():
    async def op(scope):
        scope.cleanup("blocked", lambda: asyncio.Event().wait())

    report = Storm([Scenario()], timeout=0.01).run(op)
    assert "cleanup blocked: TimeoutError" in str(report)


def test_two_scenarios_have_independent_task_scopes():
    async def op(scope):
        await asyncio.create_task(asyncio.sleep(0))
        await checkpoint("x")

    Storm([Scenario(), Scenario("x")]).run(op).assert_passed()
