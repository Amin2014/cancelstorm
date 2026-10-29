"""One-shot, named fault injection; no scheduler replacement or monkeypatches."""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import math
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, ParamSpec, TypeVar
from weakref import WeakKeyDictionary


class Fault(StrEnum):
    CANCEL = "cancel"
    TIMEOUT = "timeout"
    ERROR = "error"


class InjectedError(Exception):
    """Default ordinary injected exception."""


@dataclass(frozen=True)
class Scenario:
    checkpoint: str | None = None
    fault: Fault = Fault.CANCEL
    occurrence: int = 1

    def __post_init__(self) -> None:
        if self.checkpoint is not None and not self.checkpoint:
            raise ValueError("checkpoint must be nonempty")
        if not isinstance(self.occurrence, int) or self.occurrence < 1:
            raise ValueError("occurrence must be a positive integer")
        object.__setattr__(self, "fault", Fault(self.fault))

    @property
    def id(self) -> str:
        if self.checkpoint is None:
            return "baseline"
        return f"{self.checkpoint}#{self.occurrence}:{self.fault.value}"


def matrix(
    checkpoints: Iterable[str],
    *,
    faults: Iterable[Fault] = tuple(Fault),
    occurrences: Iterable[int] = (1,),
    baseline: bool = True,
) -> tuple[Scenario, ...]:
    """Materialize a finite, ordered single-fault matrix. No discovery/replay magic."""
    names, kinds, hits = tuple(checkpoints), tuple(faults), tuple(occurrences)
    cases = [Scenario()] if baseline else []
    cases.extend(Scenario(n, f, h) for n in names for h in hits for f in kinds)
    if len({s.id for s in cases}) != len(cases):
        raise ValueError("duplicate scenario IDs")
    return tuple(cases)


_active: contextvars.ContextVar[Scope | None] = contextvars.ContextVar("cancelstorm", default=None)
_busy: WeakKeyDictionary[Any, bool] = WeakKeyDictionary()
P = ParamSpec("P")
T = TypeVar("T")
Check = Callable[[], object]


async def checkpoint(name: str) -> None:
    """Raise the configured fault here, before yielding; no-op outside a scenario."""
    if not name:
        raise ValueError("checkpoint must be nonempty")
    scope = _active.get()
    if scope is not None:
        scope._hit(name)
    # No extra scheduling boundary: await of this coroutine completes inline.


def at_checkpoint(name: str) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Inject immediately before an async function's body."""
    if not name:
        raise ValueError("checkpoint must be nonempty")

    def decorate(fn: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @functools.wraps(fn)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            await checkpoint(name)
            return await fn(*args, **kwargs)

        return wrapped

    return decorate


@dataclass
class Scope:
    scenario: Scenario
    error_factory: Callable[[], Exception] = InjectedError
    trace: list[str] = field(default_factory=list, init=False)
    fired: bool = field(default=False, init=False)
    _counts: dict[str, int] = field(default_factory=dict, init=False)
    _checks: list[tuple[str, Check]] = field(default_factory=list, init=False)
    _tasks: list[asyncio.Future[Any]] = field(default_factory=list, init=False)
    _injected: BaseException | None = field(default=None, init=False)

    def invariant(self, name: str, check: Check) -> None:
        """Register a sync/async assertion. False also fails; None means success."""
        self._checks.append((f"invariant {name}", check))

    def cleanup(self, name: str, check: Check) -> None:
        self._checks.append((f"cleanup {name}", check))

    def track(self, task: asyncio.Future[T]) -> asyncio.Future[T]:
        """Explicitly register a preexisting task or a task in a detached Context."""
        if task.get_loop() is not asyncio.get_running_loop():
            raise ValueError("task belongs to another loop")
        if task not in self._tasks:
            self._tasks.append(task)
        return task

    def _hit(self, name: str) -> None:
        self.trace.append(name)
        self._counts[name] = self._counts.get(name, 0) + 1
        if (
            not self.fired
            and name == self.scenario.checkpoint
            and self._counts[name] == self.scenario.occurrence
        ):
            self.fired = True
            match self.scenario.fault:
                case Fault.CANCEL:
                    error: BaseException = asyncio.CancelledError(self.scenario.id)
                case Fault.TIMEOUT:
                    error = TimeoutError(self.scenario.id)
                case Fault.ERROR:
                    error = self.error_factory()
                    if not isinstance(error, Exception):
                        raise TypeError("error_factory must return an Exception")
            self._injected = error
            raise error


@dataclass(frozen=True)
class Result:
    scenario: Scenario
    trace: tuple[str, ...]
    injected: bool
    outcome: str
    issues: tuple[str, ...]
    leaked_tasks: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.issues


@dataclass(frozen=True)
class Report:
    results: tuple[Result, ...]

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    def __str__(self) -> str:
        lines = [f"{sum(r.passed for r in self.results)}/{len(self.results)} scenarios passed"]
        for result in self.results:
            lines.append(
                f"{'PASS' if result.passed else 'FAIL'} {result.scenario.id} ({result.outcome})"
            )
            lines.extend(f"  {issue}" for issue in result.issues)
        return "\n".join(lines)

    def assert_passed(self) -> None:
        if not self.passed:
            raise AssertionError(str(self))


class ContainmentError(RuntimeError):
    """Tasks ignored teardown cancellation; matrix stopped to prevent contamination."""


class Storm:
    def __init__(
        self,
        scenarios: Iterable[Scenario],
        *,
        timeout: float = 1.0,
        cleanup_timeout: float = 0.2,
        error_factory: Callable[[], Exception] = InjectedError,
    ) -> None:
        self.scenarios = tuple(scenarios)
        if not self.scenarios or len({s.id for s in self.scenarios}) != len(self.scenarios):
            raise ValueError("provide a nonempty matrix with unique IDs")
        for value in (timeout, cleanup_timeout):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("timeouts must be finite and positive")
        self.timeout = timeout
        self.cleanup_timeout = cleanup_timeout
        self.error_factory = error_factory

    def run(self, operation: Callable[[Scope], Awaitable[object]]) -> Report:
        """Synchronous pytest-friendly entry point; use arun inside an existing loop."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.arun(operation))
        raise RuntimeError("use await storm.arun(operation) inside an event loop")

    async def arun(self, operation: Callable[[Scope], Awaitable[object]]) -> Report:
        loop = asyncio.get_running_loop()
        if loop in _busy:
            raise RuntimeError("overlapping or nested storms on one loop are unsupported")
        _busy[loop] = True
        try:
            results = []
            for scenario in self.scenarios:
                results.append(await self._scenario(operation, scenario))
            return Report(tuple(results))
        finally:
            del _busy[loop]

    async def _scenario(
        self, operation: Callable[[Scope], Awaitable[object]], scenario: Scenario
    ) -> Result:
        scope = Scope(scenario, self.error_factory)
        loop = asyncio.get_running_loop()
        previous = loop.get_task_factory()
        issues: list[str] = []
        outcome = "returned"

        def factory(loop: Any, coro: Any, **kwargs: Any) -> Any:
            task = (
                previous(loop, coro, **kwargs)
                if previous
                else asyncio.Task(coro, loop=loop, **kwargs)
            )
            context = kwargs.get("context")
            owner = context.get(_active) if context is not None else _active.get()
            if owner is scope:
                scope.track(task)
            return task

        async def invoke() -> object:
            return await operation(scope)

        loop.set_task_factory(factory)
        root: asyncio.Task[Any] | None = None
        try:
            token = _active.set(scope)
            try:
                root = asyncio.create_task(invoke(), name="cancelstorm.operation")
            finally:
                _active.reset(token)
            done, _ = await asyncio.wait({root}, timeout=self.timeout)
            if not done:
                issues.append("operation exceeded watchdog timeout")
                outcome = "watchdog"
            else:
                try:
                    root.result()
                except (asyncio.CancelledError, Exception) as error:
                    outcome = type(error).__name__
                    if error is not scope._injected:
                        issues.append(f"unexpected {type(error).__name__}: {error}")
            if scenario.checkpoint is not None and not scope.fired:
                issues.append("selected checkpoint occurrence was not reached")

            # Observe BEFORE harness cleanup. No grace sleep hides unfinished tasks.
            leaked = tuple(
                t.get_name() if hasattr(t, "get_name") else repr(t)
                for t in scope._tasks
                if not t.done()
            )
            if leaked:
                issues.append("unfinished tasks: " + ", ".join(leaked))
            # Assertions run before cancellation so harness teardown cannot hide a leak.
            for name, check in scope._checks:
                try:
                    async with asyncio.timeout(self.timeout):
                        value = check()
                        if inspect.isawaitable(value):
                            value = await value
                        if value is False:
                            raise AssertionError("returned False")
                except (asyncio.CancelledError, Exception) as error:
                    # External cancellation of the harness must propagate.
                    if asyncio.current_task().cancelling():
                        raise
                    issues.append(f"{name}: {type(error).__name__}: {error}")
        finally:
            try:
                # Drain descendants created during cancellation too; one shared deadline.
                deadline = loop.time() + self.cleanup_timeout
                while pending := {t for t in scope._tasks if not t.done()}:
                    for task in pending:
                        task.cancel()
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise ContainmentError(f"{scenario.id}: tasks resisted cleanup")
                    await asyncio.wait(pending, timeout=remaining)
                for task in scope._tasks:
                    if not task.cancelled():
                        error = task.exception()
                        if task is not root and error is not None:
                            issues.append(f"background task: {type(error).__name__}: {error}")
            finally:
                loop.set_task_factory(previous)
        return Result(scenario, tuple(scope.trace), scope.fired, outcome, tuple(issues), leaked)
