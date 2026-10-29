"""Fake money only: local reversible mutation and mocked external services."""

import argparse
import asyncio
from dataclasses import dataclass, field

from cancelstorm import Scope, Storm, checkpoint, matrix

CHECKPOINTS = ("payment.authorize", "payment.after_debit", "payment.notify")


@dataclass
class Accounts:
    buyer: int = 100
    merchant: int = 0
    phase: str = "new"
    connections: int = 0
    calls: list[str] = field(default_factory=list)


class FakeServices:
    def __init__(self, accounts: Accounts):
        self.accounts = accounts

    async def authorize(self):
        self.accounts.calls.append("authorize")
        await checkpoint("payment.authorize")
        await asyncio.sleep(0)

    async def notify(self):
        self.accounts.calls.append("notify")
        await checkpoint("payment.notify")
        await asyncio.sleep(0)


async def broken(accounts: Accounts, services: FakeServices):
    accounts.connections += 1
    accounts.phase = "pending"
    try:
        await services.authorize()
        accounts.buyer -= 25
        await checkpoint("payment.after_debit")
        accounts.merchant += 25
        await services.notify()
        accounts.phase = "committed"
    except Exception:
        # BUG: cancellation derives from BaseException, so skips rollback and close.
        accounts.buyer, accounts.merchant = 100, 0
        accounts.phase = "aborted"
        accounts.connections -= 1
        raise
    accounts.connections -= 1


async def corrected(accounts: Accounts, services: FakeServices):
    accounts.connections += 1
    accounts.phase = "pending"
    snapshot = accounts.buyer, accounts.merchant
    try:
        await services.authorize()
        accounts.buyer -= 25
        await checkpoint("payment.after_debit")
        accounts.merchant += 25
        await services.notify()
        accounts.phase = "committed"
    finally:
        # Synchronous compensation cannot itself be interrupted by cancellation.
        if accounts.phase != "committed":
            accounts.buyer, accounts.merchant = snapshot
            accounts.phase = "aborted"
        accounts.connections -= 1


def operation(workflow):
    async def run(scope: Scope):
        accounts = Accounts()  # Fresh state for EVERY scenario.
        scope.invariant("money conserved", lambda: accounts.buyer + accounts.merchant == 100)
        scope.invariant("terminal phase", lambda: accounts.phase in {"committed", "aborted"})
        scope.invariant(
            "balances match phase",
            lambda: (
                (accounts.buyer, accounts.merchant)
                == ((75, 25) if accounts.phase == "committed" else (100, 0))
            ),
        )
        scope.cleanup("connection closed", lambda: accounts.connections == 0)
        await workflow(accounts, FakeServices(accounts))

    return run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("broken", "fixed", "both"), default="both", nargs="?")
    args = parser.parse_args()
    workflows = {"broken": broken, "fixed": corrected}
    for name in workflows if args.mode == "both" else [args.mode]:
        print(f"\n{name.upper()}")
        report = Storm(matrix(CHECKPOINTS)).run(operation(workflows[name]))
        print(report)
        if name == "fixed":
            report.assert_passed()
        elif report.passed:
            raise AssertionError("expected the demonstration to expose a bug")


if __name__ == "__main__":
    main()
