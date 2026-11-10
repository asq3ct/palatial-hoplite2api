"""Reported ACP counters. Context occupancy must never become token billing."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


class UsageError(ValueError):
    pass


@dataclass(frozen=True)
class UsageSnapshot:
    input_tokens: int
    output_tokens: int
    run_count: int
    model: str
    cost_usd: Decimal | None = None

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict):
            raise UsageError("Missing reported usage")
        # usage_update wraps counters; _hoplite/thread/usage returns them flat.
        meta = value.get("_meta", {}).get("hoplite", {}) if isinstance(value.get("_meta", {}), dict) else {}
        counters = meta if isinstance(meta, dict) and "inputTokens" in meta else value
        values = [counters.get(k) for k in ("inputTokens", "outputTokens", "runCount")]
        if any(type(v) is not int or not 0 <= v <= 2**53 - 1 for v in values):
            raise UsageError("Missing or invalid reported token/run counters")
        model = counters.get("modelId")
        if not isinstance(model, str) or not model or len(model) > 256:
            raise UsageError("Missing reported usage model")
        cost = value.get("cost")
        amount = None
        if cost is not None:
            if not isinstance(cost, dict) or cost.get("currency") != "USD" or isinstance(cost.get("amount"), bool):
                raise UsageError("Invalid reported cost")
            try:
                amount = Decimal(str(cost["amount"]))
                if not amount.is_finite() or amount < 0:
                    raise InvalidOperation
            except (KeyError, InvalidOperation, ValueError):
                raise UsageError("Invalid reported cost") from None
        return cls(*values, model, amount)

    def delta(self, previous):
        if previous is None:
            return self
        if self.model != previous.model:
            raise UsageError("Usage model changed; counters cannot be combined")
        if any(a < b for a, b in zip(
            (self.input_tokens, self.output_tokens, self.run_count),
            (previous.input_tokens, previous.output_tokens, previous.run_count),
        )):
            raise UsageError("Reported usage counters regressed")
        return UsageSnapshot(self.input_tokens - previous.input_tokens,
                             self.output_tokens - previous.output_tokens,
                             self.run_count - previous.run_count, self.model)

    @classmethod
    def parse_rest(cls, value):
        if not isinstance(value, dict):
            raise UsageError("Missing reported REST usage")
        snapshot = cls.parse({"inputTokens": value.get("inputTokens"), "outputTokens": value.get("outputTokens"),
                              "runCount": value.get("runCount"), "modelId": value.get("topModel")})
        micros = value.get("totalCostMicros")
        if type(micros) is not int or micros < 0:
            raise UsageError("Missing reported REST cost")
        return cls(snapshot.input_tokens, snapshot.output_tokens, snapshot.run_count, snapshot.model,
                   Decimal(micros) / Decimal(1000000))

    def public(self):
        # Deliberately not OpenAI `usage`: this is cumulative agent-thread work,
        # which may span several HTTP tool-result continuations and model calls.
        return {"scope": "hoplite_thread", "basis": "provider_reported",
                "input_tokens": self.input_tokens, "output_tokens": self.output_tokens,
                "run_count": self.run_count, "model": self.model,
                "cost_usd": str(self.cost_usd) if self.cost_usd is not None else None}
