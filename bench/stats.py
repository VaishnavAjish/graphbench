"""Latency statistics.

One decision worth defending in review: **percentiles use nearest-rank, not
linear interpolation.**

Nearest-rank on a sorted sample of n values defines the p-th percentile as the
value at index ``ceil(p/100 * n) - 1``. Every number this module reports is
therefore a latency that some request actually experienced. Interpolated
percentiles (numpy's default, and what most quick benchmark scripts emit)
invent a value between two observations that no request ever saw.

For a benchmark whose entire credibility rests on "these numbers are real",
reporting only observed values is the conservative choice. The cost is a small
upward bias on tiny samples -- which is why the runner enforces a minimum
sample size and this module flags samples below it.

Nothing here imports numpy. The dependency surface of a benchmark harness is
part of its reproducibility story, and the statistics module in the standard
library is sufficient for a few thousand floats.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Sequence

#: Samples smaller than this cannot support a meaningful p99: with n < 100 the
#: nearest-rank p99 is simply max(). Summaries below this threshold carry a
#: warning that the report surfaces rather than silently printing the number.
MIN_SAMPLE_FOR_P99 = 100

#: Relative standard deviation above which a measurement is considered too
#: noisy to compare against another platform. Reported, never silently dropped.
HIGH_VARIANCE_CV = 0.50


def percentile(sorted_samples: Sequence[float], p: float) -> float:
    """Nearest-rank percentile of an already-sorted sequence.

    Args:
        sorted_samples: ascending, non-empty.
        p: percentile in [0, 100].

    Returns:
        An element of ``sorted_samples`` -- never an interpolated value.
    """
    if not sorted_samples:
        return float("nan")
    if p <= 0:
        return sorted_samples[0]
    rank = math.ceil((p / 100.0) * len(sorted_samples))
    idx = min(max(rank - 1, 0), len(sorted_samples) - 1)
    return sorted_samples[idx]


@dataclass
class LatencySummary:
    """Everything reported about one measured workload on one platform.

    Millisecond units throughout. ``errors`` counts iterations that raised;
    those iterations contribute no latency sample, so ``count`` is the number
    of *successful* observations and the two together describe the run.
    """

    label: str
    count: int = 0
    errors: int = 0
    p50_ms: float = float("nan")
    p95_ms: float = float("nan")
    p99_ms: float = float("nan")
    mean_ms: float = float("nan")
    stdev_ms: float = float("nan")
    min_ms: float = float("nan")
    max_ms: float = float("nan")
    notes: str = ""
    error_samples: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def cv(self) -> float:
        """Coefficient of variation: stdev / mean.

        Unitless, so it is comparable across workloads whose absolute latencies
        differ by orders of magnitude. This is the number the report uses to
        decide whether two platforms are actually distinguishable.
        """
        if not self.mean_ms or math.isnan(self.mean_ms) or math.isnan(self.stdev_ms):
            return float("nan")
        return self.stdev_ms / self.mean_ms

    @property
    def ok(self) -> bool:
        return self.count > 0

    @property
    def error_rate(self) -> float:
        total = self.count + self.errors
        return self.errors / total if total else 0.0

    def as_dict(self) -> dict:
        return {
            "label": self.label,
            "count": self.count,
            "errors": self.errors,
            "error_rate": round(self.error_rate, 4),
            "p50_ms": _r(self.p50_ms),
            "p95_ms": _r(self.p95_ms),
            "p99_ms": _r(self.p99_ms),
            "mean_ms": _r(self.mean_ms),
            "stdev_ms": _r(self.stdev_ms),
            "cv": _r(self.cv, 4),
            "min_ms": _r(self.min_ms),
            "max_ms": _r(self.max_ms),
            "notes": self.notes,
            "warnings": self.warnings,
            "error_samples": self.error_samples[:5],
        }


def _r(v: float, digits: int = 3) -> float | None:
    """Round for JSON, preserving NaN as null rather than emitting 'NaN'.

    ``json.dumps`` writes bare ``NaN``, which is invalid JSON and breaks any
    downstream consumer. Results files are a deliverable, so they stay strict.
    """
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return round(float(v), digits)


def summarize(label: str, samples: Sequence[float], errors: int = 0,
              error_samples: Sequence[str] | None = None,
              notes: str = "") -> LatencySummary:
    """Reduce raw millisecond timings to the reported summary.

    Sorting is done once here rather than per-percentile; the samples sequence
    is copied so callers keep their arrival-order list intact (the concurrency
    sweep needs it for throughput).
    """
    warnings: list[str] = []
    errs = list(error_samples or [])

    if not samples:
        warnings.append("no successful samples -- workload failed on every iteration")
        return LatencySummary(label=label, count=0, errors=errors, notes=notes,
                              error_samples=errs, warnings=warnings)

    ordered = sorted(samples)
    n = len(ordered)
    mean = statistics.fmean(ordered)
    stdev = statistics.stdev(ordered) if n > 1 else 0.0

    if n < MIN_SAMPLE_FOR_P99:
        warnings.append(
            f"n={n} < {MIN_SAMPLE_FOR_P99}: p99 is effectively max() and should "
            f"not be compared across platforms")
    if errors:
        warnings.append(f"{errors} iteration(s) raised; latencies exclude them")

    summary = LatencySummary(
        label=label,
        count=n,
        errors=errors,
        p50_ms=percentile(ordered, 50),
        p95_ms=percentile(ordered, 95),
        p99_ms=percentile(ordered, 99),
        mean_ms=mean,
        stdev_ms=stdev,
        min_ms=ordered[0],
        max_ms=ordered[-1],
        notes=notes,
        error_samples=errs,
        warnings=warnings,
    )

    if summary.cv == summary.cv and summary.cv > HIGH_VARIANCE_CV:  # not NaN
        summary.warnings.append(
            f"high variance (cv={summary.cv:.2f}): free-tier throttling or "
            f"network jitter likely; treat cross-platform gaps under this "
            f"spread as not significant")

    return summary


def comparable(a: LatencySummary, b: LatencySummary) -> bool:
    """Whether the gap between two p50s exceeds the noise in both samples.

    Deliberately crude: the difference in medians must be larger than the sum
    of the two standard deviations. This is a screening heuristic for the
    report's "too close to call" flag, not a significance test, and the README
    says so. Two platforms that fail this check are reported as tied.
    """
    if not (a.ok and b.ok):
        return False
    if math.isnan(a.stdev_ms) or math.isnan(b.stdev_ms):
        return False
    return abs(a.p50_ms - b.p50_ms) > (a.stdev_ms + b.stdev_ms)
