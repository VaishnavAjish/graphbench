"""Graph database benchmark harness.

Package layout:
    adapters/   one class per platform, all implementing GraphAdapter
    config.py   YAML + fail-loud environment interpolation
    dataset.py  SNAP download and reproducible seeded sampling
    workloads.py cold/warm timing policy and the concurrency sweep
    stats.py    nearest-rank percentiles and variance flags
    report.py   results JSON -> REPORT.md

Nothing in this package imports a driver at module scope except the adapter
that needs it, so a missing optional dependency disables one platform rather
than the whole harness.
"""

__version__ = "1.0.0"
