# TDigest Distribution Enhancement - Session Refresh Document

## Goal
Extend Beam's existing `Distribution` metric to internally use TDigest (via `fastdigest` package), enabling percentile queries (p50, p90, p95, p99) **without any user code changes**.

Users continue calling:
```python
dist = Metrics.distribution("namespace", "name")
dist.update(value)
```

But can now query:
```python
result.p50, result.p95, result.p99, result.quantile(0.75)
```

## Key Design Decisions
- **Package**: Use `fastdigest` (Rust-based, fast, compatible API)
- **Compression factor**: Use default (100)
- **Backwards compatible**: Old payloads decode with `tdigest=None`; percentile methods return `None` when unavailable
- **Conditional import**: Graceful fallback if fastdigest not installed

## Files to Modify

| File | Purpose |
|------|---------|
| `setup.py` | Add `fastdigest>=0.6.0,<1` to install_requires |
| `apache_beam/metrics/cells.py` | Core changes: DistributionData, DistributionCell, DistributionResult |
| `apache_beam/metrics/monitoring_infos.py` | Serialization: _encode/_decode_distribution |
| `apache_beam/metrics/cells_test.py` | Unit tests for cells changes |
| `apache_beam/metrics/monitoring_infos_test.py` | Unit tests for serialization |
| `apache_beam/metrics/metric_test.py` | Integration tests |

## Files to Read for Context

**Essential** (read these first):
1. `apache_beam/metrics/cells.py` lines 164-220 (DistributionCell), 349-402 (DistributionResult), 496-563 (DistributionData)
2. `apache_beam/metrics/monitoring_infos.py` lines 249-281 (int64_user_distribution), 501-510 (distribution_payload_combiner), 561-581 (_encode/_decode_distribution)

**Detailed plan**:
- `/Users/jtran/.claude/plans/tdigest-distribution-plan.md`

## fastdigest API Reference
```python
from fastdigest import TDigest

# Create
t = TDigest()                          # Empty
t = TDigest.from_values([1, 2, 3])     # From values

# Update
t += TDigest.from_values([4, 5])       # Merge with +

# Query
t.quantile(0.5)                        # Value at percentile
t.cdf(value)                           # Percentile of value

# Serialize
d = t.to_dict()                        # To dict
t2 = TDigest.from_dict(d)              # From dict
```

## Implementation Status: COMPLETE

### Phase 1: Dependencies
- [x] Add fastdigest to setup.py
- [x] Verify installation

### Phase 2: DistributionData (cells.py)
- [x] Add TDigest import (conditional)
- [x] Extend __init__ with tdigest param
- [x] Extend __eq__, __hash__, __repr__
- [x] Extend get_cumulative (copy tdigest)
- [x] Extend combine (merge tdigests)
- [x] Extend singleton, identity_element
- [x] Add tests (7 tests)

### Phase 3: DistributionCell (cells.py)
- [x] Update _update to feed tdigest
- [x] Add tests (2 tests)

### Phase 4: DistributionResult (cells.py)
- [x] Add p50, p90, p95, p99 properties
- [x] Add quantile(q) method
- [x] Update __repr__
- [x] Add tests (4 tests)

### Phase 5: Serialization (monitoring_infos.py)
- [x] Add TDigest import
- [x] Extend _encode_distribution (append tdigest bytes)
- [x] Extend _decode_distribution (read tdigest bytes)
- [x] Update int64_user_distribution, int64_distribution
- [x] Update extract_metric_result_map_value
- [x] Update distribution_payload_combiner
- [x] Add tests (4 tests)

### Phase 6-7: Integration & Full Tests
- [x] All cells_test.py tests pass (42 tests)
- [x] All monitoring_infos_test.py tests pass (15 tests)

### Demo Script
- [x] Created `tdigest_demo.py` to visualize TDigest quantiles
- Generates normal, bimodal, and longtail distributions
- Shows percentile comparison table and ASCII visualization
- Compares TDigest results with numpy ground truth

## Known Limitations

### Prism Runner Issue
As of 2024, DirectRunner uses PrismRunner by default, which is a Go-based portable runner.
Prism does not properly preserve TDigest data in distribution metric payloads. The Python SDK correctly encodes TDigest data (verified with 2000+ byte payloads), but Prism truncates the payload to only 4-5 bytes (basic count/sum/min/max).

**Root Cause**: The issue is in the Go Prism implementation, not the Python SDK. The Python worker correctly creates and serializes MonitoringInfo protobufs with full TDigest payloads, but Prism does not properly handle the extended distribution format.

**Workaround**: Use `BundleBasedDirectRunner` for local testing:
```python
with beam.Pipeline(runner='BundleBasedDirectRunner') as p:
    ...
```

**Investigation Status**:
- ✅ Python SDK creates MonitoringInfos with TDigest correctly (290+ bytes)
- ✅ Protobuf serialization/deserialization preserves TDigest
- ✅ BundleBasedDirectRunner works perfectly
- ❌ PrismRunner (DirectRunner default) truncates TDigest payloads

**Next Steps**: This requires a fix in the Go Prism codebase to properly handle extended distribution payloads. The portable runner protocol and protobuf schema support the extended format, but Prism's implementation needs to be updated.

## Current Branch
`tdigestdistribution`
