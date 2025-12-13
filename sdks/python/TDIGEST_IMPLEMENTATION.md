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

## Implementation Status

### Phase 1: Dependencies
- [ ] Add fastdigest to setup.py
- [ ] Verify installation

### Phase 2: DistributionData (cells.py)
- [ ] Add TDigest import (conditional)
- [ ] Extend __init__ with tdigest param
- [ ] Extend __eq__, __hash__, __repr__
- [ ] Extend get_cumulative (copy tdigest)
- [ ] Extend combine (merge tdigests)
- [ ] Extend singleton, identity_element
- [ ] Add tests

### Phase 3: DistributionCell (cells.py)
- [ ] Update _update to feed tdigest
- [ ] Add tests

### Phase 4: DistributionResult (cells.py)
- [ ] Add p50, p90, p95, p99 properties
- [ ] Add quantile(q) method
- [ ] Update __repr__
- [ ] Add tests

### Phase 5: Serialization (monitoring_infos.py)
- [ ] Add TDigest import
- [ ] Extend _encode_distribution (append tdigest bytes)
- [ ] Extend _decode_distribution (read tdigest bytes)
- [ ] Update int64_user_distribution, int64_distribution
- [ ] Update extract_metric_result_map_value
- [ ] Update distribution_payload_combiner
- [ ] Add tests

### Phase 6-7: Integration & Full Tests
- [ ] End-to-end pipeline test
- [ ] Run full test suites

## Current Branch
`tdigestdistribution`
