#!/usr/bin/env python
"""Debug script to check if TDigest encoding is working."""

from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.cells import DistributionData
from fastdigest import TDigest

# Create a DistributionData with TDigest
tdigest = TDigest()
for i in range(1, 101):
    tdigest += TDigest.from_values([i])

dist_data = DistributionData(sum=5050, count=100, min=1, max=100, tdigest=tdigest)

print(f"Original DistributionData:")
print(f"  count: {dist_data.count}")
print(f"  sum: {dist_data.sum}")
print(f"  min: {dist_data.min}")
print(f"  max: {dist_data.max}")
print(f"  tdigest: {dist_data.tdigest is not None}")

# Create monitoring info
mi = monitoring_infos.int64_user_distribution('test_ns', 'test_name', dist_data)

print(f"\nMonitoringInfo created:")
print(f"  urn: {mi.urn}")
print(f"  type: {mi.type}")
print(f"  payload length: {len(mi.payload)} bytes")

# Decode it back
result = monitoring_infos.extract_distribution(mi)
count, sum_val, min_val, max_val = result[:4]
tdigest_decoded = result[4] if len(result) > 4 else None

print(f"\nDecoded distribution:")
print(f"  count: {count}")
print(f"  sum: {sum_val}")
print(f"  min: {min_val}")
print(f"  max: {max_val}")
print(f"  tdigest: {tdigest_decoded is not None}")

if tdigest_decoded:
    print(f"  p50: {tdigest_decoded.quantile(0.5)}")
else:
    print(f"  ❌ TDigest was lost during encoding/decoding!")
