#!/usr/bin/env python
"""Test if MonitoringInfo protobuf serialization preserves TDigest."""

from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.cells import DistributionData
from fastdigest import TDigest

# Create a DistributionData with TDigest
tdigest = TDigest()
for i in range(1, 101):
    tdigest += TDigest.from_values([i])

dist_data = DistributionData(sum=5050, count=100, min=1, max=100, tdigest=tdigest)

print(f"Original DistributionData:")
print(f"  TDigest present: {dist_data.tdigest is not None}")

# Create monitoring info
mi = monitoring_infos.int64_user_distribution('test_ns', 'test_name', dist_data)

print(f"\nMonitoringInfo created:")
print(f"  payload length: {len(mi.payload)} bytes")

# Serialize to protobuf bytes
proto_bytes = mi.SerializeToString()
print(f"\nSerialized to protobuf:")
print(f"  protobuf length: {len(proto_bytes)} bytes")

# Deserialize from protobuf bytes
from apache_beam.portability.api import metrics_pb2
mi_decoded = metrics_pb2.MonitoringInfo()
mi_decoded.ParseFromString(proto_bytes)

print(f"\nDeserialized from protobuf:")
print(f"  payload length: {len(mi_decoded.payload)} bytes")

# Decode the distribution
result = monitoring_infos.extract_distribution(mi_decoded)
count, sum_val, min_val, max_val = result[:4]
tdigest_decoded = result[4] if len(result) > 4 else None

print(f"\nDecoded distribution:")
print(f"  count: {count}")
print(f"  TDigest present: {tdigest_decoded is not None}")

if tdigest_decoded:
    print(f"  p50: {tdigest_decoded.quantile(0.5)}")
    print(f"  ✓ TDigest survived protobuf serialization!")
else:
    print(f"  ❌ TDigest was lost during protobuf serialization!")
