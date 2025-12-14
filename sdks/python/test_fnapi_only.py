#!/usr/bin/env python
"""Test to force FnApiRunner instead of PrismRunner."""

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, DebugOptions

def process_element(element):
    dist = Metrics.distribution('test_ns', 'test_dist')
    dist.update(element)
    return [element]

# Create options that disable Prism
options = PipelineOptions()
options.view_as(DebugOptions).experiments = ['disable_prism_runner']

with beam.Pipeline(runner='DirectRunner', options=options) as p:
    (p
     | beam.Create(range(1, 101))
     | beam.Map(process_element))

# Query metrics
result = p.result
metrics = result.metrics().query()

for dist in metrics['distributions']:
    if dist.key.metric.name == 'test_dist':
        print(f"\nDistribution: {dist.key.metric.namespace}:{dist.key.metric.name}")
        print(f"  Count: {dist.committed.count}")
        print(f"  TDigest available: {dist.committed.data.tdigest is not None}")
        if dist.committed.data.tdigest is not None:
            print(f"  p50: {dist.committed.p50}")
            print(f"  ✓ TDigest works!")
        else:
            print(f"  ❌ TDigest is None")
