#!/usr/bin/env python
"""Test using a feature that forces FnApiRunner (stateful DoFn with timers)."""

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms.userstate import TimerSpec, on_timer
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms import DoFn

class StatefulDoFn(DoFn):
    """A stateful DoFn that should force FnApiRunner over PrismRunner."""
    TIMER_SPEC = TimerSpec('timer', TimeDomain.REAL_TIME)

    @on_timer(TIMER_SPEC)
    def process_timer(self):
        pass

    def process(self, element):
        dist = Metrics.distribution('test_ns', 'test_dist')
        dist.update(element)
        yield element

with beam.Pipeline(runner='DirectRunner') as p:
    (p
     | beam.Create([(1, i) for i in range(1, 101)])
     | beam.ParDo(StatefulDoFn()))

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
            print(f"  ✓ TDigest works with FnApiRunner!")
        else:
            print(f"  ❌ TDigest is None")
