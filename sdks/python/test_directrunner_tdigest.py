#!/usr/bin/env python
"""Test script to reproduce TDigest loss with DirectRunner vs BundleBasedDirectRunner."""

import logging
logging.basicConfig(level=logging.INFO, format='%(name)s - %(message)s')

import apache_beam as beam
from apache_beam.metrics import Metrics

def process_element(element):
    dist = Metrics.distribution('test_ns', 'test_dist')
    dist.update(element)
    return [element]

def test_runner(runner_name):
    print(f"\n{'='*60}")
    print(f"Testing with {runner_name}")
    print(f"{'='*60}")

    with beam.Pipeline(runner=runner_name) as p:
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
            print(f"  Sum: {dist.committed.sum}")
            print(f"  Min: {dist.committed.min}")
            print(f"  Max: {dist.committed.max}")
            print(f"  TDigest available: {dist.committed.data.tdigest is not None}")

            if dist.committed.data.tdigest is not None:
                print(f"  p50: {dist.committed.p50}")
                print(f"  p90: {dist.committed.p90}")
                print(f"  p95: {dist.committed.p95}")
                print(f"  p99: {dist.committed.p99}")
            else:
                print(f"  ❌ TDigest is None - percentiles not available!")

if __name__ == '__main__':
    # Test with BundleBasedDirectRunner (should work)
    test_runner('BundleBasedDirectRunner')

    # Test with DirectRunner (should fail)
    test_runner('DirectRunner')
