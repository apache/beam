#!/usr/bin/env python
"""Debug script to trace where TDigest is lost in DirectRunner."""

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.metrics import monitoring_infos
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Monkey-patch to add debugging
original_extract_distribution = monitoring_infos.extract_distribution

def debug_extract_distribution(monitoring_info_proto):
    result = original_extract_distribution(monitoring_info_proto)
    count, sum_val, min_val, max_val = result[:4]
    tdigest = result[4] if len(result) > 4 else None
    logger.info(f"extract_distribution called: count={count}, tdigest present: {tdigest is not None}")
    if tdigest is None:
        # Log the payload to see what's there
        logger.info(f"  Payload length: {len(monitoring_info_proto.payload)} bytes")
    return result

monitoring_infos.extract_distribution = debug_extract_distribution

# Monkey-patch consolidate to debug
original_consolidate = monitoring_infos.consolidate

def debug_consolidate(metrics, key=monitoring_infos.to_key):
    logger.info(f"consolidate called with {len(list(metrics))} metrics")
    # Need to consume the iterator to count, so we recreate it
    metrics_list = list(metrics)
    for i, metric in enumerate(metrics_list):
        if monitoring_infos.is_distribution(metric):
            result = monitoring_infos.extract_distribution(metric)
            tdigest = result[4] if len(result) > 4 else None
            logger.info(f"  Metric {i}: distribution, tdigest present: {tdigest is not None}, payload len: {len(metric.payload)}")

    # Call original with list
    return original_consolidate(metrics_list, key)

monitoring_infos.consolidate = debug_consolidate

def process_element(element):
    dist = Metrics.distribution('test_ns', 'test_dist')
    dist.update(element)
    return [element]

def main():
    logger.info("Starting DirectRunner test with debugging...")

    with beam.Pipeline(runner='DirectRunner') as p:
        (p
         | beam.Create(range(1, 11))  # Smaller dataset for easier debugging
         | beam.Map(process_element))

    # Query metrics
    result = p.result
    metrics = result.metrics().query()

    for dist in metrics['distributions']:
        if dist.key.metric.name == 'test_dist':
            logger.info(f"\nFinal result:")
            logger.info(f"  Count: {dist.committed.count}")
            logger.info(f"  TDigest present: {dist.committed.data.tdigest is not None}")

if __name__ == '__main__':
    main()
