#!/usr/bin/env python
"""Debug what the runner receives in MonitoringInfos."""

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.metrics import monitoring_infos as mi
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Monkey-patch consolidate to see what's received
original_consolidate = mi.consolidate

def debug_consolidate(metrics, key=mi.to_key):
    metrics_list = list(metrics)
    logger.info(f"\n=== consolidate called with {len(metrics_list)} metrics ===")

    for i, metric in enumerate(metrics_list):
        if mi.is_distribution(metric):
            logger.info(f"\nMetric {i}:")
            logger.info(f"  urn: {metric.urn}")
            logger.info(f"  payload length: {len(metric.payload)} bytes")

            # Try to decode
            try:
                result = mi.extract_distribution(metric)
                count, sum_val, min_val, max_val = result[:4]
                tdigest = result[4] if len(result) > 4 else None
                logger.info(f"  Decoded: count={count}, sum={sum_val}, min={min_val}, max={max_val}")
                logger.info(f"  TDigest present: {tdigest is not None}")

                # Hex dump first 20 bytes of payload
                payload_hex = metric.payload[:20].hex()
                logger.info(f"  Payload hex (first 20 bytes): {payload_hex}")
            except Exception as e:
                logger.error(f"  Failed to decode: {e}")

    return original_consolidate(metrics_list, key)

mi.consolidate = debug_consolidate

def process_element(element):
    dist = Metrics.distribution('test_ns', 'test_dist')
    dist.update(element)
    return [element]

logger.info("Starting pipeline...")
with beam.Pipeline(runner='DirectRunner') as p:
    (p
     | beam.Create(range(1, 11))
     | beam.Map(process_element))

logger.info("\nPipeline completed!")
