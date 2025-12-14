#!/usr/bin/env python
"""Debug worker-side metric creation."""

import apache_beam as beam
from apache_beam.metrics import Metrics, monitoring_infos
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Monkey-patch to debug
original_int64_user_distribution = monitoring_infos.int64_user_distribution

def debug_int64_user_distribution(namespace, name, metric, ptransform=None):
    logger.info(f"int64_user_distribution called:")
    logger.info(f"  namespace: {namespace}, name: {name}")
    logger.info(f"  metric type: {type(metric)}")
    logger.info(f"  metric.count: {metric.count}")
    logger.info(f"  metric has tdigest attr: {hasattr(metric, 'tdigest')}")
    if hasattr(metric, 'tdigest'):
        logger.info(f"  metric.tdigest is None: {metric.tdigest is None}")
    result = original_int64_user_distribution(namespace, name, metric, ptransform)
    logger.info(f"  resulting payload length: {len(result.payload)} bytes")
    return result

monitoring_infos.int64_user_distribution = debug_int64_user_distribution

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
