import logging

import apache_beam as beam

from framework import prism_options, dataflow_options
from framework import periodic_source
from framework import dump_to_log

logging.basicConfig(level=logging.INFO)

with beam.Pipeline(options=prism_options) as p:
  _ = (
      p
      | periodic_source
      | beam.BatchElements(min_batch_size=5, max_batch_size=100, max_batch_duration_secs=4)
      | dump_to_log
  )