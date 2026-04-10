import logging

import apache_beam as beam

from apache_beam.examples.timestamp_buffer.framework import prism_options
from apache_beam.examples.timestamp_buffer.framework import dataflow_options
from apache_beam.examples.timestamp_buffer.framework import periodic_source
from apache_beam.examples.timestamp_buffer.framework import dump_to_log

logging.basicConfig(level=logging.INFO)

with beam.Pipeline(options=prism_options) as p:
  _ = (
      p
      | periodic_source
      | beam.BatchElements(min_batch_size=5, max_batch_size=100, max_batch_duration_secs=4)
      | dump_to_log
  )