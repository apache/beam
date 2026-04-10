import logging

import apache_beam as beam
from apache_beam.transforms import DoFn

from framework import prism_options, dataflow_options
from framework import periodic_source
from framework import dump_to_log
from timestamp_buffer import TimestampBufferDoFnBag

logging.basicConfig(level=logging.INFO)

WINDOW_SIZE = 6
SLIDE_INTERVAL = 6

with beam.Pipeline(options=prism_options) as p:
  _ = (
      p
      | periodic_source
      | beam.Map(lambda x, t=DoFn.TimestampParam: (x[0], (t, x[1])))
      | beam.ParDo(TimestampBufferDoFnBag(40))
      | dump_to_log)
