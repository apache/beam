import logging

import apache_beam as beam
from apache_beam.transforms import DoFn

from apache_beam.examples.timestamp_buffer.framework import prism_options
from apache_beam.examples.timestamp_buffer.framework import dataflow_options
from apache_beam.examples.timestamp_buffer.framework import periodic_source
from apache_beam.examples.timestamp_buffer.framework import dump_to_log
from apache_beam.examples.timestamp_buffer.timestamp_buffer import TimestampBufferDoFnBag

logging.basicConfig(level=logging.INFO)

WINDOW_SIZE = 6
SLIDE_INTERVAL = 6

class MyBufferDoFn(TimestampBufferDoFnBag):
  def process_element(self, key, element_ts, value, context,
                      **extra_state):
    yield [v[1] for v in context] + [value]

with beam.Pipeline(options=dataflow_options) as p:
  _ = (
      p
      | periodic_source
      | beam.Map(lambda x, t=DoFn.TimestampParam: (x[0], (t, x[1])))
      | beam.ParDo(MyBufferDoFn(40))
      | dump_to_log)
