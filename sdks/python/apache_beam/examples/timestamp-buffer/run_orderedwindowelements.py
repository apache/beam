import logging

import apache_beam as beam
from apache_beam.transforms import DoFn

from framework import prism_options, dataflow_options
from framework import periodic_source
from framework import dump_to_log
from ordered_window_elements import OrderedWindowElementsDoFn

logging.basicConfig(level=logging.INFO)

WINDOW_SIZE = 6
SLIDE_INTERVAL = 6

with beam.Pipeline(options=prism_options) as p:
  _ = (
      p
      | periodic_source
      | beam.Map(lambda x, t=DoFn.TimestampParam: (x[0], (t, x[1])))
      | beam.ParDo(
          OrderedWindowElementsDoFn(
              duration=WINDOW_SIZE,
              slide_interval=SLIDE_INTERVAL,
              offset=0,
              allowed_lateness=0))
      | dump_to_log)
