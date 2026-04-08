import logging
import operator

import apache_beam as beam
from apache_beam.transforms import DoFn
from apache_beam.transforms.window import SlidingWindows

from framework import prism_options, dataflow_options
from framework import periodic_source
from framework import dump_to_log

logging.basicConfig(level=logging.INFO)

WINDOW_SIZE = 6
SLIDE_INTERVAL = 6

with beam.Pipeline(options=prism_options) as p:
  _ = (
      p
      | periodic_source
      | beam.WindowInto(SlidingWindows(WINDOW_SIZE, SLIDE_INTERVAL))
      | beam.Map(lambda x, t=DoFn.TimestampParam: (x[0], (t, x[1])))
      | beam.CombinePerKey(beam.combiners.ToListCombineFn())
      | beam.Map(lambda x: (x[0], sorted(x[1], key=operator.itemgetter(0))))
      | dump_to_log
  )
