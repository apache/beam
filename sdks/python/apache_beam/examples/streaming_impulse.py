"""A streaming word-counting workflow.
"""

from __future__ import absolute_import

import logging
import sys

from apache_beam.io.streaming_impulse_source import StreamingImpulseSource
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, Repeatedly

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions

def split(s):
  a = s.split("-")
  return a[0], int(a[1])


def count(x):
  return x[0], sum(x[1])

def apply_timestamp(element):
  import time
  import apache_beam.transforms.window as window
  yield window.TimestampedValue(element, time.time())

def run(argv=None):
  """Build and run the pipeline."""
  options_string = sys.argv.extend([
    "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--streaming"
  ])

  pipeline_options = PipelineOptions(options_string)

  p = beam.Pipeline(options=pipeline_options)

  messages = (p | StreamingImpulseSource())

  (messages | 'decode' >> beam.Map(lambda x: ('', 1))
   | 'window' >> beam.WindowInto(window.GlobalWindows(),
                                 trigger=Repeatedly(AfterProcessingTime(5 * 1000)),
                                 accumulation_mode=AccumulationMode.DISCARDING)
   | 'group' >> beam.GroupByKey()
   | 'count' >> beam.Map(count)
   | 'log' >> beam.Map(lambda x: logging.info("%d" % x)))

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
