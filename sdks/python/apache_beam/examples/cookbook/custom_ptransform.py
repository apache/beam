#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Various implementations of a Count custom PTransform.

These example show the different ways you can write custom PTransforms.
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

# pylint doesn't understand our pipeline syntax:
# pylint:disable=expression-not-assigned


class Count1(beam.PTransform):
  """Count as a subclass of PTransform, with an apply method."""

  def expand(self, pcoll):
    return (
        pcoll
        | 'ParWithOne' >> beam.Map(lambda v: (v, 1))
        | beam.CombinePerKey(sum))


def run_count1(known_args, options):
  """Runs the first example pipeline."""
  logging.info('Running first pipeline')
  with beam.Pipeline(options=options) as p:
    (p | beam.io.ReadFromText(known_args.input)
     | Count1()
     | beam.io.WriteToText(known_args.output))


@beam.ptransform_fn
def Count2(pcoll):  # pylint: disable=invalid-name
  """Count as a decorated function."""
  return (
      pcoll
      | 'PairWithOne' >> beam.Map(lambda v: (v, 1))
      | beam.CombinePerKey(sum))


def run_count2(known_args, options):
  """Runs the second example pipeline."""
  logging.info('Running second pipeline')
  with beam.Pipeline(options=options) as p:
    (p | ReadFromText(known_args.input)
     | Count2()  # pylint: disable=no-value-for-parameter
     | WriteToText(known_args.output))


@beam.ptransform_fn
def Count3(pcoll, factor=1):  # pylint: disable=invalid-name
  """Count as a decorated function with a side input.

  Args:
    pcoll: the PCollection passed in from the previous transform
    factor: the amount by which to count

  Returns:
    A PCollection counting the number of times each unique element occurs.
  """
  return (
      pcoll
      | 'PairWithOne' >> beam.Map(lambda v: (v, factor))
      | beam.CombinePerKey(sum))


def run_count3(known_args, options):
  """Runs the third example pipeline."""
  logging.info('Running third pipeline')
  with beam.Pipeline(options=options) as p:
    (p | ReadFromText(known_args.input)
     | Count3(2)  # pylint: disable=no-value-for-parameter
     | WriteToText(known_args.output))


def get_args(argv):
  """Determines user specified arguments from the given list of arguments.

  Args:
    argv: all arguments.

  Returns:
    A pair of argument lists containing known and remaining arguments.
  """

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      required=True,
                      help='Input file to process.')
  parser.add_argument('--output',
                      required=True,
                      help='Output file to write results to.')
  return parser.parse_known_args(argv)


def run(argv=None):
  known_args, pipeline_args = get_args(argv)
  options = PipelineOptions(pipeline_args)

  run_count1(known_args, options)
  run_count2(known_args, options)
  run_count3(known_args, options)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
