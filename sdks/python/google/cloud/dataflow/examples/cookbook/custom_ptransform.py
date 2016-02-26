# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Various implementations of a Count custom PTransform.

These example show the different ways you can write custom PTransforms.
"""

from __future__ import absolute_import

import argparse
import logging

import google.cloud.dataflow as df

from google.cloud.dataflow.utils.options import PipelineOptions


# pylint doesn't understand our pipeline syntax:
# pylint:disable=expression-not-assigned


def run_count1(known_args, options):
  """Runs the first example pipeline."""

  class Count(df.PTransform):
    """Count as a subclass of PTransform, with an apply method."""

    def apply(self, pcoll):
      return (
          pcoll
          | df.Map('Init', lambda v: (v, 1))
          | df.CombinePerKey(sum))

  logging.info('Running first pipeline')
  p = df.Pipeline(options=options)
  (p | df.io.Read(df.io.TextFileSource(known_args.input)) | Count()
   | df.io.Write(df.io.TextFileSink(known_args.output)))
  p.run()


def run_count2(known_args, options):
  """Runs the second example pipeline."""

  @df.ptransform_fn
  def Count(label, pcoll):      # pylint: disable=invalid-name,unused-argument
    """Count as a decorated function."""
    return (
        pcoll
        | df.Map('Init', lambda v: (v, 1))
        | df.CombinePerKey(sum))

  logging.info('Running second pipeline')
  p = df.Pipeline(options=options)
  (p | df.io.Read(df.io.TextFileSource(known_args.input))
   | Count()  # pylint: disable=no-value-for-parameter
   | df.io.Write(df.io.TextFileSink(known_args.output)))
  p.run()


def run_count3(known_args, options):
  """Runs the third example pipeline."""

  @df.ptransform_fn
  # pylint: disable=invalid-name,unused-argument
  def Count(label, pcoll, factor=1):
    """Count as a decorated function with a side input.

    Args:
      label: optional label for this transform
      pcoll: the PCollection passed in from the previous transform
      factor: the amount by which to count

    Returns:
      A PCollection counting the number of times each unique element occurs.
    """
    return (
        pcoll
        | df.Map('Init', lambda v: (v, factor))
        | df.CombinePerKey(sum))

  logging.info('Running third pipeline')
  p = df.Pipeline(options=options)
  (p | df.io.Read(df.io.TextFileSource(known_args.input))
   | Count(2)  # pylint: disable=no-value-for-parameter
   | df.io.Write(df.io.TextFileSink(known_args.output)))
  p.run()


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
