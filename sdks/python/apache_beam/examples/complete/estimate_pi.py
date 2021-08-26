# -*- coding: utf-8 -*-
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

"""A workflow that uses a simple Monte Carlo method to estimate π.

The algorithm computes the fraction of points drawn uniformly within the unit
square that also fall in the quadrant of the unit circle that overlaps the
square. A simple area calculation shows that this fraction should be π/4, so
we multiply our counts ratio by four to estimate π.
"""

# pytype: skip-file

import argparse
import json
import logging
import random
from typing import Any
from typing import Iterable
from typing import Tuple

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


@beam.typehints.with_output_types(Tuple[int, int, int])
@beam.typehints.with_input_types(int)
def run_trials(runs):
  """Run trials and return a 3-tuple representing the results.

  Args:
    runs: Number of trial runs to be executed.

  Returns:
    A 3-tuple (total trials, inside trials, 0).

  The final zero is needed solely to make sure that the combine_results function
  has same type for inputs and outputs (a requirement for combiner functions).
  """
  inside_runs = 0
  for _ in range(runs):
    x = random.uniform(0, 1)
    y = random.uniform(0, 1)
    inside_runs += 1 if x * x + y * y <= 1.0 else 0
  return runs, inside_runs, 0


@beam.typehints.with_output_types(Tuple[int, int, float])
@beam.typehints.with_input_types(Iterable[Tuple[int, int, Any]])
def combine_results(results):
  """Combiner function to sum up trials and compute the estimate.

  Args:
    results: An iterable of 3-tuples (total trials, inside trials, ignored).

  Returns:
    A 3-tuple containing the sum of total trials, sum of inside trials, and
    the probability computed from the two numbers.
  """
  # TODO(silviuc): Do we guarantee that argument can be iterated repeatedly?
  # Should document one way or the other.
  total, inside = sum(r[0] for r in results), sum(r[1] for r in results)
  return total, inside, 4 * float(inside) / total


class JsonCoder(object):
  """A JSON coder used to format the final result."""
  def encode(self, x):
    return json.dumps(x)


class EstimatePiTransform(beam.PTransform):
  """Runs 10M trials, and combine the results to estimate pi."""
  def __init__(self, tries_per_work_item=100000):
    self.tries_per_work_item = tries_per_work_item

  def expand(self, pcoll):
    # A hundred work items of a hundred thousand tries each.
    return (
        pcoll
        | 'Initialize' >> beam.Create(
            [self.tries_per_work_item] * 100).with_output_types(int)
        | 'Run trials' >> beam.Map(run_trials)
        | 'Sum' >> beam.CombineGlobally(combine_results).without_defaults())


def run(argv=None):

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output', required=True, help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:

    (  # pylint: disable=expression-not-assigned
        p
        | EstimatePiTransform()
        | WriteToText(known_args.output, coder=JsonCoder()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
