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

"""An example of using custom classes and coder for grouping operations.

This workflow demonstrates registration and usage of a custom coder for a user-
defined class. A deterministic custom coder is needed to use a class as a key in
a combine or group operation.

This example assumes an input file with, on each line, a comma-separated name
and score.
"""

# pytype: skip-file

import argparse
import logging
import sys

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.typehints.decorators import with_output_types


class Player(object):
  """A custom class used as a key in combine/group transforms."""
  def __init__(self, name):
    self.name = name


class PlayerCoder(coders.Coder):
  """A custom coder for the Player class."""
  def encode(self, o):
    """Encode to bytes with a trace that coder was used."""
    # Our encoding prepends an 'x:' prefix.
    return b'x:%s' % str(o.name).encode('utf-8')

  def decode(self, s):
    # To decode, we strip off the prepended 'x:' prefix.
    s = s.decode('utf-8')
    assert s[0:2] == 'x:'
    return Player(s[2:])

  def is_deterministic(self):
    # Since coded Player objects are used as keys below with
    # beam.CombinePerKey(sum), we require that this coder is deterministic
    # (i.e., two equivalent instances of the classes are encoded into the same
    # byte string) in order to guarantee consistent results.
    return True


# Annotate the get_players function so that the typehint system knows that the
# input to the CombinePerKey operation is a key-value pair of a Player object
# and an integer.
@with_output_types(tuple[Player, int])
def get_players(descriptor):
  name, points = descriptor.split(',')
  return Player(name), int(points)


def run(args=None, save_main_session=True):
  """Runs the workflow computing total points from a collection of matches."""

  if args is None:
    args = sys.argv[1:]
  parser = argparse.ArgumentParser()
  parser.add_argument('--input', required=True, help='Input file to process.')
  parser.add_argument(
      '--output', required=True, help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(args)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:

    # Register the custom coder for the Player class, so that it will be used in
    # the computation.
    coders.registry.register_coder(Player, PlayerCoder)

    (  # pylint: disable=expression-not-assigned
        p
        | ReadFromText(known_args.input)
        # The get_players function is annotated with a type hint above,
        # so the type system knows the output type of the following operation
        # is a key-value pair of a Player and an int. Please see the
        # documentation for details on types that are inferred automatically
        # as well as other ways to specify type hints.
        | beam.Map(get_players)
        # The output type hint of the previous step is used to infer that the
        # key type of the following operation is the Player type. Since a
        # custom coder is registered for the Player class above,
        # a PlayerCoder will be used to encode Player objects as keys for this
        # combine operation.
        | beam.CombinePerKey(sum)
        | beam.Map(lambda k_v: '%s,%d' % (k_v[0].name, k_v[1]))
        | WriteToText(known_args.output))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
