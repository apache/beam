# coding=utf-8
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

# pytype: skip-file
# pylint:disable=line-too-long

# beam-playground:
#   name: MapContext
#   description: Demonstration of Map transform usage with a context parameters.
#   multifile: false
#   default_example: false
#   context_line: 41
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings
#     - map


def map_context(test=None):
  # [START map_context]
  import apache_beam as beam

  import random
  from contextlib import contextmanager

  @contextmanager
  def random_nonce():
    """A context manager which typically would be used with a `with` statement.

    See https://peps.python.org/pep-0343/
    """
    # Setup code here runs once.
    # Here we just create a (shared) random value, but in practice one
    # could use this to set up network connections, caches, or other
    # re-usable state that could require cleanup.
    nonce = random.randrange(0, 100)

    # This (re-used) value is passed on every element call.
    yield nonce

    # Cleanup/closing code, if any, would go here to be called once the
    # processing is done.

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '🍓Strawberry',
            '🥕Carrot',
            '🍆Eggplant',
            '🍅Tomato',
            '🥔Potato',
        ],
                                            reshuffle=False)
        | 'Strip header' >> beam.Map(
            lambda text,
            a=beam.DoFn.SetupContextParam(random_nonce),
            b=beam.DoFn.BundleContextParam(random_nonce): f"{text} {a} {b}")
        | beam.Map(print))
    # [END map_context]
    if test:
      test(plants)


if __name__ == '__main__':
  map_context()
