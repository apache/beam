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
#   name: CombineGloballyFunction
#   description: Demonstration of CombineGlobally transform usage with a function.
#   multifile: false
#   default_example: false
#   context_line: 39
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - strings
#     - group


def combineglobally_function(test=None):
  # [START combineglobally_function]
  import apache_beam as beam

  def get_common_items(sets):
    # set.intersection() takes multiple sets as separete arguments.
    # We unpack the `sets` list into multiple arguments with the * operator.
    # The combine transform might give us an empty list of `sets`,
    # so we use a list with an empty set as a default value.
    return set.intersection(*(sets or [set()]))

  with beam.Pipeline() as pipeline:
    common_items = (
        pipeline
        | 'Create produce' >> beam.Create([
            {'🍓', '🥕', '🍌', '🍅', '🌶️'},
            {'🍇', '🥕', '🥝', '🍅', '🥔'},
            {'🍉', '🥕', '🍆', '🍅', '🍍'},
            {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])
        | 'Get common items' >> beam.CombineGlobally(get_common_items)
        | beam.Map(print))
    # [END combineglobally_function]
    if test:
      test(common_items)


if __name__ == '__main__':
  combineglobally_function()
