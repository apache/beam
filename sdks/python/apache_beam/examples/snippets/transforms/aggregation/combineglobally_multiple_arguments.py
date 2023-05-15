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
#   name: CombineGloballyMultipleArguments
#   description: Demonstration of CombineGlobally transform usage with multiple arguments.
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


def combineglobally_multiple_arguments(test=None):
  # [START combineglobally_multiple_arguments]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    common_items_with_exceptions = (
        pipeline
        | 'Create produce' >> beam.Create([
            {'🍓', '🥕', '🍌', '🍅', '🌶️'},
            {'🍇', '🥕', '🥝', '🍅', '🥔'},
            {'🍉', '🥕', '🍆', '🍅', '🍍'},
            {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])
        | 'Get common items with exceptions' >> beam.CombineGlobally(
            lambda sets, exclude: \
                set.intersection(*(sets or [set()])) - exclude,
            exclude={'🥕'})
        | beam.Map(print)
    )
    # [END combineglobally_multiple_arguments]
    if test:
      test(common_items_with_exceptions)


if __name__ == '__main__':
  combineglobally_multiple_arguments()
