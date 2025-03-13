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


def combineglobally_side_inputs_singleton(test=None):
  # [START combineglobally_side_inputs_singleton]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    single_exclude = pipeline | 'Create single_exclude' >> beam.Create(['🥕'])

    common_items_with_exceptions = (
        pipeline
        | 'Create produce' >> beam.Create([
            {'🍓', '🥕', '🍌', '🍅', '🌶️'},
            {'🍇', '🥕', '🥝', '🍅', '🥔'},
            {'🍉', '🥕', '🍆', '🍅', '🍍'},
            {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])
        | 'Get common items with exceptions' >> beam.CombineGlobally(
            lambda sets, single_exclude: \
                set.intersection(*(sets or [set()])) - {single_exclude},
            single_exclude=beam.pvalue.AsSingleton(single_exclude))
        | beam.Map(print)
    )
    # [END combineglobally_side_inputs_singleton]
    if test:
      test(common_items_with_exceptions)


if __name__ == '__main__':
  combineglobally_side_inputs_singleton()
