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


def combineglobally_side_inputs_dict(test=None):
  # [START combineglobally_side_inputs_dict]
  import apache_beam as beam

  def get_custom_common_items(sets, options):
    sets = sets or [set()]
    common_items = set.intersection(*sets)
    common_items |= options['include']  # union
    common_items &= options['exclude']  # intersection
    return common_items

  with beam.Pipeline() as pipeline:
    options = pipeline | 'Create options' >> beam.Create([
        ('exclude', {'🥕'}),
        ('include', {'🍇', '🌽'}),
    ])

    custom_common_items = (
        pipeline
        | 'Create produce' >> beam.Create([
            {'🍓', '🥕', '🍌', '🍅', '🌶️'},
            {'🍇', '🥕', '🥝', '🍅', '🥔'},
            {'🍉', '🥕', '🍆', '🍅', '🍍'},
            {'🥑', '🥕', '🌽', '🍅', '🥥'},
        ])
        | 'Get common items' >> beam.CombineGlobally(
            get_custom_common_items, options=beam.pvalue.AsDict(options))
        | beam.Map(print))
    # [END combineglobally_side_inputs_dict]
    if test:
      test(custom_common_items)


if __name__ == '__main__':
  combineglobally_side_inputs_dict()
