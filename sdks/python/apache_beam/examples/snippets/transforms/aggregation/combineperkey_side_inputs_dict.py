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


def combineperkey_side_inputs_dict(test=None):
  # [START combineperkey_side_inputs_dict]
  import apache_beam as beam

  def bounded_sum(values, data_range):
    min_value = data_range['min']
    result = sum(values)
    if result < min_value:
      return min_value
    max_value = data_range['max']
    if result > max_value:
      return max_value
    return result

  with beam.Pipeline() as pipeline:
    data_range = pipeline | 'Create data_range' >> beam.Create([
        ('min', 2),
        ('max', 8),
    ])

    bounded_total = (
        pipeline
        | 'Create plant counts' >> beam.Create([
            ('🥕', 3),
            ('🥕', 2),
            ('🍆', 1),
            ('🍅', 4),
            ('🍅', 5),
            ('🍅', 3),
        ])
        | 'Bounded sum' >> beam.CombinePerKey(
            bounded_sum, data_range=beam.pvalue.AsDict(data_range))
        | beam.Map(print))
    # [END combineperkey_side_inputs_dict]
    if test:
      test(bounded_total)


if __name__ == '__main__':
  combineperkey_side_inputs_dict()
