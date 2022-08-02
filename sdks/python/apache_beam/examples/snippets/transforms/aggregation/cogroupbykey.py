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


def cogroupbykey(test=None):
  # [START cogroupbykey]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    icon_pairs = pipeline | 'Create icons' >> beam.Create([
        ('Apple', '🍎'),
        ('Apple', '🍏'),
        ('Eggplant', '🍆'),
        ('Tomato', '🍅'),
    ])

    duration_pairs = pipeline | 'Create durations' >> beam.Create([
        ('Apple', 'perennial'),
        ('Carrot', 'biennial'),
        ('Tomato', 'perennial'),
        ('Tomato', 'annual'),
    ])

    plants = (({
        'icons': icon_pairs, 'durations': duration_pairs
    })
              | 'Merge' >> beam.CoGroupByKey()
              | beam.Map(print))
    # [END cogroupbykey]
    if test:
      test(plants)
