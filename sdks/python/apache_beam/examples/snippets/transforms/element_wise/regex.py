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

from __future__ import absolute_import
from __future__ import print_function


def regex_match(test=None):
  # [START regex_match]
  import apache_beam as beam
  import re

  def parse_plant(text):
    m = re.match(r'^([^\s-]+)\s*-\s*(\w+)\s*-\s*(?P<duration>\w+)$', text)
    if m:
      yield {
          'match': m.group(0),              # contains the entire matched text
          'icon': m.group(1),               # ([^\s-]+) - group
          'name': m.group(2),               # (\w+) - group
          'duration': m.group('duration'),  # (?P<duration>\w+) - named group
      }

  with beam.Pipeline() as pipeline:
    plant_matches = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓   -   Strawberry   -   perennial',
            '🥕 - Carrot - biennial',
            '# 🍌 - invalid - format',
            '🍆\t-\tEggplant\t-\tperennial',
            '🍅 - Tomato - annual',
            '🍉 - invalid - format with trailing words',
            '🥔-Potato-perennial',
        ])
        | 'Parse plants' >> beam.FlatMap(parse_plant)
        | beam.Map(print)
    )
    # [END regex_match]
    if test:
      test(plant_matches)


def regex_search(test=None):
  # [START regex_search]
  import apache_beam as beam
  import re

  def parse_plant_duration(text):
    m = re.search(r'([^\s-]+)\s*-\s*(\w*)\s*-\s*(?P<duration>\w+)', text)
    if m:
      yield {
          'match': m.group(0),              # contains the entire matched text
          'icon': m.group(1),               # ([^\s-]+) - group
          'name': m.group(2),               # (\w+) - group
          'duration': m.group('duration'),  # (?P<duration>\w+) - named group
      }

  with beam.Pipeline() as pipeline:
    plant_matches = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '# 🍓   -   Strawberry   -   perennial',
            '# 🥕 - Carrot - biennial',
            '# 🍆\t-\tEggplant\t-\tperennial',
            '# 🍅 - Tomato - annual',
            '# 🥔-Potato-perennial',
        ])
        | 'Parse plants' >> beam.FlatMap(parse_plant_duration)
        | beam.Map(print)
    )
    # [END regex_search]
    if test:
      test(plant_matches)


def regex_find_all(test=None):
  # [START regex_find_all]
  import apache_beam as beam
  import re

  def parse_words(text):
    for m in re.finditer(r'[^\s-]+', text):
      yield m.group()

  with beam.Pipeline() as pipeline:
    words = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓   -   Strawberry   -   perennial',
            '🥕 - Carrot - biennial',
            '🍆\t-\tEggplant\t-\tperennial',
            '🍅 - Tomato - annual',
            '🥔-Potato-perennial',
        ])
        | 'Parse words' >> beam.FlatMap(parse_words)
        | beam.Map(print)
    )
    # [END regex_find_all]
    if test:
      test(words)


def regex_replace(test=None):
  # [START regex_replace]
  import apache_beam as beam
  import re

  with beam.Pipeline() as pipeline:
    plants_csv = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓   -   Strawberry   -   perennial',
            '🥕 - Carrot - biennial',
            '🍆\t-\tEggplant\t-\tperennial',
            '🍅 - Tomato - annual',
            '🥔-Potato-perennial',
        ])
        | 'To CSV' >> beam.Map(lambda text: re.sub(r'\s*-\s*', ',', text))
        | beam.Map(print)
    )
    # [END regex_replace]
    if test:
      test(plants_csv)


def regex_split(test=None):
  # [START regex_split]
  import apache_beam as beam
  import re

  with beam.Pipeline() as pipeline:
    plants_columns = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓   -   Strawberry   -   perennial',
            '🥕 - Carrot - biennial',
            '🍆\t-\tEggplant\t-\tperennial',
            '🍅 - Tomato - annual',
            '🥔-Potato-perennial',
        ])
        | 'Split' >> beam.Map(lambda text: re.split(r'\s*-\s*', text))
        | beam.Map(print)
    )
    # [END regex_split]
    if test:
      test(plants_columns)
