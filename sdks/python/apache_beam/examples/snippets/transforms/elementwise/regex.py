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


def regex_matches(test=None):
  # [START regex_matches]
  import apache_beam as beam

  # Matches a named group 'icon', and then two comma-separated groups.
  regex = r'(?P<icon>[^\s,]+), *(\w+), *(\w+)'
  with beam.Pipeline() as pipeline:
    plants_matches = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓, Strawberry, perennial',
            '🥕, Carrot, biennial ignoring trailing words',
            '🍆, Eggplant, perennial',
            '🍅, Tomato, annual',
            '🥔, Potato, perennial',
            '# 🍌, invalid, format',
            'invalid, 🍉, format',
        ])
        | 'Parse plants' >> beam.Regex.matches(regex)
        | beam.Map(print))
    # [END regex_matches]
    if test:
      test(plants_matches)


def regex_all_matches(test=None):
  # [START regex_all_matches]
  import apache_beam as beam

  # Matches a named group 'icon', and then two comma-separated groups.
  regex = r'(?P<icon>[^\s,]+), *(\w+), *(\w+)'
  with beam.Pipeline() as pipeline:
    plants_all_matches = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓, Strawberry, perennial',
            '🥕, Carrot, biennial ignoring trailing words',
            '🍆, Eggplant, perennial',
            '🍅, Tomato, annual',
            '🥔, Potato, perennial',
            '# 🍌, invalid, format',
            'invalid, 🍉, format',
        ])
        | 'Parse plants' >> beam.Regex.all_matches(regex)
        | beam.Map(print))
    # [END regex_all_matches]
    if test:
      test(plants_all_matches)


def regex_matches_kv(test=None):
  # [START regex_matches_kv]
  import apache_beam as beam

  # Matches a named group 'icon', and then two comma-separated groups.
  regex = r'(?P<icon>[^\s,]+), *(\w+), *(\w+)'
  with beam.Pipeline() as pipeline:
    plants_matches_kv = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓, Strawberry, perennial',
            '🥕, Carrot, biennial ignoring trailing words',
            '🍆, Eggplant, perennial',
            '🍅, Tomato, annual',
            '🥔, Potato, perennial',
            '# 🍌, invalid, format',
            'invalid, 🍉, format',
        ])
        | 'Parse plants' >> beam.Regex.matches_kv(regex, keyGroup='icon')
        | beam.Map(print))
    # [END regex_matches_kv]
    if test:
      test(plants_matches_kv)


def regex_find(test=None):
  # [START regex_find]
  import apache_beam as beam

  # Matches a named group 'icon', and then two comma-separated groups.
  regex = r'(?P<icon>[^\s,]+), *(\w+), *(\w+)'
  with beam.Pipeline() as pipeline:
    plants_matches = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '# 🍓, Strawberry, perennial',
            '# 🥕, Carrot, biennial ignoring trailing words',
            '# 🍆, Eggplant, perennial - 🍌, Banana, perennial',
            '# 🍅, Tomato, annual - 🍉, Watermelon, annual',
            '# 🥔, Potato, perennial',
        ])
        | 'Parse plants' >> beam.Regex.find(regex)
        | beam.Map(print))
    # [END regex_find]
    if test:
      test(plants_matches)


def regex_find_all(test=None):
  # [START regex_find_all]
  import apache_beam as beam

  # Matches a named group 'icon', and then two comma-separated groups.
  regex = r'(?P<icon>[^\s,]+), *(\w+), *(\w+)'
  with beam.Pipeline() as pipeline:
    plants_find_all = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '# 🍓, Strawberry, perennial',
            '# 🥕, Carrot, biennial ignoring trailing words',
            '# 🍆, Eggplant, perennial - 🍌, Banana, perennial',
            '# 🍅, Tomato, annual - 🍉, Watermelon, annual',
            '# 🥔, Potato, perennial',
        ])
        | 'Parse plants' >> beam.Regex.find_all(regex)
        | beam.Map(print))
    # [END regex_find_all]
    if test:
      test(plants_find_all)


def regex_find_kv(test=None):
  # [START regex_find_kv]
  import apache_beam as beam

  # Matches a named group 'icon', and then two comma-separated groups.
  regex = r'(?P<icon>[^\s,]+), *(\w+), *(\w+)'
  with beam.Pipeline() as pipeline:
    plants_matches_kv = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '# 🍓, Strawberry, perennial',
            '# 🥕, Carrot, biennial ignoring trailing words',
            '# 🍆, Eggplant, perennial - 🍌, Banana, perennial',
            '# 🍅, Tomato, annual - 🍉, Watermelon, annual',
            '# 🥔, Potato, perennial',
        ])
        | 'Parse plants' >> beam.Regex.find_kv(regex, keyGroup='icon')
        | beam.Map(print))
    # [END regex_find_kv]
    if test:
      test(plants_matches_kv)


def regex_replace_all(test=None):
  # [START regex_replace_all]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants_replace_all = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓 : Strawberry : perennial',
            '🥕 : Carrot : biennial',
            '🍆\t:\tEggplant\t:\tperennial',
            '🍅 : Tomato : annual',
            '🥔 : Potato : perennial',
        ])
        | 'To CSV' >> beam.Regex.replace_all(r'\s*:\s*', ',')
        | beam.Map(print))
    # [END regex_replace_all]
    if test:
      test(plants_replace_all)


def regex_replace_first(test=None):
  # [START regex_replace_first]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants_replace_first = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓, Strawberry, perennial',
            '🥕, Carrot, biennial',
            '🍆,\tEggplant, perennial',
            '🍅, Tomato, annual',
            '🥔, Potato, perennial',
        ])
        | 'As dictionary' >> beam.Regex.replace_first(r'\s*,\s*', ': ')
        | beam.Map(print))
    # [END regex_replace_first]
    if test:
      test(plants_replace_first)


def regex_split(test=None):
  # [START regex_split]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants_split = (
        pipeline
        | 'Garden plants' >> beam.Create([
            '🍓 : Strawberry : perennial',
            '🥕 : Carrot : biennial',
            '🍆\t:\tEggplant : perennial',
            '🍅 : Tomato : annual',
            '🥔 : Potato : perennial',
        ])
        | 'Parse plants' >> beam.Regex.split(r'\s*:\s*')
        | beam.Map(print))
    # [END regex_split]
    if test:
      test(plants_split)
