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

# Wrapping hurts the readability of the docs.
# pylint: disable=line-too-long

# beam-playground:
#   name: GroupByAttr
#   description: Demonstration of GroupBy transform usage by an attribute.
#   multifile: false
#   default_example: false
#   context_line: 51
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - group

import apache_beam as beam

# [START groupby_table]
GROCERY_LIST = [
    beam.Row(recipe='pie', fruit='raspberry', quantity=1, unit_price=3.50),
    beam.Row(recipe='pie', fruit='blackberry', quantity=1, unit_price=4.00),
    beam.Row(recipe='pie', fruit='blueberry', quantity=1, unit_price=2.00),
    beam.Row(recipe='muffin', fruit='blueberry', quantity=2, unit_price=2.00),
    beam.Row(recipe='muffin', fruit='banana', quantity=3, unit_price=1.00),
]
# [END groupby_table]


def groupby_attr(test=None):
  with beam.Pipeline() as p:
    # [START groupby_attr]
    grouped = (
        p
        | beam.Create(GROCERY_LIST)
        | beam.GroupBy('recipe')
        | beam.Map(print))
    # [END groupby_attr]

  if test:
    test(grouped)


if __name__ == '__main__':
  groupby_attr()
