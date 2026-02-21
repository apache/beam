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
#   name: GroupBySimpleAggregate
#   description: Demonstration of GroupBy transform usage with a simple aggregate.
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

GROCERY_LIST = [
    beam.Row(recipe='pie', fruit='raspberry', quantity=1, unit_price=3.50),
    beam.Row(recipe='pie', fruit='blackberry', quantity=1, unit_price=4.00),
    beam.Row(recipe='pie', fruit='blueberry', quantity=1, unit_price=2.00),
    beam.Row(recipe='muffin', fruit='blueberry', quantity=2, unit_price=2.00),
    beam.Row(recipe='muffin', fruit='banana', quantity=3, unit_price=1.00),
    beam.Row(recipe='pie', fruit='strawberry', quantity=3, unit_price=1.50),
]


def simple_aggregate(test=None):
  def to_grocery_row(x):
    # If it's already a Beam Row / schema object, keep it
    if hasattr(x, 'recipe') and hasattr(x, 'fruit') and hasattr(
        x, 'quantity') and hasattr(x, 'unit_price'):
      return beam.Row(
          recipe=x.recipe,
          fruit=x.fruit,
          quantity=x.quantity,
          unit_price=x.unit_price)

    # If dict
    if isinstance(x, dict):
      return beam.Row(
          recipe=x['recipe'],
          fruit=x['fruit'],
          quantity=x['quantity'],
          unit_price=x['unit_price'],
      )

    # If tuple/list (recipe, fruit, quantity, unit_price)
    return beam.Row(recipe=x[0], fruit=x[1], quantity=x[2], unit_price=x[3])

  with beam.Pipeline() as p:
    # [START simple_aggregate]
    grouped = (
        p
        | beam.Create(GROCERY_LIST)
        | 'ToGroceryRows' >> beam.Map(to_grocery_row)
        | beam.GroupBy('fruit').aggregate_field(
            'quantity', sum, 'total_quantity'))
    # [END simple_aggregate]

    if test:
      test(grouped)


if __name__ == '__main__':
  simple_aggregate()
