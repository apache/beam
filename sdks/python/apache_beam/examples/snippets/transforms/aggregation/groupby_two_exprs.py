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
#   name: GroupByTwoExprs
#   description: Demonstration of GroupBy transform usage with two expressions.
#   multifile: false
#   default_example: false
#   context_line: 41
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - group

import apache_beam as beam


def groupby_two_exprs(test=None):
  with beam.Pipeline() as p:
    # [START groupby_two_exprs]
    grouped = (
        p
        | beam.Create(
            ['strawberry', 'raspberry', 'blueberry', 'blackberry', 'banana'])
        | beam.GroupBy(letter=lambda s: s[0], is_berry=lambda s: 'berry' in s)
        | beam.Map(print))
    # [END groupby_two_exprs]

  if test:
    test(grouped)


if __name__ == '__main__':
  groupby_two_exprs()
