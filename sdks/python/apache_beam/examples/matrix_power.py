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

"""An example that computes the matrix power y = A^m * v.

A is square matrix and v is a given vector with appropriate dimension.

In this computation, each element of the matrix is represented by ((i,j), a)
where a is the element in the i-th row and j-th column. Each element of the
vector is computed as a PCollection (i, v) where v is the element of the i-th
row. For multiplication, the vector is converted into a dict side input.
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline


def extract_matrix(line):
  tokens = line.split(':')
  row = int(tokens[0])
  numbers = tokens[1].strip().split()
  for column, number in enumerate(numbers):
    yield ((row, column), float(number))


def extract_vector(line):
  return enumerate(map(float, line.split()))


def multiply_elements(element, vector):
  ((row, col), value) = element
  return (row, value * vector[col])


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_matrix', required=True, help='Input file containing the matrix.')
  parser.add_argument(
      '--input_vector',
      required=True,
      help='Input file containing initial vector.')
  parser.add_argument(
      '--output', required=True, help='Output file to write results to.')
  parser.add_argument(
      '--exponent',
      required=True,
      type=int,
      help='Exponent of input square matrix.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  p = TestPipeline(options=PipelineOptions(pipeline_args))

  # Read the matrix from the input file and extract into the ((i,j), a) format.
  matrix = (
      p | 'read matrix' >> beam.io.ReadFromText(known_args.input_matrix)
      | 'extract matrix' >> beam.FlatMap(extract_matrix))

  # Read and extract the vector from its input file.
  vector = (
      p | 'read vector' >> beam.io.ReadFromText(known_args.input_vector)
      | 'extract vector' >> beam.FlatMap(extract_vector))

  for i in range(known_args.exponent):
    # Multiply the matrix by the current vector once,
    # and keep the resulting vector for the next iteration.
    vector = (
        matrix
        # Convert vector into side-input dictionary, compute the product.
        | 'multiply elements %d' % i >> beam.Map(
            multiply_elements, beam.pvalue.AsDict(vector))
        | 'sum element products %d' % i >> beam.CombinePerKey(sum))

  # Format and output final vector.
  _ = (
      vector  # pylint: disable=expression-not-assigned
      | 'format' >> beam.Map(repr)
      | 'write' >> beam.io.WriteToText(known_args.output))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
