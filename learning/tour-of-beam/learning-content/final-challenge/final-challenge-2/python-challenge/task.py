#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# beam-playground:
#   name: FinalChallenge2
#   description: Final challenge 2.
#   multifile: true
#   files:
#     - name: analysis.csv
#   context_line: 50
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window, trigger
from apache_beam.transforms.combiners import CountCombineFn


class SplitWords(beam.DoFn):
    def process(self, element):
        return element.lower().split(" ")


class Analysis:
    def __init__(self, word, negative, positive, uncertainty, litigious, strong, weak, constraining):
        self.word = word
        self.negative = negative
        self.positive = positive
        self.uncertainty = uncertainty
        self.litigious = litigious
        self.strong = strong
        self.weak = weak
        self.constraining = constraining

    def __str__(self):
        return (f'Analysis(word={self.word}, negative={self.negative}, positive={self.positive}, '
                f'uncertainty={self.uncertainty}, litigious={self.litigious}, strong={self.strong}, '
                f'weak={self.weak}, constraining={self.constraining})')

def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
      shakespeare = (p
                       | 'Read from text file' >> ReadFromText('gs://apache-beam-samples/shakespeare/kinglear.txt')
                       | 'Split into words' >> beam.ParDo(SplitWords())
                       | 'Filter empty words' >> beam.Filter(bool))

if __name__ == "__main__":
    run()
