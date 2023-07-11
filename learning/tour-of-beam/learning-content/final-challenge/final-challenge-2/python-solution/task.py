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
#   name: FinalSolution2
#   description: Final challenge solution 2.
#   multifile: true
#   files:
#     - name: analysis.csv
#   context_line: 57
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
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


class ExtractAnalysis(beam.DoFn):
    def process(self, element):
        items = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', element)
        if items[1] != 'Negative':
            yield Analysis(items[0].lower(), items[1], items[2], items[3], items[4], items[5], items[6], items[7])


class Partition(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Partition(self._analysis_partition_fn, 3)

    @staticmethod
    def _analysis_partition_fn(analysis, num_partitions):
        if analysis.positive != "0":
            return 0
        elif analysis.negative != "0":
            return 1
        else:
            return 2


class LogOutput(beam.DoFn):
    def __init__(self, message):
        self.message = message

    def process(self, element):
        print(f"{self.message}: {element}")


class MatchWordDoFn(beam.DoFn):
    def process(self, element, analysis):
        for a in analysis:
            if a.word == element:
                yield a


def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
      shakespeare = (p
                       | 'Read from text file' >> ReadFromText('gs://apache-beam-samples/shakespeare/kinglear.txt')
                       | 'Split into words' >> beam.ParDo(SplitWords())
                       | 'Filter empty words' >> beam.Filter(bool))

      analysis = (p
                    | 'Read from csv file' >> ReadFromText('analysis.csv')
                    | 'Extract Analysis' >> beam.ParDo(ExtractAnalysis()))

      matches = shakespeare | beam.ParDo(MatchWordDoFn(), beam.pvalue.AsList(analysis))

      result = matches | Partition()

      positive_words = result[0]
      negative_words = result[1]

      (positive_words
         | 'Count Positive Words' >> beam.CombineGlobally(CountCombineFn()).without_defaults()
         | 'Log Positive Words' >> beam.ParDo(LogOutput('Positive word count')))

      (positive_words
         | 'Filter Strong or Weak Positive Words' >> beam.Filter(
                    lambda analysis: analysis.strong != '0' or analysis.weak != '0')
         | 'Count Strong or Weak Positive Words' >> beam.CombineGlobally(CountCombineFn()).without_defaults()
         | 'Log Strong or Weak Positive Words' >> beam.ParDo(LogOutput('Positive words with enhanced effect count')))

      (negative_words
         | 'Count Negative Words' >> beam.CombineGlobally(CountCombineFn()).without_defaults()
         | 'Log Negative Words' >> beam.ParDo(LogOutput('Negative word count')))

      (negative_words
         | 'Filter Strong or Weak Negative Words' >> beam.Filter(
                    lambda analysis: analysis.strong != '0' or analysis.weak != '0')
         | 'Count Strong or Weak Negative Words' >> beam.CombineGlobally(CountCombineFn()).without_defaults()
         | 'Log Strong or Weak Negative Words' >> beam.ParDo(LogOutput('Negative words with enhanced effect count')))

if __name__ == "__main__":
    run()
