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

#   beam-playground:
#     name: FinalSolution3
#     description: Final challenge solution 3.
#     multifile: false
#     context_line: 57
#     categories:
#       - Quickstart
#     complexity: ADVANCED
#     tags:
#       - hellobeam


import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms import DoFn, ParDo, WindowInto, FixedWindows
from apache_beam.transforms.window import AfterProcessingTime

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

class ExtractWordsFn(beam.DoFn):
    def process(self, element):
        # Remove punctuation and non-alphanumeric characters, convert to lowercase and split the line into words
        return re.sub(r"[^A-Za-z0-9 ]", "").split(" ")

class PartitionFn(beam.DoFn):
    def process(self, element):
        if not element.positive == '0':
            yield beam.pvalue.TaggedOutput('positive', element)
        elif not element.negative == '0':
            yield beam.pvalue.TaggedOutput('negative', element)

class FilterWordsFn(beam.DoFn):
    def process(self, element, analysis_list):
        for analysis in analysis_list:
            if analysis.word == element:
                yield analysis

def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        shakespeare = (p
                       | "Read Text" >> ReadFromText("gs://apache-beam-samples/shakespeare/kinglear.txt")
                       | "Extract Words" >> ParDo(ExtractWordsFn())
                       )

        analysis = (p
                    | "Read CSV" >> ReadFromText("analysis.csv")
                    | "Extract Analysis" >> ParDo(SentimentAnalysisExtractFn())
                    )

        analysis_list = beam.pvalue.AsList(analysis)

        result = (shakespeare
                  | "Filter Words" >> ParDo(FilterWordsFn(), analysis_list)
                  | "Partition" >> beam.Partition(PartitionFn(), 2)
                  )

        positive, negative = result

        # TODO: Apply further transforms to the 'positive' and 'negative' PCollections

class SentimentAnalysisExtractFn(beam.DoFn):
    def process(self, element):
        items = re.split(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", element)
        if items[1] != "Negative":
            yield Analysis(items[0].lower(), items[1], items[2], items[3], items[4], items[5], items[6], items[7])

if __name__ == '__main__':
    run()
