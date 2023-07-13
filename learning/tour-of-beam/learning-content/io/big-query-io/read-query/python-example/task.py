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
#   name: read-query
#   description: BigQueryIO read query example.
#   multifile: false
#   never_run: true
#   always_run: true
#   context_line: 34
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

import argparse
import os
import warnings

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQueryRequest, ReadAllFromBigQuery

class WeatherData:
    def __init__(self, station_number, wban_number, year, month, day):
        self.station_number = station_number
        self.wban_number = wban_number
        self.year = year
        self.month = month
        self.day = day

    def __str__(self):
        return f"Weather Data: Station {self.station_number} (WBAN {self.wban_number}), Date: {self.year}-{self.month}-{self.day}"

def run(argv=None):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(PipelineOptions)


    with beam.Pipeline(options=pipeline_options, argv=argv) as p:
      (p
         # | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query='select * from `apache-beam-testing.clouddataflow_samples.weather_stations`',use_standard_sql=True,
         #                                                    method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
         # | beam.combiners.Sample.FixedSizeGlobally(5)
         # | beam.FlatMap(lambda line: line)
         # | beam.Map(lambda element: WeatherData(element['station_number'],element['wban_number'],element['year'],element['month'],element['day']))
         # | beam.Map(print)
         )


if __name__ == '__main__':
    run()
