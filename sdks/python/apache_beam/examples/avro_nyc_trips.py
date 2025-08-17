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

"""An example that reads New York City for hire vehicle trips.

An example that reads New York City for hire vehicle trips data from Google
Cloud Storage and calculates various statistics including the passenger's
price per trip, price per mile, and price per minute.

Data originally downloaded as parquet files from the New York City
government
`website <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>`_
before being converted to avro format. Here is the
`documentation
<https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf>`_
for the data format and schema.

To execute this pipeline locally using the DirectRunner, specify an
output prefix on GCS:::

  --output gs://YOUR_OUTPUT_PREFIX

To execute this pipeline using the Google Cloud Dataflow service, specify
pipeline configuration in addition to the above:::

  --job_name NAME_FOR_YOUR_JOB
  --project YOUR_PROJECT_ID
  --region GCE_REGION
  --staging_location gs://YOUR_STAGING_DIRECTORY
  --temp_location gs://YOUR_TEMPORARY_DIRECTORY
  --runner DataflowRunner

The default input is
``gs://apache-beam-samples/nyc_trip/avro/fhvhv_tripdata_2023-02.avro`` and
takes about 15 minutes with 6 workers running on Dataflow. The default input
can be overridden with --input argument. More data can be accessed at
``gs://apache-beam-samples/nyc_trip/avro/*``.

Additionally, the original parquet files can be found at
``gs://apache-beam-samples/nyc_trip/parquet/*``.
"""

# pytype: skip-file

import argparse
import datetime
import logging

import pytz

import apache_beam as beam
from apache_beam.io import ReadFromAvro
from apache_beam.io import WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

SCHEMA = {
    'fields': [
        {
            'name': 'service', 'type': 'string'
        },
        {
            'name': 'day', 'type': 'string'
        },
        {
            'name': 'total_price', 'type': 'double'
        },
        {
            'name': 'total_driver_pay', 'type': 'double'
        },
        {
            'name': 'total_trip_miles', 'type': 'double'
        },
        {
            'name': 'total_trip_minutes', 'type': 'double'
        },
        {
            'name': 'total_number_of_trips', 'type': 'long'
        },
        {
            'name': 'price_per_trip', 'type': 'double'
        },
        {
            'name': 'price_per_mile', 'type': 'double'
        },
        {
            'name': 'price_per_minute', 'type': 'double'
        },
        {
            'name': 'driver_pay_per_trip', 'type': 'double'
        },
        {
            'name': 'driver_pay_per_mile', 'type': 'double'
        },
        {
            'name': 'driver_pay_per_minute', 'type': 'double'
        },
        {
            'name': 'miles_per_hour', 'type': 'double'
        },
    ],
    'name': 'nyc_trip_prices',
    'type': 'record',
}


class CreateKeyWithServiceAndDay(beam.DoFn):
  """Creates a key, value group.

  The key is the combination of the HVFHS Code converted to the ride-sharing
  company name and the day of the week of the ride. The value is the original
  record dictionary object.
  """
  def process(self, record: dict):
    options = {
        'HV0002': 'Juno', 'HV0003': 'Uber', 'HV0004': 'Via', 'HV0005': 'Lyft'
    }
    service = options.get(record['hvfhs_license_num'])
    if service:
      timestamp = None
      for k in ('request_datetime',
                'on_scene_datetime',
                'pickup_datetime',
                'dropoff_datetime'):
        timestamp = timestamp or record[k]
        if timestamp:
          break

      day_of_the_week = datetime.datetime.fromtimestamp(
          timestamp / 1000.0,
          tz=pytz.timezone('America/New_York')).strftime('%a')

      yield (service, day_of_the_week), record


class CalculatePricePerAttribute(beam.CombineFn):
  """Calculates the price for attribute.

  Calculates the total driver pay, price, miles, minutes, and trips per for
  hire vehicle service. And calculates the price per mile, minute, and trip
  for both the driver and passenger.
  """
  def create_accumulator(self):
    total_price = 0.0
    total_driver_pay = 0.0
    total_trip_miles = 0.0
    total_trip_time = 0.0
    total_number_of_trips = 0
    accumulator = (
        total_price,
        total_driver_pay,
        total_trip_miles,
        total_trip_time,
        total_number_of_trips,
    )
    return accumulator

  def add_input(self, accumulator, record):
    (
        total_price,
        total_driver_pay,
        total_trip_miles,
        total_trip_time,
        total_number_of_trips,
    ) = accumulator
    return (
        total_price + sum(
            record[name] for name in (
                'base_passenger_fare', 'tolls', 'bcf', 'sales_tax',
                'congestion_surcharge', 'airport_fee', 'tips')
            if record[name] is not None),
        total_driver_pay + record['driver_pay'] + record['tips'],
        total_trip_miles + record['trip_miles'],
        total_trip_time + record['trip_time'],
        total_number_of_trips + 1,
    )

  def merge_accumulators(self, accumulators):
    # calculates the sum for each item in the accumulator
    return tuple(sum(item) for item in zip(*accumulators))

  def extract_output(self, accumulator):
    (
        total_price,
        total_driver_pay,
        total_trip_miles,
        total_trip_time,
        total_number_of_trips,
    ) = accumulator
    total_trip_minutes = total_trip_time / 60
    return {
        'total_driver_pay': total_driver_pay,
        'total_price': total_price,
        'total_trip_miles': total_trip_miles,
        'total_trip_minutes': total_trip_minutes,
        'total_number_of_trips': total_number_of_trips,
        'price_per_trip': total_price / total_number_of_trips,
        'price_per_mile': total_price / total_trip_miles,
        'price_per_minute': total_price / total_trip_minutes,
        'driver_pay_per_trip': total_driver_pay / total_number_of_trips,
        'driver_pay_per_mile': total_driver_pay / total_trip_miles,
        'driver_pay_per_minute': total_driver_pay / total_trip_minutes,
        'miles_per_hour': total_trip_miles / (total_trip_minutes / 60),
    }


def flatten_group(element):
  """Flattens the key, value pair to a single record dictionary."""
  key, record = element
  service, day_of_the_week = key
  record['service'] = service
  record['day'] = day_of_the_week
  return record


def run(argv=None):
  """Runs the New York City trips pipeline.

  Args:
    argv: Pipeline options as a list of arguments.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default=
      'gs://apache-beam-samples/nyc_trip/avro/fhvhv_tripdata_2023-02.avro',
      help='Input file of NYC FHV data to process. Larger dataset can be found '
      'here: gs://apache-beam-samples/nyc_trip/avro/*',
  )
  parser.add_argument(
      '--output',
      dest='output',
      help='Output file to write results to.',
      required=True,
  )
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=pipeline_options) as p:
    (  # pylint: disable=expression-not-assigned
        p
        | ReadFromAvro(known_args.input)
        # filter records that have keys that are not None
        # and at least one timestamp that is not None
        | beam.Filter(
            lambda record: all(
                record[k] is not None for k in (
                    'hvfhs_license_num',
                    'trip_miles',
                    'trip_time',
                    'base_passenger_fare',
                    'tips',
                    'driver_pay')) and any(
                        record[k] is not None for k in (
                            'request_datetime',
                            'on_scene_datetime',
                            'pickup_datetime',
                            'dropoff_datetime')))
        | beam.ParDo(CreateKeyWithServiceAndDay())
        | beam.CombinePerKey(CalculatePricePerAttribute())
        | beam.Map(flatten_group)
        | WriteToAvro(known_args.output, SCHEMA, file_name_suffix='.avro'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
