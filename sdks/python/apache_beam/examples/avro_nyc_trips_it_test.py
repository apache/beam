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

"""End-to-end test for the avro New York City Trips example."""

# pytype: skip-file

import logging
import unittest
import uuid
from io import BytesIO

import fastavro
import pytest

from apache_beam.examples import avro_nyc_trips
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline


class AvroNycTripsIT(unittest.TestCase):
  SCHEMA = {
      'fields': [
          {
              'name': 'hvfhs_license_num', 'type': ['null', 'string']
          },
          {
              'name': 'request_datetime',
              'logicalType': 'timestamp-millis',
              'type': ['null', 'long']
          },
          {
              'name': 'trip_miles', 'type': ['null', 'double']
          },
          {
              'name': 'trip_time', 'type': ['null', 'long']
          },
          {
              'name': 'base_passenger_fare', 'type': ['null', 'double']
          },
          {
              'name': 'tolls', 'type': ['null', 'double']
          },
          {
              'name': 'bcf', 'type': ['null', 'double']
          },
          {
              'name': 'sales_tax', 'type': ['null', 'double']
          },
          {
              'name': 'congestion_surcharge', 'type': ['null', 'double']
          },
          {
              'name': 'airport_fee', 'type': ['null', 'long']
          },
          {
              'name': 'tips', 'type': ['null', 'double']
          },
          {
              'name': 'driver_pay', 'type': ['null', 'double']
          },
      ],
      'name': 'nyc_fhv_trips',
      'type': 'record',
  }

  RECORDS = [
      {
          'hvfhs_license_num': 'HV0003',
          'request_datetime': 1549008086000,
          'trip_miles': 2.45,
          'trip_time': 579,
          'base_passenger_fare': 9.35,
          'tolls': 0.0,
          'bcf': 0.23,
          'sales_tax': 0.83,
          'congestion_surcharge': 0.0,
          'airport_fee': None,
          'tips': 0.0,
          'driver_pay': 7.48
      },
      {
          'hvfhs_license_num': 'HV0003',
          'request_datetime': 1549009568000,
          'trip_miles': 1.71,
          'trip_time': 490,
          'base_passenger_fare': 7.91,
          'tolls': 0.0,
          'bcf': 0.2,
          'sales_tax': 0.7,
          'congestion_surcharge': 0.0,
          'airport_fee': None,
          'tips': 2.0,
          'driver_pay': 7.93
      },
      {
          'hvfhs_license_num': 'HV0005',
          'request_datetime': 1549010613000,
          'trip_miles': 11.24,
          'trip_time': 1739,
          'base_passenger_fare': 29.77,
          'tolls': 0.72,
          'bcf': 0.76,
          'sales_tax': 2.71,
          'congestion_surcharge': 0.0,
          'airport_fee': None,
          'tips': 0.0,
          'driver_pay': 22.09
      },
      {
          'hvfhs_license_num': 'HV0005',
          'request_datetime': 1549010420000,
          'trip_miles': 5.71,
          'trip_time': 1559,
          'base_passenger_fare': 21.69,
          'tolls': 0.24,
          'bcf': 0.55,
          'sales_tax': 1.95,
          'congestion_surcharge': 0.0,
          'airport_fee': None,
          'tips': 0.0,
          'driver_pay': 14.87
      },
  ]

  EXPECTED = [
      {
          'service': 'Lyft',
          'day': 'Fri',
          'total_price': 58.39,
          'total_driver_pay': 36.96,
          'total_trip_miles': 16.95,
          'total_trip_minutes': 54.96666666666667,
          'total_number_of_trips': 2,
          'price_per_trip': 29.195,
          'price_per_mile': 3.4448377581120946,
          'price_per_minute': 1.0622801697998787,
          'driver_pay_per_trip': 18.48,
          'driver_pay_per_mile': 2.1805309734513276,
          'driver_pay_per_minute': 0.6724075197089144,
          'miles_per_hour': 18.502122498483928
      },
      {
          'service': 'Uber',
          'day': 'Fri',
          'total_price': 21.22,
          'total_driver_pay': 17.41,
          'total_trip_miles': 4.16,
          'total_trip_minutes': 17.816666666666666,
          'total_number_of_trips': 2,
          'price_per_trip': 10.61,
          'price_per_mile': 5.100961538461538,
          'price_per_minute': 1.1910196445275958,
          'driver_pay_per_trip': 8.705,
          'driver_pay_per_mile': 4.185096153846153,
          'driver_pay_per_minute': 0.9771749298409729,
          'miles_per_hour': 14.00935453695042
      },
  ]

  @pytest.mark.sickbay_dataflow
  @pytest.mark.no_xdist
  @pytest.mark.examples_postcommit
  def test_avro_nyc_trips_output_files_on_small_input(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # set up the files with expected content.
    temp_location = test_pipeline.get_option('temp_location')
    test_output = '/'.join((temp_location, str(uuid.uuid4()), 'result'))
    test_input = '/'.join((temp_location, str(uuid.uuid4()), 'input.avro'))

    # create avro data
    fo = BytesIO()
    fastavro.writer(fo, self.SCHEMA, self.RECORDS)
    fo.seek(0)

    # write avro test case
    with FileSystems.create(test_input) as f:
      f.write(fo.read())

    extra_opts = {'input': test_input, 'output': test_output}
    avro_nyc_trips.run(test_pipeline.get_full_options_as_args(**extra_opts))

    # load result avro file and compare
    metadata_list = FileSystems.match([f'{test_output}*'])[0].metadata_list
    result = []

    for metadata in metadata_list:
      with FileSystems.open(metadata.path) as f:
        avro_reader = fastavro.reader(f)

        result.extend(avro_reader)

    result.sort(key=lambda x: x['service'])

    self.assertEqual(self.EXPECTED, result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
