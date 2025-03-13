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

"""Tests for the avro New York City Trips example."""

# pytype: skip-file

import unittest

import apache_beam as beam
from apache_beam.examples import avro_nyc_trips
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class AvroNycTripsTest(unittest.TestCase):
  def test_create_key_with_service_and_day(self):
    RECORDS = [
        {
            'hvfhs_license_num': 'HV0002',
            'request_datetime': 1557705616000,
            'on_scene_datetime': 1557792016000,
            'pickup_datetime': 1557878416000,
            'dropoff_datetime': 1557964816000,
            'trip_miles': 2.45,
        },
        {
            'hvfhs_license_num': 'HV0003',
            'request_datetime': None,
            'on_scene_datetime': 1557792016000,
            'pickup_datetime': 1557878416000,
            'dropoff_datetime': 1557964816000,
            'trip_miles': 3.45,
        },
        {
            'hvfhs_license_num': 'HV0004',
            'request_datetime': None,
            'on_scene_datetime': None,
            'pickup_datetime': 1557878416000,
            'dropoff_datetime': 1557964816000,
            'trip_miles': 4.45,
        },
        {
            'hvfhs_license_num': 'HV0005',
            'request_datetime': None,
            'on_scene_datetime': None,
            'pickup_datetime': None,
            'dropoff_datetime': 1557964816000,
            'trip_miles': 5.45,
        },
    ]

    EXPECTED = [
        (('Juno', 'Sun'), RECORDS[0]),
        (('Uber', 'Mon'), RECORDS[1]),
        (('Via', 'Tue'), RECORDS[2]),
        (('Lyft', 'Wed'), RECORDS[3]),
    ]
    with TestPipeline() as p:
      records = p | beam.Create(RECORDS)
      result = records | beam.ParDo(avro_nyc_trips.CreateKeyWithServiceAndDay())

      assert_that(result, equal_to(EXPECTED))

  def test_calculate_price_per_attribute(self):
    RECORDS = [
        (('Uber', 'Fri'),
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
         }),
        (('Uber', 'Fri'),
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
         }),
    ]

    EXPECTED = [
        (('Uber', 'Fri'),
         {
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
         }),
    ]

    with TestPipeline() as p:
      records = p | beam.Create(RECORDS)
      result = records | beam.CombinePerKey(
          avro_nyc_trips.CalculatePricePerAttribute())

      assert_that(result, equal_to(EXPECTED))

  def test_flatten_group(self):
    record = {'total_driver_pay': 123.54}
    element = (('Uber', 'Fri'), record)
    expected_record = {
        'service': 'Uber', 'day': 'Fri', 'total_driver_pay': 123.54
    }
    output_record = avro_nyc_trips.flatten_group(element)

    self.assertEqual(expected_record, output_record)


if __name__ == '__main__':
  unittest.main()
