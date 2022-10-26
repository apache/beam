<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

### Common Transforms motivating challenge

You are provided with a `PCollection` of the array of taxi order prices in a csv file. Your task is to count how many positive even numbers and how many positive odd numbers there are. Although there are many ways to do this, try using another transformation presented in this module.

Please note that 0 is an even number.

Taxi csv consists of 16 columns:

`VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount`

Required columns number: **16 - order price**.