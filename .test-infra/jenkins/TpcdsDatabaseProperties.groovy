/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import InfluxDBCredentialsHelper

// contains Big query and InfluxDB related properties for TPC-DS runs
class TpcdsDatabaseProperties {

  static Map<String, Object> tpcdsBigQueryArgs = [
    'bigQueryTable'          : 'tpcds',
    'bigQueryDataset'        : 'tpcds',
    'project'                : 'apache-beam-testing',
    'resourceNameMode'       : 'QUERY_RUNNER_AND_MODE',
    'exportSummaryToBigQuery': true,
    'tempLocation'           : 'gs://temp-storage-for-perf-tests/tpcds',
  ]

  static Map<String, Object> tpcdsInfluxDBArgs = [
    'influxDatabase'         : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
    'influxHost'             : InfluxDBCredentialsHelper.InfluxDBHostUrl,
    'baseInfluxMeasurement'  : 'tpcds',
    'exportSummaryToInfluxDB': true,
    'influxRetentionPolicy'  : 'forever',
  ]

  static String tpcdsQueriesArg = '3,7,10,25,26,29,35,38,40,42,43,52,55,69,79,83,84,87,93,96'
}
