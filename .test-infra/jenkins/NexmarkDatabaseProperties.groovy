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

// contains Big query and InfluxDB related properties for Nexmark runs
class NexmarkDatabaseProperties {

    static Map<String, Object> nexmarkBigQueryArgs = [
          'bigQueryTable'          : 'nexmark',
          'bigQueryDataset'        : 'nexmark',
          'project'                : 'apache-beam-testing',
          'resourceNameMode'       : 'QUERY_RUNNER_AND_MODE',
          'exportSummaryToBigQuery': true,
          'tempLocation'           : 'gs://temp-storage-for-perf-tests/nexmark',
    ]

    static Map<String, Object> nexmarkInfluxDBArgs = [
          'influxDatabase'         : InfluxDBCredentialsHelper.InfluxDBDatabaseName,
          'influxHost'             : InfluxDBCredentialsHelper.InfluxDBHostname,
          'baseInfluxMeasurement'  : 'nexmark',
          'exportSummaryToInfluxDB': true,
          'influxRetentionPolicy'  : 'forever',
    ]
}
