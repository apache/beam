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

import CommonJobProperties as commonJobProperties

// This job runs the Beam performance tests on PerfKit Benchmarker.
job('beam_PerformanceTests_Dataflow'){
    // Set default Beam job properties.
    commonJobProperties.setTopLevelMainJobProperties(delegate)

    // Run job in postcommit every 6 hours, don't trigger every push, and
    // don't email individual committers.
    commonJobProperties.setAutoJob(
        delegate,
        'H */6 * * *')

    def argMap = [
      benchmarks: 'dpb_wordcount_benchmark',
      dpb_dataflow_staging_location: 'gs://temp-storage-for-perf-tests/staging',
      dpb_wordcount_input: 'dataflow-samples/shakespeare/kinglear.txt',
      config_override: 'dpb_wordcount_benchmark.dpb_service.service_type=dataflow'
    ]

    commonJobProperties.buildPerformanceTest(delegate, argMap)

    // [BEAM-2141] Perf tests do not pass.
    disabled()
}
