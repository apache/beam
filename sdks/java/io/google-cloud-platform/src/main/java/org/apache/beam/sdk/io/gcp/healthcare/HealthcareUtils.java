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

 import org.apache.beam.sdk.metrics.Counter;
 
 public class HealthcareUtils{
    public static class deidentifyLroCounters {
        private static final Counter DEIDENTIFY_OPERATION_SUCCESS =
            Metrics.counter(
                DeidentifyFn.class, BASE_METRIC_PREFIX + "deidentify_operation_success_count");
        private static final Counter DEIDENTIFY_OPERATION_ERRORS =
            Metrics.counter(
                DeidentifyFn.class, BASE_METRIC_PREFIX + "deidentify_operation_failure_count");
        private static final Counter RESOURCES_DEIDENTIFIED_SUCCESS =
            Metrics.counter(
                DeidentifyFn.class, BASE_METRIC_PREFIX + "resources_deidentified_success_count");
        private static final Counter RESOURCES_DEIDENTIFIED_ERRORS =
            Metrics.counter(
                DeidentifyFn.class, BASE_METRIC_PREFIX + "resources_deidentified_failure_count");
    }
 }