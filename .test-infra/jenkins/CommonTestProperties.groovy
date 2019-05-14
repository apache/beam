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



class CommonTestProperties {
    enum SDK {
        PYTHON,
        JAVA
    }

    enum Runner {
        DATAFLOW("DataflowRunner"),
        SPARK("SparkRunner"),
        FLINK("TestFlinkRunner"),
        DIRECT("DirectRunner"),

        def RUNNER_DEPENDENCY_MAP = [
                JAVA: [
                        DATAFLOW: ":beam-runners-google-cloud-dataflow-java",
                        SPARK: ":beam-runners-spark",
                        FLINK: ":beam-runners-flink_2.11",
                        DIRECT: ":beam-runners-direct-java"
                ],
                PYTHON: [
                        DATAFLOW: "TestDataflowRunner",
                        DIRECT: "DirectRunner",
                ]
        ]

        private final String option

        Runner(String option) {
            this.option = option
        }


        String getDepenedencyBySDK(SDK sdk) {
            RUNNER_DEPENDENCY_MAP.get(sdk.toString()).get(this.toString())
        }

    }

    enum TriggeringContext {
        PR,
        POST_COMMIT
    }
}