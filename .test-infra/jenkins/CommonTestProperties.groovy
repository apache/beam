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
    PYTHON_37,
    JAVA
  }

  enum Runner {
    DATAFLOW("DataflowRunner"),
    TEST_DATAFLOW("TestDataflowRunner"),
    SPARK("SparkRunner"),
    SPARK_STRUCTURED_STREAMING("SparkStructuredStreamingRunner"),
    FLINK("FlinkRunner"),
    DIRECT("DirectRunner"),
    PORTABLE("PortableRunner")

    def RUNNER_DEPENDENCY_MAP = [
      JAVA: [
        DATAFLOW: ":runners:google-cloud-dataflow-java",
        TEST_DATAFLOW: ":runners:google-cloud-dataflow-java",
        SPARK: ":runners:spark",
        SPARK_STRUCTURED_STREAMING: ":runners:spark",
        FLINK: ":runners:flink:1.10",
        DIRECT: ":runners:direct-java"
      ],
      PYTHON: [
        DATAFLOW: "DataflowRunner",
        TEST_DATAFLOW: "TestDataflowRunner",
        DIRECT: "DirectRunner",
        PORTABLE: "PortableRunner"
      ],
      PYTHON_37: [
        DATAFLOW: "DataflowRunner",
        TEST_DATAFLOW: "TestDataflowRunner",
        DIRECT: "DirectRunner",
        PORTABLE: "PortableRunner"
      ]
    ]

    private final String option

    Runner(String option) {
      this.option = option
    }

    String getDependencyBySDK(SDK sdk) {
      RUNNER_DEPENDENCY_MAP.get(sdk.toString()).get(this.toString())
    }
  }

  enum TriggeringContext {
    PR,
    POST_COMMIT
  }
}
