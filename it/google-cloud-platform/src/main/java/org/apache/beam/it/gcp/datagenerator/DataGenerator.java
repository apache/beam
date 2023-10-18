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
package org.apache.beam.it.gcp.datagenerator;

import static org.apache.beam.it.gcp.LoadTestBase.createConfig;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.auth.Credentials;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.dataflow.FlexTemplateClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class for starting a Streaming Data Generator Dataflow template job. */
public class DataGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
  private static final String SPEC_PATH =
      "gs://dataflow-templates/latest/flex/Streaming_Data_Generator";
  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  private static final String MESSAGES_GENERATED_METRIC_NAME =
      "Generate Fake Messages-out0-ElementCount";
  private static final String MESSAGES_LIMIT = "messagesLimit";
  private final LaunchConfig dataGeneratorOptions;
  private final PipelineLauncher pipelineLauncher;
  private final PipelineOperator pipelineOperator;

  private DataGenerator(Builder builder) {
    pipelineLauncher = FlexTemplateClient.builder(CREDENTIALS).build();
    pipelineOperator = new PipelineOperator(pipelineLauncher);
    this.dataGeneratorOptions =
        LaunchConfig.builder(builder.getJobName(), SPEC_PATH)
            .setParameters(builder.getParameters())
            .addParameter("experiments", "disable_runner_v2")
            .build();
  }

  public static DataGenerator.Builder builderWithSchemaLocation(
      String testName, String schemaLocation) {
    return new DataGenerator.Builder(testName + "-data-generator")
        .setSchemaLocation(schemaLocation)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
  }

  public static DataGenerator.Builder builderWithSchemaTemplate(
      String testName, String schemaTemplate) {
    return new DataGenerator.Builder(testName + "-data-generator")
        .setSchemaTemplate(schemaTemplate)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
  }

  /**
   * Executes the data generator using the config provided. If messageLimit is provided, we wait
   * until the data generator finishes, or we reach timeout. If a messageLimit is not provided we
   * wait until timeout and cancel the data generator.
   *
   * <p>Note: This is a blocking call. For backlog tests, start the Data generator before executing
   * the template under test. For testing a pipeline against live incoming data, execute the data
   * generator after starting the pipeline under test.
   *
   * @param timeout time to wait before cancelling the data generator.
   * @return approximate number of messages generated
   * @throws IOException if any errors are encountered.
   */
  public Integer execute(Duration timeout) throws IOException {
    LaunchInfo dataGeneratorLaunchInfo =
        pipelineLauncher.launch(PROJECT, REGION, dataGeneratorOptions);
    assertThatPipeline(dataGeneratorLaunchInfo).isRunning();
    PipelineOperator.Config config = createConfig(dataGeneratorLaunchInfo, timeout);
    // check if the job will be BATCH or STREAMING
    if (dataGeneratorOptions.parameters().containsKey(MESSAGES_LIMIT)) {
      // Batch job, wait till data generator job finishes
      Result dataGeneratorResult = pipelineOperator.waitUntilDone(config);
      assertThatResult(dataGeneratorResult).isLaunchFinished();
    } else {
      // Streaming job, wait till timeout and drain job
      Result dataGeneratorResult = pipelineOperator.waitUntilDoneAndFinish(config);
      assertThatResult(dataGeneratorResult).hasTimedOut();
    }
    @SuppressWarnings("nullness")
    int generatedMessages =
        pipelineLauncher
            .getMetric(
                PROJECT, REGION, dataGeneratorLaunchInfo.jobId(), MESSAGES_GENERATED_METRIC_NAME)
            .intValue();
    LOG.info("Data generator finished. Generated {} messages.", generatedMessages);
    return generatedMessages;
  }

  /** Builder for the {@link DataGenerator}. */
  public static final class Builder {
    private final String jobName;
    private final Map<String, String> parameters;

    private Builder(String jobName) {
      this.jobName = jobName;
      this.parameters = new HashMap<>();
    }

    public String getJobName() {
      return jobName;
    }

    public Map<String, String> getParameters() {
      return parameters;
    }

    public DataGenerator.Builder setSchemaTemplate(String value) {
      parameters.put("schemaTemplate", value);
      return this;
    }

    public DataGenerator.Builder setSchemaLocation(String value) {
      parameters.put("schemaLocation", value);
      return this;
    }

    public DataGenerator.Builder setMessagesLimit(String value) {
      parameters.put(MESSAGES_LIMIT, value);
      return this;
    }

    public DataGenerator.Builder setQPS(String value) {
      parameters.put("qps", value);
      return this;
    }

    public DataGenerator.Builder setSinkType(String value) {
      parameters.put("sinkType", value);
      return this;
    }

    public Builder setWorkerMachineType(String value) {
      parameters.put("workerMachineType", value);
      return this;
    }

    public Builder setNumWorkers(String value) {
      parameters.put("numWorkers", value);
      return this;
    }

    public DataGenerator.Builder setMaxNumWorkers(String value) {
      parameters.put("maxNumWorkers", value);
      return this;
    }

    public DataGenerator.Builder setAutoscalingAlgorithm(AutoscalingAlgorithmType value) {
      parameters.put("autoscalingAlgorithm", value.toString());
      return this;
    }

    public DataGenerator.Builder setOutputDirectory(String value) {
      parameters.put("outputDirectory", value);
      return this;
    }

    public DataGenerator.Builder setOutputType(String value) {
      parameters.put("outputType", value);
      return this;
    }

    public DataGenerator.Builder setNumShards(String value) {
      parameters.put("numShards", value);
      return this;
    }

    public DataGenerator.Builder setAvroSchemaLocation(String value) {
      parameters.put("avroSchemaLocation", value);
      return this;
    }

    public DataGenerator.Builder setTopic(String value) {
      parameters.put("topic", value);
      return this;
    }

    public DataGenerator.Builder setProjectId(String value) {
      parameters.put("projectId", value);
      return this;
    }

    public DataGenerator.Builder setSpannerInstanceName(String value) {
      parameters.put("spannerInstanceName", value);
      return this;
    }

    public DataGenerator.Builder setSpannerDatabaseName(String value) {
      parameters.put("spannerDatabaseName", value);
      return this;
    }

    public DataGenerator.Builder setSpannerTableName(String value) {
      parameters.put("spannerTableName", value);
      return this;
    }

    public DataGenerator.Builder setDriverClassName(String value) {
      parameters.put("driverClassName", value);
      return this;
    }

    public DataGenerator.Builder setConnectionUrl(String value) {
      parameters.put("connectionUrl", value);
      return this;
    }

    public DataGenerator.Builder setUsername(String value) {
      parameters.put("username", value);
      return this;
    }

    public DataGenerator.Builder setPassword(String value) {
      parameters.put("password", value);
      return this;
    }

    public DataGenerator.Builder setConnectionProperties(String value) {
      parameters.put("connectionProperties", value);
      return this;
    }

    public DataGenerator.Builder setStatement(String value) {
      parameters.put("statement", value);
      return this;
    }

    public DataGenerator build() {
      return new DataGenerator(this);
    }
  }

  /** Enum representing Autoscaling algorithm types. */
  public enum AutoscalingAlgorithmType {
    NONE("NONE"),
    THROUGHPUT_BASED("THROUGHPUT_BASED");

    private final String text;

    AutoscalingAlgorithmType(String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return this.text;
    }
  }
}
