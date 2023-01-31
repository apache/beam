/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it;

import static com.google.cloud.teleport.it.PerformanceBenchmarkingBase.createConfig;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;

import com.google.auth.Credentials;
import com.google.cloud.teleport.it.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.it.launcher.PipelineLauncher;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Helper class for starting a Streaming Data generator dataflow template job. */
public class DataGenerator {
  private static final String SPEC_PATH =
      "gs://dataflow-templates/latest/flex/Streaming_Data_Generator";
  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  private static final String MESSAGES_LIMIT = "messagesLimit";
  private final LaunchConfig dataGeneratorOptions;
  private final PipelineLauncher pipelineLauncher;
  private final PipelineOperator pipelineOperator;

  private DataGenerator(Builder builder) {
    pipelineLauncher = FlexTemplateClient.builder().setCredentials(CREDENTIALS).build();
    pipelineOperator = new PipelineOperator(pipelineLauncher);
    this.dataGeneratorOptions =
        LaunchConfig.builder(builder.getJobName(), SPEC_PATH)
            .setParameters(builder.getParameters())
            .build();
  }

  public static DataGenerator.Builder builderWithSchemaLocation(
      String jobName, String schemaLocation) {
    return new DataGenerator.Builder(jobName)
        .setSchemaLocation(schemaLocation)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
  }

  public static DataGenerator.Builder builderWithSchemaTemplate(
      String jobName, String schemaTemplate) {
    return new DataGenerator.Builder(jobName)
        .setSchemaTemplate(schemaTemplate)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
  }

  /**
   * Executes the data generator using the config provided. If messageLimit is provided, we wait
   * until the data generator finishes or we reach timeout. If a messageLimit is not provided we
   * wait until timeout and cancel the data generator.
   *
   * <p>Note: This is a blocking call. For backlog tests, start the Data generator before executing
   * the template under test. For testing a pipeline against live incoming data, execute the data
   * generator after starting the pipeline under test.
   *
   * @param timeout time to wait before cancelling the data generator.
   * @throws IOException if any errors are encountered.
   */
  public void execute(Duration timeout) throws IOException {
    LaunchInfo dataGeneratorLaunchInfo =
        pipelineLauncher.launch(PROJECT, REGION, dataGeneratorOptions);
    assertThatPipeline(dataGeneratorLaunchInfo).isRunning();
    PipelineOperator.Config config = createConfig(dataGeneratorLaunchInfo, timeout);
    // check if the job will be BATCH or STREAMING
    Result dataGeneratorResult;
    if (dataGeneratorOptions.parameters().containsKey(MESSAGES_LIMIT)) {
      // BATCH job, wait till data generator job finishes
      dataGeneratorResult = pipelineOperator.waitUntilDone(config);
      assertThatResult(dataGeneratorResult).isLaunchFinished();
    } else {
      // STREAMING job, wait till timeout and drain job
      dataGeneratorResult = pipelineOperator.waitUntilDoneAndFinish(config);
      assertThatResult(dataGeneratorResult).hasTimedOut();
    }
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

    public DataGenerator.Builder setTopic(String value) {
      parameters.put("topic", value);
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

    public String toString() {
      return this.text;
    }
  }
}
