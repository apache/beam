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
package org.apache.beam.runners.kafka.streams;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.junit.Test;

/** Tests for {@link KafkaStreamsPipelineOptions}. */
public class KafkaStreamsPipelineOptionsTest {

  @Test
  public void testDefaultValues() {
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.fromArgs("--applicationId=test-app")
            .as(KafkaStreamsPipelineOptions.class);

    assertThat(options.getBootstrapServers(), is("localhost:9092"));
    assertThat(options.getMaxBundleSize(), is(1000));
    assertThat(options.getMaxBundleTimeMs(), is(1000));
    assertThat(options.getStateDir(), is(notNullValue()));
    assertThat(options.getStateDir(), containsString("beam-kafka-streams-state"));
    assertThat(options.getStateDir(), containsString(options.getJobName()));
  }

  @Test
  public void testApplicationIdIsRequired() {
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> PipelineOptionsValidator.validate(KafkaStreamsPipelineOptions.class, options));
    assertThat(ex.getMessage(), containsString("getApplicationId"));
  }

  @Test
  public void testOverrides() {
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.fromArgs(
                "--bootstrapServers=broker-1:9092,broker-2:9092",
                "--applicationId=custom-app",
                "--maxBundleSize=500",
                "--maxBundleTimeMs=250",
                "--stateDir=/var/data/beam-ks")
            .as(KafkaStreamsPipelineOptions.class);

    assertThat(options.getBootstrapServers(), is("broker-1:9092,broker-2:9092"));
    assertThat(options.getApplicationId(), is("custom-app"));
    assertThat(options.getMaxBundleSize(), is(500));
    assertThat(options.getMaxBundleTimeMs(), is(250));
    assertThat(options.getStateDir(), is("/var/data/beam-ks"));
  }

  @Test
  public void testStateDirIsolatesByJobName() {
    KafkaStreamsPipelineOptions optionsA =
        PipelineOptionsFactory.fromArgs("--jobName=job-a", "--applicationId=app-a")
            .as(KafkaStreamsPipelineOptions.class);
    KafkaStreamsPipelineOptions optionsB =
        PipelineOptionsFactory.fromArgs("--jobName=job-b", "--applicationId=app-b")
            .as(KafkaStreamsPipelineOptions.class);

    assertThat(optionsA.getStateDir(), containsString("job-a"));
    assertThat(optionsB.getStateDir(), containsString("job-b"));
    assertThat(optionsA.getStateDir().equals(optionsB.getStateDir()), is(false));
  }
}
