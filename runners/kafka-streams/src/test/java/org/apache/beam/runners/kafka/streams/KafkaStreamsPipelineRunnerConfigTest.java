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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import java.util.Properties;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the Kafka Streams configuration the runner builds from its pipeline options. */
@RunWith(JUnit4.class)
public class KafkaStreamsPipelineRunnerConfigTest {

  private static final String SESSION_TIMEOUT =
      StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
  private static final String HEARTBEAT =
      StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);

  private static Properties configFor(Integer sessionTimeoutMs) {
    KafkaStreamsPipelineOptions options =
        PipelineOptionsFactory.create().as(KafkaStreamsPipelineOptions.class);
    options.setApplicationId("an-application");
    options.setBootstrapServers("localhost:9092");
    if (sessionTimeoutMs != null) {
      options.setSessionTimeoutMs(sessionTimeoutMs);
    }
    JobInfo jobInfo =
        JobInfo.create("a-job", "a-job", "", PipelineOptionsTranslation.toProto(options));
    return new KafkaStreamsPipelineRunner(options).streamsConfig(jobInfo);
  }

  @Test
  public void theSessionTimeoutDefaultsToKafkasOwn() {
    // Changing this would change how long a lost instance goes unnoticed, so it is deliberate that
    // the runner keeps Kafka's default rather than choosing its own.
    assertThat(configFor(null).get(SESSION_TIMEOUT), is(45_000));
  }

  @Test
  public void theSessionTimeoutIsWhateverThePipelineAskedFor() {
    assertThat(configFor(6_000).get(SESSION_TIMEOUT), is(6_000));
  }

  @Test
  public void theHeartbeatIsAThirdOfTheSessionTimeout() {
    assertThat(configFor(6_000).get(HEARTBEAT), is(2_000));
  }

  @Test
  public void theHeartbeatStaysShorterThanEvenATinySessionTimeout() {
    // Kafka rejects a heartbeat that is not shorter than the session timeout, so deriving it has
    // to hold for small values too — a fixed floor would not. One millisecond is excluded because
    // no positive heartbeat is shorter than it, and a broker would refuse such a timeout anyway.
    for (int sessionTimeoutMs : new int[] {2, 10, 100, 200, 1_000, 6_000, 45_000}) {
      Properties config = configFor(sessionTimeoutMs);
      assertThat(
          "heartbeat must stay under the session timeout for " + sessionTimeoutMs + "ms",
          (Integer) config.get(HEARTBEAT),
          lessThan((Integer) config.get(SESSION_TIMEOUT)));
    }
  }
}
