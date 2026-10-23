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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

/** Tests for {@link KafkaStreamsJobServerDriver}. */
public class KafkaStreamsJobServerDriverTest {

  @Test
  public void testConfigurationDefaults() {
    KafkaStreamsJobServerDriver.KafkaStreamsServerConfiguration config =
        new KafkaStreamsJobServerDriver.KafkaStreamsServerConfiguration();

    assertThat(config.getHost(), is("localhost"));
    assertThat(config.getPort(), is(8099));
    assertThat(config.getArtifactPort(), is(8098));
    assertThat(config.getExpansionPort(), is(8097));
    assertThat(config.isCleanArtifactsPerJob(), is(true));

    KafkaStreamsJobServerDriver driver = KafkaStreamsJobServerDriver.fromConfig(config);
    assertThat(driver, is(not(nullValue())));
  }

  @Test
  public void testConfigurationFromArgs() {
    KafkaStreamsJobServerDriver.KafkaStreamsServerConfiguration config =
        KafkaStreamsJobServerDriver.parseArgs(
            new String[] {
              "--job-host=test-host",
              "--job-port",
              "42",
              "--artifact-port",
              "43",
              "--expansion-port",
              "44",
              "--clean-artifacts-per-job=false",
            });

    assertThat(config.getHost(), is("test-host"));
    assertThat(config.getPort(), is(42));
    assertThat(config.getArtifactPort(), is(43));
    assertThat(config.getExpansionPort(), is(44));
    assertThat(config.isCleanArtifactsPerJob(), is(false));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidArgsRejected() {
    KafkaStreamsJobServerDriver.parseArgs(new String[] {"--unknown-flag=value"});
  }
}
