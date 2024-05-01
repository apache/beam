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
package org.apache.beam.runners.dataflow.worker.streaming.config;

import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.MapTask;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingApplianceConfigFetcherTest {
  private final WindmillServerStub mockWindmillServer = mock(WindmillServerStub.class);

  @Test
  public void testGetComputationConfig() throws IOException {
    List<Windmill.GetConfigResponse.NameMapEntry> nameMapEntries =
        ImmutableList.of(
            Windmill.GetConfigResponse.NameMapEntry.newBuilder()
                .setUserName("userName1")
                .setSystemName("systemName1")
                .build(),
            Windmill.GetConfigResponse.NameMapEntry.newBuilder()
                .setUserName("userName2")
                .setSystemName("userName2")
                .build());
    Windmill.GetConfigResponse getConfigResponse =
        Windmill.GetConfigResponse.newBuilder()
            .addAllNameMap(nameMapEntries)
            .addCloudWorks(
                Transport.getJsonFactory()
                    .toString(
                        new MapTask()
                            .setSystemName("systemName")
                            .setStageName("stageName")
                            .setInstructions(ImmutableList.of())))
            .build();
    StreamingPipelineConfig expectedConfig =
        StreamingPipelineConfig.builder()
            .setUserStepToStateFamilyNameMap(
                nameMapEntries.stream()
                    .collect(
                        toMap(
                            Windmill.GetConfigResponse.NameMapEntry::getUserName,
                            Windmill.GetConfigResponse.NameMapEntry::getSystemName)))
            .build();
    Set<StreamingPipelineConfig> configs = new HashSet<>();
    StreamingApplianceConfigFetcher configLoader =
        createStreamingApplianceConfigLoader(configs::add);
    when(mockWindmillServer.getConfig(any())).thenReturn(getConfigResponse);
    Optional<ComputationConfig> config = configLoader.getConfig("someComputationId");
    assertTrue(config.isPresent());
    assertThat(configs).containsExactly(expectedConfig);
  }

  @Test
  public void testGetComputationConfig_whenNoConfigReturned() {
    Set<StreamingPipelineConfig> configs = new HashSet<>();
    StreamingApplianceConfigFetcher configLoader =
        createStreamingApplianceConfigLoader(configs::add);
    when(mockWindmillServer.getConfig(any())).thenReturn(null);
    Optional<ComputationConfig> configResponse = configLoader.getConfig("someComputationId");
    assertFalse(configResponse.isPresent());
    assertThat(configs).isEmpty();
  }

  private StreamingApplianceConfigFetcher createStreamingApplianceConfigLoader(
      Consumer<StreamingPipelineConfig> onConfigResponse) {
    return new StreamingApplianceConfigFetcher(
        mockWindmillServer, onConfigResponse, Function.identity());
  }
}
