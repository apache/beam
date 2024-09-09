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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.MapTask;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingApplianceComputationConfigFetcherTest {
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
    String serializedMapTask =
        Transport.getJsonFactory()
            .toString(
                new MapTask()
                    .setSystemName("systemName")
                    .setStageName("stageName")
                    .setInstructions(ImmutableList.of()));
    Windmill.GetConfigResponse getConfigResponse =
        Windmill.GetConfigResponse.newBuilder()
            .addAllNameMap(nameMapEntries)
            .addComputationConfigMap(
                Windmill.GetConfigResponse.ComputationConfigMapEntry.newBuilder()
                    .setComputationId("systemName")
                    .setComputationConfig(
                        Windmill.ComputationConfig.newBuilder()
                            .addTransformUserNameToStateFamily(
                                Windmill.ComputationConfig.TransformUserNameToStateFamilyEntry
                                    .newBuilder()
                                    .setStateFamily("stateFamilyName")
                                    .setTransformUserName("transformUserName")
                                    .build())
                            .build())
                    .build())
            .addCloudWorks(serializedMapTask)
            .build();
    ComputationConfig expectedConfig =
        ComputationConfig.create(
            Transport.getJsonFactory().fromString(serializedMapTask, MapTask.class),
            getConfigResponse.getComputationConfigMapList().stream()
                .map(Windmill.GetConfigResponse.ComputationConfigMapEntry::getComputationConfig)
                .flatMap(
                    computationConfig ->
                        computationConfig.getTransformUserNameToStateFamilyList().stream())
                .collect(
                    toMap(
                        Windmill.ComputationConfig.TransformUserNameToStateFamilyEntry
                            ::getTransformUserName,
                        Windmill.ComputationConfig.TransformUserNameToStateFamilyEntry
                            ::getStateFamily)),
            nameMapEntries.stream()
                .collect(
                    toMap(
                        Windmill.GetConfigResponse.NameMapEntry::getUserName,
                        Windmill.GetConfigResponse.NameMapEntry::getSystemName)));
    StreamingApplianceComputationConfigFetcher configLoader =
        createStreamingApplianceConfigLoader();
    when(mockWindmillServer.getConfig(any())).thenReturn(getConfigResponse);
    Optional<ComputationConfig> config = configLoader.fetchConfig("someComputationId");
    assertTrue(config.isPresent());
    assertThat(config.get()).isEqualTo(expectedConfig);
  }

  @Test
  public void testGetComputationConfig_whenNoConfigReturned() {
    StreamingApplianceComputationConfigFetcher configLoader =
        createStreamingApplianceConfigLoader();
    when(mockWindmillServer.getConfig(any())).thenReturn(null);
    Optional<ComputationConfig> configResponse = configLoader.fetchConfig("someComputationId");
    assertFalse(configResponse.isPresent());
  }

  @Test
  public void testGetComputationConfig_errorOnNoComputationConfig() {
    StreamingApplianceComputationConfigFetcher configLoader =
        createStreamingApplianceConfigLoader();
    when(mockWindmillServer.getConfig(any()))
        .thenReturn(Windmill.GetConfigResponse.newBuilder().build());
    assertThrows(NoSuchElementException.class, () -> configLoader.fetchConfig("someComputationId"));
  }

  @Test
  public void testGetComputationConfig_onFetchConfigError() {
    StreamingApplianceComputationConfigFetcher configLoader =
        createStreamingApplianceConfigLoader();
    RuntimeException e = new RuntimeException("something bad happened.");
    when(mockWindmillServer.getConfig(any())).thenThrow(e);
    Throwable fetchConfigError =
        assertThrows(RuntimeException.class, () -> configLoader.fetchConfig("someComputationId"));
    assertThat(fetchConfigError).isSameInstanceAs(e);
  }

  private StreamingApplianceComputationConfigFetcher createStreamingApplianceConfigLoader() {
    return new StreamingApplianceComputationConfigFetcher(
        mockWindmillServer::getConfig,
        new FixedGlobalConfigHandle(StreamingGlobalConfig.builder().build()));
  }
}
