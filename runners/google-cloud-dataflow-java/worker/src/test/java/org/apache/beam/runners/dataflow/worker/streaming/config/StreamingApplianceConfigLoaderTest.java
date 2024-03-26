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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingApplianceConfigLoaderTest {
  private final WindmillServerStub mockWindmillServer = mock(WindmillServerStub.class);

  @Test
  public void testGetComputationConfig() {
    Windmill.GetConfigResponse getConfigResponse = Windmill.GetConfigResponse.newBuilder().build();
    Set<Windmill.GetConfigResponse> configResponses = new HashSet<>();
    StreamingApplianceConfigLoader configLoader =
        createStreamingApplianceConfigLoader(configResponses::add);
    when(mockWindmillServer.getConfig(any())).thenReturn(getConfigResponse);
    Optional<Windmill.GetConfigResponse> configResponse =
        configLoader.getComputationConfig("someComputationId");
    assertTrue(configResponse.isPresent());
    assertThat(configResponses).containsExactly(getConfigResponse);
  }

  @Test
  public void testGetComputationConfig_whenNoConfigReturned() {
    Set<Windmill.GetConfigResponse> configResponses = new HashSet<>();
    StreamingApplianceConfigLoader configLoader =
        createStreamingApplianceConfigLoader(configResponses::add);
    when(mockWindmillServer.getConfig(any())).thenReturn(null);
    Optional<Windmill.GetConfigResponse> configResponse =
        configLoader.getComputationConfig("someComputationId");
    assertFalse(configResponse.isPresent());
    assertThat(configResponses).isEmpty();
  }

  private StreamingApplianceConfigLoader createStreamingApplianceConfigLoader(
      Consumer<Windmill.GetConfigResponse> onConfigResponse) {
    return new StreamingApplianceConfigLoader(mockWindmillServer, onConfigResponse);
  }
}
