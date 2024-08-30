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
package org.apache.beam.runners.samza.util;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.junit.Assert;
import org.junit.Test;

public class PortableConfigUtilsTest {

  @Test
  public void testNonPortableMode() {
    SamzaPipelineOptions mockOptions = mock(SamzaPipelineOptions.class);
    Map<String, String> config = new HashMap<>();
    config.put(PortableConfigUtils.BEAM_PORTABLE_MODE, "false");
    doReturn(config).when(mockOptions).getConfigOverride();
    Assert.assertFalse(
        "Expected false for portable mode ", PortableConfigUtils.isPortable(mockOptions));
  }

  @Test
  public void testNonPortableModeNullConfig() {
    SamzaPipelineOptions mockOptions = mock(SamzaPipelineOptions.class);
    doReturn(null).when(mockOptions).getConfigOverride();
    Assert.assertFalse(
        "Expected false for portable mode ", PortableConfigUtils.isPortable(mockOptions));
  }

  @Test
  public void testPortableMode() {
    SamzaPipelineOptions mockOptions = mock(SamzaPipelineOptions.class);
    Map<String, String> config = new HashMap<>();
    config.put(PortableConfigUtils.BEAM_PORTABLE_MODE, "true");
    doReturn(config).when(mockOptions).getConfigOverride();
    Assert.assertTrue(
        "Expected true for portable runner", PortableConfigUtils.isPortable(mockOptions));
  }
}
