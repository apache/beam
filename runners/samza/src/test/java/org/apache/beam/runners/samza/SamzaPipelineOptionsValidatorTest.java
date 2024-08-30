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
package org.apache.beam.runners.samza;

import static org.apache.beam.runners.samza.SamzaPipelineOptionsValidator.validateBundlingRelatedOptions;
import static org.apache.samza.config.JobConfig.JOB_CONTAINER_THREAD_POOL_SIZE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** Test for {@link SamzaPipelineOptionsValidator}. */
public class SamzaPipelineOptionsValidatorTest {

  @Test(expected = IllegalArgumentException.class)
  public void testBundleEnabledInMultiThreadedModeThrowsException() {
    SamzaPipelineOptions mockOptions = mock(SamzaPipelineOptions.class);
    Map<String, String> config = ImmutableMap.of(JOB_CONTAINER_THREAD_POOL_SIZE, "10");

    when(mockOptions.getMaxBundleSize()).thenReturn(2L);
    when(mockOptions.getConfigOverride()).thenReturn(config);
    validateBundlingRelatedOptions(mockOptions);
  }

  @Test
  public void testBundleEnabledInSingleThreadedMode() {
    SamzaPipelineOptions mockOptions = mock(SamzaPipelineOptions.class);
    when(mockOptions.getMaxBundleSize()).thenReturn(2L);

    try {
      Map<String, String> config = ImmutableMap.of(JOB_CONTAINER_THREAD_POOL_SIZE, "1");
      when(mockOptions.getConfigOverride()).thenReturn(config);
      validateBundlingRelatedOptions(mockOptions);

      // In the absence of configuration make sure it is treated as single threaded mode.
      when(mockOptions.getConfigOverride()).thenReturn(Collections.emptyMap());
      validateBundlingRelatedOptions(mockOptions);
    } catch (Exception e) {
      throw new AssertionError("Bundle size > 1 should be supported in single threaded mode");
    }
  }
}
