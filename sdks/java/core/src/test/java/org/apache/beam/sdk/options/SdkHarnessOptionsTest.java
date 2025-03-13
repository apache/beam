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
package org.apache.beam.sdk.options;

import static org.apache.beam.sdk.options.SdkHarnessOptions.LogLevel.WARN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.options.SdkHarnessOptions.DefaultMaxCacheMemoryUsageMb;
import org.apache.beam.sdk.options.SdkHarnessOptions.SdkHarnessLogLevelOverrides;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SdkHarnessOptions}. */
@RunWith(JUnit4.class)
public class SdkHarnessOptionsTest {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  @Test
  public void testDefaultMaxCacheMemoryUsageMbWhenRuntimeReturnsInvalidValue() {
    assertEquals(
        100,
        new DefaultMaxCacheMemoryUsageMb()
            .getMaxCacheMemoryUsage(PipelineOptionsFactory.create(), Long.MAX_VALUE));
  }

  @Test
  public void testDefaultMaxCacheMemoryUsageMbWhenRuntimeReturnsValidValue() {
    assertEquals(
        20,
        new DefaultMaxCacheMemoryUsageMb()
            .getMaxCacheMemoryUsage(PipelineOptionsFactory.create(), 100 * 1024 * 1024));
  }

  @Test
  public void testDefaultMaxCacheMemoryUsageMbWhenInvalidPercentage() {
    assertThrows(
        "maxCacheMemoryUsagePercent must be between 0 and 100",
        IllegalArgumentException.class,
        () ->
            new DefaultMaxCacheMemoryUsageMb()
                .getMaxCacheMemoryUsage(
                    PipelineOptionsFactory.fromArgs("--maxCacheMemoryUsagePercent=-1").create(),
                    100 * 1024 * 1024));
    assertThrows(
        "maxCacheMemoryUsagePercent must be between 0 and 100",
        IllegalArgumentException.class,
        () ->
            new DefaultMaxCacheMemoryUsageMb()
                .getMaxCacheMemoryUsage(
                    PipelineOptionsFactory.fromArgs("--maxCacheMemoryUsagePercent=101").create(),
                    100 * 1024 * 1024));
  }

  @Test
  public void testSdkHarnessLogLevelOverrideWithInvalidLogLevel() {
    assertThrows(
        "Unsupported log level",
        IllegalArgumentException.class,
        () -> SdkHarnessLogLevelOverrides.from(ImmutableMap.of("Name", "FakeLevel")));
  }

  @Test
  public void testSdkHarnessLogLevelOverrideForClass() throws Exception {
    assertEquals(
        "{\"org.junit.Test\":\"WARN\"}",
        MAPPER.writeValueAsString(
            new SdkHarnessLogLevelOverrides().addOverrideForClass(Test.class, WARN)));
  }

  @Test
  public void testSdkHarnessLogLevelOverrideForPackage() throws Exception {
    assertEquals(
        "{\"org.junit\":\"WARN\"}",
        MAPPER.writeValueAsString(
            new SdkHarnessLogLevelOverrides()
                .addOverrideForPackage(Test.class.getPackage(), WARN)));
  }

  @Test
  public void testSdkHarnessLogLevelOverrideForName() throws Exception {
    assertEquals(
        "{\"A\":\"WARN\"}",
        MAPPER.writeValueAsString(new SdkHarnessLogLevelOverrides().addOverrideForName("A", WARN)));
  }

  @Test
  public void testSerializationAndDeserializationOf() throws Exception {
    String testValue = "{\"A\":\"WARN\"}";
    assertEquals(
        testValue,
        MAPPER.writeValueAsString(MAPPER.readValue(testValue, SdkHarnessLogLevelOverrides.class)));
  }
}
