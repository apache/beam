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
package org.apache.beam.runners.dataflow.options;

import static org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level.WARN;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.WorkerLogLevelOverrides;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataflowWorkerLoggingOptions}. */
@RunWith(JUnit4.class)
public class DataflowWorkerLoggingOptionsTest {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testWorkerLogLevelOverrideWithInvalidLogLevel() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unsupported log level");
    WorkerLogLevelOverrides.from(ImmutableMap.of("Name", "FakeLevel"));
  }

  @Test
  public void testWorkerLogLevelOverrideForClass() throws Exception {
    assertEquals(
        "{\"org.junit.Test\":\"WARN\"}",
        MAPPER.writeValueAsString(
            new WorkerLogLevelOverrides().addOverrideForClass(Test.class, WARN)));
  }

  @Test
  public void testWorkerLogLevelOverrideForPackage() throws Exception {
    assertEquals(
        "{\"org.junit\":\"WARN\"}",
        MAPPER.writeValueAsString(
            new WorkerLogLevelOverrides().addOverrideForPackage(Test.class.getPackage(), WARN)));
  }

  @Test
  public void testWorkerLogLevelOverrideForName() throws Exception {
    assertEquals(
        "{\"A\":\"WARN\"}",
        MAPPER.writeValueAsString(new WorkerLogLevelOverrides().addOverrideForName("A", WARN)));
  }

  @Test
  public void testSerializationAndDeserializationOf() throws Exception {
    String testValue = "{\"A\":\"WARN\"}";
    assertEquals(
        testValue,
        MAPPER.writeValueAsString(MAPPER.readValue(testValue, WorkerLogLevelOverrides.class)));
  }
}
