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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PipelineOptions}. */
@RunWith(JUnit4.class)
public class PipelineOptionsTest {
  private static final String DEFAULT_USER_AGENT_NAME = "Apache_Beam_SDK_for_Java";

  @Rule public ExpectedException expectedException = ExpectedException.none();

  /** Interfaces used for testing that {@link PipelineOptions#as(Class)} functions. */
  public interface DerivedTestOptions extends BaseTestOptions {
    int getDerivedValue();

    void setDerivedValue(int derivedValue);

    @Override
    @JsonIgnore
    Set<String> getIgnoredValue();

    @Override
    void setIgnoredValue(Set<String> ignoredValue);
  }

  /** Test interface. */
  public interface ConflictedTestOptions extends BaseTestOptions {
    String getDerivedValue();

    void setDerivedValue(String derivedValue);

    @Override
    @JsonIgnore
    Set<String> getIgnoredValue();

    @Override
    void setIgnoredValue(Set<String> ignoredValue);
  }

  /** Test interface. */
  public interface BaseTestOptions extends PipelineOptions {
    List<Boolean> getBaseValue();

    void setBaseValue(List<Boolean> baseValue);

    @JsonIgnore
    Set<String> getIgnoredValue();

    void setIgnoredValue(Set<String> ignoredValue);
  }

  @Test
  public void testDynamicAs() {
    BaseTestOptions options = PipelineOptionsFactory.create().as(BaseTestOptions.class);
    assertNotNull(options);
  }

  /** Test interface. */
  public interface ValueProviderOptions extends PipelineOptions {
    ValueProvider<Boolean> getBool();

    void setBool(ValueProvider<Boolean> value);

    ValueProvider<String> getString();

    void setString(ValueProvider<String> value);

    String getNotAValueProvider();

    void setNotAValueProvider(String value);
  }

  @Test
  public void testOutputRuntimeOptions() {
    ValueProviderOptions options =
        PipelineOptionsFactory.fromArgs("--string=baz").as(ValueProviderOptions.class);
    Map<String, ?> expected = ImmutableMap.of("bool", ImmutableMap.of("type", Boolean.class));
    assertEquals(expected, options.outputRuntimeOptions());
  }

  @Test
  public void testPipelineOptionsIdIsUniquePerInstance() {
    Set<Long> ids = new HashSet<>();
    for (int i = 0; i < 1000; ++i) {
      long id = PipelineOptionsFactory.create().getOptionsId();
      if (!ids.add(id)) {
        fail(String.format("Generated duplicate id %s, existing generated ids %s", id, ids));
      }
    }
  }

  @Test
  public void testUserAgentFactory() {
    PipelineOptions options = PipelineOptionsFactory.create();
    String userAgent = options.getUserAgent();
    assertNotNull(userAgent);
    assertTrue(userAgent.contains(DEFAULT_USER_AGENT_NAME));
  }
}
