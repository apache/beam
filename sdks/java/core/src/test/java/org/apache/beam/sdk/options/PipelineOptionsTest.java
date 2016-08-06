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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/** Unit tests for {@link PipelineOptions}. */
@RunWith(JUnit4.class)
public class PipelineOptionsTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  /** Interfaces used for testing that {@link PipelineOptions#as(Class)} functions. */
  private static interface DerivedTestOptions extends BaseTestOptions {
    int getDerivedValue();
    void setDerivedValue(int derivedValue);

    @Override
    @JsonIgnore
    Set<String> getIgnoredValue();
    @Override
    void setIgnoredValue(Set<String> ignoredValue);
  }

  private static interface ConflictedTestOptions extends BaseTestOptions {
    String getDerivedValue();
    void setDerivedValue(String derivedValue);

    @Override
    @JsonIgnore
    Set<String> getIgnoredValue();
    @Override
    void setIgnoredValue(Set<String> ignoredValue);
  }

  private static interface BaseTestOptions extends PipelineOptions {
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

  @Test
  public void testCloneAs() throws IOException {
    DerivedTestOptions options = PipelineOptionsFactory.create().as(DerivedTestOptions.class);
    options.setBaseValue(Lists.<Boolean>newArrayList());
    options.setIgnoredValue(Sets.<String>newHashSet());
    options.getIgnoredValue().add("ignoredString");
    options.setDerivedValue(0);

    BaseTestOptions clonedOptions = options.cloneAs(BaseTestOptions.class);
    assertNotSame(clonedOptions, options);
    assertNotSame(clonedOptions.getBaseValue(), options.getBaseValue());

    clonedOptions.getBaseValue().add(true);
    assertFalse(clonedOptions.getBaseValue().isEmpty());
    assertTrue(options.getBaseValue().isEmpty());

    assertNull(clonedOptions.getIgnoredValue());

    ObjectMapper mapper = new ObjectMapper();
    mapper.readValue(mapper.writeValueAsBytes(clonedOptions), PipelineOptions.class);
  }

  @Test
  public void testCloneAsConflicted() throws Exception {
    DerivedTestOptions options = PipelineOptionsFactory.create().as(DerivedTestOptions.class);
    options.setBaseValue(Lists.<Boolean>newArrayList());
    options.setIgnoredValue(Sets.<String>newHashSet());
    options.getIgnoredValue().add("ignoredString");
    options.setDerivedValue(0);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("incompatible return types");
    options.cloneAs(ConflictedTestOptions.class);
  }
}
