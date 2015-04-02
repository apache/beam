/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Set;

/** Unit tests for {@link PipelineOptions}. */
@RunWith(JUnit4.class)
public class PipelineOptionsTest {
  /** Interface used for testing that {@link PipelineOptions#as(Class)} functions. */
  public static interface TestOptions extends PipelineOptions {
    List<Boolean> getTestValue();
    void setTestValue(List<Boolean> testValue);

    @JsonIgnore
    Set<String> getIgnoredValue();
    void setIgnoredValue(Set<String> ignoredValue);
  }

  @Test
  public void testDynamicAs() {
    TestOptions options = PipelineOptionsFactory.create().as(TestOptions.class);
    assertNotNull(options);
  }

  @Test
  public void testDefaultRunnerIsSet() {
    assertEquals(DirectPipelineRunner.class, PipelineOptionsFactory.create().getRunner());
  }

  @Test
  public void testCloneAs() {
    TestOptions options = PipelineOptionsFactory.create().as(TestOptions.class);
    options.setTestValue(Lists.<Boolean>newArrayList());
    options.setIgnoredValue(Sets.<String>newHashSet());
    options.getIgnoredValue().add("ignoredString");

    TestOptions clonedOptions = options.cloneAs(TestOptions.class);
    assertNotSame(clonedOptions, options);
    assertNotSame(clonedOptions.getTestValue(), options.getTestValue());

    clonedOptions.getTestValue().add(true);
    assertFalse(clonedOptions.getTestValue().isEmpty());
    assertTrue(options.getTestValue().isEmpty());

    assertNull(clonedOptions.getIgnoredValue());
  }
}
