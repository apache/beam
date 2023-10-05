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
package org.apache.beam.runners.dataflow.worker.profiler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.ProfileScope;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.ProfilerWrapper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ScopedProfiler}. */
@RunWith(JUnit4.class)
public class ScopedProfilerTest {

  @Test
  public void testProfilerPackage() throws Exception {
    assertEquals(
        "The Profiler class must stay in this package since the implementation is via JNI",
        // Construct the string so IDEs don't "fix it"
        Joiner.on(".").join("org", "apache", "beam", "runners", "dataflow", "worker", "profiler"),
        Profiler.class.getPackage().getName());
  }

  @Test
  public void testNoProfiler() throws Exception {
    MockProfiler profiler = new MockProfiler(false);
    ScopedProfiler underTest = new ScopedProfiler(profiler);
    ProfileScope original = underTest.currentScope();
    underTest.registerScope("attribute").activate();
    original.activate();

    assertThat(profiler.interactions, Matchers.equalTo(1));
  }

  @Test
  public void testProfilerOneBlock() throws Exception {
    MockProfiler profiler = new MockProfiler(true);
    ScopedProfiler underTest = new ScopedProfiler(profiler);

    ProfileScope original = underTest.currentScope();
    underTest.registerScope("attribute").activate();
    assertThat(
        profiler.currentState, Matchers.equalTo(profiler.registeredAttributes.get("attribute")));
    original.activate();

    assertThat(profiler.currentState, Matchers.equalTo(0));
  }

  @Test
  public void testProfilerNestedBlocks() throws Exception {
    MockProfiler profiler = new MockProfiler(true);
    ScopedProfiler underTest = new ScopedProfiler(profiler);
    assertThat(profiler.interactions, Matchers.equalTo(1));

    ProfileScope original = underTest.currentScope();
    ProfileScope block1 = underTest.registerScope("attribute1");
    ProfileScope block2 = underTest.registerScope("attribute2");

    block1.activate();
    assertThat(
        profiler.currentState, Matchers.equalTo(profiler.registeredAttributes.get("attribute1")));
    block2.activate();
    assertThat(
        profiler.currentState, Matchers.equalTo(profiler.registeredAttributes.get("attribute2")));
    block1.activate();
    assertThat(
        profiler.currentState, Matchers.equalTo(profiler.registeredAttributes.get("attribute1")));
    original.activate();
    assertThat(profiler.currentState, Matchers.equalTo(0));
  }

  private static class MockProfiler extends ProfilerWrapper {
    private int interactions = 0;
    private final boolean isProfilerPresent;
    private int currentState;
    private final HashMap<String, Integer> registeredAttributes = new HashMap<>();

    private MockProfiler(boolean isProfilerPresent) {
      this.isProfilerPresent = isProfilerPresent;
      this.currentState = 0;
    }

    private void interact() {
      interactions++;
      if (!isProfilerPresent) {
        throw new UnsatisfiedLinkError("Profiler is not present");
      }
    }

    @Override
    public int getAttribute() {
      interact();
      return currentState;
    }

    @Override
    public int setAttribute(int value) {
      interact();

      // Check that value could have been assigned by this instance.
      assertThat(
          "Should only setAttribute with values assigned by this instance",
          value,
          Matchers.allOf(
              Matchers.greaterThanOrEqualTo(0),
              Matchers.lessThanOrEqualTo(registeredAttributes.size())));
      int oldState = currentState;
      currentState = value;
      return oldState;
    }

    @Override
    public int registerAttribute(String value) {
      interact();

      Integer attribute = registeredAttributes.get(value);
      if (attribute == null) {
        attribute = registeredAttributes.size() + 1;
        registeredAttributes.put(value, attribute);
      }
      return attribute;
    }
  }
}
