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
package org.apache.beam.runners.core;

import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.state.State;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Suite;

/**
 * Tests for {@link InMemoryStateInternals}. This is based on {@link StateInternalsTest}.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    InMemoryStateInternalsTest.StandardStateInternalsTests.class,
    InMemoryStateInternalsTest.OtherTests.class
})
public class InMemoryStateInternalsTest {

  /**
   * A standard StateInternals test.
   */
  @RunWith(JUnit4.class)
  public static class StandardStateInternalsTests extends StateInternalsTest {
    @Override
    protected StateInternals createStateInternals() {
      return new InMemoryStateInternals<>("dummyKey");
    }
  }

  /**
   * A specific test of InMemoryStateInternals.
   */
  @RunWith(JUnit4.class)
  public static class OtherTests {

    StateInternals underTest = new InMemoryStateInternals<>("dummyKey");

    @Test
    public void testSameInstance() {
      assertSameInstance(StateInternalsTest.STRING_VALUE_ADDR);
      assertSameInstance(StateInternalsTest.SUM_INTEGER_ADDR);
      assertSameInstance(StateInternalsTest.STRING_BAG_ADDR);
      assertSameInstance(StateInternalsTest.STRING_SET_ADDR);
      assertSameInstance(StateInternalsTest.STRING_MAP_ADDR);
      assertSameInstance(StateInternalsTest.WATERMARK_EARLIEST_ADDR);
    }

    private <T extends State> void assertSameInstance(StateTag<T> address) {
      assertThat(underTest.state(StateInternalsTest.NAMESPACE_1, address),
          Matchers.sameInstance(underTest.state(StateInternalsTest.NAMESPACE_1, address)));
    }
  }

}
