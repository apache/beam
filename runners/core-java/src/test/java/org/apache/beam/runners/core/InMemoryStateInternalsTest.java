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

import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link InMemoryStateInternals}. This is based on {@link StateInternalsTest}. */
public class InMemoryStateInternalsTest {

  /** A standard StateInternals test. */
  @RunWith(JUnit4.class)
  public static class StandardStateInternalsTests extends StateInternalsTest {
    @Override
    protected StateInternals createStateInternals() {
      return new InMemoryStateInternals<>("dummyKey");
    }
  }

  /** A specific test of InMemoryStateInternals. */
  @RunWith(JUnit4.class)
  public static class OtherTests {

    private static final StateNamespace NAMESPACE = new StateNamespaceForTest("ns");

    private static final StateTag<ValueState<String>> STRING_VALUE_ADDR =
        StateTags.value("stringValue", StringUtf8Coder.of());
    private static final StateTag<CombiningState<Integer, int[], Integer>> SUM_INTEGER_ADDR =
        StateTags.combiningValueFromInputInternal("sumInteger", VarIntCoder.of(), Sum.ofIntegers());
    private static final StateTag<BagState<String>> STRING_BAG_ADDR =
        StateTags.bag("stringBag", StringUtf8Coder.of());
    private static final StateTag<SetState<String>> STRING_SET_ADDR =
        StateTags.set("stringSet", StringUtf8Coder.of());
    private static final StateTag<MapState<String, Integer>> STRING_MAP_ADDR =
        StateTags.map("stringMap", StringUtf8Coder.of(), VarIntCoder.of());
    private static final StateTag<WatermarkHoldState> WATERMARK_EARLIEST_ADDR =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);

    StateInternals underTest = new InMemoryStateInternals<>("dummyKey");

    @Test
    public void testSameInstance() {
      assertSameInstance(STRING_VALUE_ADDR);
      assertSameInstance(SUM_INTEGER_ADDR);
      assertSameInstance(STRING_BAG_ADDR);
      assertSameInstance(STRING_SET_ADDR);
      assertSameInstance(STRING_MAP_ADDR);
      assertSameInstance(WATERMARK_EARLIEST_ADDR);
    }

    private <T extends State> void assertSameInstance(StateTag<T> address) {
      assertThat(
          underTest.state(NAMESPACE, address),
          Matchers.sameInstance(underTest.state(NAMESPACE, address)));
    }
  }
}
