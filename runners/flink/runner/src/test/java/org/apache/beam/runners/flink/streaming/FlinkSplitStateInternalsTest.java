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
package org.apache.beam.runners.flink.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkSplitStateInternals;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FlinkSplitStateInternals}. This is based on the tests for
 * {@code InMemoryStateInternals}.
 */
@RunWith(JUnit4.class)
public class FlinkSplitStateInternalsTest {
  private static final StateNamespace NAMESPACE_1 = new StateNamespaceForTest("ns1");
  private static final StateNamespace NAMESPACE_2 = new StateNamespaceForTest("ns2");

  private static final StateTag<Object, BagState<String>> STRING_BAG_ADDR =
      StateTags.bag("stringBag", StringUtf8Coder.of());

  FlinkSplitStateInternals<String> underTest;

  @Before
  public void initStateInternals() {
    MemoryStateBackend backend = new MemoryStateBackend();
    try {
      OperatorStateBackend operatorStateBackend =
          backend.createOperatorStateBackend(new DummyEnvironment("test", 1, 0), "");
      underTest = new FlinkSplitStateInternals<>(operatorStateBackend);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testBag() throws Exception {
    BagState<String> value = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);

    assertEquals(value, underTest.state(NAMESPACE_1, STRING_BAG_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, STRING_BAG_ADDR)));

    assertThat(value.read(), Matchers.emptyIterable());
    value.add("hello");
    assertThat(value.read(), Matchers.containsInAnyOrder("hello"));

    value.add("world");
    assertThat(value.read(), Matchers.containsInAnyOrder("hello", "world"));

    value.clear();
    assertThat(value.read(), Matchers.emptyIterable());
    assertEquals(underTest.state(NAMESPACE_1, STRING_BAG_ADDR), value);

  }

  @Test
  public void testBagIsEmpty() throws Exception {
    BagState<String> value = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    ReadableState<Boolean> readFuture = value.isEmpty();
    value.add("hello");
    assertThat(readFuture.read(), Matchers.is(false));

    value.clear();
    assertThat(readFuture.read(), Matchers.is(true));
  }

}
