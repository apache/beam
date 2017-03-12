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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.beam.runners.core.StateMerging;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkKeyGroupStateInternals;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FlinkKeyGroupStateInternals}. This is based on the tests for
 * {@code InMemoryStateInternals}.
 */
@RunWith(JUnit4.class)
public class FlinkKeyGroupStateInternalsTest {
  private static final StateNamespace NAMESPACE_1 = new StateNamespaceForTest("ns1");
  private static final StateNamespace NAMESPACE_2 = new StateNamespaceForTest("ns2");
  private static final StateNamespace NAMESPACE_3 = new StateNamespaceForTest("ns3");

  private static final StateTag<Object, BagState<String>> STRING_BAG_ADDR =
      StateTags.bag("stringBag", StringUtf8Coder.of());

  FlinkKeyGroupStateInternals<String> underTest;
  private KeyedStateBackend keyedStateBackend;

  @Before
  public void initStateInternals() {
    try {
      keyedStateBackend = getKeyedStateBackend(2, new KeyGroupRange(0, 1));
      underTest = new FlinkKeyGroupStateInternals<>(StringUtf8Coder.of(), keyedStateBackend);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private KeyedStateBackend getKeyedStateBackend(int numberOfKeyGroups,
                                                   KeyGroupRange keyGroupRange) {
    MemoryStateBackend backend = new MemoryStateBackend();
    try {
      AbstractKeyedStateBackend<ByteBuffer> keyedStateBackend = backend.createKeyedStateBackend(
          new DummyEnvironment("test", 1, 0),
          new JobID(),
          "test_op",
          new GenericTypeInfo<>(ByteBuffer.class).createSerializer(new ExecutionConfig()),
          numberOfKeyGroups,
          keyGroupRange,
          new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()));
      keyedStateBackend.setCurrentKey(ByteBuffer.wrap(
          CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "1")));
      return keyedStateBackend;
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

  @Test
  public void testMergeBagIntoSource() throws Exception {
    BagState<String> bag1 = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);
    BagState<String> bag2 = underTest.state(NAMESPACE_2, STRING_BAG_ADDR);

    bag1.add("Hello");
    bag2.add("World");
    bag1.add("!");

    StateMerging.mergeBags(Arrays.asList(bag1, bag2), bag1);

    // Reading the merged bag gets both the contents
    assertThat(bag1.read(), Matchers.containsInAnyOrder("Hello", "World", "!"));
    assertThat(bag2.read(), Matchers.emptyIterable());
  }

  @Test
  public void testMergeBagIntoNewNamespace() throws Exception {
    BagState<String> bag1 = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);
    BagState<String> bag2 = underTest.state(NAMESPACE_2, STRING_BAG_ADDR);
    BagState<String> bag3 = underTest.state(NAMESPACE_3, STRING_BAG_ADDR);

    bag1.add("Hello");
    bag2.add("World");
    bag1.add("!");

    StateMerging.mergeBags(Arrays.asList(bag1, bag2, bag3), bag3);

    // Reading the merged bag gets both the contents
    assertThat(bag3.read(), Matchers.containsInAnyOrder("Hello", "World", "!"));
    assertThat(bag1.read(), Matchers.emptyIterable());
    assertThat(bag2.read(), Matchers.emptyIterable());
  }

  @Test
  public void testKeyGroupAndCheckpoint() throws Exception {
    // assign to keyGroup 0
    ByteBuffer key0 = ByteBuffer.wrap(
        CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "11111111"));
    // assign to keyGroup 1
    ByteBuffer key1 = ByteBuffer.wrap(
        CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "22222222"));
    FlinkKeyGroupStateInternals<String> allState;
    {
      KeyedStateBackend keyedStateBackend = getKeyedStateBackend(2, new KeyGroupRange(0, 1));
      allState = new FlinkKeyGroupStateInternals<>(
          StringUtf8Coder.of(), keyedStateBackend);
      BagState<String> valueForNamespace0 = allState.state(NAMESPACE_1, STRING_BAG_ADDR);
      BagState<String> valueForNamespace1 = allState.state(NAMESPACE_2, STRING_BAG_ADDR);
      keyedStateBackend.setCurrentKey(key0);
      valueForNamespace0.add("0");
      valueForNamespace1.add("2");
      keyedStateBackend.setCurrentKey(key1);
      valueForNamespace0.add("1");
      valueForNamespace1.add("3");
      assertThat(valueForNamespace0.read(), Matchers.containsInAnyOrder("0", "1"));
      assertThat(valueForNamespace1.read(), Matchers.containsInAnyOrder("2", "3"));
    }

    ClassLoader classLoader = FlinkKeyGroupStateInternalsTest.class.getClassLoader();

    // 1. scale up
    ByteArrayOutputStream out0 = new ByteArrayOutputStream();
    allState.snapshotKeyGroupState(0, new DataOutputStream(out0));
    DataInputStream in0 = new DataInputStream(
        new ByteArrayInputStream(out0.toByteArray()));
    {
      KeyedStateBackend keyedStateBackend = getKeyedStateBackend(2, new KeyGroupRange(0, 0));
      FlinkKeyGroupStateInternals<String> state0 =
          new FlinkKeyGroupStateInternals<>(
              StringUtf8Coder.of(), keyedStateBackend);
      state0.restoreKeyGroupState(0, in0, classLoader);
      BagState<String> valueForNamespace0 = state0.state(NAMESPACE_1, STRING_BAG_ADDR);
      BagState<String> valueForNamespace1 = state0.state(NAMESPACE_2, STRING_BAG_ADDR);
      assertThat(valueForNamespace0.read(), Matchers.containsInAnyOrder("0"));
      assertThat(valueForNamespace1.read(), Matchers.containsInAnyOrder("2"));
    }

    ByteArrayOutputStream out1 = new ByteArrayOutputStream();
    allState.snapshotKeyGroupState(1, new DataOutputStream(out1));
    DataInputStream in1 = new DataInputStream(
        new ByteArrayInputStream(out1.toByteArray()));
    {
      KeyedStateBackend keyedStateBackend = getKeyedStateBackend(2, new KeyGroupRange(1, 1));
      FlinkKeyGroupStateInternals<String> state1 =
          new FlinkKeyGroupStateInternals<>(
              StringUtf8Coder.of(), keyedStateBackend);
      state1.restoreKeyGroupState(1, in1, classLoader);
      BagState<String> valueForNamespace0 = state1.state(NAMESPACE_1, STRING_BAG_ADDR);
      BagState<String> valueForNamespace1 = state1.state(NAMESPACE_2, STRING_BAG_ADDR);
      assertThat(valueForNamespace0.read(), Matchers.containsInAnyOrder("1"));
      assertThat(valueForNamespace1.read(), Matchers.containsInAnyOrder("3"));
    }

    // 2. scale down
    {
      KeyedStateBackend keyedStateBackend = getKeyedStateBackend(2, new KeyGroupRange(0, 1));
      FlinkKeyGroupStateInternals<String> newAllState = new FlinkKeyGroupStateInternals<>(
          StringUtf8Coder.of(), keyedStateBackend);
      in0.reset();
      in1.reset();
      newAllState.restoreKeyGroupState(0, in0, classLoader);
      newAllState.restoreKeyGroupState(1, in1, classLoader);
      BagState<String> valueForNamespace0 = newAllState.state(NAMESPACE_1, STRING_BAG_ADDR);
      BagState<String> valueForNamespace1 = newAllState.state(NAMESPACE_2, STRING_BAG_ADDR);
      assertThat(valueForNamespace0.read(), Matchers.containsInAnyOrder("0", "1"));
      assertThat(valueForNamespace1.read(), Matchers.containsInAnyOrder("2", "3"));
    }

  }

  private static class TestKeyContext implements KeyContext {

    private Object key;

    @Override
    public void setCurrentKey(Object key) {
      this.key = key;
    }

    @Override
    public Object getCurrentKey() {
      return key;
    }
  }

}
