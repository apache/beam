/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.jstorm.translation.runtime.state;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.alibaba.jstorm.cache.IKvStoreManager;
import com.alibaba.jstorm.cache.rocksdb.RocksDbKvStoreManagerFactory;
import com.alibaba.jstorm.utils.KryoSerializer;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.jstorm.translation.runtime.TimerServiceImpl;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link JStormStateInternals}.
 */
@RunWith(JUnit4.class)
public class JStormStateInternalsTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  private JStormStateInternals<String> jstormStateInternals;

  @Before
  public void setup() throws Exception {
    IKvStoreManager kvStoreManager = RocksDbKvStoreManagerFactory.getManager(
        Maps.newHashMap(),
        "test",
        tmp.toString(),
        new KryoSerializer(Maps.newHashMap()));
    jstormStateInternals = new JStormStateInternals(
        "key-1", kvStoreManager, new TimerServiceImpl(), 0);
  }

  @Test
  public void testValueState() throws Exception {
    ValueState<Integer> valueState = jstormStateInternals.state(
        StateNamespaces.global(), StateTags.value("state-id-a", BigEndianIntegerCoder.of()));
    valueState.write(Integer.MIN_VALUE);
    assertEquals(Integer.MIN_VALUE, valueState.read().longValue());
    valueState.write(Integer.MAX_VALUE);
    assertEquals(Integer.MAX_VALUE, valueState.read().longValue());
  }

  @Test
  public void testValueStateIdenticalId() throws Exception {
    ValueState<Integer> valueState = jstormStateInternals.state(
        StateNamespaces.global(), StateTags.value("state-id-a", BigEndianIntegerCoder.of()));
    ValueState<Integer> valueStateIdentical = jstormStateInternals.state(
        StateNamespaces.global(), StateTags.value("state-id-a", BigEndianIntegerCoder.of()));

    valueState.write(Integer.MIN_VALUE);
    assertEquals(Integer.MIN_VALUE, valueState.read().longValue());
    assertEquals(Integer.MIN_VALUE, valueStateIdentical.read().longValue());
    valueState.write(Integer.MAX_VALUE);
    assertEquals(Integer.MAX_VALUE, valueState.read().longValue());
    assertEquals(Integer.MAX_VALUE, valueStateIdentical.read().longValue());
  }

  @Test
  public void testBagState() throws Exception {
    BagState<Integer> bagStateA = jstormStateInternals.state(
        StateNamespaces.global(), StateTags.bag("state-id-a", BigEndianIntegerCoder.of()));
    BagState<Integer> bagStateB = jstormStateInternals.state(
        StateNamespaces.global(), StateTags.bag("state-id-b", BigEndianIntegerCoder.of()));

    bagStateA.add(1);
    bagStateA.add(0);
    bagStateA.add(Integer.MAX_VALUE);

    bagStateB.add(0);
    bagStateB.add(Integer.MIN_VALUE);

    Iterable<Integer> bagA = bagStateA.read();
    Iterable<Integer> bagB = bagStateB.read();
    assertThat(bagA, containsInAnyOrder(1, 0, Integer.MAX_VALUE));
    assertThat(bagB, containsInAnyOrder(0, Integer.MIN_VALUE));

    bagStateA.clear();
    bagStateA.add(1);
    bagStateB.add(0);
    assertThat(bagStateA.read(), containsInAnyOrder(1));
    assertThat(bagStateB.read(), containsInAnyOrder(0, 0, Integer.MIN_VALUE));
  }

  @Test
  public void testCombiningState() throws Exception {
    Combine.CombineFn<Integer, int[], Integer> combineFn = Max.ofIntegers();
    Coder<int[]> accumCoder = combineFn.getAccumulatorCoder(
        CoderRegistry.createDefault(), BigEndianIntegerCoder.of());

    CombiningState<Integer, int[], Integer> combiningState = jstormStateInternals.state(
        StateNamespaces.global(),
        StateTags.combiningValue(
            "state-id-a",
            accumCoder,
            combineFn));
    assertEquals(Integer.MIN_VALUE, combiningState.read().longValue());
    combiningState.add(10);
    assertEquals(10, combiningState.read().longValue());
    combiningState.add(1);
    assertEquals(10, combiningState.read().longValue());
    combiningState.add(Integer.MAX_VALUE);
    assertEquals(Integer.MAX_VALUE, combiningState.read().longValue());
  }

  @Test
  public void testWatermarkHoldState() throws Exception {
    WatermarkHoldState watermarkHoldState = jstormStateInternals.state(
        StateNamespaces.global(),
        StateTags.watermarkStateInternal(
            "state-id-a",
            TimestampCombiner.EARLIEST));
    watermarkHoldState.add(new Instant(1));
    assertEquals(1, watermarkHoldState.read().getMillis());
    watermarkHoldState.add(new Instant(Integer.MIN_VALUE));
    assertEquals(Integer.MIN_VALUE, watermarkHoldState.read().getMillis());
    watermarkHoldState.add(new Instant(Integer.MAX_VALUE));
    assertEquals(Integer.MIN_VALUE, watermarkHoldState.read().getMillis());
  }

  @Test
  public void testMapState() throws Exception {
    MapState<Integer, Integer> mapStateA = jstormStateInternals.state(
        StateNamespaces.global(),
        StateTags.map("state-id-a", BigEndianIntegerCoder.of(), BigEndianIntegerCoder.of()));
    mapStateA.put(1, 1);
    mapStateA.put(2, 22);
    mapStateA.put(1, 12);

    Iterable<Integer> keys = mapStateA.keys().read();
    Iterable<Integer> values = mapStateA.values().read();
    assertThat(keys, containsInAnyOrder(1, 2));
    assertThat(values, containsInAnyOrder(12, 22));

    Iterable<Map.Entry<Integer, Integer>> entries = mapStateA.entries().read();
    Iterator<Map.Entry<Integer, Integer>> itr = entries.iterator();
    Map.Entry<Integer, Integer> entry = itr.next();
    assertEquals((long) entry.getKey(), 1L);
    assertEquals((long) entry.getValue(), 12L);
    entry = itr.next();
    assertEquals((long) entry.getKey(), 2L);
    assertEquals((long) entry.getValue(), 22L);
    assertEquals(false, itr.hasNext());

    mapStateA.remove(1);
    keys = mapStateA.keys().read();
    values = mapStateA.values().read();
    assertThat(keys, containsInAnyOrder(2));
    assertThat(values, containsInAnyOrder(22));

    entries = mapStateA.entries().read();
    itr = entries.iterator();
    entry = itr.next();
    assertEquals((long) entry.getKey(), 2L);
    assertEquals((long) entry.getValue(), 22L);
    assertEquals(false, itr.hasNext());
  }

  @Test
  public void testMassiveDataOfBagState() {
    BagState<Integer> bagStateA = jstormStateInternals.state(
        StateNamespaces.global(), StateTags.bag("state-id-a", BigEndianIntegerCoder.of()));

    int count = 10000;
    int n = 1;
    while (n <= count) {
      bagStateA.add(n);
      n++;
    }

    int readCount = 0;
    int readN = 0;
    Iterator<Integer> itr = bagStateA.read().iterator();
    while (itr.hasNext()) {
      readN += itr.next();
      readCount++;
    }

    assertEquals((long) readN, ((1 + count) * count) / 2);
    assertEquals((long) readCount, count);
  }
}
