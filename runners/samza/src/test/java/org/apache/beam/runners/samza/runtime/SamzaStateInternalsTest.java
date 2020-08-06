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
package org.apache.beam.runners.samza.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.TestSamzaRunner;
import org.apache.beam.runners.samza.runtime.SamzaStateInternals.StateValue;
import org.apache.beam.runners.samza.runtime.SamzaStateInternals.StateValueSerdeFactory;
import org.apache.beam.runners.samza.state.SamzaMapState;
import org.apache.beam.runners.samza.state.SamzaSetState;
import org.apache.beam.runners.samza.translation.ConfigBuilder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.StorageEngineFactory;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.KeyValueStoreMetrics;
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory;
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStore;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

/** Tests for SamzaStoreStateInternals. */
public class SamzaStateInternalsTest implements Serializable {
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testMapStateIterator() {
    final String stateId = "foo";
    final String countStateId = "count";

    DoFn<KV<String, KV<String, Integer>>, KV<String, Integer>> fn =
        new DoFn<KV<String, KV<String, Integer>>, KV<String, Integer>>() {

          @StateId(stateId)
          private final StateSpec<MapState<String, Integer>> mapState =
              StateSpecs.map(StringUtf8Coder.of(), VarIntCoder.of());

          @StateId(countStateId)
          private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
              StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

          @ProcessElement
          public void processElement(
              ProcessContext c,
              @StateId(stateId) MapState<String, Integer> mapState,
              @StateId(countStateId) CombiningState<Integer, int[], Integer> count) {
            SamzaMapState<String, Integer> state = (SamzaMapState<String, Integer>) mapState;
            KV<String, Integer> value = c.element().getValue();
            state.put(value.getKey(), value.getValue());
            count.add(1);
            if (count.read() >= 4) {
              final List<KV<String, Integer>> content = new ArrayList<>();
              final Iterator<Map.Entry<String, Integer>> iterator = state.readIterator().read();
              while (iterator.hasNext()) {
                Map.Entry<String, Integer> entry = iterator.next();
                content.add(KV.of(entry.getKey(), entry.getValue()));
                c.output(KV.of(entry.getKey(), entry.getValue()));
              }

              assertEquals(
                  content, ImmutableList.of(KV.of("a", 97), KV.of("b", 42), KV.of("c", 12)));
            }
          }
        };

    PCollection<KV<String, Integer>> output =
        pipeline
            .apply(
                Create.of(
                    KV.of("hello", KV.of("a", 97)),
                    KV.of("hello", KV.of("b", 42)),
                    KV.of("hello", KV.of("b", 42)),
                    KV.of("hello", KV.of("c", 12))))
            .apply(ParDo.of(fn));

    PAssert.that(output).containsInAnyOrder(KV.of("a", 97), KV.of("b", 42), KV.of("c", 12));

    TestSamzaRunner.fromOptions(
            PipelineOptionsFactory.fromArgs(
                    "--runner=org.apache.beam.runners.samza.TestSamzaRunner")
                .create())
        .run(pipeline);
  }

  @Test
  public void testSetStateIterator() {
    final String stateId = "foo";
    final String countStateId = "count";

    DoFn<KV<String, Integer>, Set<Integer>> fn =
        new DoFn<KV<String, Integer>, Set<Integer>>() {

          @StateId(stateId)
          private final StateSpec<SetState<Integer>> setState = StateSpecs.set(VarIntCoder.of());

          @StateId(countStateId)
          private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
              StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

          @ProcessElement
          public void processElement(
              ProcessContext c,
              @StateId(stateId) SetState<Integer> setState,
              @StateId(countStateId) CombiningState<Integer, int[], Integer> count) {
            SamzaSetState<Integer> state = (SamzaSetState<Integer>) setState;
            ReadableState<Boolean> isEmpty = state.isEmpty();
            state.add(c.element().getValue());
            assertFalse(isEmpty.read());
            count.add(1);
            if (count.read() >= 4) {
              final Set<Integer> content = new HashSet<>();
              final Iterator<Integer> iterator = state.readIterator().read();
              while (iterator.hasNext()) {
                Integer value = iterator.next();
                content.add(value);
              }
              c.output(content);

              assertEquals(content, Sets.newHashSet(97, 42, 12));
            }
          }
        };

    PCollection<Set<Integer>> output =
        pipeline
            .apply(
                Create.of(
                    KV.of("hello", 97), KV.of("hello", 42), KV.of("hello", 42), KV.of("hello", 12)))
            .apply(ParDo.of(fn));

    PAssert.that(output).containsInAnyOrder(Sets.newHashSet(97, 42, 12));

    TestSamzaRunner.fromOptions(
            PipelineOptionsFactory.fromArgs(
                    "--runner=org.apache.beam.runners.samza.TestSamzaRunner")
                .create())
        .run(pipeline);
  }

  /** A storage engine to create test stores. */
  public static class TestStorageEngine extends InMemoryKeyValueStorageEngineFactory {

    @Override
    public KeyValueStore<byte[], byte[]> getKVStore(
        String storeName,
        File storeDir,
        MetricsRegistry registry,
        SystemStreamPartition changeLogSystemStreamPartition,
        JobContext jobContext,
        ContainerContext containerContext,
        StorageEngineFactory.StoreMode readWrite) {
      KeyValueStoreMetrics metrics = new KeyValueStoreMetrics(storeName, registry);
      return new TestStore(metrics);
    }
  }

  /** A test store based on InMemoryKeyValueStore. */
  public static class TestStore extends InMemoryKeyValueStore {
    static List<TestKeyValueIteraor> iterators = Collections.synchronizedList(new ArrayList<>());

    public TestStore(KeyValueStoreMetrics metrics) {
      super(metrics);
    }

    @Override
    public KeyValueIterator<byte[], byte[]> range(byte[] from, byte[] to) {
      TestKeyValueIteraor iter = new TestKeyValueIteraor(super.range(from, to));
      iterators.add(iter);
      return iter;
    }

    static class TestKeyValueIteraor implements KeyValueIterator<byte[], byte[]> {
      private final KeyValueIterator<byte[], byte[]> iter;
      boolean closed = false;

      TestKeyValueIteraor(KeyValueIterator<byte[], byte[]> iter) {
        this.iter = iter;
      }

      @Override
      public void close() {
        iter.close();
        closed = true;
      }

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Entry<byte[], byte[]> next() {
        return iter.next();
      }
    }
  }

  @Test
  public void testIteratorClosed() {
    final String stateId = "foo";

    DoFn<KV<String, Integer>, Set<Integer>> fn =
        new DoFn<KV<String, Integer>, Set<Integer>>() {

          @StateId(stateId)
          private final StateSpec<SetState<Integer>> setState = StateSpecs.set(VarIntCoder.of());

          @ProcessElement
          public void processElement(
              ProcessContext c, @StateId(stateId) SetState<Integer> setState) {
            SamzaSetState<Integer> state = (SamzaSetState<Integer>) setState;
            state.add(c.element().getValue());

            // the iterator for size needs to be closed
            int size = Iterators.size(state.readIterator().read());

            if (size > 1) {
              final Iterator<Integer> iterator = state.readIterator().read();
              assertTrue(iterator.hasNext());
              // this iterator should be closed too
              iterator.next();
            }
          }
        };

    pipeline
        .apply(
            Create.of(
                KV.of("hello", 97), KV.of("hello", 42), KV.of("hello", 42), KV.of("hello", 12)))
        .apply(ParDo.of(fn));

    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setRunner(TestSamzaRunner.class);
    Map<String, String> configs = new HashMap<>(ConfigBuilder.localRunConfig());
    configs.put("stores.foo.factory", TestStorageEngine.class.getName());
    options.setConfigOverride(configs);

    TestSamzaRunner.fromOptions(options).run(pipeline).waitUntilFinish();

    // The test code creates 7 underlying iterators, and 1 more is created during state.clear()
    // Verify all of them are closed
    assertEquals(8, TestStore.iterators.size());
    TestStore.iterators.forEach(iter -> assertTrue(iter.closed));
  }

  @Test
  public void testStateValueSerde() throws IOException {
    StateValueSerdeFactory stateValueSerdeFactory = new StateValueSerdeFactory();
    Serde<StateValue<Integer>> serde = (Serde) stateValueSerdeFactory.getSerde("Test", null);
    int value = 123;
    Coder<Integer> coder = VarIntCoder.of();

    byte[] valueBytes = serde.toBytes(StateValue.of(value, coder));
    StateValue<Integer> stateValue1 = serde.fromBytes(valueBytes);
    StateValue<Integer> stateValue2 = StateValue.of(valueBytes);
    assertEquals(stateValue1.getValue(coder).intValue(), value);
    assertEquals(stateValue2.getValue(coder).intValue(), value);

    Integer nullValue = null;
    byte[] nullBytes = serde.toBytes(StateValue.of(nullValue, coder));
    StateValue<Integer> nullStateValue = serde.fromBytes(nullBytes);
    assertNull(nullBytes);
    assertNull(nullStateValue.getValue(coder));
  }
}
