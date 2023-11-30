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
package org.apache.beam.fn.harness.state;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

/** Tests for {@link StateBackedIterable}. */
@RunWith(Enclosed.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class StateBackedIterableTest {

  @RunWith(Parameterized.class)
  public static class IterationTest {
    @Parameterized.Parameters
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {Collections.emptyList(), "emptySuffix", ImmutableList.of()})
          .add(new Object[] {ImmutableList.of("A", "B"), "emptySuffix", ImmutableList.of("A", "B")})
          .add(
              new Object[] {
                Collections.emptyList(),
                "nonEmptySuffix",
                ImmutableList.of("C", "D", "E", "F", "G", "H", "I", "J", "K")
              })
          .add(
              new Object[] {
                ImmutableList.of("A", "B"),
                "nonEmptySuffix",
                ImmutableList.of("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K")
              })
          .build();
    }

    @Parameterized.Parameter(0)
    public List<String> prefix;

    @Parameterized.Parameter(1)
    public String suffixKey;

    @Parameterized.Parameter(2)
    public List<String> expected;

    @Test
    public void testReiteration() throws Exception {
      FakeBeamFnStateClient fakeBeamFnStateClient =
          new FakeBeamFnStateClient(
              StringUtf8Coder.of(),
              ImmutableMap.of(
                  key("nonEmptySuffix"), asList("C", "D", "E", "F", "G", "H", "I", "J", "K"),
                  key("emptySuffix"), asList()));

      StateBackedIterable<String> iterable =
          new StateBackedIterable<>(
              Caches.noop(),
              fakeBeamFnStateClient,
              "instruction",
              key(suffixKey),
              StringUtf8Coder.of(),
              prefix);

      assertEquals(expected, Lists.newArrayList(iterable));
      assertEquals(expected, Lists.newArrayList(iterable));
      assertEquals(expected, Lists.newArrayList(iterable));
    }

    @Test
    public void testReiterationCached() throws Exception {
      FakeBeamFnStateClient fakeBeamFnStateClient =
          new FakeBeamFnStateClient(
              StringUtf8Coder.of(),
              ImmutableMap.of(
                  key("nonEmptySuffix"), asList("C", "D", "E", "F", "G", "H", "I", "J", "K"),
                  key("emptySuffix"), asList()));

      StateBackedIterable<String> iterable =
          new StateBackedIterable<>(
              Caches.eternal(),
              fakeBeamFnStateClient,
              "instruction",
              key(suffixKey),
              StringUtf8Coder.of(),
              prefix);

      // Ensure that the load is lazy
      assertEquals(0, fakeBeamFnStateClient.getCallCount());
      assertEquals(expected, Lists.newArrayList(iterable));
      // We expect future reiterations to not perform any loads
      int callCount = fakeBeamFnStateClient.getCallCount();
      assertEquals(expected, Lists.newArrayList(iterable));
      assertEquals(expected, Lists.newArrayList(iterable));
      assertEquals(callCount, fakeBeamFnStateClient.getCallCount());
    }

    @Test
    public void testCacheKeyIsUnique() throws Exception {
      // Share a cache for multiple iterables leads to distinct keys being used.
      Cache cache = Caches.eternal();
      FakeBeamFnStateClient fakeBeamFnStateClient =
          new FakeBeamFnStateClient(
              StringUtf8Coder.of(),
              ImmutableMap.of(
                  key("nonEmptySuffix"), asList("C", "D", "E", "F", "G", "H", "I", "J", "K"),
                  key("emptySuffix"), asList(),
                  key("otherIterable"), asList("Z")));

      StateBackedIterable<String> otherIterable =
          new StateBackedIterable<>(
              cache,
              fakeBeamFnStateClient,
              "instruction",
              key("otherIterable"),
              StringUtf8Coder.of(),
              Collections.emptyList());
      // Ensure that the load is lazy
      assertEquals(0, fakeBeamFnStateClient.getCallCount());
      assertEquals(asList("Z"), Lists.newArrayList(otherIterable));

      StateBackedIterable<String> iterable =
          new StateBackedIterable<>(
              cache,
              fakeBeamFnStateClient,
              "instruction",
              key(suffixKey),
              StringUtf8Coder.of(),
              prefix);

      assertEquals(expected, Lists.newArrayList(iterable));
      // We expect future reiterations to not perform any loads
      int callCount = fakeBeamFnStateClient.getCallCount();
      assertEquals(expected, Lists.newArrayList(iterable));
      assertEquals(expected, Lists.newArrayList(iterable));
      assertEquals(callCount, fakeBeamFnStateClient.getCallCount());
    }

    @Test
    public void testUsingInterleavedReiteration() throws Exception {
      FakeBeamFnStateClient fakeBeamFnStateClient =
          new FakeBeamFnStateClient(
              StringUtf8Coder.of(),
              ImmutableMap.of(
                  key("nonEmptySuffix"), asList("C", "D", "E", "F", "G", "H", "I", "J", "K"),
                  key("emptySuffix"), asList()));

      StateBackedIterable<String> iterable =
          new StateBackedIterable<>(
              Caches.noop(),
              fakeBeamFnStateClient,
              "instruction",
              key(suffixKey),
              StringUtf8Coder.of(),
              prefix);

      List<Iterator<String>> iterators = new ArrayList<>();
      List<List<String>> results = new ArrayList<>();
      for (int i = 0; i < 3; ++i) {
        iterators.add(iterable.iterator());
        results.add(new ArrayList<>());
      }

      Random random = new Random(42L);
      while (!iterators.isEmpty()) {
        int current = random.nextInt(iterators.size());
        if (!iterators.get(current).hasNext()) {
          iterators.remove(current);
          assertEquals(expected, results.remove(current));
        } else {
          results.get(current).add(iterators.get(current).next());
        }
      }
    }

    private static class TestByteObserver extends ElementByteSizeObserver {
      public long total = 0;

      @Override
      protected void reportElementSize(long elementByteSize) {
        total += elementByteSize;
      }
    };

    @Test
    public void testByteObservingStateBackedIterable() throws Exception {
      FakeBeamFnStateClient fakeBeamFnStateClient =
          new FakeBeamFnStateClient(
              StringUtf8Coder.of(),
              ImmutableMap.of(
                  key("nonEmptySuffix"), asList("C", "D", "E", "F", "G", "H", "I", "J", "K"),
                  key("emptySuffix"), asList()));

      StateBackedIterable<String> iterable =
          new StateBackedIterable<>(
              Caches.noop(),
              fakeBeamFnStateClient,
              "instruction",
              key(suffixKey),
              StringUtf8Coder.of(),
              prefix);
      StateBackedIterable.Coder<String> coder =
          new StateBackedIterable.Coder<>(
              () -> Caches.noop(),
              fakeBeamFnStateClient,
              () -> "instructionId",
              StringUtf8Coder.of());

      assertTrue(coder.isRegisterByteSizeObserverCheap(iterable));
      TestByteObserver observer = new TestByteObserver();
      coder.registerByteSizeObserver(iterable, observer);
      assertTrue(observer.getIsLazy());

      long iterateBytes =
          Streams.stream(iterable)
              .mapToLong(
                  s -> {
                    try {
                      // 1 comes from hasNext = true flag (see IterableLikeCoder)
                      return 1 + StringUtf8Coder.of().getEncodedElementByteSize(s);
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  })
              .sum();
      observer.advance();
      // 5 comes from size and hasNext (see IterableLikeCoder)
      assertEquals(iterateBytes + 5, observer.total);
    }
  }

  @RunWith(JUnit4.class)
  public static class CoderTest {
    @Test
    public void testDecodeEncodeRegularIterable() throws Exception {
      Iterable<String> iterable = FluentIterable.of("A", "B", "C");
      StateBackedIterable.Coder<String> coder =
          new StateBackedIterable.Coder<>(
              () -> Caches.noop(), null, () -> "instructionId", StringUtf8Coder.of());

      // We can't rely on CoderProperties since it requires serialization of the coder
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      coder.encode(iterable, baos);
      Iterable<String> result = coder.decode(new ByteArrayInputStream(baos.toByteArray()));

      assertEquals(Lists.newArrayList(iterable), Lists.newArrayList(result));
    }

    @Test
    public void testEncodeDecodeStateBackedIterable() throws Exception {
      StateBackedIterable<String> iterable =
          new StateBackedIterable(
              Caches.noop(),
              null,
              "instructionId",
              key("key"),
              StringUtf8Coder.of(),
              Arrays.asList("A", "B"));
      StateBackedIterable.Coder<String> coder =
          new StateBackedIterable.Coder<>(
              () -> Caches.noop(), null, () -> "instructionId", StringUtf8Coder.of());

      // We can't rely on CoderProperties since it requires serialization of the coder
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      coder.encode(iterable, baos);
      StateBackedIterable<String> result =
          (StateBackedIterable<String>) coder.decode(new ByteArrayInputStream(baos.toByteArray()));

      // Ensure that when we round trip the iterable we correctly convert it back to a state backed
      // iterable
      assertEquals(iterable.prefix, result.prefix);
      assertEquals(iterable.request, result.request);
    }

    @Test
    public void testSerializability() throws Exception {
      FakeBeamFnStateClient fakeBeamFnStateClient =
          new FakeBeamFnStateClient(
              StringUtf8Coder.of(),
              ImmutableMap.of(
                  key("suffix"), asList("C", "D", "E"),
                  key("emptySuffix"), asList()));

      StateBackedIterable<String> iterable =
          new StateBackedIterable<>(
              Caches.noop(),
              fakeBeamFnStateClient,
              "instruction",
              key("suffix"),
              StringUtf8Coder.of(),
              ImmutableList.of("A", "B"));

      List<String> expected = ImmutableList.of("A", "B", "C", "D", "E");

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bout);
      out.writeObject(iterable);
      out.flush();
      ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
      ObjectInputStream in = new ObjectInputStream(bin);
      Iterable<String> deserialized = (Iterable<String>) in.readObject();

      // Check that the contents are the same.
      assertEquals(expected, Lists.newArrayList(deserialized));
      // Check that we can still iterate over it as before.
      assertEquals(expected, Lists.newArrayList(iterable));
    }
  }

  private static StateKey key(String id) throws IOException {
    return StateKey.newBuilder().setRunner(StateKey.Runner.newBuilder().setKey(encode(id))).build();
  }

  private static ByteString encode(String... values) throws IOException {
    ByteStringOutputStream out = new ByteStringOutputStream();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }
}
