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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

/** Tests for {@link StateBackedIterable}. */
@RunWith(Enclosed.class)
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
              ImmutableMap.of(
                  key("nonEmptySuffix"), encode("C", "D", "E", "F", "G", "H", "I", "J", "K"),
                  key("emptySuffix"), encode()));

      StateBackedIterable<String> iterable =
          new StateBackedIterable<>(
              fakeBeamFnStateClient,
              "instruction",
              encode(suffixKey),
              StringUtf8Coder.of(),
              prefix);

      assertEquals(expected, Lists.newArrayList(iterable));
      assertEquals(expected, Lists.newArrayList(iterable));
      assertEquals(expected, Lists.newArrayList(iterable));
    }

    @Test
    public void testUsingInterleavedReiteration() throws Exception {
      FakeBeamFnStateClient fakeBeamFnStateClient =
          new FakeBeamFnStateClient(
              ImmutableMap.of(
                  key("nonEmptySuffix"), encode("C", "D", "E", "F", "G", "H", "I", "J", "K"),
                  key("emptySuffix"), encode()));

      StateBackedIterable<String> iterable =
          new StateBackedIterable<>(
              fakeBeamFnStateClient,
              "instruction",
              encode(suffixKey),
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
  }

  @RunWith(JUnit4.class)
  public static class CoderTest {
    @Test
    public void testDecodeEncodeRegularIterable() throws Exception {
      Iterable<String> iterable = FluentIterable.of("A", "B", "C");
      StateBackedIterable.Coder<String> coder =
          new StateBackedIterable.Coder<>(null, () -> "instructionId", StringUtf8Coder.of());

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
              null, "instructionId", encode("key"), StringUtf8Coder.of(), Arrays.asList("A", "B"));
      StateBackedIterable.Coder<String> coder =
          new StateBackedIterable.Coder<>(null, () -> "instructionId", StringUtf8Coder.of());

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
  }

  private static StateKey key(String id) throws IOException {
    return StateKey.newBuilder().setRunner(StateKey.Runner.newBuilder().setKey(encode(id))).build();
  }

  private static ByteString encode(String... values) throws IOException {
    ByteString.Output out = ByteString.newOutput();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }
}
