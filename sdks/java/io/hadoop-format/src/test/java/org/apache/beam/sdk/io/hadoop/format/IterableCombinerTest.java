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
package org.apache.beam.sdk.io.hadoop.format;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests Iterable combiner whether works correctly. */
@RunWith(JUnit4.class)
public class IterableCombinerTest {

  private static final TypeDescriptor<String> STRING_TYPE_DESCRIPTOR = TypeDescriptors.strings();

  private static final List<String> FIRST_ITEMS = Arrays.asList("a", "b", "c");
  private static final List<String> SECOND_ITEMS = Arrays.asList("c", "d", "e");

  @Test
  public void testCombining() {

    IterableCombinerFn<String> tested = new IterableCombinerFn<>(STRING_TYPE_DESCRIPTOR);

    IterableCombinerFn.CollectionAccumulator<String> first = tested.createAccumulator();
    FIRST_ITEMS.forEach(first::addInput);

    IterableCombinerFn.CollectionAccumulator<String> second = tested.createAccumulator();
    SECOND_ITEMS.forEach(second::addInput);

    IterableCombinerFn.CollectionAccumulator<String> merged =
        tested.mergeAccumulators(Arrays.asList(first, second));

    IterableCombinerFn.CollectionAccumulator<String> compacted = tested.compact(merged);

    String[] allItems =
        Stream.of(FIRST_ITEMS, SECOND_ITEMS).flatMap(List::stream).toArray(String[]::new);

    MatcherAssert.assertThat(compacted.extractOutput(), Matchers.containsInAnyOrder(allItems));
  }

  @Test
  public void testSerializing() throws IOException {

    IterableCombinerFn<String> tested = new IterableCombinerFn<>(STRING_TYPE_DESCRIPTOR);
    IterableCombinerFn.CollectionAccumulator<String> originalAccumulator =
        tested.createAccumulator();

    FIRST_ITEMS.forEach(originalAccumulator::addInput);

    Coder<IterableCombinerFn.CollectionAccumulator<String>> accumulatorCoder =
        tested.getAccumulatorCoder(null, StringUtf8Coder.of());

    byte[] bytes;

    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      accumulatorCoder.encode(originalAccumulator, byteArrayOutputStream);
      byteArrayOutputStream.flush();

      bytes = byteArrayOutputStream.toByteArray();
    }

    IterableCombinerFn.CollectionAccumulator<String> decodedAccumulator;

    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
      decodedAccumulator = accumulatorCoder.decode(byteArrayInputStream);
    }

    String[] originalItems = FIRST_ITEMS.toArray(new String[0]);

    MatcherAssert.assertThat(
        originalAccumulator.extractOutput(), Matchers.containsInAnyOrder(originalItems));
    MatcherAssert.assertThat(
        decodedAccumulator.extractOutput(), Matchers.containsInAnyOrder(originalItems));
  }
}
