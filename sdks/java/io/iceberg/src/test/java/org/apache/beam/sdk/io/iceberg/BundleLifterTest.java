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
package org.apache.beam.sdk.io.iceberg;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.BundleLifter.BundleLiftDoFn;
import org.apache.beam.sdk.testing.TestOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BundleLifterTest {

  private static final TupleTag<Integer> INTEGER_SMALL = new TupleTag<Integer>() {};
  private static final TupleTag<Integer> INTEGER_LARGE = new TupleTag<Integer>() {};
  private static final TupleTag<String> STRING_SMALL = new TupleTag<String>() {};
  private static final TupleTag<String> STRING_LARGE = new TupleTag<String>() {};

  private static class SimpleMultiOutputReceiver implements MultiOutputReceiver {
    private final Map<TupleTag<?>, TestOutputReceiver<?>> receivers = new HashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public @UnknownKeyFor @NonNull @Initialized <T> OutputReceiver<T> get(
        @UnknownKeyFor @NonNull @Initialized TupleTag<T> tag) {
      return (OutputReceiver<T>) receivers.computeIfAbsent(tag, k -> new TestOutputReceiver<T>());
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized <T>
        OutputReceiver<@UnknownKeyFor @NonNull @Initialized Row> getRowReceiver(
            @UnknownKeyFor @NonNull @Initialized TupleTag<T> tag) {
      throw new UnsupportedOperationException("getRowReceiver not supported in test");
    }

    @SuppressWarnings("unchecked")
    public <T> java.util.List<T> getOutput(TupleTag<T> tag) {
      TestOutputReceiver<T> receiver = (TestOutputReceiver<T>) receivers.get(tag);
      return receiver == null ? java.util.Collections.emptyList() : receiver.getOutputs();
    }
  }

  @Test
  public void testSmallBundle() throws Exception {
    BundleLiftDoFn<Integer> fn = new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 3, x -> 1);
    SimpleMultiOutputReceiver receiver = new SimpleMultiOutputReceiver();

    fn.startBundle();
    fn.processElement(1, receiver);
    fn.processElement(2, receiver);
    fn.finishBundle();

    assertThat(receiver.getOutput(INTEGER_SMALL), containsInAnyOrder(1, 2));
    assertThat(receiver.getOutput(INTEGER_LARGE), empty());
  }

  @Test
  public void testLargeBundle() throws Exception {
    BundleLiftDoFn<Integer> fn = new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 3, x -> 1);
    SimpleMultiOutputReceiver receiver = new SimpleMultiOutputReceiver();

    fn.startBundle();
    fn.processElement(1, receiver);
    fn.processElement(2, receiver);
    fn.processElement(3, receiver);
    fn.finishBundle();

    assertThat(receiver.getOutput(INTEGER_SMALL), empty());
    assertThat(receiver.getOutput(INTEGER_LARGE), containsInAnyOrder(1, 2, 3));
  }

  @Test
  public void testSmallBundleWithSizer() throws Exception {
    BundleLiftDoFn<String> fn =
        new BundleLiftDoFn<>(STRING_SMALL, STRING_LARGE, 10, e -> e.length());
    SimpleMultiOutputReceiver receiver = new SimpleMultiOutputReceiver();

    fn.startBundle();
    fn.processElement("123", receiver);
    fn.processElement("456", receiver);
    fn.processElement("789", receiver);
    fn.finishBundle();

    assertThat(receiver.getOutput(STRING_SMALL), containsInAnyOrder("123", "456", "789"));
    assertThat(receiver.getOutput(STRING_LARGE), empty());
  }

  @Test
  public void testLargeBundleWithSizer() throws Exception {
    BundleLiftDoFn<String> fn =
        new BundleLiftDoFn<>(STRING_SMALL, STRING_LARGE, 10, e -> e.length());
    SimpleMultiOutputReceiver receiver = new SimpleMultiOutputReceiver();

    fn.startBundle();
    fn.processElement("123", receiver);
    fn.processElement("456", receiver);
    fn.processElement("789", receiver);
    fn.processElement("0", receiver);
    fn.finishBundle();

    assertThat(receiver.getOutput(STRING_SMALL), empty());
    assertThat(receiver.getOutput(STRING_LARGE), containsInAnyOrder("123", "456", "789", "0"));
  }
}
