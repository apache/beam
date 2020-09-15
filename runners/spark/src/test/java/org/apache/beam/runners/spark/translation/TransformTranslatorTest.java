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
package org.apache.beam.runners.spark.translation;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.joda.time.Instant;
import org.junit.Test;
import scala.Tuple2;

/** Test suite for {@link TransformTranslator}. */
public class TransformTranslatorTest {

  @Test
  public void testIteratorFlatten() {
    List<Integer> first = Arrays.asList(1, 2, 3);
    List<Integer> second = Arrays.asList(4, 5, 6);
    List<Integer> result = new ArrayList<>();
    Iterators.addAll(
        result,
        TransformTranslator.flatten(Arrays.asList(first.iterator(), second.iterator()).iterator()));
    assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), result);
  }

  @Test
  public void testSplitBySameKey() {
    VarIntCoder coder = VarIntCoder.of();
    WindowedValue.WindowedValueCoder<Integer> wvCoder =
        WindowedValue.FullWindowedValueCoder.of(coder, GlobalWindow.Coder.INSTANCE);
    Instant now = Instant.now();
    List<GlobalWindow> window = Arrays.asList(GlobalWindow.INSTANCE);
    PaneInfo paneInfo = PaneInfo.NO_FIRING;
    List<Tuple2<ByteArray, byte[]>> firstKey =
        Arrays.asList(
            new Tuple2(
                new ByteArray(CoderHelpers.toByteArrayWithTs(1, coder, now)),
                CoderHelpers.toByteArray(WindowedValue.of(1, now, window, paneInfo), wvCoder)),
            new Tuple2(
                new ByteArray(CoderHelpers.toByteArrayWithTs(1, coder, now.plus(1))),
                CoderHelpers.toByteArray(
                    WindowedValue.of(2, now.plus(1), window, paneInfo), wvCoder)));

    List<Tuple2<ByteArray, byte[]>> secondKey =
        Arrays.asList(
            new Tuple2(
                new ByteArray(CoderHelpers.toByteArrayWithTs(2, coder, now)),
                CoderHelpers.toByteArray(WindowedValue.of(3, now, window, paneInfo), wvCoder)),
            new Tuple2(
                new ByteArray(CoderHelpers.toByteArrayWithTs(2, coder, now.plus(2))),
                CoderHelpers.toByteArray(
                    WindowedValue.of(4, now.plus(2), window, paneInfo), wvCoder)));

    Iterable<Tuple2<ByteArray, byte[]>> concat = Iterables.concat(firstKey, secondKey);
    Iterator<Iterator<WindowedValue<KV<Integer, Integer>>>> keySplit;
    keySplit = TransformTranslator.splitBySameKey(concat.iterator(), coder, wvCoder);

    for (int i = 0; i < 2; i++) {
      Iterator<WindowedValue<KV<Integer, Integer>>> iter = keySplit.next();
      List<WindowedValue<KV<Integer, Integer>>> list = new ArrayList<>();
      Iterators.addAll(list, iter);
      if (i == 0) {
        // first key
        assertEquals(
            Arrays.asList(
                WindowedValue.of(KV.of(1, 1), now, window, paneInfo),
                WindowedValue.of(KV.of(1, 2), now.plus(1), window, paneInfo)),
            list);
      } else {
        // second key
        assertEquals(
            Arrays.asList(
                WindowedValue.of(KV.of(2, 3), now, window, paneInfo),
                WindowedValue.of(KV.of(2, 4), now.plus(2), window, paneInfo)),
            list);
      }
    }
  }
}
