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
package org.apache.beam.sdk.extensions.sketching.frequency;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.extensions.sketching.frequency.KMostFrequent.StreamSummaryCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for {@link KMostFrequent}.
 */
public class KMostFrequentTest {

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  private List<Integer> smallStream = Arrays.asList(
          1,
          2, 2,
          3, 3, 3,
          4, 4, 4, 4,
          5, 5, 5, 5, 5,
          6, 6, 6, 6, 6, 6,
          7, 7, 7, 7, 7, 7, 7,
          8, 8, 8, 8, 8, 8, 8, 8,
          9, 9, 9, 9, 9, 9, 9, 9, 9,
          10, 10, 10, 10, 10, 10, 10, 10, 10, 10);

  @Test
  public void smallStream() {
    Collections.shuffle(smallStream, new Random(1234));
    PCollection<Integer> col = tp.apply(Create.of(smallStream))
            .apply(KMostFrequent.<Integer>globally(8))
            .apply("For print Big Top", ParDo.of(new OutputTopK<Integer>(3)))
              .setCoder(BigEndianIntegerCoder.of());
    PAssert.that(col).containsInAnyOrder(10, 9, 8);
    tp.run();
  }

  @Test
  public void bigStream() {
    List<Integer> bigStream = new ArrayList<>();
    // 1000 * 1, 2000 * 2, 3000 * 3, etc
    for (int i = 1; i < 11; i++) {
      bigStream.addAll(Collections.nCopies(i * 1000, i));
    }
    Collections.shuffle(bigStream, new Random(1234));
    PCollection<Integer> col = tp.apply(Create.of(bigStream))
            .apply(KMostFrequent.<Integer>globally(8))
            .apply("For print Big Top", ParDo.of(new OutputTopK<Integer>(5)))
            .setCoder(BigEndianIntegerCoder.of());
    PAssert.that(col).containsInAnyOrder(10, 9, 8, 7, 6);
    tp.run();
  }

  @Test
  public void testCoder() throws Exception {
    StreamSummary<Integer> ssSketch = new StreamSummary<>(5);
    for (Integer i : smallStream) {
      ssSketch.offer(i);
    }
    Assert.assertTrue(encodeDecode(ssSketch));
  }

  private <T> boolean encodeDecode(StreamSummary<T> ss) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StreamSummaryCoder<T> ssCoder = new StreamSummaryCoder<>();

    ssCoder.encode(ss, baos);
    byte[] bytes = baos.toByteArray();

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    StreamSummary<T> decoded = ssCoder.decode(bais);

    return ss.toString().equals(decoded.toString());
  }

  private static class OutputTopK<T> extends DoFn<StreamSummary<T>, T> {

    private int k = 0;

    private OutputTopK(int k) {
          this.k = k;
      }

    @ProcessElement
    public void apply(ProcessContext c) {
      List<Counter<T>> li = c.element().topK(k);
      for (Counter<T> counter : li) {
        c.output(counter.getItem());
      }
    }
  }
}
