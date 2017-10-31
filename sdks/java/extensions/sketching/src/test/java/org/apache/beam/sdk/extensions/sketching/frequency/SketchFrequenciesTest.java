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

import com.clearspring.analytics.stream.frequency.CountMinSketch;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.extensions.sketching.frequency.SketchFrequencies.CountMinSketchFn;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for {@link SketchFrequencies}.
 */
public class SketchFrequenciesTest {

  @Rule public final transient TestPipeline tp = TestPipeline.create();

  private List<Long> smallStream = Arrays.asList(
          1L,
          2L, 2L,
          3L, 3L, 3L,
          4L, 4L, 4L, 4L,
          5L, 5L, 5L, 5L, 5L,
          6L, 6L, 6L, 6L, 6L, 6L,
          7L, 7L, 7L, 7L, 7L, 7L, 7L,
          8L, 8L, 8L, 8L, 8L, 8L, 8L, 8L,
          9L, 9L, 9L, 9L, 9L, 9L, 9L, 9L, 9L,
          10L, 10L, 10L, 10L, 10L, 10L, 10L, 10L, 10L, 10L);

  @Test
  public void defaultConstruct() {
    PCollection<Long> col = tp.apply(Create.of(smallStream))
            .apply(ToString.elements())
            .apply(SketchFrequencies.globally(1234))
            .apply(ParDo.of(new QueryFrequencies()));
    PAssert.that(col).containsInAnyOrder(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
    tp.run();
  }

  @Test
  public void createDimensions() {
    CountMinSketchFn cmsFn = CountMinSketchFn.create(1234).withDimensions(200, 3);
    PCollection<Long> col = tp.apply(Create.of(smallStream))
            .apply(ToString.elements())
            .apply(Combine.globally(cmsFn))
            .apply(ParDo.of(new QueryFrequencies()));
    PAssert.that(col).containsInAnyOrder(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
    tp.run();
  }

  @Test
  public void createAccuracy() {
    CountMinSketchFn cmsFn = CountMinSketchFn.create(1234).withAccuracy(0.01, 0.80);
    PCollection<Long> col = tp.apply(Create.of(smallStream))
            .apply(ToString.elements())
            .apply(Combine.globally(cmsFn))
            .apply(ParDo.of(new QueryFrequencies()));
    PAssert.that(col).containsInAnyOrder(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
    tp.run();
  }

  @Test
  public void createPerKey() {
    CountMinSketchFn cmsFn = CountMinSketchFn.create(1234);
    PCollection<Long> col = tp.apply(Create.of(smallStream))
            .apply(ToString.elements())
            .apply(WithKeys.<Integer, String>of(1))
            .apply(Combine.<Integer, String, CountMinSketch>perKey(cmsFn))
            .apply(Values.<CountMinSketch>create())
            .apply(ParDo.of(new QueryFrequencies()));
    PAssert.that(col).containsInAnyOrder(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
    tp.run();
  }

  @Test
  public void testCoder() throws Exception {
    CountMinSketch cMSketch = new CountMinSketch(200, 7, 12345);
    for (long i = 0L; i < 10L; i++) {
      cMSketch.add(i, 1);
    }
    CoderProperties.<CountMinSketch>coderDecodeEncodeEqual(
            new SketchFrequencies.CountMinSketchCoder(), cMSketch);
  }

  static class QueryFrequencies extends DoFn<CountMinSketch, Long> {
    @ProcessElement public void processElement(ProcessContext c) {
      CountMinSketch countMinSketch = c.element();
      for (Long i = 1L; i < 11L; i++) {
        c.output(countMinSketch.estimateCount(i.toString()));
      }
    }
  }
}
