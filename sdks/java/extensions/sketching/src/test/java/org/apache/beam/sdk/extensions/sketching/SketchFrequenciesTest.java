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
package org.apache.beam.sdk.extensions.sketching;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.sketching.SketchFrequencies.CountMinSketchFn;
import org.apache.beam.sdk.extensions.sketching.SketchFrequencies.Sketch;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Tests for {@link SketchFrequencies}. */
public class SketchFrequenciesTest implements Serializable {

  @Rule public final transient TestPipeline tp = TestPipeline.create();

  private final List<Long> smallStream =
      Arrays.asList(
          1L, 2L, 2L, 3L, 3L, 3L, 4L, 4L, 4L, 4L, 5L, 5L, 5L, 5L, 5L, 6L, 6L, 6L, 6L, 6L, 6L, 7L,
          7L, 7L, 7L, 7L, 7L, 7L, 8L, 8L, 8L, 8L, 8L, 8L, 8L, 8L, 9L, 9L, 9L, 9L, 9L, 9L, 9L, 9L,
          9L);

  private final Long[] distinctElems = {1L, 2L, 3L, 4L, 5L, 6L, 8L, 9L};
  private final Long[] frequencies = distinctElems.clone();

  @Test
  public void perKeyDefault() {
    PCollection<Long> stream = tp.apply(Create.of(smallStream));
    PCollection<Sketch<Long>> sketch =
        stream.apply(WithKeys.of(1)).apply(SketchFrequencies.perKey()).apply(Values.create());

    Coder<Long> coder = stream.getCoder();

    PAssert.thatSingleton("Verify number of hits", sketch)
        .satisfies(new VerifyStreamFrequencies<>(coder, distinctElems, frequencies));

    tp.run();
  }

  @Test
  public void globallyWithTunedParameters() {
    double eps = 0.01;
    double conf = 0.8;
    PCollection<Long> stream = tp.apply(Create.of(smallStream));
    PCollection<Sketch<Long>> sketch =
        stream.apply(
            SketchFrequencies.<Long>globally().withRelativeError(eps).withConfidence(conf));

    Coder<Long> coder = stream.getCoder();

    PAssert.thatSingleton("Verify number of hits", sketch)
        .satisfies(new VerifyStreamFrequencies<>(coder, distinctElems, frequencies));

    tp.run();
  }

  @Test
  public void merge() {
    double eps = 0.01;
    double conf = 0.8;
    long nOccurrences = 2L;
    int size = 3;

    List<Sketch<Integer>> sketches = new ArrayList<>();
    Coder<Integer> coder = VarIntCoder.of();

    // n sketches each containing [0, 1, 2]
    for (int i = 0; i < nOccurrences; i++) {
      Sketch<Integer> sketch = Sketch.create(eps, conf);
      for (int j = 0; j < size; j++) {
        sketch.add(j, coder);
      }
      sketches.add(sketch);
    }

    CountMinSketchFn<Integer> fn = CountMinSketchFn.create(coder).withAccuracy(eps, conf);
    Sketch<Integer> merged = fn.mergeAccumulators(sketches);
    for (int i = 0; i < size; i++) {
      assertEquals(nOccurrences, merged.estimateCount(i, coder));
    }
  }

  @Test
  public void customObject() {
    int nUsers = 10;
    long occurrences = 2L; // occurrence of each user in the stream
    double eps = 0.01;
    double conf = 0.8;
    Sketch<GenericRecord> sketch = Sketch.create(eps, conf);
    Schema schema =
        SchemaBuilder.record("User")
            .fields()
            .requiredString("Pseudo")
            .requiredInt("Age")
            .endRecord();
    Coder<GenericRecord> coder = AvroCoder.of(schema);

    for (int i = 1; i <= nUsers; i++) {
      GenericData.Record newRecord = new GenericData.Record(schema);
      newRecord.put("Pseudo", "User" + i);
      newRecord.put("Age", i);
      sketch.add(newRecord, occurrences, coder);
      assertEquals("Test API", occurrences, sketch.estimateCount(newRecord, coder));
    }
  }

  @Test
  public void testCoder() throws Exception {
    Sketch<Integer> cMSketch = Sketch.create(0.01, 0.8);
    Coder<Integer> coder = VarIntCoder.of();
    for (int i = 0; i < 3; i++) {
      cMSketch.add(i, coder);
    }

    CoderProperties.coderDecodeEncodeEqual(new SketchFrequencies.CountMinSketchCoder<>(), cMSketch);
  }

  @Test
  public void testDisplayData() {
    double eps = 0.01;
    double conf = 0.8;
    int width = (int) Math.ceil(2 / eps);
    int depth = (int) Math.ceil(-Math.log(1 - conf) / Math.log(2));

    final CountMinSketchFn<Integer> fn =
        CountMinSketchFn.create(VarIntCoder.of()).withAccuracy(eps, conf);

    assertThat(DisplayData.from(fn), hasDisplayItem("width", width));
    assertThat(DisplayData.from(fn), hasDisplayItem("depth", depth));
    assertThat(DisplayData.from(fn), hasDisplayItem("eps", eps));
    assertThat(DisplayData.from(fn), hasDisplayItem("conf", conf));
  }

  static class VerifyStreamFrequencies<T> implements SerializableFunction<Sketch<T>, Void> {
    final Coder<T> coder;
    final Long[] expectedHits;
    final T[] elements;

    VerifyStreamFrequencies(Coder<T> coder, T[] elements, Long[] expectedHits) {
      this.coder = coder;
      this.elements = elements;
      this.expectedHits = expectedHits;
    }

    @Override
    public Void apply(Sketch<T> sketch) {
      for (int i = 0; i < elements.length; i++) {
        assertEquals((long) expectedHits[i], sketch.estimateCount(elements[i], coder));
      }
      return null;
    }
  }
}
