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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.clearspring.analytics.stream.Counter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.sketching.MostFrequent.ElementWrapper;
import org.apache.beam.sdk.extensions.sketching.MostFrequent.MostFrequentFn;
import org.apache.beam.sdk.extensions.sketching.MostFrequent.StreamSummarySketch;
import org.apache.beam.sdk.extensions.sketching.MostFrequent.StreamSummarySketchCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for {@link MostFrequent}.
 */
public class MostFrequentTest implements Serializable {

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  private static Schema schema = SchemaBuilder.record("User").fields()
      .requiredString("Pseudo")
      .requiredInt("Age")
      .endRecord();
  private static Coder<GenericRecord> coder = AvroCoder.of(schema);
  private List<GenericRecord> avroStream = generateAvroStream();
  private List<Integer> smallStream = initList();

  private List<Integer> initList() {
    List<Integer> list = Arrays.asList(
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
    Collections.shuffle(list, new Random(1234));
    return list;
  }

  private List<GenericRecord> generateAvroStream() {
    List<GenericRecord> stream = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      GenericData.Record newRecord = new GenericData.Record(schema);
      newRecord.put("Pseudo", "User" + i);
      newRecord.put("Age", i);
      stream.addAll(Collections.nCopies(i, newRecord));
    }
    Collections.shuffle(stream, new Random(1234));
    return stream;
  }

  @Test
  public void globally() {
    PCollection<Integer> col = tp.apply(Create.of(smallStream))
            .apply(
                    MostFrequent.<Integer>globally()
                            .withCapacity(10)
                            .topKElements(3))
            .apply(Keys.<Integer>create());

    PAssert.that(col).containsInAnyOrder(10, 9, 8);
    tp.run();
  }

  @Test
  public void perKey() {
    PCollection<Integer> col = tp.apply(Create.of(smallStream))
            .apply(WithKeys.of(1))
            .apply(
                    MostFrequent.<Integer, Integer>perKey()
                            .withCapacity(10)
                            .topKElements(3))
            .apply(Values.create())
            .apply(Keys.<Integer>create());

    PAssert.that(col).containsInAnyOrder(10, 9, 8);
    tp.run();
  }

  @Test
  public void customObject() {
    PCollection<GenericRecord> col = tp
        .apply(
            "Create stream",
            Create.of(avroStream).withCoder(coder))
        .apply(
            "Test custom object",
            MostFrequent.<GenericRecord>globally()
                .withCapacity(10)
                .topKElements(3))
        .apply(Keys.<GenericRecord>create());

    tp.run();
  }

  @Test
  public void addWrapper() {
    MostFrequentFn.NonSerializableElements<GenericRecord> fn =
        MostFrequentFn.NonSerializableElements
            .create(coder)
            .withCapacity(10);
    StreamSummarySketch<ElementWrapper<GenericRecord>> ss1 = fn.createAccumulator();
    for (GenericRecord record : avroStream) {
      ss1 = fn.addInput(ss1, record);
    }

    List<Counter<ElementWrapper<GenericRecord>>> elements = ss1.topK(10);
    int ind = 0;
    for (int i = 10; i > 0; i--) {
      GenericRecord record = elements.get(ind).getItem().getElement();
      Long count = elements.get(ind).getCount();
      ind++;

      assertTrue("Not expected element ! Expected : " + i + " Actual : " + record
          + " with count=" + count, (int) record.get("Age") == i);
      assertTrue("Not expected count ! Expected : " + i + " Actual : "
          + count + " of element " + record, count == i);
    }
  }

  @Test
  public void mergeAccum() {
    Coder<Integer> coder = VarIntCoder.of();
    MostFrequentFn.SerializableElements<Integer> fn = MostFrequentFn.SerializableElements
        .create(10);
    StreamSummarySketch<Integer> ss1 = fn.createAccumulator();
    StreamSummarySketch<Integer> ss2 = fn.createAccumulator();

    for (Integer elem : smallStream) {
      ss1.offer(elem);
      ss2.offer(elem);
    }

    StreamSummarySketch<Integer> ss3 = fn.mergeAccumulators(Arrays.asList(ss1, ss2));
    List<Counter<Integer>> elements = ss3.topK(10);
    int ind = 0;
    for (int i = 10; i > 0; i--) {
      Integer element = elements.get(ind).getItem();
      Long count = elements.get(ind).getCount();
      ind++;

      assertTrue("Not expected element ! Expected : " + i + " Actual : " + element
              + " with count=" + count, element == i);
      assertTrue("Not expected count ! Expected : " + (2 * i) + " Actual : "
              + count + " of element " + element, count == 2 * i);
    }
  }

  @Test
  public void wrapperCoder() throws Exception {
    ElementWrapper<GenericRecord> wrapper = ElementWrapper.of(avroStream.get(0), coder);
    CoderProperties.coderDecodeEncodeEqual(new MostFrequent.ElementWrapperCoder<>(), wrapper);
  }

  @Test
  public void testSketchCoder() throws Exception {
    StreamSummarySketch<Integer> ssSketch = new StreamSummarySketch<>(5);
    for (int i = 0; i < 5; i++) {
      ssSketch.offer(i);
    }
    CoderProperties.coderDecodeEncodeEqual(new StreamSummarySketchCoder<>(), ssSketch);
  }

  @Test
  public void testDisplayData() {
    final MostFrequentFn fn = MostFrequentFn.SerializableElements
        .create(100);
    assertThat(DisplayData.from(fn), hasDisplayItem("capacity", 100));
  }
}
