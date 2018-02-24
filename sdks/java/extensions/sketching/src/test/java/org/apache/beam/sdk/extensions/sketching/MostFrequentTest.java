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
import com.clearspring.analytics.stream.StreamSummary;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import org.apache.beam.sdk.extensions.sketching.MostFrequent.StreamSummaryCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for {@link MostFrequent}.
 */
public class MostFrequentTest implements Serializable {

  @Rule
  public final transient TestPipeline tp = TestPipeline.create();

  private List<Integer> smallStream = initList();

  private static List<Integer> initList() {
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
    int nUsers = 10;
    Schema schema =
            SchemaBuilder.record("User")
                    .fields()
                    .requiredString("Pseudo")
                    .requiredInt("Age")
                    .endRecord();
    Coder<GenericRecord> coder = AvroCoder.of(schema);

    List<GenericRecord> stream = new ArrayList<>();
    for (int i = 1; i <= nUsers; i++) {
      GenericData.Record newRecord = new GenericData.Record(schema);
      newRecord.put("Pseudo", "User" + i);
      newRecord.put("Age", i);
      stream.addAll(Collections.nCopies(i, newRecord));
    }
    PCollection<GenericRecord> results = tp
            .apply(
                    "Create stream",
                    Create.of(stream).withCoder(coder))
            .apply(
                    "Test custom object",
                    MostFrequent.<GenericRecord>globally()
                            .withCapacity(nUsers)
                            .topKElements(3))
            .apply(Keys.<GenericRecord>create());
    tp.run();
  }

  @Test
  public void addWrapper() {
    int nUsers = 10;
    Schema schema =
        SchemaBuilder.record("User")
            .fields()
            .requiredString("Pseudo")
            .requiredInt("Age")
            .endRecord();
    Coder<GenericRecord> coder = AvroCoder.of(schema);
    List<GenericRecord> stream = new ArrayList<>();
    for (int i = 1; i <= nUsers; i++) {
      GenericData.Record newRecord = new GenericData.Record(schema);
      newRecord.put("Pseudo", "User" + i);
      newRecord.put("Age", i);
      stream.addAll(Collections.nCopies(i, newRecord));
    }

    MostFrequentFn.NonSerializableElements<GenericRecord> fn =
        MostFrequentFn.NonSerializableElements
            .create(coder)
            .withCapacity(nUsers);
    StreamSummary<ElementWrapper<GenericRecord>> ss1 = fn.createAccumulator();
    for (GenericRecord record : stream) {
      ss1 = fn.addInput(ss1, record);
    }

    List<Counter<ElementWrapper<GenericRecord>>> elements = ss1.topK(nUsers);
    for (int i = nUsers; i > 0; i--) {
      GenericRecord record = elements.get(i - 1).getItem().getElement();
      Long count = elements.get(i - 1).getCount();

      assertTrue("Not expected element ! Expected : " + i + " Actual : " + record
          + " with count=" + count, (int) record.get("Age") == i);
      assertTrue("Not expected element ! Expected : " + i + " Actual : "
          + count + " of element " + record, count == i);
    }
  }

  @Test
  public void mergeAccum() {
    Coder<Integer> coder = VarIntCoder.of();
    MostFrequentFn.SerializableElements<Integer> fn = MostFrequentFn.SerializableElements
        .create(10);
    StreamSummary<Integer> ss1 = fn.createAccumulator();
    StreamSummary<Integer> ss2 = fn.createAccumulator();

    for (Integer elem : smallStream) {
      ss1.offer(elem);
      ss2.offer(elem);
    }

    StreamSummary<Integer> ss3 = fn.mergeAccumulators(Arrays.asList(ss1, ss2));

    List<Counter<Integer>> elements = ss3.topK(10);

    for (int i = 10; i > 0; i--) {
      Integer element = elements.get(i - 1).getItem();
      Long count = elements.get(i - 1).getCount();

      assertTrue("Not expected element ! Expected : " + i + " Actual : " + element
              + " with count=" + count, element == i);
      assertTrue("Not expected element ! Expected : " + (2 * i) + " Actual : "
              + count + " of element " + element, count == 2 * i);
    }
  }

  @Test
  public void writeReadWrapper() throws IOException, ClassNotFoundException {

    ElementWrapper<Integer> wrapper = ElementWrapper.of(33544, VarIntCoder.of());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);

    wrapper.writeObject(oos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);

    ElementWrapper<Integer> read = new ElementWrapper<>();
    read.readObject(ois);

    assertTrue(wrapper.hashCode() == read.hashCode());
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
    StreamSummary<T> decoded = CoderUtils.clone(new StreamSummaryCoder<T>(), ss);
    return ss.toString().equals(decoded.toString());
  }

  @Test
  public void testDisplayData() {
    final MostFrequentFn fn = MostFrequentFn.SerializableElements
        .create(100);
    assertThat(DisplayData.from(fn), hasDisplayItem("capacity", 100));
  }
}
