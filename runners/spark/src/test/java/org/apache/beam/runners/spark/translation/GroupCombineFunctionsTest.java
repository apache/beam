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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.GroupCombineFunctions.KryoAccumulatorSerializer;
import org.apache.beam.runners.spark.translation.GroupCombineFunctions.SerializableAccumulator;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;
import org.junit.Test;

/** Unit tests of {@link GroupCombineFunctions}. */
public class GroupCombineFunctionsTest {

  @Test
  public void serializableAccumulatorTest() {
    Iterable<WindowedValue<Integer>> accumulatedValue =
        Arrays.asList(winVal(0), winVal(1), winVal(3), winVal(4));

    final WindowedValue.FullWindowedValueCoder<Integer> wvaCoder =
        WindowedValue.FullWindowedValueCoder.of(
            BigEndianIntegerCoder.of(), GlobalWindow.Coder.INSTANCE);

    final IterableCoder<WindowedValue<Integer>> iterAccumCoder = IterableCoder.of(wvaCoder);

    SerializableAccumulator<Integer> accUnderTest =
        SerializableAccumulator.of(accumulatedValue, iterAccumCoder);
    assertEquals(accumulatedValue, accUnderTest.getOrDecode(iterAccumCoder));

    byte[] bytes = accUnderTest.toBytes();
    assertEquals(accumulatedValue, CoderHelpers.fromByteArray(bytes, iterAccumCoder));

    SerializableAccumulator<Integer> accFromBytes = SerializableAccumulator.ofBytes(bytes);
    assertEquals(accumulatedValue, accFromBytes.getOrDecode(iterAccumCoder));
  }

  @Test
  public void serializableAccumulatorSerializationTest()
      throws IOException, ClassNotFoundException {
    @SuppressWarnings("unchecked")
    Iterable<WindowedValue<Integer>> accumulatedValue =
        Arrays.asList(winVal(0), winVal(1), winVal(3), winVal(4));

    final WindowedValue.FullWindowedValueCoder<Integer> wvaCoder =
        WindowedValue.FullWindowedValueCoder.of(
            BigEndianIntegerCoder.of(), GlobalWindow.Coder.INSTANCE);

    final IterableCoder<WindowedValue<Integer>> iterAccumCoder = IterableCoder.of(wvaCoder);

    SerializableAccumulator<Integer> accUnderTest =
        SerializableAccumulator.of(accumulatedValue, iterAccumCoder);

    ByteArrayOutputStream inMemOut = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(inMemOut);
    oos.writeObject(accUnderTest);

    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(inMemOut.toByteArray()));

    @SuppressWarnings("unchecked")
    SerializableAccumulator<Integer> materialized =
        (SerializableAccumulator<Integer>) ois.readObject();
    assertEquals(accumulatedValue, materialized.getOrDecode(iterAccumCoder));
  }

  @Test
  public void serializableAccumulatorKryoTest() {
    Iterable<WindowedValue<Integer>> accumulatedValue =
        Arrays.asList(winVal(0), winVal(1), winVal(3), winVal(4));

    final WindowedValue.FullWindowedValueCoder<Integer> wvaCoder =
        WindowedValue.FullWindowedValueCoder.of(
            BigEndianIntegerCoder.of(), GlobalWindow.Coder.INSTANCE);

    final IterableCoder<WindowedValue<Integer>> iterAccumCoder = IterableCoder.of(wvaCoder);

    SerializableAccumulator<Integer> accUnderTest =
        SerializableAccumulator.of(accumulatedValue, iterAccumCoder);

    KryoAccumulatorSerializer kryoSerializer = new KryoAccumulatorSerializer();
    Kryo kryo = new Kryo();
    kryo.register(SerializableAccumulator.class, kryoSerializer);

    ByteArrayOutputStream inMemOut = new ByteArrayOutputStream();
    Output out = new Output(inMemOut);
    kryo.writeObject(out, accUnderTest);
    out.close();

    Input input = new Input(new ByteArrayInputStream(inMemOut.toByteArray()));

    @SuppressWarnings("unchecked")
    SerializableAccumulator<Integer> materialized =
        (SerializableAccumulator<Integer>) kryo.readObject(input, SerializableAccumulator.class);
    input.close();

    assertEquals(accumulatedValue, materialized.getOrDecode(iterAccumCoder));
  }

  private <T> WindowedValue<T> winVal(T val) {
    return WindowedValue.of(val, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
  }
}
