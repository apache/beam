/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.CoderUtils.makeCloudEncoding;
import static com.google.cloud.dataflow.sdk.util.SerializableUtils.serializeToByteArray;
import static com.google.cloud.dataflow.sdk.util.StringUtils.byteArrayToJsonString;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.Receiver;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.reflect.TypeToken;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for CombineValuesFn.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class CombineValuesFnTest {
  /** Example AccumulatingCombineFn. */
  public static class MeanInts extends
      Combine.AccumulatingCombineFn<Integer, MeanInts.CountSum, String> {

    class CountSum implements
        Combine.AccumulatingCombineFn.Accumulator<Integer, CountSum, String> {

      long count;
      double sum;

      @Override
      public void addInput(Integer element) {
        count++;
        sum += element.doubleValue();
      }

      @Override
      public void mergeAccumulator(CountSum accumulator) {
        count += accumulator.count;
        sum += accumulator.sum;
      }

      @Override
      public String extractOutput() {
        return String.format("%.1f", count == 0 ? 0.0 : sum / count);
      }

      public CountSum(long count, double sum) {
        this.count = count;
        this.sum = sum;
      }

      @Override
      public int hashCode() {
        return KV.of(count, sum).hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof CountSum)) {
          return false;
        }
        if (obj == this) {
          return true;
        }

        CountSum other = (CountSum) obj;
        return (this.count == other.count)
            && (Math.abs(this.sum - other.sum) < 0.1);
      }
    }

    @Override
    public CountSum createAccumulator() {
      return new CountSum(0, 0.0);
    }

    @Override
    public Coder<CountSum> getAccumulatorCoder(
        CoderRegistry registry, Coder<Integer> inputCoder) {
      return new CountSumCoder();
    }
  }

  /**
   * An example "cheap" accumulator coder.
   */
  public static class CountSumCoder implements Coder<MeanInts.CountSum> {
    public CountSumCoder() { }

    @Override
    public void encode(
        MeanInts.CountSum value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      DataOutputStream dataStream = new DataOutputStream(outStream);
      dataStream.writeLong(value.count);
      dataStream.writeDouble(value.sum);
    }

    @Override
    public MeanInts.CountSum decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      DataInputStream dataStream = new DataInputStream(inStream);
      long count = dataStream.readLong();
      double sum = dataStream.readDouble();
      return (new MeanInts ()).new CountSum(count, sum);
    }

    @Override
    public boolean isDeterministic() { return true; }

    public CloudObject asCloudObject() {
      return makeCloudEncoding(this.getClass().getName());
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() { return null; }

    public List<Object> getInstanceComponents(MeanInts.CountSum exampleValue) {
      return null;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(
        MeanInts.CountSum value, Context context) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(
        MeanInts.CountSum value, ElementByteSizeObserver observer, Context ctx)
        throws Exception {
      observer.update((long) 16);
    }
  }

  static class TestReceiver implements Receiver {
    List<Object> receivedElems = new ArrayList<>();

    @Override
    public void process(Object outputElem) {
      receivedElems.add(outputElem);
    }
  }

  @SuppressWarnings("rawtypes")
  private static ParDoFn createCombineValuesFn(
      String phase, Combine.KeyedCombineFn combineFn) throws Exception {
    // This partially mirrors the work that
    // com.google.cloud.dataflow.sdk.transforms.Combine.translateHelper
    // does, at least for the KeyedCombineFn. The phase is generated
    // by the back-end.
    CloudObject spec = CloudObject.forClassName("CombineValuesFn");
    addString(spec, PropertyNames.SERIALIZED_FN,
        byteArrayToJsonString(serializeToByteArray(combineFn)));
    addString(spec, PropertyNames.PHASE, phase);

    return CombineValuesFn.create(
            PipelineOptionsFactory.create(),
            spec,
            "name",
            null, // no side inputs
            null, // no side outputs
            1, // single main output
            new BatchModeExecutionContext(),
            (new CounterSet()).getAddCounterMutator(),
            null);
  }

  @Test
  public void testCombineValuesFnAll() throws Exception {
    TestReceiver receiver = new TestReceiver();

    Combine.KeyedCombineFn<String, Integer,
        MeanInts.CountSum, String> combiner =
        (new MeanInts()).asKeyedFn();

    ParDoFn combineParDoFn = createCombineValuesFn(
        CombineValuesFn.CombinePhase.ALL, combiner);

    combineParDoFn.startBundle(receiver);
    combineParDoFn.processElement(WindowedValue.valueInGlobalWindow(
        KV.of("a", Arrays.asList(5, 6, 7))));
    combineParDoFn.processElement(WindowedValue.valueInGlobalWindow(
        KV.of("b", Arrays.asList(1, 3, 7))));
    combineParDoFn.processElement(WindowedValue.valueInGlobalWindow(
        KV.of("c", Arrays.asList(3, 6, 8, 9))));
    combineParDoFn.finishBundle();

    Object[] expectedReceivedElems = {
      WindowedValue.valueInGlobalWindow(KV.of("a", "6.0")),
      WindowedValue.valueInGlobalWindow(KV.of("b", "3.7")),
      WindowedValue.valueInGlobalWindow(KV.of("c", "6.5")),
    };
    assertArrayEquals(expectedReceivedElems, receiver.receivedElems.toArray());
  }

  @Test
  public void testCombineValuesFnAdd() throws Exception {
    TestReceiver receiver = new TestReceiver();
    MeanInts mean = new MeanInts();

    Combine.KeyedCombineFn<String, Integer,
        MeanInts.CountSum, String> combiner = mean.asKeyedFn();

    ParDoFn combineParDoFn = createCombineValuesFn(
        CombineValuesFn.CombinePhase.ADD, combiner);

    combineParDoFn.startBundle(receiver);
    combineParDoFn.processElement(WindowedValue.valueInGlobalWindow(
        KV.of("a", Arrays.asList(5, 6, 7))));
    combineParDoFn.processElement(WindowedValue.valueInGlobalWindow(
        KV.of("b", Arrays.asList(1, 3, 7))));
    combineParDoFn.processElement(WindowedValue.valueInGlobalWindow(
        KV.of("c", Arrays.asList(3, 6, 8, 9))));
    combineParDoFn.finishBundle();

    Object[] expectedReceivedElems = {
      WindowedValue.valueInGlobalWindow(KV.of("a", mean.new CountSum(3, 18))),
      WindowedValue.valueInGlobalWindow(KV.of("b", mean.new CountSum(3, 11))),
      WindowedValue.valueInGlobalWindow(KV.of("c", mean.new CountSum(4, 26)))
    };
    assertArrayEquals(expectedReceivedElems, receiver.receivedElems.toArray());
  }

  @Test
  public void testCombineValuesFnMerge() throws Exception {
    TestReceiver receiver = new TestReceiver();
    MeanInts mean = new MeanInts();

    Combine.KeyedCombineFn<String, Integer,
        MeanInts.CountSum, String> combiner = mean.asKeyedFn();

    ParDoFn combineParDoFn = createCombineValuesFn(
        CombineValuesFn.CombinePhase.MERGE, combiner);

    combineParDoFn.startBundle(receiver);
    combineParDoFn.processElement(WindowedValue.valueInGlobalWindow(
        KV.of("a",
            Arrays.asList(
                mean.new CountSum(3, 6),
                mean.new CountSum(2, 9),
                mean.new CountSum(1, 12)))));
    combineParDoFn.processElement(WindowedValue.valueInGlobalWindow(
        KV.of("b",
            Arrays.asList(
                mean.new CountSum(2, 20),
                mean.new CountSum(1, 1)))));
    combineParDoFn.finishBundle();

    Object[] expectedReceivedElems = {
      WindowedValue.valueInGlobalWindow(KV.of("a", mean.new CountSum(6, 27))),
      WindowedValue.valueInGlobalWindow(KV.of("b", mean.new CountSum(3, 21))),
    };
    assertArrayEquals(expectedReceivedElems, receiver.receivedElems.toArray());
  }

  @Test
  public void testCombineValuesFnExtract() throws Exception {
    TestReceiver receiver = new TestReceiver();
    MeanInts mean = new MeanInts();

    Combine.KeyedCombineFn<String, Integer,
        MeanInts.CountSum, String> combiner = mean.asKeyedFn();

    ParDoFn combineParDoFn = createCombineValuesFn(
        CombineValuesFn.CombinePhase.EXTRACT, combiner);

    combineParDoFn.startBundle(receiver);
    combineParDoFn.processElement(WindowedValue.valueInGlobalWindow(
        KV.of("a", mean.new CountSum(6, 27))));
    combineParDoFn.processElement(WindowedValue.valueInGlobalWindow(
        KV.of("b", mean.new CountSum(3, 21))));
    combineParDoFn.finishBundle();

    assertArrayEquals(
        new Object[]{ WindowedValue.valueInGlobalWindow(KV.of("a", "4.5")),
                      WindowedValue.valueInGlobalWindow(KV.of("b", "7.0")) },
        receiver.receivedElems.toArray());
  }

  @Test
  public void testCombineValuesFnCoders() throws Exception {
    CoderRegistry registry = new CoderRegistry();
    registry.registerStandardCoders();

    MeanInts meanInts = new MeanInts();
    MeanInts.CountSum countSum = meanInts.new CountSum(6, 27);

    Coder<MeanInts.CountSum> coder = meanInts.getAccumulatorCoder(
        registry, registry.getDefaultCoder(TypeToken.of(Integer.class)));

    assertEquals(
        countSum,
        CoderUtils.decodeFromByteArray(coder,
            CoderUtils.encodeToByteArray(coder, countSum)));
  }
}
