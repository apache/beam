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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CombineValuesFnFactory}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class CombineValuesFnFactoryTest {
  /** Example AccumulatingCombineFn. */
  public static class MeanInts extends Combine.AccumulatingCombineFn<Integer, CountSum, String> {

    @Override
    public CountSum createAccumulator() {
      return new CountSum(0, 0.0);
    }

    @Override
    public Coder<CountSum> getAccumulatorCoder(CoderRegistry registry, Coder<Integer> inputCoder) {
      return new CountSumCoder();
    }
  }

  static class CountSum
      implements Combine.AccumulatingCombineFn.Accumulator<Integer, CountSum, String> {

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
      return Objects.hash(count, sum);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof CountSum)) {
        return false;
      }
      CountSum other = (CountSum) obj;
      return (this.count == other.count) && (Math.abs(this.sum - other.sum) < 0.1);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("count", count).add("sum", sum).toString();
    }
  }

  /** An example "cheap" accumulator coder. */
  public static class CountSumCoder extends CustomCoder<CountSum> {
    public CountSumCoder() {}

    @Override
    public void encode(CountSum value, OutputStream outStream) throws CoderException, IOException {
      DataOutputStream dataStream = new DataOutputStream(outStream);
      dataStream.writeLong(value.count);
      dataStream.writeDouble(value.sum);
    }

    @Override
    public CountSum decode(InputStream inStream) throws CoderException, IOException {
      DataInputStream dataStream = new DataInputStream(inStream);
      long count = dataStream.readLong();
      double sum = dataStream.readDouble();
      return new CountSum(count, sum);
    }

    @Override
    public void verifyDeterministic() {}

    @Override
    public boolean isRegisterByteSizeObserverCheap(CountSum value) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(CountSum value, ElementByteSizeObserver observer)
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

  private static final ParDoFnFactory parDoFnFactory = new CombineValuesFnFactory();
  private static final TupleTag<?> MAIN_OUTPUT = new TupleTag<>("output");

  private <K, InputT, AccumT, OutputT> ParDoFn createCombineValuesFn(
      String phase,
      Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
      Coder<K> keyCoder,
      Coder<InputT> inputCoder,
      Coder<AccumT> accumCoder,
      WindowingStrategy<?, ?> windowingStrategy)
      throws Exception {
    // This partially mirrors the work that
    // org.apache.beam.runners.dataflow.worker.transforms.Combine.translateHelper
    // does, at least for the KeyedCombineFn. The phase is generated
    // by the back-end.
    CloudObject spec = CloudObject.forClassName("CombineValuesFn");
    @SuppressWarnings("unchecked")
    AppliedCombineFn appliedCombineFn =
        AppliedCombineFn.withAccumulatorCoder(
            combineFn,
            accumCoder,
            Collections.emptyList(),
            KvCoder.of(keyCoder, inputCoder),
            windowingStrategy);
    addString(
        spec,
        PropertyNames.SERIALIZED_FN,
        byteArrayToJsonString(serializeToByteArray(appliedCombineFn)));
    addString(spec, WorkerPropertyNames.PHASE, phase);

    return parDoFnFactory.create(
        PipelineOptionsFactory.create(),
        spec,
        ImmutableList.<SideInputInfo>of(),
        MAIN_OUTPUT,
        ImmutableMap.<TupleTag<?>, Integer>of(MAIN_OUTPUT, 0),
        BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "testStage"),
        TestOperationContext.create());
  }

  @Test
  public void testCombineValuesFnAll() throws Exception {
    TestReceiver receiver = new TestReceiver();

    Combine.CombineFn<Integer, CountSum, String> combiner = (new MeanInts());

    ParDoFn combineParDoFn =
        createCombineValuesFn(
            CombinePhase.ALL,
            combiner,
            StringUtf8Coder.of(),
            BigEndianIntegerCoder.of(),
            new CountSumCoder(),
            WindowingStrategy.globalDefault());

    combineParDoFn.startBundle(receiver);
    combineParDoFn.processElement(
        WindowedValue.valueInGlobalWindow(KV.of("a", Arrays.asList(5, 6, 7))));
    combineParDoFn.processElement(
        WindowedValue.valueInGlobalWindow(KV.of("b", Arrays.asList(1, 3, 7))));
    combineParDoFn.processElement(
        WindowedValue.valueInGlobalWindow(KV.of("c", Arrays.asList(3, 6, 8, 9))));
    combineParDoFn.finishBundle();

    Object[] expectedReceivedElems = {
      WindowedValue.valueInGlobalWindow(KV.of("a", String.format("%.1f", 6.0))),
      WindowedValue.valueInGlobalWindow(KV.of("b", String.format("%.1f", 3.7))),
      WindowedValue.valueInGlobalWindow(KV.of("c", String.format("%.1f", 6.5))),
    };
    assertArrayEquals(expectedReceivedElems, receiver.receivedElems.toArray());
  }

  @Test
  public void testCombineValuesFnAdd() throws Exception {
    TestReceiver receiver = new TestReceiver();
    MeanInts mean = new MeanInts();

    Combine.CombineFn<Integer, CountSum, String> combiner = mean;

    ParDoFn combineParDoFn =
        createCombineValuesFn(
            CombinePhase.ADD,
            combiner,
            StringUtf8Coder.of(),
            BigEndianIntegerCoder.of(),
            new CountSumCoder(),
            WindowingStrategy.globalDefault());

    combineParDoFn.startBundle(receiver);
    combineParDoFn.processElement(
        WindowedValue.valueInGlobalWindow(KV.of("a", Arrays.asList(5, 6, 7))));
    combineParDoFn.processElement(
        WindowedValue.valueInGlobalWindow(KV.of("b", Arrays.asList(1, 3, 7))));
    combineParDoFn.processElement(
        WindowedValue.valueInGlobalWindow(KV.of("c", Arrays.asList(3, 6, 8, 9))));
    combineParDoFn.finishBundle();

    Object[] expectedReceivedElems = {
      WindowedValue.valueInGlobalWindow(KV.of("a", new CountSum(3, 18))),
      WindowedValue.valueInGlobalWindow(KV.of("b", new CountSum(3, 11))),
      WindowedValue.valueInGlobalWindow(KV.of("c", new CountSum(4, 26)))
    };
    assertArrayEquals(expectedReceivedElems, receiver.receivedElems.toArray());
  }

  @Test
  public void testCombineValuesFnMerge() throws Exception {
    TestReceiver receiver = new TestReceiver();
    MeanInts mean = new MeanInts();

    Combine.CombineFn<Integer, CountSum, String> combiner = mean;

    ParDoFn combineParDoFn =
        createCombineValuesFn(
            CombinePhase.MERGE,
            combiner,
            StringUtf8Coder.of(),
            BigEndianIntegerCoder.of(),
            new CountSumCoder(),
            WindowingStrategy.globalDefault());

    combineParDoFn.startBundle(receiver);
    combineParDoFn.processElement(
        WindowedValue.valueInGlobalWindow(
            KV.of(
                "a", Arrays.asList(new CountSum(3, 6), new CountSum(2, 9), new CountSum(1, 12)))));
    combineParDoFn.processElement(
        WindowedValue.valueInGlobalWindow(
            KV.of("b", Arrays.asList(new CountSum(2, 20), new CountSum(1, 1)))));
    combineParDoFn.finishBundle();

    Object[] expectedReceivedElems = {
      WindowedValue.valueInGlobalWindow(KV.of("a", new CountSum(6, 27))),
      WindowedValue.valueInGlobalWindow(KV.of("b", new CountSum(3, 21))),
    };
    assertArrayEquals(expectedReceivedElems, receiver.receivedElems.toArray());
  }

  @Test
  public void testCombineValuesFnExtract() throws Exception {
    TestReceiver receiver = new TestReceiver();
    MeanInts mean = new MeanInts();

    Combine.CombineFn<Integer, CountSum, String> combiner = mean;

    ParDoFn combineParDoFn =
        createCombineValuesFn(
            CombinePhase.EXTRACT,
            combiner,
            StringUtf8Coder.of(),
            BigEndianIntegerCoder.of(),
            new CountSumCoder(),
            WindowingStrategy.globalDefault());

    combineParDoFn.startBundle(receiver);
    combineParDoFn.processElement(
        WindowedValue.valueInGlobalWindow(KV.of("a", new CountSum(6, 27))));
    combineParDoFn.processElement(
        WindowedValue.valueInGlobalWindow(KV.of("b", new CountSum(3, 21))));
    combineParDoFn.finishBundle();

    assertArrayEquals(
        new Object[] {
          WindowedValue.valueInGlobalWindow(KV.of("a", String.format("%.1f", 4.5))),
          WindowedValue.valueInGlobalWindow(KV.of("b", String.format("%.1f", 7.0)))
        },
        receiver.receivedElems.toArray());
  }

  @Test
  public void testCombineValuesFnCoders() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();

    MeanInts meanInts = new MeanInts();
    CountSum countSum = new CountSum(6, 27);

    Coder<CountSum> coder =
        meanInts.getAccumulatorCoder(registry, registry.getCoder(TypeDescriptor.of(Integer.class)));

    assertEquals(
        countSum,
        CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, countSum)));
  }
}
