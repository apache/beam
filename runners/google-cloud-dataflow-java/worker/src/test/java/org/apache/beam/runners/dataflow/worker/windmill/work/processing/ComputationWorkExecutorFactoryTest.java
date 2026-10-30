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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing;

import static org.apache.beam.runners.dataflow.util.Structs.addObject;
import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Source;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.HotKeyLogger;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.MultiKeyBundleOptions;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationWorkExecutor;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.config.FakeGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcherFactory;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ComputationWorkExecutorFactory}. */
@RunWith(JUnit4.class)
public class ComputationWorkExecutorFactoryTest {

  private static class StringSplittableDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      if (tracker.tryClaim(tracker.currentRestriction().getFrom())) {
        c.output(c.element());
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@SuppressWarnings("unused") @Element String element) {
      return new OffsetRange(0, 1);
    }
  }

  private static class IntegerSplittableDoFn extends DoFn<String, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      if (tracker.tryClaim(tracker.currentRestriction().getFrom())) {
        c.output(c.element().length());
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@SuppressWarnings("unused") @Element String element) {
      return new OffsetRange(0, 1);
    }
  }

  private static class IdentityDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  private static ParallelInstruction createWindmillReadInstruction(
      String name, Coder<?> outputCoder) {
    CloudObject encodedCoder = CloudObjects.asCloudObject(outputCoder, /* sdkComponents= */ null);

    Source cloudSource = new Source();
    cloudSource.setSpec(CloudObject.forClassName("UngroupedWindmillReader"));
    cloudSource.setCodec(encodedCoder);

    ReadInstruction readInstruction = new ReadInstruction();
    readInstruction.setSource(cloudSource);

    InstructionOutput output = new InstructionOutput();
    output.setName(name + "_output");
    output.setCodec(encodedCoder);
    output.setOriginalName(name + "Original");
    output.setSystemName(name + "System");

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setName(name);
    instruction.setSystemName(name + "System");
    instruction.setOriginalName(name + "Original");
    instruction.setRead(readInstruction);
    instruction.setOutputs(Collections.singletonList(output));
    return instruction;
  }

  private static <OutputT> ParallelInstruction createParDoInstruction(
      int producerIndex,
      int producerOutputNum,
      String systemName,
      DoFn<String, OutputT> fn,
      Coder<OutputT> elementOutputCoder,
      Coder<WindowedValue<OutputT>> windowedOutputCoder,
      boolean isSplittable) {
    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(producerIndex);
    cloudInput.setOutputNum(producerOutputNum);

    TupleTag<OutputT> mainOutputTag = new TupleTag<>(PropertyNames.OUTPUT);
    String serializedFn =
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(
                DoFnInfo.forFn(
                    fn,
                    WindowingStrategy.globalDefault(),
                    null /* side input views */,
                    StringUtf8Coder.of() /* input coder */,
                    Collections.singletonMap(mainOutputTag, elementOutputCoder),
                    mainOutputTag,
                    DoFnSchemaInformation.create(),
                    Collections.emptyMap())));

    CloudObject cloudUserFn =
        CloudObject.forClassName(isSplittable ? "SplittableProcessFn" : "DoFn");
    addString(cloudUserFn, PropertyNames.SERIALIZED_FN, serializedFn);
    if (isSplittable) {
      addObject(
          cloudUserFn,
          WorkerPropertyNames.RESTRICTION_CODER,
          CloudObjects.asCloudObject(
              KvCoder.of(OffsetRange.Coder.of(), VoidCoder.of()), /* sdkComponents= */ null));
    }

    MultiOutputInfo multiOutputInfo = new MultiOutputInfo();
    multiOutputInfo.setTag(PropertyNames.OUTPUT);

    ParDoInstruction parDoInstruction = new ParDoInstruction();
    parDoInstruction.setInput(cloudInput);
    parDoInstruction.setNumOutputs(1);
    parDoInstruction.setMultiOutputInfos(ImmutableList.of(multiOutputInfo));
    parDoInstruction.setUserFn(cloudUserFn);

    InstructionOutput output = new InstructionOutput();
    output.setName(systemName + "_output");
    output.setCodec(CloudObjects.asCloudObject(windowedOutputCoder, /* sdkComponents= */ null));
    output.setOriginalName(systemName + "Original");
    output.setSystemName(systemName);

    ParallelInstruction instruction = new ParallelInstruction();
    instruction.setParDo(parDoInstruction);
    instruction.setOutputs(Collections.singletonList(output));
    instruction.setName(systemName);
    instruction.setSystemName(systemName);
    instruction.setOriginalName(systemName + "Original");
    return instruction;
  }

  @Test
  public void
      testCreateComputationWorkExecutor_multipleReadReportingParDoOperationsWithDifferentCoders()
          throws Exception {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    ComputationWorkExecutorFactory factory =
        new ComputationWorkExecutorFactory(
            options,
            IntrinsicMapTaskExecutorFactory.defaultFactory(),
            mock(ReaderCache.class),
            computationId -> mock(WindmillStateCache.ForComputation.class),
            DataflowExecutionStateSampler.instance(),
            StreamingCounters.create(),
            mock(FailureTracker.class),
            IdGenerators.decrementingLongs(),
            new FakeGlobalConfigHandle(StreamingGlobalConfig.builder().build()),
            mock(HotKeyLogger.class),
            SideInputStateFetcherFactory.fromOptions(options),
            MultiKeyBundleOptions.fromOptions(options));

    Coder<WindowedValue<String>> windowedStringCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    Coder<WindowedValue<Integer>> windowedIntCoder =
        WindowedValues.getFullCoder(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE);

    List<ParallelInstruction> instructions =
        Arrays.asList(
            createWindmillReadInstruction("Read", windowedStringCoder),
            createParDoInstruction(
                0,
                0,
                "SplittableParDoString",
                new StringSplittableDoFn(),
                StringUtf8Coder.of(),
                windowedStringCoder,
                true),
            createParDoInstruction(
                0,
                0,
                "SplittableParDoInt",
                new IntegerSplittableDoFn(),
                VarIntCoder.of(),
                windowedIntCoder,
                true),
            createParDoInstruction(
                0,
                0,
                "RegularParDo",
                new IdentityDoFn(),
                StringUtf8Coder.of(),
                windowedStringCoder,
                false));

    MapTask mapTask = new MapTask();
    mapTask.setStageName("testStage");
    mapTask.setSystemName("testSystem");
    mapTask.setInstructions(instructions);
    mapTask.setFactory(Transport.getJsonFactory());

    StageInfo stageInfo = StageInfo.create("testStage", "testSystem");
    ComputationState computationState =
        new ComputationState(
            "computationId",
            mapTask,
            mock(BoundedQueueExecutor.class),
            Collections.emptyMap(),
            mock(WindmillStateCache.ForComputation.class));

    ComputationWorkExecutor workExecutor =
        factory.createComputationWorkExecutor(stageInfo, computationState, "workLatencyTrackingId");

    DataflowMapTaskExecutor mapTaskExecutor = (DataflowMapTaskExecutor) workExecutor.workExecutor();
    List<ParDoOperation> allParDoOps = mapTaskExecutor.getParDoOperations();
    assertEquals(3, allParDoOps.size());

    ParDoOperation stringSplittableOp =
        allParDoOps.stream()
            .filter(op -> windowedStringCoder.equals(op.getBytesReadCoder(op.receivers[0])))
            .findFirst()
            .orElseThrow(
                () -> new AssertionError("Expected ParDoOperation with windowedStringCoder"));
    ParDoOperation intSplittableOp =
        allParDoOps.stream()
            .filter(op -> windowedIntCoder.equals(op.getBytesReadCoder(op.receivers[0])))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected ParDoOperation with windowedIntCoder"));

    String counterName = computationState.sourceBytesProcessCounterName();
    assertNotNull(stringSplittableOp.receivers[0].getOutputCounters().get(counterName));
    assertNotNull(intSplittableOp.receivers[0].getOutputCounters().get(counterName));

    // Verify that the non-reporting ParDoOperation does not have the source bytes counter attached.
    ParDoOperation regularParDoOp =
        allParDoOps.stream()
            .filter(op -> !op.shouldReportBytesRead())
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected non-reporting ParDoOperation"));
    assertTrue(regularParDoOp.getBytesReadCoders().isEmpty());
    assertNull(regularParDoOp.getBytesReadCoder(regularParDoOp.receivers[0]));
    assertNull(regularParDoOp.receivers[0].getOutputCounters().get(counterName));

    // Process elements through each operation's receiver and verify each uses its own coder.
    WindowedValue<String> stringElem = WindowedValues.valueInGlobalWindow("hello");
    WindowedValue<Integer> intElem = WindowedValues.valueInGlobalWindow(12345);
    stringSplittableOp.receivers[0].process(stringElem);
    intSplittableOp.receivers[0].process(intElem);
    regularParDoOp.receivers[0].process(stringElem);

    long expectedStringBytes =
        CoderUtils.encodeToByteArray(windowedStringCoder, stringElem, Coder.Context.NESTED).length;
    long expectedIntBytes =
        CoderUtils.encodeToByteArray(windowedIntCoder, intElem, Coder.Context.NESTED).length;

    @SuppressWarnings("unchecked")
    Counter<Long, Long> bytesReadCounter =
        (Counter<Long, Long>) mapTaskExecutor.getOutputCounters().getExistingCounter(counterName);
    assertNotNull(bytesReadCounter);
    assertEquals(
        Long.valueOf(expectedStringBytes + expectedIntBytes), bytesReadCounter.getAggregate());
  }

  @Test
  public void testCreateComputationWorkExecutor_multiOutputSplittableParDoWithDifferentCoders()
      throws Exception {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    ComputationWorkExecutorFactory factory =
        new ComputationWorkExecutorFactory(
            options,
            IntrinsicMapTaskExecutorFactory.defaultFactory(),
            mock(ReaderCache.class),
            computationId -> mock(WindmillStateCache.ForComputation.class),
            DataflowExecutionStateSampler.instance(),
            StreamingCounters.create(),
            mock(FailureTracker.class),
            IdGenerators.decrementingLongs(),
            new FakeGlobalConfigHandle(StreamingGlobalConfig.builder().build()),
            mock(HotKeyLogger.class),
            SideInputStateFetcherFactory.fromOptions(options),
            MultiKeyBundleOptions.fromOptions(options));

    Coder<WindowedValue<String>> windowedStringCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    Coder<WindowedValue<Integer>> windowedIntCoder =
        WindowedValues.getFullCoder(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE);

    TupleTag<String> mainOutputTag = new TupleTag<>("main");
    TupleTag<Integer> sideOutputTag = new TupleTag<>("side");
    String serializedFn =
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(
                DoFnInfo.forFn(
                    new StringSplittableDoFn(),
                    WindowingStrategy.globalDefault(),
                    null /* side input views */,
                    StringUtf8Coder.of() /* input coder */,
                    ImmutableMap.of(
                        mainOutputTag, StringUtf8Coder.of(), sideOutputTag, VarIntCoder.of()),
                    mainOutputTag,
                    DoFnSchemaInformation.create(),
                    Collections.emptyMap())));

    CloudObject cloudUserFn = CloudObject.forClassName("SplittableProcessFn");
    addString(cloudUserFn, PropertyNames.SERIALIZED_FN, serializedFn);
    addObject(
        cloudUserFn,
        WorkerPropertyNames.RESTRICTION_CODER,
        CloudObjects.asCloudObject(
            KvCoder.of(OffsetRange.Coder.of(), VoidCoder.of()), /* sdkComponents= */ null));

    InstructionInput cloudInput = new InstructionInput();
    cloudInput.setProducerInstructionIndex(0);
    cloudInput.setOutputNum(0);

    MultiOutputInfo mainOutputInfo = new MultiOutputInfo();
    mainOutputInfo.setTag("main");
    MultiOutputInfo sideOutputInfo = new MultiOutputInfo();
    sideOutputInfo.setTag("side");

    ParDoInstruction parDoInstruction = new ParDoInstruction();
    parDoInstruction.setInput(cloudInput);
    parDoInstruction.setNumOutputs(2);
    parDoInstruction.setMultiOutputInfos(ImmutableList.of(mainOutputInfo, sideOutputInfo));
    parDoInstruction.setUserFn(cloudUserFn);

    InstructionOutput mainOutput = new InstructionOutput();
    mainOutput.setName("MultiOutputSplittableParDo_main");
    mainOutput.setCodec(CloudObjects.asCloudObject(windowedStringCoder, /* sdkComponents= */ null));
    mainOutput.setOriginalName("MultiOutputSplittableParDoOriginal");
    mainOutput.setSystemName("MultiOutputSplittableParDo");

    InstructionOutput sideOutput = new InstructionOutput();
    sideOutput.setName("MultiOutputSplittableParDo_side");
    sideOutput.setCodec(CloudObjects.asCloudObject(windowedIntCoder, /* sdkComponents= */ null));
    sideOutput.setOriginalName("MultiOutputSplittableParDoOriginal");
    sideOutput.setSystemName("MultiOutputSplittableParDo");

    ParallelInstruction multiOutputSdfInstruction = new ParallelInstruction();
    multiOutputSdfInstruction.setParDo(parDoInstruction);
    multiOutputSdfInstruction.setOutputs(ImmutableList.of(mainOutput, sideOutput));
    multiOutputSdfInstruction.setName("MultiOutputSplittableParDo");
    multiOutputSdfInstruction.setSystemName("MultiOutputSplittableParDo");
    multiOutputSdfInstruction.setOriginalName("MultiOutputSplittableParDoOriginal");

    MapTask mapTask = new MapTask();
    mapTask.setStageName("testStage");
    mapTask.setSystemName("testSystem");
    mapTask.setInstructions(
        Arrays.asList(
            createWindmillReadInstruction("Read", windowedStringCoder), multiOutputSdfInstruction));
    mapTask.setFactory(Transport.getJsonFactory());

    StageInfo stageInfo = StageInfo.create("testStage", "testSystem");
    ComputationState computationState =
        new ComputationState(
            "computationId",
            mapTask,
            mock(BoundedQueueExecutor.class),
            Collections.emptyMap(),
            mock(WindmillStateCache.ForComputation.class));

    ComputationWorkExecutor workExecutor =
        factory.createComputationWorkExecutor(stageInfo, computationState, "workLatencyTrackingId");

    DataflowMapTaskExecutor mapTaskExecutor = (DataflowMapTaskExecutor) workExecutor.workExecutor();
    List<ParDoOperation> allParDoOps = mapTaskExecutor.getParDoOperations();
    assertEquals(1, allParDoOps.size());

    ParDoOperation multiOutputSdfOp = allParDoOps.get(0);
    assertTrue(multiOutputSdfOp.shouldReportBytesRead());
    assertEquals(2, multiOutputSdfOp.receivers.length);
    assertEquals(
        windowedStringCoder, multiOutputSdfOp.getBytesReadCoder(multiOutputSdfOp.receivers[0]));
    assertEquals(
        windowedIntCoder, multiOutputSdfOp.getBytesReadCoder(multiOutputSdfOp.receivers[1]));

    String counterName = computationState.sourceBytesProcessCounterName();
    assertNotNull(multiOutputSdfOp.receivers[0].getOutputCounters().get(counterName));
    assertNotNull(multiOutputSdfOp.receivers[1].getOutputCounters().get(counterName));

    // Process differently typed elements on main and side output receivers without
    // ClassCastException.
    WindowedValue<String> stringElem = WindowedValues.valueInGlobalWindow("hello");
    WindowedValue<Integer> intElem = WindowedValues.valueInGlobalWindow(12345);
    multiOutputSdfOp.receivers[0].process(stringElem);
    multiOutputSdfOp.receivers[1].process(intElem);

    long expectedStringBytes =
        CoderUtils.encodeToByteArray(windowedStringCoder, stringElem, Coder.Context.NESTED).length;
    long expectedIntBytes =
        CoderUtils.encodeToByteArray(windowedIntCoder, intElem, Coder.Context.NESTED).length;

    @SuppressWarnings("unchecked")
    Counter<Long, Long> bytesReadCounter =
        (Counter<Long, Long>) mapTaskExecutor.getOutputCounters().getExistingCounter(counterName);
    assertNotNull(bytesReadCounter);
    assertEquals(
        Long.valueOf(expectedStringBytes + expectedIntBytes), bytesReadCounter.getAggregate());
  }
}
