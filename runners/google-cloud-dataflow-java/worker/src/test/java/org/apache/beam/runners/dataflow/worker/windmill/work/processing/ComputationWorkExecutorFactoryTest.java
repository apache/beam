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

import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationWorkExecutor;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.config.FakeGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingCounters;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcherFactory;
import org.apache.beam.runners.dataflow.worker.testing.TestSplittableDoFns;
import org.apache.beam.runners.dataflow.worker.testing.TestSplittableDoFns.IntegerSplittableDoFn;
import org.apache.beam.runners.dataflow.worker.testing.TestSplittableDoFns.StringSplittableDoFn;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementByteSizeCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
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
    CloudObject cloudUserFn;
    if (isSplittable) {
      cloudUserFn =
          TestSplittableDoFns.createSplittableProcessFnCloudObject(
              fn, mainOutputTag, elementOutputCoder);
    } else {
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
      cloudUserFn = CloudObject.forClassName("DoFn");
      addString(cloudUserFn, PropertyNames.SERIALIZED_FN, serializedFn);
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
    StreamingCounters streamingCounters = StreamingCounters.create();
    ComputationWorkExecutorFactory factory =
        new ComputationWorkExecutorFactory(
            options,
            IntrinsicMapTaskExecutorFactory.defaultFactory(),
            mock(ReaderCache.class),
            computationId -> mock(WindmillStateCache.ForComputation.class),
            DataflowExecutionStateSampler.instance(),
            streamingCounters,
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

    assertTrue(
        stringSplittableOp.receivers[0].getOutputCounters().values().stream()
            .anyMatch(ElementByteSizeCounter.class::isInstance));
    assertTrue(
        intSplittableOp.receivers[0].getOutputCounters().values().stream()
            .anyMatch(ElementByteSizeCounter.class::isInstance));

    // Verify that the non-reporting ParDoOperation does not have the source bytes counter attached.
    ParDoOperation regularParDoOp =
        allParDoOps.stream()
            .filter(op -> !op.shouldReportBytesRead())
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected non-reporting ParDoOperation"));
    assertTrue(regularParDoOp.getBytesReadCoders().isEmpty());
    assertNull(regularParDoOp.getBytesReadCoder(regularParDoOp.receivers[0]));
    assertFalse(
        regularParDoOp.receivers[0].getOutputCounters().values().stream()
            .anyMatch(ElementByteSizeCounter.class::isInstance));

    // Process elements through each operation's receiver and verify each uses its own coder.
    WindowedValue<String> stringElem = WindowedValues.valueInGlobalWindow("hello");
    WindowedValue<Integer> intElem = WindowedValues.valueInGlobalWindow(12345);
    long expectedStringBytes =
        CoderUtils.encodeToByteArray(windowedStringCoder, stringElem, Coder.Context.NESTED).length;
    long expectedIntBytes =
        CoderUtils.encodeToByteArray(windowedIntCoder, intElem, Coder.Context.NESTED).length;

    assertEquals(0L, workExecutor.context().getSourceBytesProcessed());
    stringSplittableOp.receivers[0].process(stringElem);
    assertEquals(expectedStringBytes, workExecutor.context().getSourceBytesProcessed());
    intSplittableOp.receivers[0].process(intElem);
    assertEquals(
        expectedStringBytes + expectedIntBytes, workExecutor.context().getSourceBytesProcessed());
    regularParDoOp.receivers[0].process(stringElem);
    assertEquals(
        expectedStringBytes + expectedIntBytes, workExecutor.context().getSourceBytesProcessed());

    // Extracting delta counters (as StreamingWorkerStatusReporter does) must not drain SDF bytes.
    streamingCounters
        .pendingDeltaCounters()
        .extractModifiedDeltaUpdates(DataflowCounterUpdateExtractor.INSTANCE);
    assertEquals(
        expectedStringBytes + expectedIntBytes, workExecutor.context().getSourceBytesProcessed());

    // A second executor for the same computation must track its own bytes independently.
    ComputationWorkExecutor secondWorkExecutor =
        factory.createComputationWorkExecutor(stageInfo, computationState, "workLatencyTrackingId");
    ParDoOperation secondIntSplittableOp =
        ((DataflowMapTaskExecutor) secondWorkExecutor.workExecutor())
            .getParDoOperations().stream()
                .filter(op -> windowedIntCoder.equals(op.getBytesReadCoder(op.receivers[0])))
                .findFirst()
                .orElseThrow(
                    () -> new AssertionError("Expected ParDoOperation with windowedIntCoder"));
    secondIntSplittableOp.receivers[0].process(intElem);
    assertEquals(expectedIntBytes, secondWorkExecutor.context().getSourceBytesProcessed());
    assertEquals(
        expectedStringBytes + expectedIntBytes, workExecutor.context().getSourceBytesProcessed());
  }

  @Test
  public void testCreateComputationWorkExecutor_multiOutputSplittableParDoWithDifferentCoders()
      throws Exception {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    StreamingCounters streamingCounters = StreamingCounters.create();
    ComputationWorkExecutorFactory factory =
        new ComputationWorkExecutorFactory(
            options,
            IntrinsicMapTaskExecutorFactory.defaultFactory(),
            mock(ReaderCache.class),
            computationId -> mock(WindmillStateCache.ForComputation.class),
            DataflowExecutionStateSampler.instance(),
            streamingCounters,
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
    CloudObject cloudUserFn =
        TestSplittableDoFns.createSplittableProcessFnCloudObject(
            new StringSplittableDoFn(),
            mainOutputTag,
            ImmutableMap.of(mainOutputTag, StringUtf8Coder.of(), sideOutputTag, VarIntCoder.of()));

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
    OutputReceiver stringReceiver =
        windowedStringCoder.equals(
                multiOutputSdfOp.getBytesReadCoder(multiOutputSdfOp.receivers[0]))
            ? multiOutputSdfOp.receivers[0]
            : multiOutputSdfOp.receivers[1];
    OutputReceiver intReceiver =
        multiOutputSdfOp.receivers[0] == stringReceiver
            ? multiOutputSdfOp.receivers[1]
            : multiOutputSdfOp.receivers[0];

    assertEquals(windowedStringCoder, multiOutputSdfOp.getBytesReadCoder(stringReceiver));
    assertEquals(windowedIntCoder, multiOutputSdfOp.getBytesReadCoder(intReceiver));

    assertTrue(
        stringReceiver.getOutputCounters().values().stream()
            .anyMatch(ElementByteSizeCounter.class::isInstance));
    assertTrue(
        intReceiver.getOutputCounters().values().stream()
            .anyMatch(ElementByteSizeCounter.class::isInstance));

    // Process differently typed elements on main and side output receivers without
    // ClassCastException.
    WindowedValue<String> stringElem = WindowedValues.valueInGlobalWindow("hello");
    WindowedValue<Integer> intElem = WindowedValues.valueInGlobalWindow(12345);
    long expectedStringBytes =
        CoderUtils.encodeToByteArray(windowedStringCoder, stringElem, Coder.Context.NESTED).length;
    long expectedIntBytes =
        CoderUtils.encodeToByteArray(windowedIntCoder, intElem, Coder.Context.NESTED).length;

    assertEquals(0L, workExecutor.context().getSourceBytesProcessed());
    stringReceiver.process(stringElem);
    assertEquals(expectedStringBytes, workExecutor.context().getSourceBytesProcessed());
    intReceiver.process(intElem);

    streamingCounters
        .pendingDeltaCounters()
        .extractModifiedDeltaUpdates(DataflowCounterUpdateExtractor.INSTANCE);
    assertEquals(
        expectedStringBytes + expectedIntBytes, workExecutor.context().getSourceBytesProcessed());
  }
}
