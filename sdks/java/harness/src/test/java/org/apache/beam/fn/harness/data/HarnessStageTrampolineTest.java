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
package org.apache.beam.fn.harness.data;

import static org.apache.beam.sdk.values.WindowedValues.valueInGlobalWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.HandlesSplits;
import org.apache.beam.fn.harness.control.BundleProgressReporter;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardRunnerProtocols;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Labels;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link HarnessStageTrampoline} and {@link PCollectionConsumerRegistry} cuts. */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked", "SameNameButDifferent"})
public class HarnessStageTrampolineTest {

  private static final AtomicInteger MAX_HARNESS_FRAMES = new AtomicInteger();
  private ExecutionStateSampler sampler;

  @Before
  public void setUp() {
    MAX_HARNESS_FRAMES.set(0);
    sampler =
        new ExecutionStateSampler(PipelineOptionsFactory.create(), System::currentTimeMillis, null);
  }

  @After
  public void tearDown() {
    sampler.stop();
  }

  private static final class ProbeChainDoFn extends DoFn<String, String> {
    private final int step;
    private final boolean measureDepth;

    ProbeChainDoFn(int step, boolean measureDepth) {
      this.step = step;
      this.measureDepth = measureDepth;
    }

    @ProcessElement
    public void process(@Element String element, OutputReceiver<String> out) {
      if (measureDepth) {
        int frames =
            StackWalker.getInstance()
                .walk(
                    s ->
                        (int)
                            s.takeWhile(
                                    f ->
                                        !f.getClassName()
                                            .equals(ProcessBundleHandler.class.getName()))
                                .count());
        MAX_HARNESS_FRAMES.accumulateAndGet(frames, Math::max);
      }
      out.output(element + "." + step);
    }
  }

  private static ProcessBundleDescriptor build50StepBundleDescriptor(
      String[] inputTransformIdOut, String[] outputTransformIdOut) throws Exception {
    Pipeline p = Pipeline.create();
    org.apache.beam.sdk.values.PCollection<String> cur = p.apply("Create", Create.of("seed"));
    for (int i = 1; i <= 50; i++) {
      cur = cur.apply("Step_" + String.format("%02d", i), ParDo.of(new ProbeChainDoFn(i, i == 50)));
    }
    RunnerApi.Pipeline proto = PipelineTranslation.toProto(p);
    RunnerApi.Components.Builder comps = proto.getComponents().toBuilder();

    Map<String, String> stepToOutPc = new HashMap<>();
    String step1InputPc = null;
    Map<String, PTransform> parDoTransforms = new HashMap<>();
    for (Map.Entry<String, PTransform> e : proto.getComponents().getTransformsMap().entrySet()) {
      PTransform pt = e.getValue();
      if (PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(pt.getSpec().getUrn())
          && pt.getUniqueName().startsWith("Step_")) {
        parDoTransforms.put(e.getKey(), pt);
        String outPc = pt.getOutputsMap().values().iterator().next();
        stepToOutPc.put(pt.getUniqueName().substring(0, 7), outPc);
        if (pt.getUniqueName().startsWith("Step_01")) {
          step1InputPc = pt.getInputsMap().values().iterator().next();
        }
      }
    }
    String finalOutPc = stepToOutPc.get("Step_50");

    SdkComponents sdkComps = SdkComponents.create();
    WindowedValues.FullWindowedValueCoder<String> wvCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    String wvCoderId = sdkComps.registerCoder(wvCoder);
    comps.putAllCoders(sdkComps.toComponents().getCodersMap());

    ApiServiceDescriptor dataPort =
        ApiServiceDescriptor.newBuilder().setUrl("inproc://test").build();
    RemoteGrpcPort inPort =
        RemoteGrpcPort.newBuilder().setApiServiceDescriptor(dataPort).setCoderId(wvCoderId).build();
    RemoteGrpcPort outPort =
        RemoteGrpcPort.newBuilder().setApiServiceDescriptor(dataPort).setCoderId(wvCoderId).build();

    String inTransformId = "grpc_source";
    String outTransformId = "grpc_sink";
    inputTransformIdOut[0] = inTransformId;
    outputTransformIdOut[0] = outTransformId;

    PTransform sourceTransform =
        PTransform.newBuilder()
            .setUniqueName("grpc_source")
            .setSpec(
                RunnerApi.FunctionSpec.newBuilder()
                    .setUrn("beam:runner:source:v1")
                    .setPayload(inPort.toByteString()))
            .putOutputs("out", step1InputPc)
            .build();
    PTransform sinkTransform =
        PTransform.newBuilder()
            .setUniqueName("grpc_sink")
            .setSpec(
                RunnerApi.FunctionSpec.newBuilder()
                    .setUrn("beam:runner:sink:v1")
                    .setPayload(outPort.toByteString()))
            .putInputs("in", finalOutPc)
            .build();

    ProcessBundleDescriptor.Builder desc =
        ProcessBundleDescriptor.newBuilder()
            .putAllCoders(comps.getCodersMap())
            .putAllPcollections(comps.getPcollectionsMap())
            .putAllWindowingStrategies(comps.getWindowingStrategiesMap())
            .putAllEnvironments(comps.getEnvironmentsMap())
            .putTransforms(inTransformId, sourceTransform)
            .putTransforms(outTransformId, sinkTransform);
    for (Map.Entry<String, PTransform> e : parDoTransforms.entrySet()) {
      desc.putTransforms(e.getKey(), e.getValue());
    }
    return desc.build();
  }

  private static ProcessBundleHandler createHandler(
      PipelineOptions options, ProcessBundleDescriptor descriptor, ShortIdMap shortIds) {
    BeamFnDataClient dummyDataClient = mock(BeamFnDataClient.class);
    when(dummyDataClient.getOutboundObserver(any(), any())).thenReturn(mock(StreamObserver.class));
    BeamFnStateGrpcClientCache dummyStateCache = mock(BeamFnStateGrpcClientCache.class);
    return new ProcessBundleHandler(
        options,
        ImmutableSet.of(
            BeamUrns.getUrn(StandardRunnerProtocols.Enum.CONTROL_RESPONSE_ELEMENTS_EMBEDDING)),
        id -> descriptor,
        dummyDataClient,
        dummyStateCache,
        mock(FinalizeBundleHandler.class),
        shortIds,
        new ExecutionStateSampler(options, System::currentTimeMillis, null),
        Caches.noop(),
        null);
  }

  @Test
  public void testDeep50StepPipelineViaProcessBundleHandlerBoundsStackDepth() throws Exception {
    String[] inId = new String[1];
    String[] outId = new String[1];
    ProcessBundleDescriptor descriptor = build50StepBundleDescriptor(inId, outId);

    WindowedValues.FullWindowedValueCoder<String> wvCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    ByteStringOutputStream bout = new ByteStringOutputStream();
    for (String s : Arrays.asList("x1", "x2", "x3")) {
      wvCoder.encode(valueInGlobalWindow(s), bout);
    }
    Elements embeddedInput =
        Elements.newBuilder()
            .addData(
                Elements.Data.newBuilder()
                    .setInstructionId("inst-1")
                    .setTransformId(inId[0])
                    .setData(bout.toByteString())
                    .setIsLast(true))
            .build();
    InstructionRequest req =
        InstructionRequest.newBuilder()
            .setInstructionId("inst-1")
            .setProcessBundle(
                ProcessBundleRequest.newBuilder()
                    .setProcessBundleDescriptorId("desc-1")
                    .setElements(embeddedInput))
            .build();

    // 1. Default HarnessStageTrampoline active (threshold = 60)
    PipelineOptions defaultOpts = PipelineOptionsFactory.create();
    ProcessBundleHandler handler1 = createHandler(defaultOpts, descriptor, new ShortIdMap());
    BeamFnApi.InstructionResponse resp1 = handler1.processBundle(req).build();
    int framesWithDefaultTrampoline = MAX_HARNESS_FRAMES.get();
    handler1.shutdown();

    // 2. Tighter threshold = 12 to verify strict <= 22 frame cap
    MAX_HARNESS_FRAMES.set(0);
    PipelineOptions tightOpts = PipelineOptionsFactory.create();
    tightOpts
        .as(ExperimentalOptions.class)
        .setExperiments(Collections.singletonList("trampoline_fusion_threshold=12"));
    ProcessBundleHandler handlerTight = createHandler(tightOpts, descriptor, new ShortIdMap());
    BeamFnApi.InstructionResponse respTight = handlerTight.processBundle(req).build();
    int framesWithTightTrampoline = MAX_HARNESS_FRAMES.get();
    handlerTight.shutdown();

    // 3. Disabled HarnessStageTrampoline for comparison
    MAX_HARNESS_FRAMES.set(0);
    PipelineOptions disabledOpts = PipelineOptionsFactory.create();
    disabledOpts
        .as(ExperimentalOptions.class)
        .setExperiments(Collections.singletonList("disable_fusion_trampoline"));
    ProcessBundleHandler handler2 = createHandler(disabledOpts, descriptor, new ShortIdMap());
    BeamFnApi.InstructionResponse resp2 = handler2.processBundle(req).build();
    int framesWithoutTrampoline = MAX_HARNESS_FRAMES.get();
    handler2.shutdown();

    assertEquals(
        resp2.getProcessBundle().getElements().getDataList(),
        resp1.getProcessBundle().getElements().getDataList());
    assertEquals(
        resp2.getProcessBundle().getElements().getDataList(),
        respTight.getProcessBundle().getElements().getDataList());
    assertThat(framesWithDefaultTrampoline, lessThanOrEqualTo(115));
    assertThat(framesWithTightTrampoline, lessThanOrEqualTo(22));
    assertThat(framesWithoutTrampoline, greaterThan(145));
  }

  @Test
  public void testOver64CutsExtraMasksAnd100kHotKeyFanOut() throws Exception {
    HarnessStageTrampoline trampoline = new HarnessStageTrampoline();
    List<String> sink = new ArrayList<>();
    FnDataReceiver<WindowedValue<String>> cur = elem -> sink.add(elem.getValue());

    // Build 70 cuts in reverse topological order (exercising extraMasks > 64 cuts)
    for (int i = 70; i >= 1; i--) {
      final FnDataReceiver<WindowedValue<String>> next = cur;
      FnDataReceiver<WindowedValue<String>> cut = trampoline.createCut(next);
      cur = elem -> cut.accept(elem);
    }
    trampoline.freezeReverseTopologicalCuts();
    assertEquals(70, trampoline.cutCount());

    // Emit 100,000 elements in one loop before explicit drain()
    for (int i = 0; i < 100_000; i++) {
      cur.accept(valueInGlobalWindow("item-" + i));
    }
    trampoline.drain();

    assertFalse(trampoline.hasPending());
    assertEquals(100_000, sink.size());
    assertEquals("item-0", sink.get(0));
    assertEquals("item-99999", sink.get(99_999));
  }

  private static final class LazyHarnessIterable
      extends ElementByteSizeObservableIterable<String, ElementByteSizeObservableIterator<String>> {
    private final List<String> items;

    LazyHarnessIterable(List<String> items) {
      this.items = items;
    }

    @Override
    protected ElementByteSizeObservableIterator<String> createIterator() {
      final Iterator<String> it = items.iterator();
      return new ElementByteSizeObservableIterator<String>() {
        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public String next() {
          String v = it.next();
          notifyValueReturned(v.length() + 1L);
          return v;
        }
      };
    }
  }

  @Test
  public void testLazyByteSizeObserverAcrossFlattenAndTrampolineCut() throws Exception {
    SdkComponents sdkComps = SdkComponents.create();
    String iterCoderId = sdkComps.registerCoder(IterableCoder.of(StringUtf8Coder.of()));
    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder()
            .putPcollections(
                "pc_in", RunnerApi.PCollection.newBuilder().setCoderId(iterCoderId).build())
            .putPcollections(
                "pc_flat_out", RunnerApi.PCollection.newBuilder().setCoderId(iterCoderId).build())
            .putTransforms(
                "flatten_tx",
                PTransform.newBuilder()
                    .setUniqueName("Flatten")
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN))
                    .putInputs("in0", "pc_in")
                    .putOutputs("out", "pc_flat_out")
                    .build())
            .putAllCoders(sdkComps.toComponents().getCodersMap())
            .build();

    ShortIdMap shortIds = new ShortIdMap();
    BundleProgressReporter.InMemory reporter = new BundleProgressReporter.InMemory();
    HarnessStageTrampoline trampoline = new HarnessStageTrampoline();
    PCollectionConsumerRegistry registry =
        new PCollectionConsumerRegistry(
            sampler.create(), shortIds, reporter, descriptor, null, trampoline, 1);

    List<String> iterated = new ArrayList<>();
    registry.register(
        "pc_flat_out",
        "sink_tx",
        "Sink",
        (WindowedValue<Iterable<String>> elem) -> {
          for (String s : elem.getValue()) {
            iterated.add(s);
          }
        },
        10);

    // FlattenRunner pattern: registers pc_flat_out's multiplexing consumer directly on pc_in
    FnDataReceiver<WindowedValue<Iterable<String>>> flatOutConsumer =
        (FnDataReceiver) registry.getMultiplexingConsumer("pc_flat_out");
    assertEquals("Flatten output must not create a cut", 0, trampoline.cutCount());

    registry.register(
        "pc_in",
        "flatten_tx",
        "Flatten",
        flatOutConsumer,
        registry.getEffectiveDepth("pc_flat_out"));
    FnDataReceiver<WindowedValue<Iterable<String>>> pcInConsumer =
        (FnDataReceiver) registry.getMultiplexingConsumer("pc_in");
    assertEquals("Upstream of Flatten must create a cut", 1, trampoline.cutCount());
    trampoline.freezeReverseTopologicalCuts();

    pcInConsumer.accept(valueInGlobalWindow(new LazyHarnessIterable(Arrays.asList("aa", "bbbb"))));
    assertTrue(trampoline.hasPending());
    assertTrue(iterated.isEmpty());

    trampoline.drain();
    assertEquals(Arrays.asList("aa", "bbbb"), iterated);

    Map<String, ByteString> monData = new HashMap<>();
    reporter.updateFinalMonitoringData(monData);
    boolean foundPcInLazyBytes = false;
    for (MonitoringInfo mi : shortIds.toMonitoringInfo(monData)) {
      if (Urns.SAMPLED_BYTE_SIZE.equals(mi.getUrn())
          && "pc_in".equals(mi.getLabelsMap().get(Labels.PCOLLECTION))) {
        foundPcInLazyBytes = true;
        // IterableCoder header (4) + "aa" (3) + "bbbb" (5) = 12 bytes
        assertThat(mi.getPayload().size(), greaterThan(0));
      }
    }
    assertTrue("Expected SAMPLED_BYTE_SIZE metric for pc_in", foundPcInLazyBytes);
  }

  private interface SplittingReceiver
      extends FnDataReceiver<WindowedValue<String>>, HandlesSplits {}

  @Test
  public void testHandlesSplitsExcludesCutAndUserCodeExceptionTruncationAndReset()
      throws Exception {
    SdkComponents sdkComps = SdkComponents.create();
    String coderId = sdkComps.registerCoder(StringUtf8Coder.of());
    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder()
            .putPcollections(
                "pc_sdf", RunnerApi.PCollection.newBuilder().setCoderId(coderId).build())
            .putAllCoders(sdkComps.toComponents().getCodersMap())
            .build();

    HarnessStageTrampoline trampoline = new HarnessStageTrampoline();
    PCollectionConsumerRegistry registry =
        new PCollectionConsumerRegistry(
            sampler.create(),
            new ShortIdMap(),
            new BundleProgressReporter.InMemory(),
            descriptor,
            null,
            trampoline,
            1);

    SplittingReceiver sdfConsumer = mock(SplittingReceiver.class);
    registry.register("pc_sdf", "sdf_tx", "Sdf", sdfConsumer, 20);
    FnDataReceiver<?> wrappedSdf = registry.getMultiplexingConsumer("pc_sdf");
    assertThat(wrappedSdf, instanceOf(HandlesSplits.class));
    assertEquals(0, trampoline.cutCount());

    // Verify UserCodeException wrapping & clean reset on failure
    HarnessStageTrampoline errTrampoline = new HarnessStageTrampoline();
    List<String> okOut = new ArrayList<>();
    FnDataReceiver<WindowedValue<String>> cut =
        errTrampoline.createCut(
            elem -> {
              if ("boom".equals(elem.getValue())) {
                throw new IllegalArgumentException("downstream-boom");
              }
              okOut.add(elem.getValue());
            });
    errTrampoline.freezeReverseTopologicalCuts();

    cut.accept(valueInGlobalWindow("boom"));
    cut.accept(valueInGlobalWindow("unreachable-in-aborted-bundle"));
    UserCodeException ex = assertThrows(UserCodeException.class, errTrampoline::drain);
    assertEquals("downstream-boom", ex.getCause().getMessage());
    assertFalse("Trampoline must reset pending state on failure", errTrampoline.hasPending());

    // Subsequent bundle reuse succeeds cleanly
    cut.accept(valueInGlobalWindow("recovered"));
    errTrampoline.drain();
    assertEquals(Collections.singletonList("recovered"), okOut);
  }

  @Test
  public void testCompiledMixedMapFilterAndParDoChainReportsAllEdgeMetrics() throws Exception {
    Pipeline p = Pipeline.create();
    p.apply("Create", Create.of("seed"))
        .apply("S1_DoFn", ParDo.of(new ProbeChainDoFn(1, false)))
        .apply("S2_Filter", Filter.by(s -> !s.startsWith("drop")))
        .apply("S3_Map", MapElements.into(TypeDescriptors.strings()).via(s -> s + ":mapped"))
        .apply("S4_DoFn", ParDo.of(new ProbeChainDoFn(4, false)));

    RunnerApi.Pipeline proto = PipelineTranslation.toProto(p);
    RunnerApi.Components.Builder comps = proto.getComponents().toBuilder();
    Map<String, PTransform> leafTransforms = new HashMap<>();
    String firstInPc = null;
    String lastOutPc = null;
    List<String> allStagePCollections = new ArrayList<>();
    for (Map.Entry<String, PTransform> e : proto.getComponents().getTransformsMap().entrySet()) {
      PTransform pt = e.getValue();
      if (pt.getSubtransformsCount() == 0
          && PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(pt.getSpec().getUrn())
          && (pt.getUniqueName().startsWith("S1_")
              || pt.getUniqueName().startsWith("S2_")
              || pt.getUniqueName().startsWith("S3_")
              || pt.getUniqueName().startsWith("S4_"))) {
        leafTransforms.put(e.getKey(), pt);
        String outPc = pt.getOutputsMap().values().iterator().next();
        allStagePCollections.add(outPc);
        if (pt.getUniqueName().startsWith("S1_")) {
          firstInPc = pt.getInputsMap().values().iterator().next();
          allStagePCollections.add(firstInPc);
        }
        if (pt.getUniqueName().startsWith("S4_")) {
          lastOutPc = outPc;
        }
      }
    }

    SdkComponents sdkComps = SdkComponents.create();
    WindowedValues.FullWindowedValueCoder<String> wvCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    String wvCoderId = sdkComps.registerCoder(wvCoder);
    comps.putAllCoders(sdkComps.toComponents().getCodersMap());

    ApiServiceDescriptor dataPort =
        ApiServiceDescriptor.newBuilder().setUrl("inproc://compiled-test").build();
    RemoteGrpcPort port =
        RemoteGrpcPort.newBuilder().setApiServiceDescriptor(dataPort).setCoderId(wvCoderId).build();

    ProcessBundleDescriptor.Builder descBuilder =
        ProcessBundleDescriptor.newBuilder()
            .putAllCoders(comps.getCodersMap())
            .putAllPcollections(comps.getPcollectionsMap())
            .putAllWindowingStrategies(comps.getWindowingStrategiesMap())
            .putAllEnvironments(comps.getEnvironmentsMap())
            .putTransforms(
                "src",
                PTransform.newBuilder()
                    .setUniqueName("src")
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:runner:source:v1")
                            .setPayload(port.toByteString()))
                    .putOutputs("out", firstInPc)
                    .build())
            .putTransforms(
                "snk",
                PTransform.newBuilder()
                    .setUniqueName("snk")
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:runner:sink:v1")
                            .setPayload(port.toByteString()))
                    .putInputs("in", lastOutPc)
                    .build());
    for (Map.Entry<String, PTransform> e : leafTransforms.entrySet()) {
      descBuilder.putTransforms(e.getKey(), e.getValue());
    }
    ProcessBundleDescriptor descriptor = descBuilder.build();

    ByteStringOutputStream bout = new ByteStringOutputStream();
    for (String s : Arrays.asList("okA", "dropB", "okC")) {
      wvCoder.encode(valueInGlobalWindow(s), bout);
    }
    Elements embeddedInput =
        Elements.newBuilder()
            .addData(
                Elements.Data.newBuilder()
                    .setInstructionId("inst-c1")
                    .setTransformId("src")
                    .setData(bout.toByteString())
                    .setIsLast(true))
            .build();
    InstructionRequest req1 =
        InstructionRequest.newBuilder()
            .setInstructionId("inst-c1")
            .setProcessBundle(
                ProcessBundleRequest.newBuilder()
                    .setProcessBundleDescriptorId("desc-compiled")
                    .setElements(embeddedInput))
            .build();

    // Default minCodegenSteps (= 6) leaves short 4-step chains uncompiled
    long classesBeforeShort = FusedSegmentCompiler.generatedClassCount();
    ProcessBundleHandler defaultShortHandler =
        createHandler(PipelineOptionsFactory.create(), descriptor, new ShortIdMap());
    defaultShortHandler.processBundle(req1);
    assertEquals(classesBeforeShort, FusedSegmentCompiler.generatedClassCount());
    defaultShortHandler.shutdown();

    PipelineOptions compileShortOpts = PipelineOptionsFactory.create();
    compileShortOpts
        .as(ExperimentalOptions.class)
        .setExperiments(Collections.singletonList("codegen_min_chain_steps=2"));
    ShortIdMap shortIds = new ShortIdMap();
    ProcessBundleHandler handler = createHandler(compileShortOpts, descriptor, shortIds);
    BeamFnApi.InstructionResponse resp = handler.processBundle(req1).build();
    assertThat(FusedSegmentCompiler.generatedClassCount(), greaterThan(classesBeforeShort));
    long classesAfterBundle1 = FusedSegmentCompiler.generatedClassCount();

    // Run a second bundle on a fresh handler to verify generated ByteBuddy class cache hit
    ProcessBundleHandler handler2 = createHandler(compileShortOpts, descriptor, new ShortIdMap());
    BeamFnApi.InstructionResponse resp2 = handler2.processBundle(req1).build();
    assertEquals(classesAfterBundle1, FusedSegmentCompiler.generatedClassCount());

    // Also compare against disable_codegen_fused_segments
    PipelineOptions noCodegenOpts = PipelineOptionsFactory.create();
    noCodegenOpts
        .as(ExperimentalOptions.class)
        .setExperiments(Collections.singletonList(FusedSegmentCompiler.DISABLE_CODEGEN_EXPERIMENT));
    ProcessBundleHandler handlerNoCodegen =
        createHandler(noCodegenOpts, descriptor, new ShortIdMap());
    BeamFnApi.InstructionResponse respNoCodegen = handlerNoCodegen.processBundle(req1).build();

    assertEquals(
        respNoCodegen.getProcessBundle().getElements().getDataList(),
        resp.getProcessBundle().getElements().getDataList());
    assertEquals(
        respNoCodegen.getProcessBundle().getElements().getDataList(),
        resp2.getProcessBundle().getElements().getDataList());

    // Every intermediate PCollection in the fused chain must still report ELEMENT_COUNT!
    List<String> reportedElementCountPcs = new ArrayList<>();
    for (MonitoringInfo mi : resp.getProcessBundle().getMonitoringInfosList()) {
      if (Urns.ELEMENT_COUNT.equals(mi.getUrn())) {
        reportedElementCountPcs.add(mi.getLabelsMap().get(Labels.PCOLLECTION));
      }
    }
    for (MonitoringInfo mi :
        shortIds.toMonitoringInfo(resp.getProcessBundle().getMonitoringDataMap())) {
      if (Urns.ELEMENT_COUNT.equals(mi.getUrn())) {
        reportedElementCountPcs.add(mi.getLabelsMap().get(Labels.PCOLLECTION));
      }
    }
    for (String pc : allStagePCollections) {
      assertTrue(
          "Expected ELEMENT_COUNT MonitoringInfo on fused PCollection " + pc,
          reportedElementCountPcs.contains(pc));
    }

    handler.shutdown();
    handler2.shutdown();
    handlerNoCodegen.shutdown();
  }

  @Test
  public void testDownstreamCutDrainsDuringOneToManyWhileUpstreamCutIsActive() throws Exception {
    AtomicInteger produced = new AtomicInteger();
    AtomicInteger consumed = new AtomicInteger();
    AtomicInteger maxInFlight = new AtomicInteger();

    HarnessStageTrampoline trampoline = new HarnessStageTrampoline();
    // Register in post-order (sink-to-root): cut1 (downstream) first, cut0 (upstream) second
    FnDataReceiver<WindowedValue<Integer>> cut1 =
        trampoline.createCut(
            elem -> {
              int curInFlight = produced.get() - consumed.incrementAndGet();
              maxInFlight.accumulateAndGet(curInFlight, Math::max);
            });
    FnDataReceiver<WindowedValue<String>> cut0 =
        trampoline.createCut(
            elem -> {
              for (int i = 0; i < 2000; i++) {
                produced.incrementAndGet();
                cut1.accept(valueInGlobalWindow(i));
              }
            });
    trampoline.freezeReverseTopologicalCuts();

    cut0.accept(valueInGlobalWindow("trigger"));
    trampoline.drain();
    assertEquals(2000, consumed.get());
    assertThat(maxInFlight.get(), lessThanOrEqualTo(256));
  }

  private static final class BadSkewDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.outputWithTimestamp(c.element(), c.timestamp().minus(org.joda.time.Duration.millis(10)));
    }
  }

  private static final org.apache.beam.sdk.values.TupleTag<String> SIDE_ONLY_MAIN_TAG =
      new org.apache.beam.sdk.values.TupleTag<>("mainTag");
  private static final org.apache.beam.sdk.values.TupleTag<String> SIDE_ONLY_SIDE_TAG =
      new org.apache.beam.sdk.values.TupleTag<>("sideTag");

  private static final class SideOnlyOutputDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.output(SIDE_ONLY_SIDE_TAG, "side-kept:" + c.element());
    }
  }

  @Test
  public void testCompiledSegmentEnforcesTimestampSkewAndSkipsSideOnlySingleOutput()
      throws Exception {
    // 1. Compiled DoFn calling outputWithTimestamp(earlierTs) must throw IllegalArgumentException
    Pipeline p = Pipeline.create();
    p.apply("src", Create.of("a").withCoder(StringUtf8Coder.of()))
        .apply("badTs", ParDo.of(new BadSkewDoFn()))
        .apply("s2", MapElements.into(TypeDescriptors.strings()).via((String s) -> s + "!"));

    SdkComponents sdkComponents = SdkComponents.create(PipelineOptionsFactory.create());
    RunnerApi.Pipeline proto = PipelineTranslation.toProto(p, sdkComponents);
    RunnerApi.Components comps = proto.getComponents();
    Map<String, PTransform> leafTransforms = new HashMap<>();
    String firstInPc = null;
    String lastOutPc = null;
    for (Map.Entry<String, PTransform> e : comps.getTransformsMap().entrySet()) {
      PTransform pt = e.getValue();
      if (pt.getSubtransformsCount() == 0
          && PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(pt.getSpec().getUrn())
          && (pt.getUniqueName().startsWith("badTs") || pt.getUniqueName().startsWith("s2"))) {
        leafTransforms.put(e.getKey(), pt);
      }
    }
    for (PTransform pt : leafTransforms.values()) {
      String inPc = pt.getInputsMap().values().iterator().next();
      boolean producedByLeaf = false;
      for (PTransform other : leafTransforms.values()) {
        if (other.getOutputsMap().containsValue(inPc)) {
          producedByLeaf = true;
          break;
        }
      }
      if (!producedByLeaf) {
        firstInPc = inPc;
      }
      String outPc = pt.getOutputsMap().values().iterator().next();
      boolean consumedByLeaf = false;
      for (PTransform other : leafTransforms.values()) {
        if (other.getInputsMap().containsValue(outPc)) {
          consumedByLeaf = true;
          break;
        }
      }
      if (!consumedByLeaf) {
        lastOutPc = outPc;
      }
    }

    WindowedValues.FullWindowedValueCoder<String> wvCoder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    String wvCoderId = sdkComponents.registerCoder(wvCoder);
    RunnerApi.Components compsWithWv =
        comps.toBuilder().putAllCoders(sdkComponents.toComponents().getCodersMap()).build();
    RemoteGrpcPort port =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(ApiServiceDescriptor.newBuilder().setUrl("test-url"))
            .setCoderId(wvCoderId)
            .build();
    ProcessBundleDescriptor.Builder descBuilder =
        ProcessBundleDescriptor.newBuilder()
            .setId("desc-skew")
            .putAllCoders(compsWithWv.getCodersMap())
            .putAllPcollections(compsWithWv.getPcollectionsMap())
            .putAllWindowingStrategies(compsWithWv.getWindowingStrategiesMap())
            .putAllEnvironments(compsWithWv.getEnvironmentsMap())
            .putTransforms(
                "src",
                PTransform.newBuilder()
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:runner:source:v1")
                            .setPayload(port.toByteString()))
                    .putOutputs("out", firstInPc)
                    .build())
            .putTransforms(
                "sink",
                PTransform.newBuilder()
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:runner:sink:v1")
                            .setPayload(port.toByteString()))
                    .putInputs("in", lastOutPc)
                    .build());
    for (Map.Entry<String, PTransform> e : leafTransforms.entrySet()) {
      descBuilder.putTransforms(e.getKey(), e.getValue());
    }

    ByteStringOutputStream bout = new ByteStringOutputStream();
    wvCoder.encode(
        WindowedValues.timestampedValueInGlobalWindow("a", new org.joda.time.Instant(1000L)), bout);
    InstructionRequest req =
        InstructionRequest.newBuilder()
            .setInstructionId("inst-skew")
            .setProcessBundle(
                ProcessBundleRequest.newBuilder()
                    .setProcessBundleDescriptorId("desc-skew")
                    .setElements(
                        Elements.newBuilder()
                            .addData(
                                Elements.Data.newBuilder()
                                    .setInstructionId("inst-skew")
                                    .setTransformId("src")
                                    .setData(bout.toByteString())
                                    .setIsLast(true))))
            .build();

    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.as(ExperimentalOptions.class)
        .setExperiments(Collections.singletonList("codegen_min_chain_steps=2"));
    ProcessBundleHandler handler = createHandler(opts, descBuilder.build(), new ShortIdMap());
    Exception ex = assertThrows(Exception.class, () -> handler.processBundle(req));
    Throwable cause = ex;
    boolean sawSkewIllegalArg = false;
    while (cause != null) {
      if (cause instanceof IllegalArgumentException
          && cause.getMessage() != null
          && cause.getMessage().contains("allowed skew")) {
        sawSkewIllegalArg = true;
        break;
      }
      cause = cause.getCause();
    }
    assertTrue("Expected timestamp skew IllegalArgumentException, got: " + ex, sawSkewIllegalArg);
    handler.shutdown();

    // 2. Single-output ParDo whose sole wired output is a side tag (main output pruned) must NOT
    // be compiled as main-output linear segment and must deliver side-tag outputs intact.
    Pipeline sideP = Pipeline.create();
    sideP
        .apply("src", Create.of("x").withCoder(StringUtf8Coder.of()))
        .apply(
            "sideParDo",
            ParDo.of(new SideOnlyOutputDoFn())
                .withOutputTags(
                    SIDE_ONLY_MAIN_TAG,
                    org.apache.beam.sdk.values.TupleTagList.of(SIDE_ONLY_SIDE_TAG)))
        .get(SIDE_ONLY_SIDE_TAG)
        .setCoder(StringUtf8Coder.of())
        .apply("tailMap", MapElements.into(TypeDescriptors.strings()).via((String s) -> s + "!"));
    SdkComponents sideSdk = SdkComponents.create(PipelineOptionsFactory.create());
    RunnerApi.Pipeline sideProto = PipelineTranslation.toProto(sideP, sideSdk);
    RunnerApi.Components sideComps = sideProto.getComponents();
    Map<String, PTransform> sideLeaves = new HashMap<>();
    String sideInPc = null;
    String sideOutPc = null;
    for (Map.Entry<String, PTransform> e : sideComps.getTransformsMap().entrySet()) {
      PTransform pt = e.getValue();
      if (pt.getSubtransformsCount() == 0
          && PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(pt.getSpec().getUrn())
          && (pt.getUniqueName().startsWith("sideParDo")
              || pt.getUniqueName().startsWith("tailMap"))) {
        // Prune mainTag from sideParDo outputs to simulate runner pruning an unconsumed main output
        if (pt.getOutputsMap().containsKey(SIDE_ONLY_MAIN_TAG.getId())
            && pt.getOutputsMap().containsKey(SIDE_ONLY_SIDE_TAG.getId())) {
          PTransform.Builder pruned = pt.toBuilder().clearOutputs();
          pruned.putOutputs(
              SIDE_ONLY_SIDE_TAG.getId(), pt.getOutputsOrThrow(SIDE_ONLY_SIDE_TAG.getId()));
          pt = pruned.build();
          sideInPc = pt.getInputsMap().values().iterator().next();
        } else {
          sideOutPc = pt.getOutputsMap().values().iterator().next();
        }
        sideLeaves.put(e.getKey(), pt);
      }
    }
    String sideWvCoderId = sideSdk.registerCoder(wvCoder);
    RunnerApi.Components sideCompsWithWv =
        sideComps.toBuilder().putAllCoders(sideSdk.toComponents().getCodersMap()).build();
    RemoteGrpcPort sidePort =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(ApiServiceDescriptor.newBuilder().setUrl("test-url"))
            .setCoderId(sideWvCoderId)
            .build();
    ProcessBundleDescriptor.Builder sideDesc =
        ProcessBundleDescriptor.newBuilder()
            .setId("desc-side-only")
            .putAllCoders(sideCompsWithWv.getCodersMap())
            .putAllPcollections(sideCompsWithWv.getPcollectionsMap())
            .putAllWindowingStrategies(sideCompsWithWv.getWindowingStrategiesMap())
            .putAllEnvironments(sideCompsWithWv.getEnvironmentsMap())
            .putTransforms(
                "src",
                PTransform.newBuilder()
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:runner:source:v1")
                            .setPayload(sidePort.toByteString()))
                    .putOutputs("out", sideInPc)
                    .build())
            .putTransforms(
                "sink",
                PTransform.newBuilder()
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:runner:sink:v1")
                            .setPayload(sidePort.toByteString()))
                    .putInputs("in", sideOutPc)
                    .build());
    for (Map.Entry<String, PTransform> e : sideLeaves.entrySet()) {
      sideDesc.putTransforms(e.getKey(), e.getValue());
    }
    ByteStringOutputStream sideInBytes = new ByteStringOutputStream();
    wvCoder.encode(valueInGlobalWindow("x"), sideInBytes);
    InstructionRequest sideReq =
        InstructionRequest.newBuilder()
            .setInstructionId("inst-side")
            .setProcessBundle(
                ProcessBundleRequest.newBuilder()
                    .setProcessBundleDescriptorId("desc-side-only")
                    .setElements(
                        Elements.newBuilder()
                            .addData(
                                Elements.Data.newBuilder()
                                    .setInstructionId("inst-side")
                                    .setTransformId("src")
                                    .setData(sideInBytes.toByteString())
                                    .setIsLast(true))))
            .build();
    ProcessBundleHandler sideHandler = createHandler(opts, sideDesc.build(), new ShortIdMap());
    BeamFnApi.InstructionResponse sideResp = sideHandler.processBundle(sideReq).build();
    ByteString sideData = sideResp.getProcessBundle().getElements().getData(0).getData();
    WindowedValue<String> decoded = wvCoder.decode(sideData.newInput());
    assertEquals("side-kept:x!", decoded.getValue());
    sideHandler.shutdown();
  }
}
