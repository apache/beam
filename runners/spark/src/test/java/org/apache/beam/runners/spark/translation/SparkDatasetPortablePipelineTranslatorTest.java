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

import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getInputId;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.getOutputId;
import static org.apache.beam.sdk.values.WindowedValues.valueInGlobalWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.spark.SparkContextRule;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.util.construction.graph.TrivialNativeTransformExpander;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.spark.sql.Dataset;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link SparkDatasetPortablePipelineTranslator}. Runner-side transforms are
 * translated on injected Datasets. Executable stages run in the embedded SDK harness.
 */
@RunWith(JUnit4.class)
public class SparkDatasetPortablePipelineTranslatorTest implements Serializable {

  @ClassRule public static SparkContextRule contextRule = new SparkContextRule("local[2]");

  private transient SparkPipelineOptions options;
  private transient SparkDatasetPortablePipelineTranslator translator;
  private transient SparkDatasetTranslationContext context;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setUseStructuredStreaming(true);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    MetricsAccumulator.clear();
    MetricsAccumulator.init(options, contextRule.getSparkContext());
    translator = new SparkDatasetPortablePipelineTranslator();
    context =
        translator.createTranslationContext(
            contextRule.getSparkContext(),
            options,
            JobInfo.create("job", "job", "token", PipelineOptionsTranslation.toProto(options)));
  }

  @After
  public void tearDown() {
    context.getSparkSession().sharedState().cacheManager().clearCache();
    MetricsAccumulator.clear();
  }

  @Test
  public void impulseIsOneEmptyElementInTheGlobalWindow() {
    Pipeline p = Pipeline.create(options);
    p.apply("impulse", Impulse.create());
    RunnerApi.Pipeline pipeline = PipelineTranslation.toProto(p);

    translator.translate(pipeline, context);

    List<WindowedValue<byte[]>> result = collect(getOutputId(transformNamed(pipeline, "impulse")));
    assertEquals(1, result.size());
    assertArrayEquals(new byte[0], result.get(0).getValue());
    assertEquals(
        Collections.singletonList(GlobalWindow.INSTANCE),
        new ArrayList<>(result.get(0).getWindows()));
  }

  @Test
  public void flattenReEncodesInputsWithAnotherCoder() {
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, Long>> plain =
        p.apply("impulse", Impulse.create())
            .apply("plain", ParDo.of(new Placeholder<KV<String, Long>>()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));
    // Same Java type as the first input, different encoding.
    PCollection<KV<String, Long>> nullable =
        p.apply("impulse2", Impulse.create())
            .apply("nullable", ParDo.of(new Placeholder<KV<String, Long>>()))
            .setCoder(KvCoder.of(NullableCoder.of(StringUtf8Coder.of()), VarLongCoder.of()));
    PCollectionList.of(plain).and(nullable).apply("flatten", Flatten.pCollections());
    RunnerApi.Pipeline pipeline = PipelineTranslation.toProto(p);
    PTransformNode flatten = transformNamed(pipeline, "flatten");
    inject(
        getOutputId(transformNamed(pipeline, "plain")),
        pipeline,
        Arrays.asList(valueInGlobalWindow(KV.of("a", 1L)), valueInGlobalWindow(KV.of("b", 2L))));
    inject(
        getOutputId(transformNamed(pipeline, "nullable")),
        pipeline,
        Collections.singletonList(valueInGlobalWindow(KV.of("c", 3L))));

    SparkDatasetPortablePipelineTranslator.translateFlatten(flatten, pipeline, context);

    assertThat(
        values(collect(getOutputId(flatten))),
        containsInAnyOrder(KV.of("a", 1L), KV.of("b", 2L), KV.of("c", 3L)));
  }

  @Test
  public void groupByKeyGroupsPerKeyAndWindow() {
    Pipeline p = Pipeline.create(options);
    p.apply("impulse", Impulse.create())
        .apply("kv", ParDo.of(new Placeholder<KV<String, Long>>()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
        .apply("window", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply("gbk", GroupByKey.create());
    RunnerApi.Pipeline pipeline = PipelineTranslation.toProto(p);
    PTransformNode gbk = transformNamed(pipeline, "gbk");
    IntervalWindow first = new IntervalWindow(new Instant(0), Duration.standardSeconds(10));
    IntervalWindow second = new IntervalWindow(new Instant(10_000), Duration.standardSeconds(10));
    inject(
        getInputId(gbk),
        pipeline,
        Arrays.asList(
            WindowedValues.of(KV.of("a", 1L), new Instant(1_000), first, PaneInfo.NO_FIRING),
            WindowedValues.of(KV.of("a", 2L), new Instant(2_000), first, PaneInfo.NO_FIRING),
            WindowedValues.of(KV.of("b", 3L), new Instant(3_000), first, PaneInfo.NO_FIRING),
            WindowedValues.of(KV.of("a", 4L), new Instant(14_000), second, PaneInfo.NO_FIRING)));

    SparkDatasetPortablePipelineTranslator.translateGroupByKey(gbk, pipeline, context);

    List<String> groups = new ArrayList<>();
    for (WindowedValue<KV<String, Iterable<Long>>> group :
        this.<KV<String, Iterable<Long>>>collect(getOutputId(gbk))) {
      List<Long> values = new ArrayList<>();
      group.getValue().getValue().forEach(values::add);
      Collections.sort(values);
      groups.add(
          group.getValue().getKey()
              + values
              + Iterables.getOnlyElement(group.getWindows())
              + "@"
              + group.getTimestamp());
    }
    assertThat(
        groups,
        containsInAnyOrder(
            "a[1, 2]" + first + "@" + first.maxTimestamp(),
            "b[3]" + first + "@" + first.maxTimestamp(),
            "a[4]" + second + "@" + second.maxTimestamp()));
  }

  @Test
  public void reshuffleRepartitionsAndKeepsEveryElement() {
    Pipeline p = Pipeline.create(options);
    p.apply("impulse", Impulse.create())
        .apply("kv", ParDo.of(new Placeholder<KV<String, Long>>()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
        .apply("reshuffle", Reshuffle.of());
    RunnerApi.Pipeline pipeline = PipelineTranslation.toProto(p);
    PTransformNode reshuffle = transformNamed(pipeline, "reshuffle");
    inject(
        getInputId(reshuffle),
        pipeline,
        Arrays.asList(
            valueInGlobalWindow(KV.of("a", 1L)),
            valueInGlobalWindow(KV.of("a", 2L)),
            valueInGlobalWindow(KV.of("b", 3L))));

    SparkDatasetPortablePipelineTranslator.translateReshuffle(reshuffle, pipeline, context);

    Dataset<WindowedValue<KV<String, Long>>> output = context.getDataset(getOutputId(reshuffle));
    assertEquals(
        contextRule.getSparkContext().defaultParallelism().intValue(),
        output.rdd().getNumPartitions());
    assertThat(
        values(output.collectAsList()),
        containsInAnyOrder(KV.of("a", 1L), KV.of("a", 2L), KV.of("b", 3L)));
  }

  @Test
  public void executableStageOutputsAreDemultiplexedPerTag() {
    TupleTag<KV<String, String>> words = new TupleTag<KV<String, String>>("words") {};
    TupleTag<KV<String, Long>> lengths = new TupleTag<KV<String, Long>>("lengths") {};
    Pipeline p = Pipeline.create(options);
    PCollectionTuple outputs =
        p.apply("impulse", Impulse.create())
            .apply(
                "split",
                ParDo.of(
                        new DoFn<byte[], KV<String, String>>() {
                          @ProcessElement
                          public void process(MultiOutputReceiver out) {
                            for (String word : Arrays.asList("one", "three")) {
                              out.get(words).output(KV.of("word", word));
                              out.get(lengths).output(KV.of("length", (long) word.length()));
                            }
                          }
                        })
                    .withOutputTags(words, TupleTagList.of(lengths)));
    // A stage only emits outputs that a runner-side transform reads. GroupByKey is one.
    outputs.get(words).apply("groupWords", GroupByKey.create());
    outputs.get(lengths).apply("groupLengths", GroupByKey.create());
    RunnerApi.Pipeline pipeline = fused(p);
    assertEquals(2, onlyStage(pipeline).getOutputsCount());

    translator.translate(pipeline, context);

    Map<String, String> split = transformNamed(pipeline, "split").getTransform().getOutputsMap();
    assertThat(
        values(collect(split.get(words.getId()))),
        containsInAnyOrder(KV.of("word", "one"), KV.of("word", "three")));
    assertThat(
        values(collect(split.get(lengths.getId()))),
        containsInAnyOrder(KV.of("length", 3L), KV.of("length", 5L)));
  }

  @Test
  public void sideInputsAreBroadcastToTheStage() {
    Pipeline p = Pipeline.create(options);
    PCollectionView<Iterable<String>> view =
        p.apply("impulse", Impulse.create())
            .apply(
                "words",
                ParDo.of(
                    new DoFn<byte[], String>() {
                      @ProcessElement
                      public void process(OutputReceiver<String> out) {
                        out.output("one");
                        out.output("three");
                      }
                    }))
            .apply("view", View.asIterable());
    p.apply("impulse2", Impulse.create())
        .apply(
            "total",
            ParDo.of(
                    new DoFn<byte[], KV<String, Long>>() {
                      @ProcessElement
                      public void process(ProcessContext c) {
                        long total = 0;
                        for (String word : c.sideInput(view)) {
                          total += word.length();
                        }
                        c.output(KV.of("total", total));
                      }
                    })
                .withSideInputs(view))
        .apply("groupTotal", GroupByKey.create());
    RunnerApi.Pipeline pipeline = fused(p);

    translator.translate(pipeline, context);

    assertThat(
        values(collect(getOutputId(transformNamed(pipeline, "total")))),
        containsInAnyOrder(KV.of("total", 8L)));
  }

  @Test
  public void unboundedInputIsRejected() {
    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putPcollections(
                        "unbounded",
                        RunnerApi.PCollection.newBuilder()
                            .setIsBounded(RunnerApi.IsBounded.Enum.UNBOUNDED)
                            .build()))
            .build();

    UnsupportedOperationException thrown =
        assertThrows(
            UnsupportedOperationException.class, () -> translator.translate(pipeline, context));
    assertThat(thrown.getMessage(), containsString("unbounded input"));
  }

  @Test
  public void stageWithUserStateIsRejected() {
    ExecutableStagePayload payload =
        ExecutableStagePayload.newBuilder()
            .setInput("input")
            .addUserStates(
                ExecutableStagePayload.UserStateId.newBuilder()
                    .setTransformId("pardo")
                    .setLocalName("count"))
            .build();

    UnsupportedOperationException thrown =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                SparkDatasetPortablePipelineTranslator.translateExecutableStage(
                    stage(payload), RunnerApi.Pipeline.getDefaultInstance(), context));
    assertThat(thrown.getMessage(), containsString("state or timers"));
  }

  @Test
  public void stageWithTimersIsRejected() {
    ExecutableStagePayload payload =
        ExecutableStagePayload.newBuilder()
            .setInput("input")
            .addTimers(
                ExecutableStagePayload.TimerId.newBuilder()
                    .setTransformId("pardo")
                    .setLocalName("expiry"))
            .build();

    UnsupportedOperationException thrown =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                SparkDatasetPortablePipelineTranslator.translateExecutableStage(
                    stage(payload), RunnerApi.Pipeline.getDefaultInstance(), context));
    assertThat(thrown.getMessage(), containsString("state or timers"));
  }

  /** A DoFn that only gives its output PCollection a coder. It is never translated or run. */
  private static class Placeholder<T> extends DoFn<byte[], T> {
    @ProcessElement
    public void process() {}
  }

  private RunnerApi.Pipeline fused(Pipeline p) {
    RunnerApi.Pipeline pipeline =
        TrivialNativeTransformExpander.forKnownUrns(
            PipelineTranslation.toProto(p), translator.knownUrns());
    return GreedyPipelineFuser.fuse(pipeline).toPipeline();
  }

  private static PTransformNode transformNamed(RunnerApi.Pipeline pipeline, String name) {
    for (Map.Entry<String, RunnerApi.PTransform> transform :
        pipeline.getComponents().getTransformsMap().entrySet()) {
      if (name.equals(transform.getValue().getUniqueName())) {
        return PipelineNode.pTransform(transform.getKey(), transform.getValue());
      }
    }
    throw new IllegalArgumentException("No transform named " + name);
  }

  private static RunnerApi.PTransform onlyStage(RunnerApi.Pipeline pipeline) {
    return Iterables.getOnlyElement(
        pipeline.getComponents().getTransformsMap().values().stream()
            .filter(transform -> ExecutableStage.URN.equals(transform.getSpec().getUrn()))
            .collect(Collectors.toList()));
  }

  private static PTransformNode stage(ExecutableStagePayload payload) {
    return PipelineNode.pTransform(
        "stage",
        RunnerApi.PTransform.newBuilder()
            .putInputs("input", payload.getInput())
            .setSpec(
                RunnerApi.FunctionSpec.newBuilder()
                    .setUrn(ExecutableStage.URN)
                    .setPayload(payload.toByteString()))
            .build());
  }

  /** Registers {@code values} as the Dataset of a PCollection, in a single partition. */
  private <T> void inject(
      String pCollectionId, RunnerApi.Pipeline pipeline, List<WindowedValue<T>> values) {
    Dataset<WindowedValue<T>> dataset =
        context
            .getSparkSession()
            .createDataset(values, context.windowedEncoder(pCollectionId, pipeline.getComponents()))
            .coalesce(1);
    context.putDataset(pCollectionId, dataset);
  }

  private <T> List<WindowedValue<T>> collect(String pCollectionId) {
    return context.<T>getDataset(pCollectionId).collectAsList();
  }

  private static <T> List<T> values(List<WindowedValue<T>> windowedValues) {
    return windowedValues.stream().map(WindowedValue::getValue).collect(Collectors.toList());
  }
}
