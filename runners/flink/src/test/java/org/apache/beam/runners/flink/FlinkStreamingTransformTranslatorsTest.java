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
package org.apache.beam.runners.flink;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.bounded.FlinkBoundedSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** Tests for Flink streaming transform translators. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class FlinkStreamingTransformTranslatorsTest {

  @Test
  public void readSourceTranslatorBoundedWithMaxParallelism() {

    final int maxParallelism = 6;
    final int parallelism = 2;

    SplittableParDo.PrimitiveBoundedRead<String> transform =
        new SplittableParDo.PrimitiveBoundedRead<>(
            Read.from(new TestBoundedSource(maxParallelism)));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    env.setMaxParallelism(maxParallelism);

    Object sourceTransform =
        applyReadSourceTransform(transform, PCollection.IsBounded.BOUNDED, env);

    FlinkBoundedSource<?> source =
        (FlinkBoundedSource<?>) ((SourceTransformation<?, ?, ?>) sourceTransform).getSource();

    assertEquals(maxParallelism, source.getNumSplits());
  }

  @Test
  public void readSourceTranslatorBoundedWithoutMaxParallelism() {

    final int parallelism = 2;

    SplittableParDo.PrimitiveBoundedRead<String> transform =
        new SplittableParDo.PrimitiveBoundedRead<>(Read.from(new TestBoundedSource(parallelism)));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);

    Object sourceTransform =
        applyReadSourceTransform(transform, PCollection.IsBounded.BOUNDED, env);

    FlinkBoundedSource<?> source =
        (FlinkBoundedSource<?>) ((SourceTransformation<?, ?, ?>) sourceTransform).getSource();

    assertEquals(parallelism, source.getNumSplits());
  }

  @Test
  public void readSourceTranslatorUnboundedWithMaxParallelism() {

    final int maxParallelism = 6;
    final int parallelism = 2;

    SplittableParDo.PrimitiveUnboundedRead<String> transform =
        new SplittableParDo.PrimitiveUnboundedRead<>(Read.from(new TestUnboundedSource()));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    env.setMaxParallelism(maxParallelism);

    OneInputTransformation<?, ?> sourceTransform =
        (OneInputTransformation)
            applyReadSourceTransform(transform, PCollection.IsBounded.UNBOUNDED, env);

    FlinkSource<?, ?> source =
        (FlinkSource<?, ?>)
            ((SourceTransformation<?, ?, ?>) Iterables.getOnlyElement(sourceTransform.getInputs()))
                .getSource();

    assertEquals(maxParallelism, source.getNumSplits());
  }

  @Test
  public void readSourceTranslatorUnboundedWithoutMaxParallelism() {

    final int parallelism = 2;

    SplittableParDo.PrimitiveUnboundedRead<String> transform =
        new SplittableParDo.PrimitiveUnboundedRead<>(Read.from(new TestUnboundedSource()));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);

    OneInputTransformation<?, ?> sourceTransform =
        (OneInputTransformation)
            applyReadSourceTransform(transform, PCollection.IsBounded.UNBOUNDED, env);

    FlinkSource<?, ?> source =
        (FlinkSource<?, ?>)
            ((SourceTransformation<?, ?, ?>) Iterables.getOnlyElement(sourceTransform.getInputs()))
                .getSource();

    assertEquals(parallelism, source.getNumSplits());
  }

  private Object applyReadSourceTransform(
      PTransform<?, ?> transform, PCollection.IsBounded isBounded, StreamExecutionEnvironment env) {

    FlinkStreamingPipelineTranslator.StreamTransformTranslator<PTransform<?, ?>> translator =
        getReadSourceTranslator();
    FlinkStreamingTranslationContext ctx =
        new FlinkStreamingTranslationContext(env, PipelineOptionsFactory.create(), true);

    Pipeline pipeline = Pipeline.create();
    PCollection<String> pc =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), isBounded, StringUtf8Coder.of());
    pc.setName("output");

    Map<TupleTag<?>, PValue> outputs = new HashMap<>();
    outputs.put(new TupleTag<>(), pc);
    AppliedPTransform<?, ?, ?> appliedTransform =
        AppliedPTransform.of(
            "test-transform",
            Collections.emptyMap(),
            PValues.fullyExpand(outputs),
            transform,
            ResourceHints.create(),
            Pipeline.create());

    ctx.setCurrentTransform(appliedTransform);
    translator.translateNode(transform, ctx);

    return ctx.getInputDataStream(pc).getTransformation();
  }

  @SuppressWarnings("unchecked")
  private FlinkStreamingPipelineTranslator.StreamTransformTranslator<PTransform<?, ?>>
      getReadSourceTranslator() {
    PTransformTranslation.RawPTransform<?, ?> t = mock(PTransformTranslation.RawPTransform.class);
    when(t.getUrn()).thenReturn(PTransformTranslation.READ_TRANSFORM_URN);
    return (FlinkStreamingPipelineTranslator.StreamTransformTranslator<PTransform<?, ?>>)
        FlinkStreamingTransformTranslators.getTranslator(t);
  }

  /** {@link BoundedSource} for testing purposes of read transform translators. */
  private static class TestBoundedSource extends BoundedSource<String> {

    private final int bytes;

    private TestBoundedSource(int bytes) {
      this.bytes = bytes;
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      List<BoundedSource<String>> splits = new ArrayList<>();
      long remaining = bytes;
      while (remaining > 0) {
        remaining -= desiredBundleSizeBytes;
        splits.add(this);
      }
      return splits;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return bytes;
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
      return null;
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  /** {@link UnboundedSource} for testing purposes of read transform translators. */
  private static class TestUnboundedSource
      extends UnboundedSource<String, UnboundedSource.CheckpointMark> {

    @Override
    public List<? extends UnboundedSource<String, CheckpointMark>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      List<UnboundedSource<String, CheckpointMark>> splits = new ArrayList<>();
      for (int i = 0; i < desiredNumSplits; i++) {
        splits.add(this);
      }
      return splits;
    }

    @Override
    public UnboundedReader<String> createReader(
        PipelineOptions options, @Nullable CheckpointMark checkpointMark) throws IOException {
      return null;
    }

    @Override
    public Coder<CheckpointMark> getCheckpointMarkCoder() {
      return null;
    }
  }
}
