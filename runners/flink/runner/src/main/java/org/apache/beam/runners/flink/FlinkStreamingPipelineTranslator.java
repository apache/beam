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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.SplittableParDo;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a {@link FlinkPipelineTranslator} for streaming jobs. Its role is to translate
 * the user-provided {@link org.apache.beam.sdk.values.PCollection}-based job into a
 * {@link org.apache.flink.streaming.api.datastream.DataStream} one.
 *
 */
class FlinkStreamingPipelineTranslator extends FlinkPipelineTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamingPipelineTranslator.class);

  /** The necessary context in the case of a straming job. */
  private final FlinkStreamingTranslationContext streamingContext;

  private int depth = 0;

  private FlinkRunner flinkRunner;

  public FlinkStreamingPipelineTranslator(
      FlinkRunner flinkRunner,
      StreamExecutionEnvironment env,
      PipelineOptions options) {
    this.streamingContext = new FlinkStreamingTranslationContext(env, options);
    this.flinkRunner = flinkRunner;
  }

  @Override
  public void translate(Pipeline pipeline) {
    List<PTransformOverride> transformOverrides =
        ImmutableList.<PTransformOverride>builder()
            .add(
                PTransformOverride.of(
                    PTransformMatchers.splittableParDoMulti(),
                    new SplittableParDoOverrideFactory()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(View.AsIterable.class),
                    new ReflectiveOneToOneOverrideFactory(
                        FlinkStreamingViewOverrides.StreamingViewAsIterable.class, flinkRunner)))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(View.AsList.class),
                    new ReflectiveOneToOneOverrideFactory(
                        FlinkStreamingViewOverrides.StreamingViewAsList.class, flinkRunner)))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(View.AsMap.class),
                    new ReflectiveOneToOneOverrideFactory(
                        FlinkStreamingViewOverrides.StreamingViewAsMap.class, flinkRunner)))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(View.AsMultimap.class),
                    new ReflectiveOneToOneOverrideFactory(
                        FlinkStreamingViewOverrides.StreamingViewAsMultimap.class, flinkRunner)))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(View.AsSingleton.class),
                    new ReflectiveOneToOneOverrideFactory(
                        FlinkStreamingViewOverrides.StreamingViewAsSingleton.class, flinkRunner)))
            // this has to be last since the ViewAsSingleton override
            // can expand to a Combine.GloballyAsSingletonView
            .add(
                PTransformOverride.of(
                    PTransformMatchers.classEqualTo(Combine.GloballyAsSingletonView.class),
                    new ReflectiveOneToOneOverrideFactory(
                        FlinkStreamingViewOverrides.StreamingCombineGloballyAsSingletonView.class,
                        flinkRunner)))
            .build();

    pipeline.replaceAll(transformOverrides);
    super.translate(pipeline);
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline Visitor Methods
  // --------------------------------------------------------------------------------------------

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    LOG.info("{} enterCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
    this.depth++;

    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {
      StreamTransformTranslator<?> translator =
          FlinkStreamingTransformTranslators.getTranslator(transform);

      if (translator != null && applyCanTranslate(transform, node, translator)) {
        applyStreamingTransform(transform, node, translator);
        LOG.info("{} translated- {}", genSpaces(this.depth), node.getFullName());
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    this.depth--;
    LOG.info("{} leaveCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.info("{} visitPrimitiveTransform- {}", genSpaces(this.depth), node.getFullName());
    // get the transformation corresponding to hte node we are
    // currently visiting and translate it into its Flink alternative.

    PTransform<?, ?> transform = node.getTransform();
    StreamTransformTranslator<?> translator =
        FlinkStreamingTransformTranslators.getTranslator(transform);

    if (translator == null || !applyCanTranslate(transform, node, translator)) {
      LOG.info(node.getTransform().getClass().toString());
      throw new UnsupportedOperationException(
          "The transform " + transform + " is currently not supported.");
    }
    applyStreamingTransform(transform, node, translator);
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    // do nothing here
  }

  private <T extends PTransform<?, ?>> void applyStreamingTransform(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      StreamTransformTranslator<?> translator) {

    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;

    @SuppressWarnings("unchecked")
    StreamTransformTranslator<T> typedTranslator = (StreamTransformTranslator<T>) translator;

    // create the applied PTransform on the streamingContext
    streamingContext.setCurrentTransform(node.toAppliedPTransform());
    typedTranslator.translateNode(typedTransform, streamingContext);
  }

  private <T extends PTransform<?, ?>> boolean applyCanTranslate(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      StreamTransformTranslator<?> translator) {

    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;

    @SuppressWarnings("unchecked")
    StreamTransformTranslator<T> typedTranslator = (StreamTransformTranslator<T>) translator;

    streamingContext.setCurrentTransform(node.toAppliedPTransform());

    return typedTranslator.canTranslate(typedTransform, streamingContext);
  }

  /**
   * The interface that every Flink translator of a Beam operator should implement.
   * This interface is for <b>streaming</b> jobs. For examples of such translators see
   * {@link FlinkStreamingTransformTranslators}.
   */
  abstract static class StreamTransformTranslator<T extends PTransform> {

    /**
     * Translate the given transform.
     */
    abstract void translateNode(T transform, FlinkStreamingTranslationContext context);

    /**
     * Returns true iff this translator can translate the given transform.
     */
    boolean canTranslate(T transform, FlinkStreamingTranslationContext context) {
      return true;
    }
  }

  private static class ReflectiveOneToOneOverrideFactory<
          InputT, OutputT, TransformT extends PTransform<PCollection<InputT>, PCollection<OutputT>>>
      extends SingleInputOutputOverrideFactory<
          PCollection<InputT>, PCollection<OutputT>, TransformT> {
    private final Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement;
    private final FlinkRunner runner;

    private ReflectiveOneToOneOverrideFactory(
        Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement,
        FlinkRunner runner) {
      this.replacement = replacement;
      this.runner = runner;
    }

    @Override
    public PTransformReplacement<PCollection<InputT>, PCollection<OutputT>> getReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, TransformT> transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          InstanceBuilder.ofType(replacement)
              .withArg(FlinkRunner.class, runner)
              .withArg(
                  (Class<PTransform<PCollection<InputT>, PCollection<OutputT>>>)
                      transform.getTransform().getClass(),
                  transform.getTransform())
              .build());
    }
  }

  /**
   * A {@link PTransformOverrideFactory} that overrides a <a
   * href="https://s.apache.org/splittable-do-fn">Splittable DoFn</a> with {@link SplittableParDo}.
   */
  static class SplittableParDoOverrideFactory<InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<InputT>, PCollectionTuple, MultiOutput<InputT, OutputT>> {
    @Override
    public PTransformReplacement<PCollection<InputT>, PCollectionTuple>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<InputT>, PCollectionTuple, MultiOutput<InputT, OutputT>>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new SplittableParDo<>(transform.getTransform()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollectionTuple newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }
}
