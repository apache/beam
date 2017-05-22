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

package org.apache.beam.runners.gearpump;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.runners.gearpump.translators.CreateGearpumpPCollectionViewTranslator;
import org.apache.beam.runners.gearpump.translators.FlattenPCollectionsTranslator;
import org.apache.beam.runners.gearpump.translators.GroupByKeyTranslator;
import org.apache.beam.runners.gearpump.translators.ParDoMultiOutputTranslator;
import org.apache.beam.runners.gearpump.translators.ReadBoundedTranslator;
import org.apache.beam.runners.gearpump.translators.ReadUnboundedTranslator;
import org.apache.beam.runners.gearpump.translators.TransformTranslator;
import org.apache.beam.runners.gearpump.translators.TranslationContext;
import org.apache.beam.runners.gearpump.translators.WindowAssignTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;

import org.apache.gearpump.util.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GearpumpPipelineTranslator} knows how to translate {@link Pipeline} objects
 * into Gearpump {@link Graph}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GearpumpPipelineTranslator implements Pipeline.PipelineVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(
      GearpumpPipelineTranslator.class);

  /**
   * A map from {@link PTransform} subclass to the corresponding
   * {@link TransformTranslator} to use to translate that transform.
   */
  private static final Map<Class<? extends PTransform>, TransformTranslator>
      transformTranslators = new HashMap<>();

  private final TranslationContext translationContext;

  static {
    // register TransformTranslators
    registerTransformTranslator(Read.Unbounded.class, new ReadUnboundedTranslator());
    registerTransformTranslator(Read.Bounded.class, new ReadBoundedTranslator());
    registerTransformTranslator(GroupByKey.class, new GroupByKeyTranslator());
    registerTransformTranslator(Flatten.PCollections.class,
        new FlattenPCollectionsTranslator());
    registerTransformTranslator(ParDo.MultiOutput.class, new ParDoMultiOutputTranslator());
    registerTransformTranslator(Window.Assign.class, new WindowAssignTranslator());
    registerTransformTranslator(CreateGearpumpPCollectionView.class,
        new CreateGearpumpPCollectionViewTranslator<>());
  }

  public GearpumpPipelineTranslator(TranslationContext translationContext) {
    this.translationContext = translationContext;
  }

  public void translate(Pipeline pipeline) {
    List<PTransformOverride> overrides =
        ImmutableList.<PTransformOverride>builder()
            .add(PTransformOverride.of(
                PTransformMatchers.classEqualTo(View.AsMap.class),
                new ReflectiveOneToOneOverrideFactory(StreamingViewAsMap.class)))
            .add(PTransformOverride.of(
                PTransformMatchers.classEqualTo(View.AsMultimap.class),
                new ReflectiveOneToOneOverrideFactory(StreamingViewAsMultimap.class)))
            .add(PTransformOverride.of(
                PTransformMatchers.classEqualTo(View.AsSingleton.class),
                new ReflectiveOneToOneOverrideFactory(StreamingViewAsSingleton.class)))
            .add(PTransformOverride.of(
                PTransformMatchers.classEqualTo(View.AsList.class),
                new ReflectiveOneToOneOverrideFactory(StreamingViewAsList.class)))
            .add(PTransformOverride.of(
                PTransformMatchers.classEqualTo(View.AsIterable.class),
                new ReflectiveOneToOneOverrideFactory(StreamingViewAsIterable.class)))
            .add(PTransformOverride.of(
                PTransformMatchers.classEqualTo(Combine.GloballyAsSingletonView.class),
                new ReflectiveOneToOneOverrideFactory(
                    StreamingCombineGloballyAsSingletonView.class)))
            .build();

    pipeline.replaceAll(overrides);
    pipeline.traverseTopologically(this);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    LOG.debug("entering composite transform {}", node.getTransform());
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    LOG.debug("leaving composite transform {}", node.getTransform());
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.debug("visiting transform {}", node.getTransform());
    PTransform transform = node.getTransform();
    TransformTranslator translator = getTransformTranslator(transform.getClass());
    if (null == translator) {
      throw new IllegalStateException(
          "no translator registered for " + transform);
    }
    translationContext.setCurrentTransform(node);
    translator.translate(transform, translationContext);
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    LOG.debug("visiting value {}", value);
  }

  /**
   * Records that instances of the specified PTransform class
   * should be translated by default by the corresponding
   * {@link TransformTranslator}.
   */
  private static <TransformT extends PTransform> void registerTransformTranslator(
      Class<TransformT> transformClass,
      TransformTranslator<? extends TransformT> transformTranslator) {
    if (transformTranslators.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException(
          "defining multiple translators for " + transformClass);
    }
  }

  /**
   * Returns the {@link TransformTranslator} to use for instances of the
   * specified PTransform class, or null if none registered.
   */
  private <TransformT extends PTransform>
  TransformTranslator<TransformT> getTransformTranslator(Class<TransformT> transformClass) {
    return transformTranslators.get(transformClass);
  }

  // The following codes are forked from DataflowRunner for View translator
  private static class ReflectiveOneToOneOverrideFactory<
          InputT, OutputT, TransformT extends PTransform<PCollection<InputT>, PCollection<OutputT>>>
      extends SingleInputOutputOverrideFactory<
          PCollection<InputT>, PCollection<OutputT>, TransformT> {
    private final Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement;

    private ReflectiveOneToOneOverrideFactory(
        Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement) {
      this.replacement = replacement;
    }

    @Override
    public PTransformReplacement<PCollection<InputT>, PCollection<OutputT>> getReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, TransformT> transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          InstanceBuilder.ofType(replacement)
              .withArg(
                  (Class<PTransform<PCollection<InputT>, PCollection<OutputT>>>)
                      transform.getTransform().getClass(),
                  transform.getTransform())
              .build());
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsMap View.AsMap}
   * for the Gearpump runner.
   */
  private static class StreamingViewAsMap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>> {

    private static final long serialVersionUID = 4791080760092950304L;

    public StreamingViewAsMap(View.AsMap<K, V> transform) {}

    @Override
    public PCollectionView<Map<K, V>> expand(PCollection<KV<K, V>> input) {
      PCollectionView<Map<K, V>> view =
          PCollectionViews.mapView(
              input,
              input.getWindowingStrategy(),
              input.getCoder());

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        inputCoder.getKeyCoder().verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        // throw new RuntimeException(e);
      }

      return input
          .apply(Combine.globally(new Concatenate<KV<K, V>>()).withoutDefaults())
          .apply(CreateGearpumpPCollectionView.<KV<K, V>, Map<K, V>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsMap";
    }
  }

  /**
   * Specialized expansion for {@link
   * org.apache.beam.sdk.transforms.View.AsMultimap View.AsMultimap} for the
   * Gearpump runner.
   */
  private static class StreamingViewAsMultimap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>> {

    private static final long serialVersionUID = 5854899081751333352L;

    public StreamingViewAsMultimap(View.AsMultimap<K, V> transform) {}

    @Override
    public PCollectionView<Map<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      PCollectionView<Map<K, Iterable<V>>> view =
          PCollectionViews.multimapView(
              input,
              input.getWindowingStrategy(),
              input.getCoder());

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        inputCoder.getKeyCoder().verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        // throw new RuntimeException(e);
      }

      return input
          .apply(Combine.globally(new Concatenate<KV<K, V>>()).withoutDefaults())
          .apply(CreateGearpumpPCollectionView.<KV<K, V>, Map<K, Iterable<V>>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsMultimap";
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsIterable View.AsIterable} for the
   * Gearpump runner.
   */
  private static class StreamingViewAsIterable<T>
      extends PTransform<PCollection<T>, PCollectionView<Iterable<T>>> {

    private static final long serialVersionUID = -3399860618995613421L;

    public StreamingViewAsIterable(View.AsIterable<T> transform) {}

    @Override
    public PCollectionView<Iterable<T>> expand(PCollection<T> input) {
      PCollectionView<Iterable<T>> view =
          PCollectionViews.iterableView(
              input,
              input.getWindowingStrategy(),
              input.getCoder());

      return input.apply(Combine.globally(new Concatenate<T>()).withoutDefaults())
          .apply(CreateGearpumpPCollectionView.<T, Iterable<T>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsIterable";
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsList View.AsList} for the
   * Gearpump runner.
   */
  private static class StreamingViewAsList<T>
      extends PTransform<PCollection<T>, PCollectionView<List<T>>> {

    private static final long serialVersionUID = -5018631473886330629L;

    public StreamingViewAsList(View.AsList<T> transform) {}

    @Override
    public PCollectionView<List<T>> expand(PCollection<T> input) {
      PCollectionView<List<T>> view =
          PCollectionViews.listView(
              input,
              input.getWindowingStrategy(),
              input.getCoder());

      return input.apply(Combine.globally(new Concatenate<T>()).withoutDefaults())
          .apply(CreateGearpumpPCollectionView.<T, List<T>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsList";
    }
  }

  private static class StreamingCombineGloballyAsSingletonView<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollectionView<OutputT>> {

    private static final long serialVersionUID = 9064900748869035738L;
    private final Combine.GloballyAsSingletonView<InputT, OutputT> transform;

    public StreamingCombineGloballyAsSingletonView(
        Combine.GloballyAsSingletonView<InputT, OutputT> transform) {
      this.transform = transform;
    }

    @Override
    public PCollectionView<OutputT> expand(PCollection<InputT> input) {
      PCollection<OutputT> combined =
          input.apply(Combine.globally(transform.getCombineFn())
              .withoutDefaults()
              .withFanout(transform.getFanout()));

      PCollectionView<OutputT> view = PCollectionViews.singletonView(
          combined,
          combined.getWindowingStrategy(),
          transform.getInsertDefault(),
          transform.getInsertDefault()
              ? transform.getCombineFn().defaultValue() : null,
          combined.getCoder());
      return combined
          .apply(ParDo.of(new WrapAsList<OutputT>()))
          .apply(CreateGearpumpPCollectionView.<OutputT, OutputT>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingCombineGloballyAsSingletonView";
    }
  }

  private static class StreamingViewAsSingleton<T>
      extends PTransform<PCollection<T>, PCollectionView<T>> {

    private static final long serialVersionUID = 5870455965625071546L;
    private final View.AsSingleton<T> transform;

    public StreamingViewAsSingleton(View.AsSingleton<T> transform) {
      this.transform = transform;
    }

    @Override
    public PCollectionView<T> expand(PCollection<T> input) {
      Combine.Globally<T, T> combine = Combine.globally(
          new SingletonCombine<>(transform.hasDefaultValue(), transform.defaultValue()));
      if (!transform.hasDefaultValue()) {
        combine = combine.withoutDefaults();
      }
      return input.apply(combine.asSingletonView());
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsSingleton";
    }

    private static class SingletonCombine<T> extends Combine.BinaryCombineFn<T> {
      private boolean hasDefaultValue;
      private T defaultValue;

      SingletonCombine(boolean hasDefaultValue, T defaultValue) {
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;
      }

      @Override
      public T apply(T left, T right) {
        throw new IllegalArgumentException("PCollection with more than one element "
            + "accessed as a singleton view. Consider using Combine.globally().asSingleton() to "
            + "combine the PCollection into a single value");
      }

      @Override
      public T identity() {
        if (hasDefaultValue) {
          return defaultValue;
        } else {
          throw new IllegalArgumentException(
              "Empty PCollection accessed as a singleton view. "
                  + "Consider setting withDefault to provide a default value");
        }
      }
    }
  }

  private static class WrapAsList<T> extends DoFn<T, List<T>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(Collections.singletonList(c.element()));
    }
  }

  /**
   * Creates a primitive {@link PCollectionView}.
   *
   * <p>For internal use only by runner implementors.
   *
   * @param <ElemT> The type of the elements of the input PCollection
   * @param <ViewT> The type associated with the {@link PCollectionView} used as a side input
   */
  public static class CreateGearpumpPCollectionView<ElemT, ViewT>
      extends PTransform<PCollection<List<ElemT>>, PCollectionView<ViewT>> {
    private static final long serialVersionUID = -2637073020800540542L;
    private PCollectionView<ViewT> view;

    private CreateGearpumpPCollectionView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    public static <ElemT, ViewT> CreateGearpumpPCollectionView<ElemT, ViewT> of(
        PCollectionView<ViewT> view) {
      return new CreateGearpumpPCollectionView<>(view);
    }

    public PCollectionView<ViewT> getView() {
      return view;
    }

    @Override
    public PCollectionView<ViewT> expand(PCollection<List<ElemT>> input) {
      return view;
    }
  }

  /**
   * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs.
   *
   * <p>For internal use by {@link StreamingViewAsMap}, {@link StreamingViewAsMultimap},
   * {@link StreamingViewAsList}, {@link StreamingViewAsIterable}.
   * They require the input {@link PCollection} fits in memory.
   * For a large {@link PCollection} this is expected to crash!
   *
   * @param <T> the type of elements to concatenate.
   */
  private static class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      List<T> result = createAccumulator();
      for (List<T> accumulator : accumulators) {
        result.addAll(accumulator);
      }
      return result;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }

    @Override
    public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }
  }
}
