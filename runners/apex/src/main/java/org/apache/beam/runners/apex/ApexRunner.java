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
package org.apache.beam.runners.apex;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.apex.translators.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.OldDoFn.ProcessContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.runners.core.AssignWindows;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.google.common.base.Throwables;

/**
 * A {@link PipelineRunner} that translates the
 * pipeline to an Apex DAG and executes it on an Apex cluster.
 * <p>
 * Currently execution is always in embedded mode,
 * launch on Hadoop cluster will be added in subsequent iteration.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ApexRunner extends PipelineRunner<ApexRunnerResult> {

  private final ApexPipelineOptions options;

  /**
   * TODO: this isn't thread safe and may cause issues when tests run in parallel
   * Holds any most resent assertion error that was raised while processing elements.
   * Used in the unit test driver in embedded to propagate the exception.
   */
  public static volatile AssertionError assertionError;

  public ApexRunner(ApexPipelineOptions options) {
    this.options = options;
  }

  public static ApexRunner fromOptions(PipelineOptions options) {
    return new ApexRunner((ApexPipelineOptions) options);
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {

    if (Window.Bound.class.equals(transform.getClass())) {
      return (OutputT) ((PCollection) input).apply(
          new AssignWindowsAndSetStrategy((Window.Bound) transform));
    } else if (Create.Values.class.equals(transform.getClass())) {
      return (OutputT) PCollection
          .<OutputT>createPrimitiveOutputInternal(
              input.getPipeline(),
              WindowingStrategy.globalDefault(),
              PCollection.IsBounded.BOUNDED);
// TODO: replace this with a mapping
    } else if (Combine.GloballyAsSingletonView.class.equals(transform.getClass())) {
      PTransform<InputT, OutputT> customTransform = (PTransform)new StreamingCombineGloballyAsSingletonView<InputT, OutputT>(this,
          (Combine.GloballyAsSingletonView)transform);
      return Pipeline.applyTransform(input, customTransform);
    } else if (View.AsSingleton.class.equals(transform.getClass())) {
      // note this assumes presence of above Combine.GloballyAsSingletonView mapping
      PTransform<InputT, OutputT> customTransform = (PTransform)new StreamingViewAsSingleton<InputT>(this,
          (View.AsSingleton)transform);
      return Pipeline.applyTransform(input, customTransform);
    } else if (View.AsIterable.class.equals(transform.getClass())) {
      PTransform<InputT, OutputT> customTransform = (PTransform)new StreamingViewAsIterable<InputT>(this,
          (View.AsIterable)transform);
      return Pipeline.applyTransform(input, customTransform);
    } else if (View.AsList.class.equals(transform.getClass())) {
      PTransform<InputT, OutputT> customTransform = (PTransform)new StreamingViewAsList<InputT>(this,
          (View.AsList)transform);
      return Pipeline.applyTransform(input, customTransform);
    } else if (View.AsMap.class.equals(transform.getClass())) {
      PTransform<InputT, OutputT> customTransform = new StreamingViewAsMap(this,
          (View.AsMap)transform);
      return Pipeline.applyTransform(input, customTransform);
    } else if (View.AsMultimap.class.equals(transform.getClass())) {
      PTransform<InputT, OutputT> customTransform = new StreamingViewAsMultimap(this,
          (View.AsMultimap)transform);
      return Pipeline.applyTransform(input, customTransform);
    } else {
      return super.apply(transform, input);
    }
  }

  @Override
  public ApexRunnerResult run(Pipeline pipeline) {

    final TranslationContext translationContext = new TranslationContext(options);
    ApexPipelineTranslator translator = new ApexPipelineTranslator(translationContext);
    translator.translate(pipeline);

    StreamingApplication apexApp = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.setAttribute(DAGContext.APPLICATION_NAME, options.getApplicationName());
        translationContext.populateDAG(dag);
      }
    };

    checkArgument(options.isEmbeddedExecution(), "only embedded execution is supported at this time");
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    try {
      lma.prepareDAG(apexApp, conf);
      LocalMode.Controller lc = lma.getController();
      if (options.isEmbeddedExecutionDebugMode()) {
        // turns off timeout checking for operator progress
        lc.setHeartbeatMonitoringEnabled(false);
      }
      assertionError = null;
      lc.runAsync();
      if (options.getRunMillis() > 0) {
        try {
          long timeout = System.currentTimeMillis() + options.getRunMillis();
          while (System.currentTimeMillis() < timeout) {
            if (assertionError != null) {
              throw assertionError;
            }
          }
        } finally {
          lc.shutdown();
        }
      }
      return new ApexRunnerResult(lma.getDAG(), lc);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * copied from DirectPipelineRunner.
   * used to replace Window.Bound till equivalent function is added in Apex
   */
  private static class AssignWindowsAndSetStrategy<T, W extends BoundedWindow>
      extends PTransform<PCollection<T>, PCollection<T>> {

    private final Window.Bound<T> wrapped;

    public AssignWindowsAndSetStrategy(Window.Bound<T> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public PCollection<T> apply(PCollection<T> input) {
      WindowingStrategy<?, ?> outputStrategy =
          wrapped.getOutputStrategyInternal(input.getWindowingStrategy());

      WindowFn<T, BoundedWindow> windowFn =
          (WindowFn<T, BoundedWindow>) outputStrategy.getWindowFn();

      // If the Window.Bound transform only changed parts other than the WindowFn, then
      // we skip AssignWindows even though it should be harmless in a perfect world.
      // The world is not perfect, and a GBK may have set it to InvalidWindows to forcibly
      // crash if another GBK is performed without explicitly setting the WindowFn. So we skip
      // AssignWindows in this case.
      if (wrapped.getWindowFn() == null) {
        return input.apply("Identity", ParDo.of(new IdentityFn<T>()))
            .setWindowingStrategyInternal(outputStrategy);
      } else {
        return input
            .apply("AssignWindows", new AssignWindows<>(windowFn))
            .setWindowingStrategyInternal(outputStrategy);
      }
    }
  }

  private static class IdentityFn<T> extends DoFn<T, T> {
    private static final long serialVersionUID = 1L;
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

////////////////////////////////////////////
// Adapted from FlinkRunner for View support

  /**
   * Records that the {@link PTransform} requires a deterministic key coder.
   */
  private void recordViewUsesNonDeterministicKeyCoder(PTransform<?, ?> ptransform) {
    //throw new UnsupportedOperationException();
  }

  /**
   * Creates a primitive {@link PCollectionView}.
   *
   * <p>For internal use only by runner implementors.
   *
   * @param <ElemT> The type of the elements of the input PCollection
   * @param <ViewT> The type associated with the {@link PCollectionView} used as a side input
   */
  public static class CreateApexPCollectionView<ElemT, ViewT>
      extends PTransform<PCollection<List<ElemT>>, PCollectionView<ViewT>> {
    private PCollectionView<ViewT> view;

    private CreateApexPCollectionView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    public static <ElemT, ViewT> CreateApexPCollectionView<ElemT, ViewT> of(
        PCollectionView<ViewT> view) {
      return new CreateApexPCollectionView<>(view);
    }

    public PCollectionView<ViewT> getView() {
      return view;
    }

    @Override
    public PCollectionView<ViewT> apply(PCollection<List<ElemT>> input) {
      return view;
    }
  }

  private static class WrapAsList<T> extends OldDoFn<T, List<T>> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(Arrays.asList(c.element()));
    }
  }

  private static class StreamingCombineGloballyAsSingletonView<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollectionView<OutputT>>
  {
    Combine.GloballyAsSingletonView<InputT, OutputT> transform;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    public StreamingCombineGloballyAsSingletonView(ApexRunner runner,
        Combine.GloballyAsSingletonView<InputT, OutputT> transform)
    {
      this.transform = transform;
    }

    @Override
    public PCollectionView<OutputT> apply(PCollection<InputT> input)
    {
      PCollection<OutputT> combined = input
          .apply(Combine.globally(transform.getCombineFn()).withoutDefaults().withFanout(transform.getFanout()));

      PCollectionView<OutputT> view = PCollectionViews.singletonView(combined.getPipeline(),
          combined.getWindowingStrategy(), transform.getInsertDefault(),
          transform.getInsertDefault() ? transform.getCombineFn().defaultValue() : null, combined.getCoder());
      return combined.apply(ParDo.of(new WrapAsList<OutputT>()))
          .apply(CreateApexPCollectionView.<OutputT, OutputT> of(view));
    }

    @Override
    protected String getKindString()
    {
      return "StreamingCombineGloballyAsSingletonView";
    }
  }

  private static class StreamingViewAsSingleton<T> extends PTransform<PCollection<T>, PCollectionView<T>>
  {
    private static final long serialVersionUID = 1L;
    private View.AsSingleton<T> transform;

    public StreamingViewAsSingleton(ApexRunner runner, View.AsSingleton<T> transform)
    {
      this.transform = transform;
    }

    @Override
    public PCollectionView<T> apply(PCollection<T> input)
    {
      Combine.Globally<T, T> combine = Combine
          .globally(new SingletonCombine<>(transform.hasDefaultValue(), transform.defaultValue()));
      if (!transform.hasDefaultValue()) {
        combine = combine.withoutDefaults();
      }
      return input.apply(combine.asSingletonView());
    }

    @Override
    protected String getKindString()
    {
      return "StreamingViewAsSingleton";
    }

    private static class SingletonCombine<T> extends Combine.BinaryCombineFn<T>
    {
      private boolean hasDefaultValue;
      private T defaultValue;

      SingletonCombine(boolean hasDefaultValue, T defaultValue)
      {
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;
      }

      @Override
      public T apply(T left, T right)
      {
        throw new IllegalArgumentException("PCollection with more than one element "
            + "accessed as a singleton view. Consider using Combine.globally().asSingleton() to "
            + "combine the PCollection into a single value");
      }

      @Override
      public T identity()
      {
        if (hasDefaultValue) {
          return defaultValue;
        } else {
          throw new IllegalArgumentException("Empty PCollection accessed as a singleton view. "
              + "Consider setting withDefault to provide a default value");
        }
      }
    }
  }

  private static class StreamingViewAsMap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>> {

    private final ApexRunner runner;

    public StreamingViewAsMap(ApexRunner runner, View.AsMap<K, V> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Map<K, V>> apply(PCollection<KV<K, V>> input) {
      PCollectionView<Map<K, V>> view =
          PCollectionViews.mapView(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.getCoder());

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        inputCoder.getKeyCoder().verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);
      }

      return input
          .apply(Combine.globally(new Concatenate<KV<K, V>>()).withoutDefaults())
          .apply(CreateApexPCollectionView.<KV<K, V>, Map<K, V>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsMap";
    }
  }

  /**
   * Specialized expansion for {@link
   * org.apache.beam.sdk.transforms.View.AsMultimap View.AsMultimap} for the
   * Flink runner in streaming mode.
   */
  private static class StreamingViewAsMultimap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>> {

    private final ApexRunner runner;

    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in FlinkRunner#apply()
    public StreamingViewAsMultimap(ApexRunner runner, View.AsMultimap<K, V> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Map<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
      PCollectionView<Map<K, Iterable<V>>> view =
          PCollectionViews.multimapView(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.getCoder());

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        inputCoder.getKeyCoder().verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);
      }

      return input
          .apply(Combine.globally(new Concatenate<KV<K, V>>()).withoutDefaults())
          .apply(CreateApexPCollectionView.<KV<K, V>, Map<K, Iterable<V>>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsMultimap";
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsList View.AsList} for the
   * Flink runner in streaming mode.
   */
  private static class StreamingViewAsList<T>
      extends PTransform<PCollection<T>, PCollectionView<List<T>>> {
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in FlinkRunner#apply()
    public StreamingViewAsList(ApexRunner runner, View.AsList<T> transform) {}

    @Override
    public PCollectionView<List<T>> apply(PCollection<T> input) {
      PCollectionView<List<T>> view =
          PCollectionViews.listView(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.getCoder());

      return input.apply(Combine.globally(new Concatenate<T>()).withoutDefaults())
          .apply(CreateApexPCollectionView.<T, List<T>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsList";
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsIterable View.AsIterable} for the
   * Flink runner in streaming mode.
   */
  private static class StreamingViewAsIterable<T>
      extends PTransform<PCollection<T>, PCollectionView<Iterable<T>>> {
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in FlinkRunner#apply()
    public StreamingViewAsIterable(ApexRunner runner, View.AsIterable<T> transform) { }

    @Override
    public PCollectionView<Iterable<T>> apply(PCollection<T> input) {
      PCollectionView<Iterable<T>> view =
          PCollectionViews.iterableView(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.getCoder());

      return input.apply(Combine.globally(new Concatenate<T>()).withoutDefaults())
          .apply(CreateApexPCollectionView.<T, Iterable<T>>of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsIterable";
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
      return new ArrayList<T>();
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
