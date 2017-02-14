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

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.gearpump.translators.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import org.apache.gearpump.cluster.ClusterConfig;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.cluster.client.RunningApplication;
import org.apache.gearpump.cluster.embedded.EmbeddedCluster;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;

/**
 * A {@link PipelineRunner} that executes the operations in the
 * pipeline by first translating them to Gearpump Stream DSL
 * and then executing them on a Gearpump cluster.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GearpumpRunner extends PipelineRunner<GearpumpPipelineResult> {

  private final GearpumpPipelineOptions options;

  private static final String GEARPUMP_SERIALIZERS = "gearpump.serializers";
  private static final String DEFAULT_APPNAME = "beam_gearpump_app";

  /** Custom transforms implementations. */
  private final Map<Class<?>, Class<?>> overrides;

  public GearpumpRunner(GearpumpPipelineOptions options) {
    this.options = options;

    ImmutableMap.Builder<Class<?>, Class<?>> builder = ImmutableMap.builder();
    builder.put(Combine.GloballyAsSingletonView.class,
        StreamingCombineGloballyAsSingletonView.class);
    builder.put(View.AsMap.class, StreamingViewAsMap.class);
    builder.put(View.AsMultimap.class, StreamingViewAsMultimap.class);
    builder.put(View.AsSingleton.class, StreamingViewAsSingleton.class);
    builder.put(View.AsList.class, StreamingViewAsList.class);
    builder.put(View.AsIterable.class, StreamingViewAsIterable.class);
    overrides = builder.build();
  }

  public static GearpumpRunner fromOptions(PipelineOptions options) {
    GearpumpPipelineOptions pipelineOptions =
        PipelineOptionsValidator.validate(GearpumpPipelineOptions.class, options);
    return new GearpumpRunner(pipelineOptions);
  }


  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    if (overrides.containsKey(transform.getClass())) {

      Class<PTransform<InputT, OutputT>> transformClass =
          (Class<PTransform<InputT, OutputT>>) transform.getClass();

      Class<PTransform<InputT, OutputT>> customTransformClass =
          (Class<PTransform<InputT, OutputT>>) overrides.get(transform.getClass());

      PTransform<InputT, OutputT> customTransform =
          InstanceBuilder.ofType(customTransformClass)
              .withArg(transformClass, transform)
              .build();

      return Pipeline.applyTransform(input, customTransform);
    } else if (Flatten.FlattenPCollectionList.class.equals(transform.getClass())
            && ((PCollectionList<?>) input).size() == 0) {
      return (OutputT) Pipeline.applyTransform(input.getPipeline().begin(), Create.of());
    } else {
      return super.apply(transform, input);
    }
  }

  @Override
  public GearpumpPipelineResult run(Pipeline pipeline) {
    String appName = options.getApplicationName();
    if (null == appName) {
      appName = DEFAULT_APPNAME;
    }
    Config config = registerSerializers(ClusterConfig.defaultConfig(),
        options.getSerializers());
    ClientContext clientContext = getClientContext(options, config);
    options.setClientContext(clientContext);
    UserConfig userConfig = UserConfig.empty();
    JavaStreamApp streamApp = new JavaStreamApp(
        appName, clientContext, userConfig);
    TranslationContext translationContext = new TranslationContext(streamApp, options);
    GearpumpPipelineTranslator translator = new GearpumpPipelineTranslator(translationContext);
    translator.translate(pipeline);
    RunningApplication app = streamApp.submit();

    return new GearpumpPipelineResult(clientContext, app);
  }

  private ClientContext getClientContext(GearpumpPipelineOptions options, Config config) {
    EmbeddedCluster cluster = options.getEmbeddedCluster();
    if (cluster != null) {
      return cluster.newClientContext();
    } else {
      return ClientContext.apply(config);
    }
  }

  /**
   * register class with default kryo serializers.
   */
  private Config registerSerializers(Config config, Map<String, String> userSerializers) {
    Map<String, String> serializers = new HashMap<>();
    serializers.put("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInSingleWindow", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.PaneInfo", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.PaneInfo$Timing", "");
    serializers.put("org.joda.time.Instant", "");
    serializers.put("org.apache.beam.sdk.values.KV", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.IntervalWindow", "");
    serializers.put("org.apache.beam.sdk.values.TimestampedValue", "");
    if (userSerializers != null && !userSerializers.isEmpty()) {
      serializers.putAll(userSerializers);
    }
    return config.withValue(GEARPUMP_SERIALIZERS, ConfigValueFactory.fromMap(serializers));
  }



  // The following codes are forked from DataflowRunner for View translator
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
              input.getPipeline(),
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
              input.getPipeline(),
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
              input.getPipeline(),
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
              input.getPipeline(),
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
          combined.getPipeline(),
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
