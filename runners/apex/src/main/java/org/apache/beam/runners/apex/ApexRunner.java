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

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.AppHandle;
import org.apache.apex.api.Launcher.LaunchMode;
import org.apache.beam.runners.apex.translation.ApexPipelineTranslator;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.PrimitiveCreate;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.SplittableParDoNaiveBounded;
import org.apache.beam.runners.core.construction.UnsupportedOverrideFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * A {@link PipelineRunner} that translates the pipeline to an Apex DAG and executes it on an Apex
 * cluster.
 */
public class ApexRunner extends PipelineRunner<ApexRunnerResult> {

  private final ApexPipelineOptions options;
  public static final String CLASSPATH_SCHEME = "classpath";
  protected boolean translateOnly = false;

  /**
   * TODO: this isn't thread safe and may cause issues when tests run in parallel Holds any most
   * resent assertion error that was raised while processing elements. Used in the unit test driver
   * in embedded mode to propagate the exception.
   */
  public static final AtomicReference<AssertionError> ASSERTION_ERROR = new AtomicReference<>();

  public ApexRunner(ApexPipelineOptions options) {
    this.options = options;
  }

  public static ApexRunner fromOptions(PipelineOptions options) {
    ApexPipelineOptions apexPipelineOptions =
        PipelineOptionsValidator.validate(ApexPipelineOptions.class, options);
    return new ApexRunner(apexPipelineOptions);
  }

  @SuppressWarnings({"rawtypes"})
  protected List<PTransformOverride> getOverrides() {
    return ImmutableList.<PTransformOverride>builder()
        .add(
            PTransformOverride.of(
                PTransformMatchers.classEqualTo(Create.Values.class),
                new PrimitiveCreate.Factory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.createViewWithViewFn(PCollectionViews.IterableViewFn.class),
                new StreamingViewAsIterable.Factory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.createViewWithViewFn(PCollectionViews.ListViewFn.class),
                new StreamingViewAsIterable.Factory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.createViewWithViewFn(PCollectionViews.MapViewFn.class),
                new StreamingViewAsIterable.Factory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.createViewWithViewFn(PCollectionViews.MultimapViewFn.class),
                new StreamingViewAsIterable.Factory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.createViewWithViewFn(PCollectionViews.SingletonViewFn.class),
                new StreamingWrapSingletonInList.Factory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.splittableParDoMulti(), new SplittableParDo.OverrideFactory()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.splittableProcessKeyedBounded(),
                new SplittableParDoNaiveBounded.OverrideFactory<>()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.splittableProcessKeyedUnbounded(),
                new SplittableParDoViaKeyedWorkItems.OverrideFactory<>()))
        // TODO: [BEAM-5360] Support @RequiresStableInput on Apex runner
        .add(
            PTransformOverride.of(
                PTransformMatchers.requiresStableInputParDoMulti(),
                UnsupportedOverrideFactory.withMessage(
                    "Apex runner currently doesn't support @RequiresStableInput annotation.")))
        .build();
  }

  @Override
  public ApexRunnerResult run(final Pipeline pipeline) {
    pipeline.replaceAll(getOverrides());

    final ApexPipelineTranslator translator = new ApexPipelineTranslator(options);
    final AtomicReference<DAG> apexDAG = new AtomicReference<>();
    final AtomicReference<File> tempDir = new AtomicReference<>();

    StreamingApplication apexApp =
        (dag, conf) -> {
          apexDAG.set(dag);
          dag.setAttribute(DAGContext.APPLICATION_NAME, options.getApplicationName());
          if (options.isEmbeddedExecution()) {
            // set unique path for application state to allow for parallel execution of unit tests
            // (the embedded cluster would set it to a fixed location under ./target)
            tempDir.set(Files.createTempDir());
            dag.setAttribute(DAGContext.APPLICATION_PATH, tempDir.get().toURI().toString());
          }
          translator.translate(pipeline, dag);
        };

    Properties configProperties = new Properties();
    try {
      if (options.getConfigFile() != null) {
        URI configURL = new URI(options.getConfigFile());
        if (CLASSPATH_SCHEME.equals(configURL.getScheme())) {
          InputStream is = this.getClass().getResourceAsStream(configURL.getPath());
          if (is != null) {
            configProperties.load(is);
            is.close();
          }
        } else {
          if (!configURL.isAbsolute()) {
            // resolve as local file name
            File f = new File(options.getConfigFile());
            configURL = f.toURI();
          }
          try (InputStream is = configURL.toURL().openStream()) {
            configProperties.load(is);
          }
        }
      }
    } catch (IOException | URISyntaxException ex) {
      throw new RuntimeException("Error loading properties", ex);
    }

    if (options.isEmbeddedExecution()) {
      EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(LaunchMode.EMBEDDED);
      Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
      launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, true);
      if (options.isEmbeddedExecutionDebugMode()) {
        // turns off timeout checking for operator progress
        launchAttributes.put(EmbeddedAppLauncher.HEARTBEAT_MONITORING, false);
      }
      Configuration conf = new Configuration(false);
      ApexYarnLauncher.addProperties(conf, configProperties);
      try {
        if (translateOnly) {
          launcher.prepareDAG(apexApp, conf);
          return new ApexRunnerResult(launcher.getDAG(), null);
        }
        ApexRunner.ASSERTION_ERROR.set(null);
        AppHandle apexAppResult = launcher.launchApp(apexApp, conf, launchAttributes);
        return new ApexRunnerResult(apexDAG.get(), apexAppResult) {
          @Override
          protected void cleanupOnCancelOrFinish() {
            if (tempDir.get() != null) {
              FileUtils.deleteQuietly(tempDir.get());
            }
          }
        };
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    } else {
      try {
        ApexYarnLauncher yarnLauncher = new ApexYarnLauncher();
        AppHandle apexAppResult = yarnLauncher.launchApp(apexApp, configProperties);
        return new ApexRunnerResult(apexDAG.get(), apexAppResult);
      } catch (IOException e) {
        throw new RuntimeException("Failed to launch the application on YARN.", e);
      }
    }
  }

  ////////////////////////////////////////////
  // Adapted from FlinkRunner for View support

  /**
   * Creates a primitive {@link PCollectionView}.
   *
   * <p>For internal use only by runner implementors.
   *
   * @param <ElemT> The type of the elements of the input PCollection
   * @param <ViewT> The type associated with the {@link PCollectionView} used as a side input
   */
  public static class CreateApexPCollectionView<ElemT, ViewT>
      extends PTransform<PCollection<ElemT>, PCollection<ElemT>> {
    private static final long serialVersionUID = 1L;
    private PCollectionView<ViewT> view;

    private CreateApexPCollectionView(PCollectionView<ViewT> view) {
      this.view = view;
    }

    public static <ElemT, ViewT> CreateApexPCollectionView<ElemT, ViewT> of(
        PCollectionView<ViewT> view) {
      return new CreateApexPCollectionView<>(view);
    }

    @Override
    public PCollection<ElemT> expand(PCollection<ElemT> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), input.getCoder());
    }

    public PCollectionView<ViewT> getView() {
      return view;
    }
  }

  private static class WrapAsList<T> extends DoFn<T, List<T>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(Collections.singletonList(c.element()));
    }
  }

  private static class StreamingWrapSingletonInList<T>
      extends PTransform<PCollection<T>, PCollection<T>> {
    private static final long serialVersionUID = 1L;
    CreatePCollectionView<T, T> transform;

    /** Builds an instance of this class from the overridden transform. */
    private StreamingWrapSingletonInList(CreatePCollectionView<T, T> transform) {
      this.transform = transform;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      input
          .apply(ParDo.of(new WrapAsList<>()))
          .apply(CreateApexPCollectionView.of(transform.getView()));
      return input;
    }

    @Override
    protected String getKindString() {
      return "StreamingWrapSingletonInList";
    }

    static class Factory<T>
        extends SingleInputOutputOverrideFactory<
            PCollection<T>, PCollection<T>, CreatePCollectionView<T, T>> {
      @Override
      public PTransformReplacement<PCollection<T>, PCollection<T>> getReplacementTransform(
          AppliedPTransform<PCollection<T>, PCollection<T>, CreatePCollectionView<T, T>>
              transform) {
        return PTransformReplacement.of(
            PTransformReplacements.getSingletonMainInput(transform),
            new StreamingWrapSingletonInList<>(transform.getTransform()));
      }
    }
  }

  private static class StreamingViewAsIterable<T>
      extends PTransform<PCollection<T>, PCollection<T>> {
    private static final long serialVersionUID = 1L;
    private final PCollectionView<Iterable<T>> view;

    private StreamingViewAsIterable(PCollectionView<Iterable<T>> view) {
      this.view = view;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return ((PCollection<T>)
              input.apply(Combine.globally(new Concatenate<T>()).withoutDefaults()))
          .apply(CreateApexPCollectionView.of(view));
    }

    @Override
    protected String getKindString() {
      return "StreamingViewAsIterable";
    }

    static class Factory<T>
        extends SingleInputOutputOverrideFactory<
            PCollection<T>, PCollection<T>, CreatePCollectionView<T, Iterable<T>>> {
      @Override
      public PTransformReplacement<PCollection<T>, PCollection<T>> getReplacementTransform(
          AppliedPTransform<PCollection<T>, PCollection<T>, CreatePCollectionView<T, Iterable<T>>>
              transform) {
        return PTransformReplacement.of(
            PTransformReplacements.getSingletonMainInput(transform),
            new StreamingViewAsIterable<>(transform.getTransform().getView()));
      }
    }
  }

  /**
   * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs. They
   * require the input {@link PCollection} fits in memory. For a large {@link PCollection} this is
   * expected to crash!
   *
   * @param <T> the type of elements to concatenate.
   */
  private static class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {
    private static final long serialVersionUID = 1L;

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
