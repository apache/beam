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
package org.apache.beam.runners.spark;

import static org.apache.beam.runners.spark.SparkCommonPipelineOptions.prepareFilesToStage;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.metrics.SparkBeamMetricSource;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformEvaluator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.Checkpoint.CheckpointDir;
import org.apache.beam.runners.spark.translation.streaming.SparkRunnerStreamingContextFactory;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.WatermarkAdvancingStreamingListener;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.util.construction.TransformInputs;
import org.apache.beam.sdk.util.construction.graph.ProjectionPushdownOptimizer;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.spark.SparkEnv$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SparkRunner translate operations defined on a pipeline to a representation executable by
 * Spark, and then submitting the job to Spark to be executed. If we wanted to run a Beam pipeline
 * with the default options of a single threaded spark instance in local mode, we would do the
 * following:
 *
 * <p>{@code Pipeline p = [logic for pipeline creation] SparkPipelineResult result =
 * (SparkPipelineResult) p.run(); }
 *
 * <p>To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * <p>{@code Pipeline p = [logic for pipeline creation] SparkPipelineOptions options =
 * SparkPipelineOptionsFactory.create(); options.setSparkMaster("spark://host:port");
 * SparkPipelineResult result = (SparkPipelineResult) p.run(); }
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class SparkRunner extends PipelineRunner<SparkPipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);

  /** Options used in this pipeline runner. */
  private final SparkPipelineOptions pipelineOptions;

  /**
   * Creates and returns a new SparkRunner with default options. In particular, against a spark
   * instance running in local mode.
   *
   * @return A pipeline runner with default options.
   */
  public static SparkRunner create() {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    return new SparkRunner(options);
  }

  /**
   * Creates and returns a new SparkRunner with specified options.
   *
   * @param options The SparkPipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkRunner create(SparkPipelineOptions options) {
    return new SparkRunner(options);
  }

  /**
   * Creates and returns a new SparkRunner with specified options.
   *
   * @param options The PipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkRunner fromOptions(PipelineOptions options) {
    return new SparkRunner(PipelineOptionsValidator.validate(SparkPipelineOptions.class, options));
  }

  /**
   * No parameter constructor defaults to running this pipeline in Spark's local mode, in a single
   * thread.
   */
  private SparkRunner(SparkPipelineOptions options) {
    pipelineOptions = options;
  }

  @Override
  public SparkPipelineResult run(final Pipeline pipeline) {
    LOG.info("Executing pipeline using the SparkRunner.");

    final SparkPipelineResult result;
    final Future<?> startPipeline;

    final SparkPipelineTranslator translator;

    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    MetricsEnvironment.setMetricsSupported(true);

    // visit the pipeline to determine the translation mode
    detectTranslationMode(pipeline, pipelineOptions);

    // Default to using the primitive versions of Read.Bounded and Read.Unbounded.
    // TODO(https://github.com/apache/beam/issues/20530): Use SDF read as default when we address
    // performance issue.
    if (!ExperimentalOptions.hasExperiment(pipeline.getOptions(), "beam_fn_api")) {
      SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReadsIfNecessary(pipeline);
    }

    if (!ExperimentalOptions.hasExperiment(pipelineOptions, "disable_projection_pushdown")) {
      ProjectionPushdownOptimizer.optimize(pipeline);
    }

    pipeline.replaceAll(SparkTransformOverrides.getDefaultOverrides(pipelineOptions.isStreaming()));

    prepareFilesToStage(pipelineOptions);

    if (pipelineOptions.isStreaming()) {
      CheckpointDir checkpointDir = new CheckpointDir(pipelineOptions.getCheckpointDir());
      SparkRunnerStreamingContextFactory streamingContextFactory =
          new SparkRunnerStreamingContextFactory(pipeline, pipelineOptions, checkpointDir);
      final JavaStreamingContext jssc =
          JavaStreamingContext.getOrCreate(
              checkpointDir.getSparkCheckpointDir().toString(), streamingContextFactory);
      // Checkpoint aggregator/metrics values
      jssc.addStreamingListener(
          new JavaStreamingListenerWrapper(
              new MetricsAccumulator.AccumulatorCheckpointingSparkListener()));

      // register user-defined listeners.
      for (JavaStreamingListener listener :
          pipelineOptions.as(SparkContextOptions.class).getListeners()) {
        LOG.info("Registered listener {}.", listener.getClass().getSimpleName());
        jssc.addStreamingListener(new JavaStreamingListenerWrapper(listener));
      }

      // register Watermarks listener to broadcast the advanced WMs.
      jssc.addStreamingListener(
          new JavaStreamingListenerWrapper(new WatermarkAdvancingStreamingListener()));

      // The reason we call initAccumulators here even though it is called in
      // SparkRunnerStreamingContextFactory is because the factory is not called when resuming
      // from checkpoint (When not resuming from checkpoint initAccumulators will be called twice
      // but this is fine since it is idempotent).
      initAccumulators(pipelineOptions, jssc.sparkContext());

      startPipeline =
          executorService.submit(
              () -> {
                LOG.info("Starting streaming pipeline execution.");
                jssc.start();
              });
      executorService.shutdown();

      result = new SparkPipelineResult.StreamingMode(startPipeline, jssc);
    } else {
      JavaSparkContext jsc = SparkContextFactory.getSparkContext(pipelineOptions);
      final EvaluationContext evaluationContext =
          new EvaluationContext(jsc, pipeline, pipelineOptions);
      translator = new TransformTranslator.Translator();

      // update the cache candidates
      updateCacheCandidates(pipeline, translator, evaluationContext);

      initAccumulators(pipelineOptions, jsc);
      startPipeline =
          executorService.submit(
              () -> {
                pipeline.traverseTopologically(new Evaluator(translator, evaluationContext));
                evaluationContext.computeOutputs();
                LOG.info("Batch pipeline execution complete.");
              });
      executorService.shutdown();

      result = new SparkPipelineResult.BatchMode(startPipeline, jsc);
    }

    if (pipelineOptions.getEnableSparkMetricSinks()) {
      registerMetricsSource(pipelineOptions.getAppName());
    }

    // it would have been better to create MetricsPusher from runner-core but we need
    // runner-specific
    // MetricsContainerStepMap
    MetricsPusher metricsPusher =
        new MetricsPusher(
            MetricsAccumulator.getInstance().value(),
            pipelineOptions.as(MetricsOptions.class),
            result);
    metricsPusher.start();

    return result;
  }

  private void registerMetricsSource(String appName) {
    final MetricsSystem metricsSystem = SparkEnv$.MODULE$.get().metricsSystem();
    final SparkBeamMetricSource metricsSource = new SparkBeamMetricSource(appName + ".Beam");
    // re-register the metrics in case of context re-use
    metricsSystem.removeSource(metricsSource);
    metricsSystem.registerSource(metricsSource);
  }

  /** Init Metrics/Aggregators accumulators. This method is idempotent. */
  public static void initAccumulators(SparkPipelineOptions opts, JavaSparkContext jsc) {
    // Init metrics accumulators
    MetricsAccumulator.init(opts, jsc);
  }

  /** Visit the pipeline to determine the translation mode (batch/streaming). */
  static void detectTranslationMode(Pipeline pipeline, SparkPipelineOptions pipelineOptions) {
    TranslationModeDetector detector = new TranslationModeDetector();
    pipeline.traverseTopologically(detector);
    if (detector.getTranslationMode().equals(TranslationMode.STREAMING)) {
      // set streaming mode if it's a streaming pipeline
      pipelineOptions.setStreaming(true);
    }
  }

  /** Evaluator that update/populate the cache candidates. */
  public static void updateCacheCandidates(
      Pipeline pipeline, SparkPipelineTranslator translator, EvaluationContext evaluationContext) {
    CacheVisitor cacheVisitor = new CacheVisitor(translator, evaluationContext);
    pipeline.traverseTopologically(cacheVisitor);
  }

  /** The translation mode of the Beam Pipeline. */
  enum TranslationMode {
    /** Uses the batch mode. */
    BATCH,
    /** Uses the streaming mode. */
    STREAMING
  }

  /** Traverses the Pipeline to determine the {@link TranslationMode} for this pipeline. */
  private static class TranslationModeDetector extends Pipeline.PipelineVisitor.Defaults {
    private static final Logger LOG = LoggerFactory.getLogger(TranslationModeDetector.class);

    private TranslationMode translationMode;

    TranslationModeDetector(TranslationMode defaultMode) {
      this.translationMode = defaultMode;
    }

    TranslationModeDetector() {
      this(TranslationMode.BATCH);
    }

    TranslationMode getTranslationMode() {
      return translationMode;
    }

    @Override
    public void visitValue(PValue value, Node producer) {
      if (translationMode.equals(TranslationMode.BATCH)) {
        if (value instanceof PCollection
            && ((PCollection) value).isBounded() == IsBounded.UNBOUNDED) {
          LOG.info(
              "Found unbounded PCollection {}. Switching to streaming execution.", value.getName());
          translationMode = TranslationMode.STREAMING;
        }
      }
    }
  }

  /** Traverses the pipeline to populate the candidates for caching. */
  static class CacheVisitor extends Evaluator {

    CacheVisitor(SparkPipelineTranslator translator, EvaluationContext evaluationContext) {
      super(translator, evaluationContext);
    }

    @Override
    public void doVisitTransform(TransformHierarchy.Node node) {
      // we populate cache candidates by updating the map with inputs of each node.
      // The goal is to detect the PCollections accessed more than one time, and so enable cache
      // on the underlying RDDs or DStreams.
      Map<TupleTag<?>, PValue> inputs = new HashMap<>(node.getInputs());
      for (TupleTag<?> tupleTag : node.getTransform().getAdditionalInputs().keySet()) {
        inputs.remove(tupleTag);
      }

      for (PValue value : inputs.values()) {
        if (value instanceof PCollection) {
          long count = 1L;
          if (ctxt.getCacheCandidates().get(value) != null) {
            count = ctxt.getCacheCandidates().get(value) + 1;
          }
          ctxt.getCacheCandidates().put((PCollection) value, count);
        }
      }
    }
  }

  /** Evaluator on the pipeline. */
  @SuppressWarnings("WeakerAccess")
  public static class Evaluator extends Pipeline.PipelineVisitor.Defaults {
    private static final Logger LOG = LoggerFactory.getLogger(Evaluator.class);

    protected final EvaluationContext ctxt;
    protected final SparkPipelineTranslator translator;

    public Evaluator(SparkPipelineTranslator translator, EvaluationContext ctxt) {
      this.translator = translator;
      this.ctxt = ctxt;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      PTransform<?, ?> transform = node.getTransform();
      if (transform != null) {
        if (translator.hasTranslation(transform) && !shouldDefer(node)) {
          LOG.info("Entering directly-translatable composite transform: '{}'", node.getFullName());
          LOG.debug("Composite transform class: '{}'", transform);
          doVisitTransform(node);
          return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
        }
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    protected boolean shouldDefer(TransformHierarchy.Node node) {
      // if the input is not a PCollection, or it is but with non merging windows, don't defer.
      Collection<PValue> nonAdditionalInputs =
          TransformInputs.nonAdditionalInputs(node.toAppliedPTransform(getPipeline()));
      if (nonAdditionalInputs.size() != 1) {
        return false;
      }
      PValue input = Iterables.getOnlyElement(nonAdditionalInputs);
      if (!(input instanceof PCollection)
          || !((PCollection) input).getWindowingStrategy().needsMerge()) {
        return false;
      }
      // so far we know that the input is a PCollection with merging windows.
      // check for sideInput in case of a Combine transform.
      PTransform<?, ?> transform = node.getTransform();
      boolean hasSideInput = false;
      if (transform instanceof Combine.PerKey) {
        List<PCollectionView<?>> sideInputs = ((Combine.PerKey<?, ?, ?>) transform).getSideInputs();
        hasSideInput = sideInputs != null && !sideInputs.isEmpty();
      } else if (transform instanceof Combine.Globally) {
        List<PCollectionView<?>> sideInputs = ((Combine.Globally<?, ?>) transform).getSideInputs();
        hasSideInput = sideInputs != null && !sideInputs.isEmpty();
      }
      // defer if sideInputs are defined.
      if (hasSideInput) {
        LOG.info(
            "Deferring combine transformation {} for job {}",
            transform,
            ctxt.getOptions().getJobName());
        return true;
      }
      // default.
      return false;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      doVisitTransform(node);
    }

    <TransformT extends PTransform<? super PInput, POutput>> void doVisitTransform(
        TransformHierarchy.Node node) {
      @SuppressWarnings("unchecked")
      TransformT transform = (TransformT) node.getTransform();
      TransformEvaluator<TransformT> evaluator = translate(node, transform);
      LOG.info("Evaluating {}", transform);
      AppliedPTransform<?, ?, ?> appliedTransform = node.toAppliedPTransform(getPipeline());
      ctxt.setCurrentTransform(appliedTransform);
      evaluator.evaluate(transform, ctxt);
      ctxt.setCurrentTransform(null);
    }

    /**
     * Determine if this Node belongs to a Bounded branch of the pipeline, or Unbounded, and
     * translate with the proper translator.
     */
    protected <TransformT extends PTransform<? super PInput, POutput>>
        TransformEvaluator<TransformT> translate(
            TransformHierarchy.Node node, TransformT transform) {
      // --- determine if node is bounded/unbounded.
      // usually, the input determines if the PCollection to apply the next transformation to
      // is BOUNDED or UNBOUNDED, meaning RDD/DStream.
      Map<TupleTag<?>, PCollection<?>> pValues;
      if (node.getInputs().isEmpty()) {
        // in case of a PBegin, it's the output.
        pValues = node.getOutputs();
      } else {
        pValues = node.getInputs();
      }
      PCollection.IsBounded isNodeBounded = isBoundedCollection(pValues.values());
      // translate accordingly.
      LOG.debug("Translating {} as {}", transform, isNodeBounded);
      return isNodeBounded.equals(PCollection.IsBounded.BOUNDED)
          ? translator.translateBounded(transform)
          : translator.translateUnbounded(transform);
    }

    protected PCollection.IsBounded isBoundedCollection(Collection<PCollection<?>> pValues) {
      // anything that is not a PCollection, is BOUNDED.
      // For PCollections:
      // BOUNDED behaves as the Identity Element, BOUNDED + BOUNDED = BOUNDED
      // while BOUNDED + UNBOUNDED = UNBOUNDED.
      PCollection.IsBounded isBounded = PCollection.IsBounded.BOUNDED;
      for (PCollection<?> pValue : pValues) {
        isBounded = isBounded.and(pValue.isBounded());
      }
      return isBounded;
    }
  }
}
