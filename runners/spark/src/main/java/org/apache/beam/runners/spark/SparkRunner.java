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

import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformEvaluator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.SparkRunnerStreamingContextFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The SparkRunner translate operations defined on a pipeline to a representation
 * executable by Spark, and then submitting the job to Spark to be executed. If we wanted to run
 * a Beam pipeline with the default options of a single threaded spark instance in local mode,
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * EvaluationResult result = (EvaluationResult) p.run();
 * }
 *
 * <p>To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
 * options.setSparkMaster("spark://host:port");
 * EvaluationResult result = (EvaluationResult) p.run();
 * }
 */
public final class SparkRunner extends PipelineRunner<EvaluationResult> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);
  /**
   * Options used in this pipeline runner.
   */
  private final SparkPipelineOptions mOptions;

  /**
   * Creates and returns a new SparkRunner with default options. In particular, against a
   * spark instance running in local mode.
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
    SparkPipelineOptions sparkOptions =
        PipelineOptionsValidator.validate(SparkPipelineOptions.class, options);
    return new SparkRunner(sparkOptions);
  }

  /**
   * No parameter constructor defaults to running this pipeline in Spark's local mode, in a single
   * thread.
   */
  private SparkRunner(SparkPipelineOptions options) {
    mOptions = options;
  }

  @Override
  public EvaluationResult run(Pipeline pipeline) {
    try {
      LOG.info("Executing pipeline using the SparkRunner.");

      detectTranslationMode(pipeline);
      if (mOptions.isStreaming()) {
        SparkRunnerStreamingContextFactory contextFactory =
            new SparkRunnerStreamingContextFactory(pipeline, mOptions);
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(mOptions.getCheckpointDir(),
            contextFactory);

        LOG.info("Starting streaming pipeline execution.");
        jssc.start();

        // if recovering from checkpoint, we have to reconstruct the EvaluationResult instance.
        return contextFactory.getCtxt() == null ? new EvaluationContext(jssc.sparkContext(),
            pipeline, jssc) : contextFactory.getCtxt();
      } else {
        JavaSparkContext jsc = SparkContextFactory.getSparkContext(mOptions);
        EvaluationContext ctxt = new EvaluationContext(jsc, pipeline);
        SparkPipelineTranslator translator = new TransformTranslator.Translator();
        pipeline.traverseTopologically(new Evaluator(translator, ctxt));
        ctxt.computeOutputs();

        LOG.info("Pipeline execution complete.");

        return ctxt;
      }
    } catch (Exception e) {
      // Scala doesn't declare checked exceptions in the bytecode, and the Java compiler
      // won't let you catch something that is not declared, so we can't catch
      // SparkException here. Instead we do an instanceof check.
      // Then we find the cause by seeing if it's a user exception (wrapped by Beam's
      // UserCodeException), or just use the SparkException cause.
      if (e instanceof SparkException && e.getCause() != null) {
        if (e.getCause() instanceof UserCodeException && e.getCause().getCause() != null) {
          throw UserCodeException.wrap(e.getCause().getCause());
        } else {
          throw new RuntimeException(e.getCause());
        }
      }
      // otherwise just wrap in a RuntimeException
      throw new RuntimeException(e);
    }
  }

  /**
   * Detect the translation mode for the pipeline and change options in case streaming
   * translation is needed.
   * @param pipeline
   */
  private void detectTranslationMode(Pipeline pipeline) {
    TranslationModeDetector detector = new TranslationModeDetector();
    pipeline.traverseTopologically(detector);
    if (detector.getTranslationMode().equals(TranslationMode.STREAMING)) {
      // set streaming mode if it's a streaming pipeline
      this.mOptions.setStreaming(true);
    }
  }

  /**
   * The translation mode of the Beam Pipeline.
   */
  enum TranslationMode {
    /** Uses the batch mode. */
    BATCH,
    /** Uses the streaming mode. */
    STREAMING
  }

  /**
   * Traverses the Pipeline to determine the {@link TranslationMode} for this pipeline.
   */
  static class TranslationModeDetector extends Pipeline.PipelineVisitor.Defaults {
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
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      if (translationMode.equals(TranslationMode.BATCH)) {
        Class<? extends PTransform> transformClass = node.getTransform().getClass();
        if (transformClass == Read.Unbounded.class) {
          LOG.info("Found {}. Switching to streaming execution.", transformClass);
          translationMode = TranslationMode.STREAMING;
        }
      }
    }
  }

  /**
   * Evaluator on the pipeline.
   */
  public static class Evaluator extends Pipeline.PipelineVisitor.Defaults {
    private static final Logger LOG = LoggerFactory.getLogger(Evaluator.class);

    private final EvaluationContext ctxt;
    private final SparkPipelineTranslator translator;

    public Evaluator(SparkPipelineTranslator translator, EvaluationContext ctxt) {
      this.translator = translator;
      this.ctxt = ctxt;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      if (node.getTransform() != null) {
        @SuppressWarnings("unchecked")
        Class<PTransform<?, ?>> transformClass =
            (Class<PTransform<?, ?>>) node.getTransform().getClass();
        if (translator.hasTranslation(transformClass) && !shouldDefer(node)) {
          LOG.info("Entering directly-translatable composite transform: '{}'", node.getFullName());
          LOG.debug("Composite transform class: '{}'", transformClass);
          doVisitTransform(node);
          return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
        }
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    private boolean shouldDefer(TransformHierarchy.Node node) {
      PInput input = node.getInput();
      // if the input is not a PCollection, or it is but with non merging windows, don't defer.
      if (!(input instanceof PCollection)
          || ((PCollection) input).getWindowingStrategy().getWindowFn().isNonMerging()) {
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
        LOG.info("Deferring combine transformation {} for job {}", transform,
            ctxt.getPipeline().getOptions().getJobName());
        return true;
      }
      // default.
      return false;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      doVisitTransform(node);
    }

    <TransformT extends PTransform<? super PInput, POutput>> void
        doVisitTransform(TransformHierarchy.Node node) {
      @SuppressWarnings("unchecked")
      TransformT transform = (TransformT) node.getTransform();
      @SuppressWarnings("unchecked")
      Class<TransformT> transformClass = (Class<TransformT>) (Class<?>) transform.getClass();
      @SuppressWarnings("unchecked") TransformEvaluator<TransformT> evaluator =
          translate(node, transform, transformClass);
      LOG.info("Evaluating {}", transform);
      AppliedPTransform<PInput, POutput, TransformT> appliedTransform =
          AppliedPTransform.of(node.getFullName(), node.getInput(), node.getOutput(), transform);
      ctxt.setCurrentTransform(appliedTransform);
      evaluator.evaluate(transform, ctxt);
      ctxt.setCurrentTransform(null);
    }

    /**
     * Determine if this Node belongs to a Bounded branch of the pipeline, or Unbounded, and
     * translate with the proper translator.
     */
    private <TransformT extends PTransform<? super PInput, POutput>>
        TransformEvaluator<TransformT> translate(
            TransformHierarchy.Node node, TransformT transform, Class<TransformT> transformClass) {
      //--- determine if node is bounded/unbounded.
      // usually, the input determines if the PCollection to apply the next transformation to
      // is BOUNDED or UNBOUNDED, meaning RDD/DStream.
      Collection<? extends PValue> pValues;
      PInput pInput = node.getInput();
      if (pInput instanceof PBegin) {
        // in case of a PBegin, it's the output.
        pValues = node.getOutput().expand();
      } else {
        pValues = pInput.expand();
      }
      PCollection.IsBounded isNodeBounded = isBoundedCollection(pValues);
      // translate accordingly.
      LOG.debug("Translating {} as {}", transform, isNodeBounded);
      return isNodeBounded.equals(PCollection.IsBounded.BOUNDED)
          ? translator.translateBounded(transformClass)
              : translator.translateUnbounded(transformClass);
    }

    private PCollection.IsBounded isBoundedCollection(Collection<? extends PValue> pValues) {
      // anything that is not a PCollection, is BOUNDED.
      // For PCollections:
      // BOUNDED behaves as the Identity Element, BOUNDED + BOUNDED = BOUNDED
      // while BOUNDED + UNBOUNDED = UNBOUNDED.
      PCollection.IsBounded isBounded = PCollection.IsBounded.BOUNDED;
      for (PValue pValue: pValues) {
        if (pValue instanceof PCollection) {
          isBounded = isBounded.and(((PCollection) pValue).isBounded());
        } else {
          isBounded = isBounded.and(PCollection.IsBounded.BOUNDED);
        }
      }
      return isBounded;
    }
  }
}

