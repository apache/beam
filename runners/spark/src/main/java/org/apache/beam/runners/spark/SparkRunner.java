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

import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineEvaluator;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.SparkProcessContext;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.StreamingEvaluationContext;
import org.apache.beam.runners.spark.translation.streaming.StreamingTransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.StreamingWindowPipelineDetector;
import org.apache.beam.runners.spark.util.SinglePrimitiveOutputPTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SparkRunner translate operations defined on a pipeline to a representation
 * executable by Spark, and then submitting the job to Spark to be executed. If we wanted to run
 * a dataflow pipeline with the default options of a single threaded spark instance in local mode,
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * EvaluationResult result = SparkRunner.create().run(p);
 * }
 *
 * To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
 * options.setSparkMaster("spark://host:port");
 * EvaluationResult result = SparkRunner.create(options).run(p);
 * }
 *
 * To create a Spark streaming pipeline runner use {@link SparkStreamingPipelineOptions}
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
   * Overrides for this runner.
   */
  @SuppressWarnings("rawtypes")
  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {

    if (transform instanceof GroupByKey) {
      return (OutputT) ((PCollection) input).apply(
          new GroupByKeyViaGroupByKeyOnly((GroupByKey) transform));
    } else if (transform instanceof Create.Values) {
      return (OutputT) super.apply(
        new SinglePrimitiveOutputPTransform((Create.Values) transform), input);
    } else {
      return super.apply(transform, input);
    }
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
      // validate streaming configuration
      if (mOptions.isStreaming() && !(mOptions instanceof SparkStreamingPipelineOptions)) {
        throw new RuntimeException("A streaming job must be configured with "
            + SparkStreamingPipelineOptions.class.getSimpleName() + ", found "
            + mOptions.getClass().getSimpleName());
      }
      LOG.info("Executing pipeline using the SparkRunner.");
      JavaSparkContext jsc = SparkContextFactory.getSparkContext(mOptions
              .getSparkMaster(), mOptions.getAppName());

      if (mOptions.isStreaming()) {
        SparkPipelineTranslator translator =
                new StreamingTransformTranslator.Translator(new TransformTranslator.Translator());
        // if streaming - fixed window should be defined on all UNBOUNDED inputs
        StreamingWindowPipelineDetector streamingWindowPipelineDetector =
            new StreamingWindowPipelineDetector(translator);
        pipeline.traverseTopologically(streamingWindowPipelineDetector);
        if (!streamingWindowPipelineDetector.isWindowing()) {
          throw new IllegalStateException("Spark streaming pipeline must be windowed!");
        }

        Duration batchInterval = streamingWindowPipelineDetector.getBatchDuration();
        LOG.info("Setting Spark streaming batchInterval to {} msec", batchInterval.milliseconds());
        EvaluationContext ctxt = createStreamingEvaluationContext(jsc, pipeline, batchInterval);

        pipeline.traverseTopologically(new SparkPipelineEvaluator(ctxt, translator));
        ctxt.computeOutputs();

        LOG.info("Streaming pipeline construction complete. Starting execution..");
        ((StreamingEvaluationContext) ctxt).getStreamingContext().start();

        return ctxt;
      } else {
        EvaluationContext ctxt = new EvaluationContext(jsc, pipeline);
        SparkPipelineTranslator translator = new TransformTranslator.Translator();
        pipeline.traverseTopologically(new SparkPipelineEvaluator(ctxt, translator));
        ctxt.computeOutputs();

        LOG.info("Pipeline execution complete.");

        return ctxt;
      }
    } catch (Exception e) {
      // Scala doesn't declare checked exceptions in the bytecode, and the Java compiler
      // won't let you catch something that is not declared, so we can't catch
      // SparkException here. Instead we do an instanceof check.
      // Then we find the cause by seeing if it's a user exception (wrapped by our
      // SparkProcessException), or just use the SparkException cause.
      if (e instanceof SparkException && e.getCause() != null) {
        if (e.getCause() instanceof SparkProcessContext.SparkProcessException
            && e.getCause().getCause() != null) {
          throw new RuntimeException(e.getCause().getCause());
        } else {
          throw new RuntimeException(e.getCause());
        }
      }
      // otherwise just wrap in a RuntimeException
      throw new RuntimeException(e);
    }
  }

  private EvaluationContext
      createStreamingEvaluationContext(JavaSparkContext jsc, Pipeline pipeline,
      Duration batchDuration) {
    SparkStreamingPipelineOptions streamingOptions = (SparkStreamingPipelineOptions) mOptions;
    JavaStreamingContext jssc = new JavaStreamingContext(jsc, batchDuration);
    return new StreamingEvaluationContext(jsc, pipeline, jssc, streamingOptions.getTimeout());
  }

  /**
   * Evaluator on the pipeline.
   */
  public abstract static class Evaluator extends Pipeline.PipelineVisitor.Defaults {
    protected static final Logger LOG = LoggerFactory.getLogger(Evaluator.class);

    protected final SparkPipelineTranslator translator;

    protected Evaluator(SparkPipelineTranslator translator) {
      this.translator = translator;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
      if (node.getTransform() != null) {
        @SuppressWarnings("unchecked")
        Class<PTransform<?, ?>> transformClass =
            (Class<PTransform<?, ?>>) node.getTransform().getClass();
        if (translator.hasTranslation(transformClass)) {
          LOG.info("Entering directly-translatable composite transform: '{}'", node.getFullName());
          LOG.debug("Composite transform class: '{}'", transformClass);
          doVisitTransform(node);
          return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
        }
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void visitPrimitiveTransform(TransformTreeNode node) {
      doVisitTransform(node);
    }

    protected abstract <TransformT extends PTransform<? super PInput, POutput>> void
        doVisitTransform(TransformTreeNode node);
  }
}

