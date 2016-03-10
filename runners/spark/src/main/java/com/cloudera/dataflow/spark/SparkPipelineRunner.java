/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;

import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.dataflow.spark.streaming.SparkStreamingPipelineOptions;
import com.cloudera.dataflow.spark.streaming.StreamingEvaluationContext;
import com.cloudera.dataflow.spark.streaming.StreamingTransformTranslator;
import com.cloudera.dataflow.spark.streaming.StreamingWindowPipelineDetector;

/**
 * The SparkPipelineRunner translate operations defined on a pipeline to a representation
 * executable by Spark, and then submitting the job to Spark to be executed. If we wanted to run
 * a dataflow pipeline with the default options of a single threaded spark instance in local mode,
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * EvaluationResult result = SparkPipelineRunner.create().run(p);
 * }
 *
 * To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
 * options.setSparkMaster("spark://host:port");
 * EvaluationResult result = SparkPipelineRunner.create(options).run(p);
 * }
 *
 * To create a Spark streaming pipeline runner use {@link SparkStreamingPipelineOptions}
 */
public final class SparkPipelineRunner extends PipelineRunner<EvaluationResult> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPipelineRunner.class);
  /**
   * Options used in this pipeline runner.
   */
  private final SparkPipelineOptions mOptions;

  /**
   * Creates and returns a new SparkPipelineRunner with default options. In particular, against a
   * spark instance running in local mode.
   *
   * @return A pipeline runner with default options.
   */
  public static SparkPipelineRunner create() {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    return new SparkPipelineRunner(options);
  }

  /**
   * Creates and returns a new SparkPipelineRunner with specified options.
   *
   * @param options The SparkPipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkPipelineRunner create(SparkPipelineOptions options) {
    return new SparkPipelineRunner(options);
  }

  /**
   * Creates and returns a new SparkPipelineRunner with specified options.
   *
   * @param options The PipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkPipelineRunner fromOptions(PipelineOptions options) {
    SparkPipelineOptions sparkOptions =
        PipelineOptionsValidator.validate(SparkPipelineOptions.class, options);
    return new SparkPipelineRunner(sparkOptions);
  }

  /**
   * No parameter constructor defaults to running this pipeline in Spark's local mode, in a single
   * thread.
   */
  private SparkPipelineRunner(SparkPipelineOptions options) {
    mOptions = options;
  }


  @Override
  public EvaluationResult run(Pipeline pipeline) {
    try {
      // validate streaming configuration
      if (mOptions.isStreaming() && !(mOptions instanceof SparkStreamingPipelineOptions)) {
        throw new RuntimeException("A streaming job must be configured with " +
            SparkStreamingPipelineOptions.class.getSimpleName() + ", found " +
            mOptions.getClass().getSimpleName());
      }
      LOG.info("Executing pipeline using the SparkPipelineRunner.");
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
        if (e.getCause() instanceof SparkProcessContext.SparkProcessException &&
                e.getCause().getCause() != null) {
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

  public abstract static class Evaluator implements Pipeline.PipelineVisitor {
    protected static final Logger LOG = LoggerFactory.getLogger(Evaluator.class);

    protected final SparkPipelineTranslator translator;

    protected Evaluator(SparkPipelineTranslator translator) {
      this.translator = translator;
    }

    // Set upon entering a composite node which can be directly mapped to a single
    // TransformEvaluator.
    private TransformTreeNode currentTranslatedCompositeNode;

    /**
     * If true, we're currently inside a subtree of a composite node which directly maps to a
     * single
     * TransformEvaluator; children nodes are ignored, and upon post-visiting the translated
     * composite node, the associated TransformEvaluator will be visited.
     */
    private boolean inTranslatedCompositeNode() {
      return currentTranslatedCompositeNode != null;
    }

    @Override
    public void enterCompositeTransform(TransformTreeNode node) {
      if (!inTranslatedCompositeNode() && node.getTransform() != null) {
        @SuppressWarnings("unchecked")
        Class<PTransform<?, ?>> transformClass =
            (Class<PTransform<?, ?>>) node.getTransform().getClass();
        if (translator.hasTranslation(transformClass)) {
          LOG.info("Entering directly-translatable composite transform: '{}'", node.getFullName());
          LOG.debug("Composite transform class: '{}'", transformClass);
          currentTranslatedCompositeNode = node;
        }
      }
    }

    @Override
    public void leaveCompositeTransform(TransformTreeNode node) {
      // NB: We depend on enterCompositeTransform and leaveCompositeTransform providing 'node'
      // objects for which Object.equals() returns true iff they are the same logical node
      // within the tree.
      if (inTranslatedCompositeNode() && node.equals(currentTranslatedCompositeNode)) {
        LOG.info("Post-visiting directly-translatable composite transform: '{}'",
                node.getFullName());
        doVisitTransform(node);
        currentTranslatedCompositeNode = null;
      }
    }

    @Override
    public void visitTransform(TransformTreeNode node) {
      if (inTranslatedCompositeNode()) {
        LOG.info("Skipping '{}'; already in composite transform.", node.getFullName());
        return;
      }
      doVisitTransform(node);
    }

    protected abstract <PT extends PTransform<? super PInput, POutput>> void
        doVisitTransform(TransformTreeNode node);

    @Override
    public void visitValue(PValue value, TransformTreeNode producer) {
    }
  }
}

