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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.StreamingTransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline runner which translates a Beam pipeline into equivalent Spark operations, without
 * running them. Used for debugging purposes.
 *
 * <p>Example:
 *
 * <pre>{@code
 * SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
 * options.setRunner(SparkRunnerDebugger.class);
 * Pipeline pipeline = Pipeline.create(options);
 * SparkRunnerDebugger.DebugSparkPipelineResult result =
 *     (SparkRunnerDebugger.DebugSparkPipelineResult) pipeline.run();
 * String sparkPipeline = result.getDebugString();
 * }</pre>
 */
public final class SparkRunnerDebugger extends PipelineRunner<SparkPipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRunnerDebugger.class);

  private final SparkPipelineOptions options;

  private SparkRunnerDebugger(SparkPipelineOptions options) {
    this.options = options;
  }

  public static SparkRunnerDebugger fromOptions(PipelineOptions options) {
    if (options instanceof TestSparkPipelineOptions) {
      TestSparkPipelineOptions testSparkPipelineOptions =
          PipelineOptionsValidator.validate(TestSparkPipelineOptions.class, options);
      return new SparkRunnerDebugger(testSparkPipelineOptions);
    } else {
      SparkPipelineOptions sparkPipelineOptions =
          PipelineOptionsValidator.validate(SparkPipelineOptions.class, options);
      return new SparkRunnerDebugger(sparkPipelineOptions);
    }
  }

  @Override
  public SparkPipelineResult run(Pipeline pipeline) {
    boolean isStreaming =
        options.isStreaming() || options.as(TestSparkPipelineOptions.class).isForceStreaming();
    // Default to using the primitive versions of Read.Bounded and Read.Unbounded if we are
    // executing an unbounded pipeline or the user specifically requested it.
    if (isStreaming
        || ExperimentalOptions.hasExperiment(
            pipeline.getOptions(), "beam_fn_api_use_deprecated_read")
        || ExperimentalOptions.hasExperiment(pipeline.getOptions(), "use_deprecated_read")) {
      SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReads(pipeline);
    }
    JavaSparkContext jsc = new JavaSparkContext("local[1]", "Debug_Pipeline");
    JavaStreamingContext jssc =
        new JavaStreamingContext(jsc, new org.apache.spark.streaming.Duration(1000));

    SparkRunner.initAccumulators(options, jsc);

    TransformTranslator.Translator translator = new TransformTranslator.Translator();

    SparkNativePipelineVisitor visitor;
    if (isStreaming) {
      SparkPipelineTranslator streamingTranslator =
          new StreamingTransformTranslator.Translator(translator);
      EvaluationContext ctxt = new EvaluationContext(jsc, pipeline, options, jssc);
      visitor = new SparkNativePipelineVisitor(streamingTranslator, ctxt);
    } else {
      EvaluationContext ctxt = new EvaluationContext(jsc, pipeline, options, jssc);
      visitor = new SparkNativePipelineVisitor(translator, ctxt);
    }

    pipeline.traverseTopologically(visitor);

    jsc.stop();

    String debugString = visitor.getDebugString();
    LOG.info("Translated Native Spark pipeline:\n" + debugString);
    return new DebugSparkPipelineResult(debugString);
  }

  /**
   * PipelineResult of running a {@link Pipeline} using {@link SparkRunnerDebugger} Use {@link
   * #getDebugString} to get a {@link String} representation of the {@link Pipeline} translated into
   * Spark native operations.
   */
  public static class DebugSparkPipelineResult extends SparkPipelineResult {
    private final String debugString;

    DebugSparkPipelineResult(String debugString) {
      super(null, null);
      this.debugString = debugString;
    }

    /** Returns Beam pipeline translated into Spark native operations. */
    String getDebugString() {
      return debugString;
    }

    @Override
    protected void stop() {
      // Empty implementation
    }

    @Override
    protected State awaitTermination(Duration duration)
        throws TimeoutException, ExecutionException, InterruptedException {
      return State.DONE;
    }
  }
}
