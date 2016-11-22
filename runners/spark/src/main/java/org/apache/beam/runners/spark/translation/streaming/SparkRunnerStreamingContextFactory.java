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

package org.apache.beam.runners.spark.translation.streaming;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link JavaStreamingContext} factory for resilience.
 * @see <a href="https://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#how-to-configure-checkpointing">how-to-configure-checkpointing</a>
 */
public class SparkRunnerStreamingContextFactory implements JavaStreamingContextFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(SparkRunnerStreamingContextFactory.class);
  private static final String KNOWN_RELIABLE_FS_PATTERN = "^(hdfs|s3|gs)";

  private final Pipeline pipeline;
  private final SparkPipelineOptions options;

  public SparkRunnerStreamingContextFactory(Pipeline pipeline, SparkPipelineOptions options) {
    this.pipeline = pipeline;
    this.options = options;
  }

  private EvaluationContext ctxt;

  @Override
  public JavaStreamingContext create() {
    LOG.info("Creating a new Spark Streaming Context");
    // validate unbounded read properties.
    checkArgument(options.getMinReadTimeMillis() < options.getBatchIntervalMillis(),
        "Minimum read time has to be less than batch time.");
    checkArgument(options.getReadTimePercentage() > 0 && options.getReadTimePercentage() < 1,
        "Read time percentage is bound to (0, 1).");

    SparkPipelineTranslator translator = new StreamingTransformTranslator.Translator(
        new TransformTranslator.Translator());
    Duration batchDuration = new Duration(options.getBatchIntervalMillis());
    LOG.info("Setting Spark streaming batchDuration to {} msec", batchDuration.milliseconds());

    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);
    JavaStreamingContext jssc = new JavaStreamingContext(jsc, batchDuration);
    ctxt = new EvaluationContext(jsc, pipeline, jssc);
    pipeline.traverseTopologically(new SparkRunner.Evaluator(translator, ctxt));
    ctxt.computeOutputs();

    // set checkpoint dir.
    String checkpointDir = options.getCheckpointDir();
    if (!checkpointDir.matches(KNOWN_RELIABLE_FS_PATTERN)) {
      LOG.warn("The specified checkpoint dir {} does not match a reliable filesystem so in case "
          + "of failures this job may not recover properly or even at all.", checkpointDir);
    }
    LOG.info("Checkpoint dir set to: {}", checkpointDir);
    jssc.checkpoint(checkpointDir);

    // register listeners.
    for (JavaStreamingListener listener: options.as(SparkContextOptions.class).getListeners()) {
      LOG.info("Registered listener {}." + listener.getClass().getSimpleName());
      jssc.addStreamingListener(new JavaStreamingListenerWrapper(listener));
    }

    return jssc;
  }

  public EvaluationContext getCtxt() {
    return ctxt;
  }
}
