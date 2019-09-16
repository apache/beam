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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.Checkpoint.CheckpointDir;
import org.apache.beam.sdk.Pipeline;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link JavaStreamingContext} factory for resilience.
 *
 * @see <a
 *     href="https://spark.apache.org/docs/1.6.3/streaming-programming-guide.html#how-to-configure-checkpointing">how-to-configure-checkpointing</a>
 */
public class SparkRunnerStreamingContextFactory implements Function0<JavaStreamingContext> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SparkRunnerStreamingContextFactory.class);

  // set members as transient to satisfy findbugs and since this only runs in driver.
  private final transient Pipeline pipeline;
  private final transient SparkPipelineOptions options;
  private final transient CheckpointDir checkpointDir;

  public SparkRunnerStreamingContextFactory(
      Pipeline pipeline, SparkPipelineOptions options, CheckpointDir checkpointDir) {
    this.pipeline = pipeline;
    this.options = options;
    this.checkpointDir = checkpointDir;
  }

  @Override
  public JavaStreamingContext call() throws Exception {
    LOG.info("Creating a new Spark Streaming Context");
    // validate unbounded read properties.
    checkArgument(
        options.getMinReadTimeMillis() < options.getBatchIntervalMillis(),
        "Minimum read time has to be less than batch time.");
    checkArgument(
        options.getReadTimePercentage() > 0 && options.getReadTimePercentage() < 1,
        "Read time percentage is bound to (0, 1).");

    SparkPipelineTranslator translator =
        new StreamingTransformTranslator.Translator(new TransformTranslator.Translator());
    Duration batchDuration = new Duration(options.getBatchIntervalMillis());
    LOG.info("Setting Spark streaming batchDuration to {} msec", batchDuration.milliseconds());

    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);
    JavaStreamingContext jssc = new JavaStreamingContext(jsc, batchDuration);

    // We must first init accumulators since translators expect them to be instantiated.
    SparkRunner.initAccumulators(options, jsc);
    // do not need to create a MetricsPusher instance here because if is called in SparkRunner.run()

    EvaluationContext ctxt = new EvaluationContext(jsc, pipeline, options, jssc);
    // update cache candidates
    SparkRunner.updateCacheCandidates(pipeline, translator, ctxt);
    pipeline.traverseTopologically(new SparkRunner.Evaluator(translator, ctxt));
    ctxt.computeOutputs();

    checkpoint(jssc, checkpointDir);

    return jssc;
  }

  private void checkpoint(JavaStreamingContext jssc, CheckpointDir checkpointDir) {
    Path rootCheckpointPath = checkpointDir.getRootCheckpointDir();
    Path sparkCheckpointPath = checkpointDir.getSparkCheckpointDir();
    Path beamCheckpointPath = checkpointDir.getBeamCheckpointDir();

    try {
      FileSystem fileSystem =
          rootCheckpointPath.getFileSystem(jssc.sparkContext().hadoopConfiguration());
      if (!fileSystem.exists(rootCheckpointPath)) {
        fileSystem.mkdirs(rootCheckpointPath);
      }
      if (!fileSystem.exists(sparkCheckpointPath)) {
        fileSystem.mkdirs(sparkCheckpointPath);
      }
      if (!fileSystem.exists(beamCheckpointPath)) {
        fileSystem.mkdirs(beamCheckpointPath);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create checkpoint dir", e);
    }

    jssc.checkpoint(sparkCheckpointPath.toString());
  }
}
