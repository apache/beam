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

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link JavaStreamingContext} factory for resilience.
 * @see <a href="https://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#how-to-configure-checkpointing">how-to-configure-checkpointing</a>
 */
public class SparkRunnerStreamingContextFactory implements JavaStreamingContextFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(SparkRunnerStreamingContextFactory.class);
  private static final Iterable<String> KNOWN_RELIABLE_FS = Arrays.asList("hdfs", "s3", "gs");

  private final Pipeline pipeline;
  private final SparkPipelineOptions options;

  public SparkRunnerStreamingContextFactory(Pipeline pipeline, SparkPipelineOptions options) {
    this.pipeline = pipeline;
    this.options = options;
  }

  private StreamingEvaluationContext ctxt;

  @Override
  public JavaStreamingContext create() {
    LOG.info("Creating a new Spark Streaming Context");

    SparkPipelineTranslator translator = new StreamingTransformTranslator.Translator(
        new TransformTranslator.Translator());
    Duration batchDuration = new Duration(options.getBatchIntervalMillis());
    LOG.info("Setting Spark streaming batchDuration to {} msec", batchDuration.milliseconds());

    JavaSparkContext jsc = SparkContextFactory.getSparkContext(options);
    JavaStreamingContext jssc = new JavaStreamingContext(jsc, batchDuration);
    ctxt = new StreamingEvaluationContext(jsc, pipeline, jssc,
        options.getTimeout());
    pipeline.traverseTopologically(new SparkRunner.Evaluator(translator, ctxt));
    ctxt.computeOutputs();

    // set checkpoint dir.
    String checkpointDir = options.getCheckpointDir();
    LOG.info("Checkpoint dir set to: {}", checkpointDir);
    try {
      // validate checkpoint dir and warn if not of a known durable filesystem.
      URL checkpointDirUrl = new URL(checkpointDir);
      if (!Iterables.any(KNOWN_RELIABLE_FS, Predicates.equalTo(checkpointDirUrl.getProtocol()))) {
        LOG.warn("Checkpoint dir URL {} does not match a reliable filesystem, in case of failures "
            + "this job may not recover properly or even at all.", checkpointDirUrl);
      }
    } catch (MalformedURLException e) {
      throw new RuntimeException("Failed to form checkpoint dir URL. CheckpointDir should be in "
          + "the form of hdfs:///path/to/dir or other reliable fs protocol, "
              + "or file:///path/to/dir for local mode.", e);
    }
    jssc.checkpoint(checkpointDir);

    return jssc;
  }

  public StreamingEvaluationContext getCtxt() {
    return ctxt;
  }
}
