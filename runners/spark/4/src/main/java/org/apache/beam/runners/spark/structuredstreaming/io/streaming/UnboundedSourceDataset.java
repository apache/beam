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
package org.apache.beam.runners.spark.structuredstreaming.io.streaming;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Translator facing entry point turning a Beam {@link UnboundedSource} into a streaming Spark
 * {@link Dataset} of rows.
 *
 * <p>The returned dataset has exactly two columns, {@value #COL_PAYLOAD} of type {@code BINARY}
 * carrying the element encoded with the supplied {@code WindowedValue} coder, and {@value
 * #COL_EVENT_TS} of type {@code TIMESTAMP} carrying the event timestamp of that element.
 *
 * <p><b>The watermark is declared here and only here.</b> Spark 4 rejects a second {@code
 * withWatermark} declaration further down the plan, so this is the single declaration point for a
 * whole Beam pipeline. Downstream translators must never call {@code withWatermark} again, they
 * simply keep transforming the dataset. Both columns are still present in the returned dataset, the
 * event timestamp column has to survive at least until the first stateful operator for the
 * watermark to be meaningful.
 */
public final class UnboundedSourceDataset {

  /** Name of the binary column holding the encoded {@code WindowedValue}. */
  public static final String COL_PAYLOAD = "payload";

  /** Name of the timestamp column holding the Beam event timestamp. */
  public static final String COL_EVENT_TS = "eventTimestamp";

  /** Upper bound on the number of splits requested from a source, keeps the POC predictable. */
  private static final int MAX_DESIRED_SPLITS = 8;

  private UnboundedSourceDataset() {}

  /**
   * Builds the streaming {@link Dataset} for {@code source}, with the event time watermark already
   * applied.
   *
   * @param session the active Spark session
   * @param source the Beam unbounded source to read
   * @param windowedValueCoder the coder used to encode the {@value #COL_PAYLOAD} column, normally a
   *     {@code WindowedValues.FullWindowedValueCoder}
   * @param options the pipeline options, supplying the watermark delay and the micro-batch limits
   * @param <T> the element type of the source
   * @param <CheckpointMarkT> the checkpoint mark type of the source
   */
  public static <T, CheckpointMarkT extends UnboundedSource.CheckpointMark> Dataset<Row> of(
      SparkSession session,
      UnboundedSource<T, CheckpointMarkT> source,
      Coder<WindowedValue<T>> windowedValueCoder,
      SparkStructuredStreamingPipelineOptions options) {

    Map<String, String> readerOptions = new HashMap<>();
    readerOptions.put(BeamStreamingSource.OPT_SOURCE, BeamStreamingSource.encode(source));
    readerOptions.put(
        BeamStreamingSource.OPT_CODER, BeamStreamingSource.encode(windowedValueCoder));
    readerOptions.put(
        BeamStreamingSource.OPT_PIPELINE_OPTIONS,
        BeamStreamingSource.encode(new SerializablePipelineOptions(options)));
    readerOptions.put(BeamStreamingSource.OPT_SOURCE_ID, UUID.randomUUID().toString());
    readerOptions.put(
        BeamStreamingSource.OPT_NUM_SPLITS, Integer.toString(desiredNumSplits(session)));
    readerOptions.put(
        BeamStreamingSource.OPT_MAX_RECORDS,
        Integer.toString(options.getMaxRecordsPerMicroBatch()));
    readerOptions.put(
        BeamStreamingSource.OPT_MAX_BATCH_DURATION_MILLIS,
        Long.toString(options.getMaxBatchDurationMillis()));

    Dataset<Row> rows =
        session.readStream().format(BeamStreamingSource.FORMAT).options(readerOptions).load();

    // Exactly one watermark declaration per pipeline, see the class javadoc.
    return rows.withWatermark(COL_EVENT_TS, options.getWatermarkDelayMillis() + " milliseconds");
  }

  private static int desiredNumSplits(SparkSession session) {
    int parallelism = session.sparkContext().defaultParallelism();
    return Math.max(1, Math.min(MAX_DESIRED_SPLITS, parallelism));
  }
}
