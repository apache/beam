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

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.classic.Dataset$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;
import scala.Option;
import scala.reflect.ClassTag;

/**
 * Translator facing entry point turning a Beam {@link UnboundedSource} into a streaming Spark
 * {@link Dataset} of rows.
 *
 * <p>The dataset has two columns, {@value #COL_PAYLOAD} of type {@code BINARY} holding the element
 * encoded with the supplied {@code WindowedValue} coder, and {@value #COL_EVENT_TS} of type {@code
 * TIMESTAMP} holding the event timestamp of that element.
 *
 * <p>The event time watermark is declared here and only here. Spark 4 rejects a second {@code
 * withWatermark} further down the plan, so downstream translators must never call it again.
 */
public final class UnboundedSourceDataset {

  public static final String COL_PAYLOAD = "payload";

  public static final String COL_EVENT_TS = "eventTimestamp";

  public static final StructType SCHEMA =
      new StructType()
          .add(COL_PAYLOAD, DataTypes.BinaryType, false)
          .add(COL_EVENT_TS, DataTypes.TimestampType, false);

  private static final String SOURCE_NAME = "beam-unbounded";

  private UnboundedSourceDataset() {}

  /**
   * Builds the streaming {@link Dataset} for {@code source} with the event time watermark applied.
   *
   * @param session the active Spark session
   * @param source the Beam unbounded source to read
   * @param windowedValueCoder the coder of the {@value #COL_PAYLOAD} column
   * @param options the pipeline options, supplying the watermark delay and the micro-batch limits
   * @param transformName the full name of the read transform, used for naming only
   * @param <T> the element type of the source
   * @param <CheckpointMarkT> the checkpoint mark type of the source
   */
  public static <T, CheckpointMarkT extends UnboundedSource.CheckpointMark> Dataset<Row> of(
      SparkSession session,
      UnboundedSource<T, CheckpointMarkT> source,
      Coder<WindowedValue<T>> windowedValueCoder,
      SparkStructuredStreamingPipelineOptions options,
      String transformName) {
    org.apache.spark.sql.classic.SparkSession classic =
        (org.apache.spark.sql.classic.SparkSession) session;
    Configuration hadoopConf = classic.sessionState().newHadoopConf();
    BeamSourceSpec<T> spec =
        new BeamSourceSpec<>(
            source,
            windowedValueCoder,
            broadcast(
                session,
                new SerializablePipelineOptions(options),
                SerializablePipelineOptions.class),
            broadcast(
                session,
                new SerializableConfiguration(hadoopConf),
                SerializableConfiguration.class),
            session.sparkContext().defaultParallelism(),
            options.getMaxRecordsPerBatch(),
            Math.max(1L, options.getMaxBatchDurationMillis()),
            options.getReaderIdleTimeoutMillis(),
            transformName);
    LogicalPlan plan =
        new StreamingRelationV2(
            Option.empty(),
            SOURCE_NAME,
            new BeamStreamingTable(spec),
            CaseInsensitiveStringMap.empty(),
            DataTypeUtils.toAttributes(SCHEMA),
            Option.empty(),
            Option.empty(),
            Option.empty());
    Dataset<Row> rows = Dataset$.MODULE$.ofRows(classic, plan);
    return rows.withWatermark(COL_EVENT_TS, options.getWatermarkDelayMillis() + " milliseconds");
  }

  private static <T> Broadcast<T> broadcast(SparkSession session, T value, Class<T> type) {
    return session.sparkContext().broadcast(value, ClassTag.apply(type));
  }
}
