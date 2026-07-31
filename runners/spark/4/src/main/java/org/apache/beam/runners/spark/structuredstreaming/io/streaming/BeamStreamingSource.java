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

import java.io.Serializable;
import java.util.Base64;
import java.util.Map;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Spark DataSourceV2 {@link TableProvider} exposing an arbitrary Beam {@link
 * org.apache.beam.sdk.io.UnboundedSource} as a micro-batch streaming source.
 *
 * <p>The produced rows always have exactly two columns:
 *
 * <ul>
 *   <li>{@code payload} of type {@code BINARY}, holding the element encoded with the Beam {@code
 *       WindowedValues.FullWindowedValueCoder} supplied by the translator.
 *   <li>{@code eventTimestamp} of type {@code TIMESTAMP}, holding the event timestamp reported by
 *       the Beam reader for that element.
 * </ul>
 *
 * <p>Deliberately no Catalyst encoder is generated for Beam types, everything stays opaque bytes
 * until a downstream translator decodes it.
 *
 * <p>Translators should not reference this class directly, use {@link UnboundedSourceDataset#of}
 * instead.
 *
 * <p>Note on the format name: this class implements {@link DataSourceRegister} and reports the
 * short name {@value #SHORT_NAME}, but no {@code META-INF/services} entry is shipped, so the short
 * name is not resolvable through the {@code ServiceLoader}. Use {@link #FORMAT}, the fully
 * qualified class name, as the argument of {@code DataStreamReader.format(String)}.
 */
public class BeamStreamingSource implements TableProvider, DataSourceRegister {

  /** Short name of this source, see the class level note about {@code META-INF/services}. */
  public static final String SHORT_NAME = "beam-unbounded";

  /** Format string to pass to {@code DataStreamReader.format(String)}. */
  public static final String FORMAT =
      "org.apache.beam.runners.spark.structuredstreaming.io.streaming.BeamStreamingSource";

  /**
   * Base64 of the Java serialized {@link org.apache.beam.sdk.io.UnboundedSource}.
   *
   * <p>All option keys are lower case on purpose, Spark funnels DataSourceV2 options through {@link
   * CaseInsensitiveStringMap}.
   */
  public static final String OPT_SOURCE = "beam.source";

  /** Base64 of the Java serialized {@code Coder<WindowedValue<T>>}. */
  public static final String OPT_CODER = "beam.coder";

  /**
   * Base64 of the Java serialized {@link
   * org.apache.beam.runners.core.construction.SerializablePipelineOptions}.
   */
  public static final String OPT_PIPELINE_OPTIONS = "beam.pipelineoptions";

  /** Identifier making the reader cache key unique per source instance. */
  public static final String OPT_SOURCE_ID = "beam.sourceid";

  /** Desired number of splits handed to {@code UnboundedSource.split}. */
  public static final String OPT_NUM_SPLITS = "beam.numsplits";

  /** Maximum number of records read per split per micro-batch. */
  public static final String OPT_MAX_RECORDS = "beam.maxrecords";

  /** Maximum wall clock duration of a single micro-batch read, in milliseconds. */
  public static final String OPT_MAX_BATCH_DURATION_MILLIS = "beam.maxbatchdurationmillis";

  /** The fixed two column schema of this source. */
  public static final StructType SCHEMA =
      new StructType()
          .add(UnboundedSourceDataset.COL_PAYLOAD, DataTypes.BinaryType, false)
          .add(UnboundedSourceDataset.COL_EVENT_TS, DataTypes.TimestampType, false);

  /** Required public no-arg constructor, Spark instantiates this provider reflectively. */
  public BeamStreamingSource() {}

  @Override
  public String shortName() {
    return SHORT_NAME;
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return SCHEMA;
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return new BeamStreamingTable(new CaseInsensitiveStringMap(properties));
  }

  @Override
  public boolean supportsExternalMetadata() {
    return false;
  }

  /** Base64 encodes the Java serialized form of {@code value}. */
  static String encode(Serializable value) {
    return Base64.getEncoder().encodeToString(SerializableUtils.serializeToByteArray(value));
  }

  /** Inverse of {@link #encode}, {@code description} is only used in error messages. */
  @SuppressWarnings("unchecked")
  static <T extends @NonNull Object> T decode(String encoded, String description) {
    return (T)
        SerializableUtils.deserializeFromByteArray(
            Base64.getDecoder().decode(encoded), description);
  }

  /** Reads a required option, failing loudly rather than silently defaulting. */
  static String required(CaseInsensitiveStringMap options, String key) {
    String value = options.get(key);
    if (value == null) {
      throw new IllegalArgumentException(
          "Missing required option '" + key + "' for " + SHORT_NAME + " source.");
    }
    return value;
  }
}
