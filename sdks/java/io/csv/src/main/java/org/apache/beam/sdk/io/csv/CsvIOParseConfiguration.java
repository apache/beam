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
package org.apache.beam.sdk.io.csv;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;

/** Stores parameters needed for CSV record parsing. */
@AutoValue
abstract class CsvIOParseConfiguration<T> implements Serializable {

  static <T> Builder<T> builder() {
    return new AutoValue_CsvIOParseConfiguration.Builder<>();
  }

  /** The expected {@link CSVFormat} of the parsed CSV record. */
  abstract CSVFormat getCsvFormat();

  /** The expected {@link Schema} of the target type. */
  abstract Schema getSchema();

  /** A map of the {@link Schema.Field#getName()} to the custom CSV processing lambda. */
  abstract Map<String, SerializableFunction<String, Object>> getCustomProcessingMap();

  /** The expected {@link Coder} of the target type. */
  abstract Coder<T> getCoder();

  /** A {@link SerializableFunction} that converts from Row to the target type. */
  abstract SerializableFunction<Row, T> getFromRowFn();

  @AutoValue.Builder
  abstract static class Builder<T> implements Serializable {
    abstract Builder<T> setCsvFormat(CSVFormat csvFormat);

    abstract Builder<T> setSchema(Schema schema);

    abstract Builder<T> setCustomProcessingMap(
        Map<String, SerializableFunction<String, Object>> customProcessingMap);

    abstract Optional<Map<String, SerializableFunction<String, Object>>> getCustomProcessingMap();

    final Map<String, SerializableFunction<String, Object>> getOrCreateCustomProcessingMap() {
      if (!getCustomProcessingMap().isPresent()) {
        setCustomProcessingMap(new HashMap<>());
      }
      return getCustomProcessingMap().get();
    }

    abstract Builder<T> setCoder(Coder<T> coder);

    abstract Builder<T> setFromRowFn(SerializableFunction<Row, T> fromRowFn);

    abstract CsvIOParseConfiguration<T> autoBuild();

    final CsvIOParseConfiguration<T> build() {
      if (!getCustomProcessingMap().isPresent()) {
        setCustomProcessingMap(new HashMap<>());
      }

      return autoBuild();
    }
  }
}
