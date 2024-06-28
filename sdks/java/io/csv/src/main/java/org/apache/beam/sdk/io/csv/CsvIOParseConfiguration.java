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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.csv.CSVFormat;

/** Stores parameters needed for CSV record parsing. */
@AutoValue
abstract class CsvIOParseConfiguration {

  static Builder builder() {
    return new AutoValue_CsvIOParseConfiguration.Builder();
  }

  /**
   * The expected <a
   * href="https://javadoc.io/doc/org.apache.commons/commons-csv/1.8/org/apache/commons/csv/CSVFormat.html">CSVFormat</a>
   * of the parsed CSV record.
   */
  abstract CSVFormat getCsvFormat();

  /** The expected {@link Schema} of the target type. */
  abstract Schema getSchema();

  /** A map of the {@link Schema.Field#getName()} to the custom CSV processing lambda. */
  abstract Map<String, SerializableFunction<String, Object>> getCustomProcessingMap();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setCsvFormat(CSVFormat csvFormat);

    abstract Builder setSchema(Schema schema);

    abstract Builder setCustomProcessingMap(
        Map<String, SerializableFunction<String, Object>> customProcessingMap);

    abstract Optional<Map<String, SerializableFunction<String, Object>>> getCustomProcessingMap();

    abstract CsvIOParseConfiguration autoBuild();

    final CsvIOParseConfiguration build() {
      if (!getCustomProcessingMap().isPresent()) {
        setCustomProcessingMap(new HashMap<>());
      }
      return autoBuild();
    }
  }
}
