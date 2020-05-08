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
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Convenience transform to write dead-letter {@link HealthcareIOError}s to BigQuery {@link
 * TableRow}s.
 *
 * <p>This can be used with {@link BigQueryIO.Write#withSchema(TableSchema)} by defining a dead
 * letter table schema
 *
 * <pre>{@code
 * ...
 * PCollection<HealthcareIOError<String>> errors = ...;
 *
 * TableSchema deadLetterSchema = new TableSchema();
 * deadLetterSchema.setFields(HealthcareIOErrorToTableRow.TABLE_FIELD_SCHEMAS);
 * TimePartitioning deadLetterPartitioning = new TimeParitioning();
 * deadLetterPartitioning.setField(HealthcareIOErrorToTableRow.TIMESTAMP_FIELD_NAME);
 *
 * errors.apply(
 *    BigQueryIO.write()
 *      .to(options.getDeadLetterTable())
 *      .withFormatFunction(new HealthcareIOErrorToTableRow())
 *      .withSchema(deadLetterSchema)
 *      .withTimePartitioning(deadLetterPartitioning)
 * );
 * }***
 * </pre>
 *
 * @param <T> the type parameter for the {@link HealthcareIOError}
 */
public class HealthcareIOErrorToTableRow<T>
    implements SerializableFunction<HealthcareIOError<T>, TableRow> {

  public HealthcareIOErrorToTableRow() {}

  private static final DateTimeFormatter DATETIME_FORMATTER = ISODateTimeFormat.dateTime();
  public static final String TIMESTAMP_FIELD_NAME = "observed_time";

  public static final List<TableFieldSchema> TABLE_FIELD_SCHEMAS =
      Stream.of(
              Pair.of("dataElement", "STRING"),
              Pair.of(TIMESTAMP_FIELD_NAME, "TIMESTAMP"),
              Pair.of("message", "STRING"),
              Pair.of("stacktrace", "STRING"),
              Pair.of("statusCode", "INTEGER"))
          .map(
              (Pair<String, String> field) -> {
                TableFieldSchema tfs = new TableFieldSchema();
                tfs.setName(field.getKey());
                tfs.setMode("NULLABLE");
                tfs.setType(field.getValue());
                return tfs;
              })
          .collect(Collectors.toList());

  @Override
  public TableRow apply(HealthcareIOError<T> err) {
    TableRow out = new TableRow();
    out.set("dataElement", err.getDataResource().toString());
    out.set(TIMESTAMP_FIELD_NAME, err.getObservedTime().toString(DATETIME_FORMATTER));
    out.set("message", err.getErrorMessage());
    out.set("stacktrace", err.getStackTrace());
    out.set("statusCode", err.getStatusCode());
    return out;
  }
}
