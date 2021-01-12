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
package org.apache.beam.examples.complete.datatokenization.utils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Transforms & DoFns & Options for Teleport Error logging. */
public class ErrorConverters {

  /** Writes all Errors to GCS, place at the end of your pipeline. */
  @AutoValue
  public abstract static class WriteStringMessageErrorsAsCsv
      extends PTransform<PCollection<FailsafeElement<String, String>>, PDone> {

    public static Builder newBuilder() {
      return new AutoValue_ErrorConverters_WriteStringMessageErrorsAsCsv.Builder();
    }

    public abstract String errorWritePath();

    public abstract String csvDelimiter();

    @Override
    public PDone expand(PCollection<FailsafeElement<String, String>> pCollection) {

      return pCollection
          .apply("GetFormattedErrorRow", ParDo.of(new FailedStringToCsvRowFn(csvDelimiter())))
          .apply(TextIO.write().to(errorWritePath()).withNumShards(1));
    }

    /** Builder for {@link WriteStringMessageErrorsAsCsv}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setErrorWritePath(String errorWritePath);

      public abstract Builder setCsvDelimiter(String csvDelimiter);

      public abstract WriteStringMessageErrorsAsCsv build();
    }
  }

  /**
   * The {@link FailedStringToCsvRowFn} converts string objects which have failed processing into
   * {@link String} objects contained CSV which can be output to a filesystem.
   */
  public static class FailedStringToCsvRowFn extends DoFn<FailsafeElement<String, String>, String> {

    /**
     * The formatter used to convert timestamps into a BigQuery compatible <a
     * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type">format</a>.
     */
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private final String csvDelimiter;

    public FailedStringToCsvRowFn(String csvDelimiter) {
      this.csvDelimiter = csvDelimiter;
    }

    public FailedStringToCsvRowFn() {
      this.csvDelimiter = ",";
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<String, String> failsafeElement = context.element();
      ArrayList<String> outputRow = new ArrayList<>();
      final String message = failsafeElement.getOriginalPayload();

      // Format the timestamp for insertion
      String timestamp =
          TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));

      outputRow.add(timestamp);
      outputRow.add(failsafeElement.getErrorMessage());

      // Only set the payload if it's populated on the message.
      if (message != null) {
        outputRow.add(message);
      }

      context.output(String.join(csvDelimiter, outputRow));
    }
  }

  public abstract static class WriteStringMessageErrors
      extends PTransform<PCollection<FailsafeElement<String, String>>, WriteResult> {

    public static Builder newBuilder() {
      return new AutoValue_ErrorConverters_WriteStringMessageErrors.Builder();
    }

    public abstract String getErrorRecordsTable();

    public abstract String getErrorRecordsTableSchema();

    @Override
    public WriteResult expand(PCollection<FailsafeElement<String, String>> failedRecords) {

      return failedRecords
          .apply("FailedRecordToTableRow", ParDo.of(new FailedStringToTableRowFn()))
          .apply(
              "WriteFailedRecordsToBigQuery",
              BigQueryIO.writeTableRows()
                  .to(getErrorRecordsTable())
                  .withJsonSchema(getErrorRecordsTableSchema())
                  .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                  .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    }

    /** Builder for {@link WriteStringMessageErrors}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setErrorRecordsTable(String errorRecordsTable);

      public abstract Builder setErrorRecordsTableSchema(String errorRecordsTableSchema);

      public abstract WriteStringMessageErrors build();
    }
  }

  /**
   * The {@link FailedStringToTableRowFn} converts string objects which have failed processing into
   * {@link TableRow} objects which can be output to a dead-letter table.
   */
  public static class FailedStringToTableRowFn
      extends DoFn<FailsafeElement<String, String>, TableRow> {

    /**
     * The formatter used to convert timestamps into a BigQuery compatible <a
     * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type">format</a>.
     */
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<String, String> failsafeElement = context.element();
      final String message = failsafeElement.getOriginalPayload();

      // Format the timestamp for insertion
      String timestamp =
          TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));

      // Build the table row
      final TableRow failedRow =
          new TableRow()
              .set("timestamp", timestamp)
              .set("errorMessage", failsafeElement.getErrorMessage())
              .set("stacktrace", failsafeElement.getStacktrace());

      // Only set the payload if it's populated on the message.
      if (message != null) {
        failedRow
            .set("payloadString", message)
            .set("payloadBytes", message.getBytes(StandardCharsets.UTF_8));
      }

      context.output(failedRow);
    }
  }
}
