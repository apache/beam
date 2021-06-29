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
package org.apache.beam.examples.complete.datatokenization.transforms.io;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import org.apache.beam.examples.complete.datatokenization.utils.FailsafeElement;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The {@link TokenizationBigQueryIO} class for writing data from template to BigTable. */
public class TokenizationBigQueryIO {

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(TokenizationBigQueryIO.class);

  public static WriteResult write(
      PCollection<Row> input, String bigQueryTableName, TableSchema schema) {
    return input
        .apply("RowToTableRow", ParDo.of(new RowToTableRowFn()))
        .apply(
            "WriteSuccessfulRecords",
            org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.writeTableRows()
                .withCreateDisposition(
                    org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition
                        .CREATE_IF_NEEDED)
                .withWriteDisposition(
                    org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition
                        .WRITE_APPEND)
                .withExtendedErrorInfo()
                .withMethod(
                    org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                .withSchema(schema)
                .to(bigQueryTableName));
  }

  /**
   * Method to wrap a {@link BigQueryInsertError} into a {@link FailsafeElement}.
   *
   * @param insertError BigQueryInsert error.
   * @return FailsafeElement object.
   */
  public static FailsafeElement<String, String> wrapBigQueryInsertError(
      BigQueryInsertError insertError) {

    FailsafeElement<String, String> failsafeElement;
    try {

      failsafeElement =
          FailsafeElement.of(
              insertError.getRow().toPrettyString(), insertError.getRow().toPrettyString());
      failsafeElement.setErrorMessage(insertError.getError().toPrettyString());

    } catch (IOException e) {
      TokenizationBigQueryIO.LOG.error("Failed to wrap BigQuery insert error.");
      throw new RuntimeException(e);
    }
    return failsafeElement;
  }

  /**
   * The {@link RowToTableRowFn} class converts a row to tableRow using {@link
   * BigQueryUtils#toTableRow()}.
   */
  public static class RowToTableRowFn extends DoFn<Row, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      Row row = context.element();
      context.output(BigQueryUtils.toTableRow(row));
    }
  }
}
