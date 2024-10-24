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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.MimeTypes;

/** Writes {@link TableRow} objects out to a file. Used when doing batch load jobs into BigQuery. */
class TableRowWriter<T> extends BigQueryRowWriter<T> {
  private static final Coder<TableRow> CODER = TableRowJsonCoder.of();
  private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);

  private final SerializableFunction<T, TableRow> toRow;

  TableRowWriter(String basename, SerializableFunction<T, TableRow> toRow) throws Exception {
    super(basename, MimeTypes.TEXT);
    this.toRow = toRow;
  }

  @Override
  void write(T value) throws IOException, BigQueryRowSerializationException {
    TableRow tableRow;
    try {
      tableRow = toRow.apply(value);
    } catch (Exception e) {
      throw new BigQueryRowSerializationException(e);
    }
    CODER.encode(tableRow, getOutputStream(), Context.OUTER);
    getOutputStream().write(NEWLINE);
  }
}
