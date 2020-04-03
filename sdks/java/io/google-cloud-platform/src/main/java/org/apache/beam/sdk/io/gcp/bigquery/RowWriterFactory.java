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
import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

abstract class RowWriterFactory<ElementT, DestinationT> implements Serializable {
  private RowWriterFactory() {}

  enum OutputType {
    JsonTableRow,
    AvroGenericRecord
  }

  abstract OutputType getOutputType();

  abstract String getSourceFormat();

  abstract BigQueryRowWriter<ElementT> createRowWriter(
      String tempFilePrefix, DestinationT destination) throws Exception;

  static <ElementT, DestinationT> RowWriterFactory<ElementT, DestinationT> tableRows(
      SerializableFunction<ElementT, TableRow> toRow) {
    return new TableRowWriterFactory<ElementT, DestinationT>(toRow);
  }

  static final class TableRowWriterFactory<ElementT, DestinationT>
      extends RowWriterFactory<ElementT, DestinationT> {

    private final SerializableFunction<ElementT, TableRow> toRow;

    private TableRowWriterFactory(SerializableFunction<ElementT, TableRow> toRow) {
      this.toRow = toRow;
    }

    public SerializableFunction<ElementT, TableRow> getToRowFn() {
      return toRow;
    }

    @Override
    public OutputType getOutputType() {
      return OutputType.JsonTableRow;
    }

    @Override
    public BigQueryRowWriter<ElementT> createRowWriter(
        String tempFilePrefix, DestinationT destination) throws Exception {
      return new TableRowWriter<>(tempFilePrefix, toRow);
    }

    @Override
    String getSourceFormat() {
      return "NEWLINE_DELIMITED_JSON";
    }
  }

  static <ElementT, DestinationT> RowWriterFactory<ElementT, DestinationT> avroRecords(
      SerializableFunction<AvroWriteRequest<ElementT>, GenericRecord> toAvro,
      SerializableFunction<TableSchema, Schema> schemaFactory,
      DynamicDestinations<?, DestinationT> dynamicDestinations) {
    return new AvroRowWriterFactory<>(toAvro, schemaFactory, dynamicDestinations);
  }

  private static final class AvroRowWriterFactory<ElementT, DestinationT>
      extends RowWriterFactory<ElementT, DestinationT> {

    private final SerializableFunction<AvroWriteRequest<ElementT>, GenericRecord> toAvro;
    private final SerializableFunction<TableSchema, Schema> schemaFactory;
    private final DynamicDestinations<?, DestinationT> dynamicDestinations;

    private AvroRowWriterFactory(
        SerializableFunction<AvroWriteRequest<ElementT>, GenericRecord> toAvro,
        SerializableFunction<TableSchema, Schema> schemaFactory,
        DynamicDestinations<?, DestinationT> dynamicDestinations) {
      this.toAvro = toAvro;
      this.schemaFactory = schemaFactory;
      this.dynamicDestinations = dynamicDestinations;
    }

    @Override
    OutputType getOutputType() {
      return OutputType.AvroGenericRecord;
    }

    @Override
    BigQueryRowWriter<ElementT> createRowWriter(String tempFilePrefix, DestinationT destination)
        throws Exception {
      TableSchema tableSchema = dynamicDestinations.getSchema(destination);
      Schema avroSchema = schemaFactory.apply(tableSchema);
      return new AvroRowWriter<>(tempFilePrefix, avroSchema, toAvro);
    }

    @Override
    String getSourceFormat() {
      return "AVRO";
    }
  }
}
