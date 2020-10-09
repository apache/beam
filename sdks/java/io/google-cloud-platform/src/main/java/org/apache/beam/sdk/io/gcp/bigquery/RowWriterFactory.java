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
import org.apache.avro.io.DatumWriter;
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
      SerializableFunction<ElementT, TableRow> toRow,
      SerializableFunction<ElementT, TableRow> toFailsafeRow) {
    return new TableRowWriterFactory<ElementT, DestinationT>(toRow, toFailsafeRow);
  }

  static final class TableRowWriterFactory<ElementT, DestinationT>
      extends RowWriterFactory<ElementT, DestinationT> {

    private final SerializableFunction<ElementT, TableRow> toRow;
    private final SerializableFunction<ElementT, TableRow> toFailsafeRow;

    private TableRowWriterFactory(
        SerializableFunction<ElementT, TableRow> toRow,
        SerializableFunction<ElementT, TableRow> toFailsafeRow) {
      this.toRow = toRow;
      this.toFailsafeRow = toFailsafeRow; // TODO yummy
    }

    public SerializableFunction<ElementT, TableRow> getToRowFn() {
      return toRow;
    }

    public SerializableFunction<ElementT, TableRow> getToFailsafeRowFn() {
      if (toFailsafeRow == null) {
        return toRow;
      }
      return toFailsafeRow;
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

  static <ElementT, AvroT, DestinationT>
      AvroRowWriterFactory<ElementT, AvroT, DestinationT> avroRecords(
          SerializableFunction<AvroWriteRequest<ElementT>, AvroT> toAvro,
          SerializableFunction<Schema, DatumWriter<AvroT>> writerFactory) {
    return new AvroRowWriterFactory<>(toAvro, writerFactory, null, null);
  }

  static final class AvroRowWriterFactory<ElementT, AvroT, DestinationT>
      extends RowWriterFactory<ElementT, DestinationT> {

    private final SerializableFunction<AvroWriteRequest<ElementT>, AvroT> toAvro;
    private final SerializableFunction<Schema, DatumWriter<AvroT>> writerFactory;
    private final SerializableFunction<TableSchema, Schema> schemaFactory;
    private final DynamicDestinations<?, DestinationT> dynamicDestinations;

    private AvroRowWriterFactory(
        SerializableFunction<AvroWriteRequest<ElementT>, AvroT> toAvro,
        SerializableFunction<Schema, DatumWriter<AvroT>> writerFactory,
        SerializableFunction<TableSchema, Schema> schemaFactory,
        DynamicDestinations<?, DestinationT> dynamicDestinations) {
      this.toAvro = toAvro;
      this.writerFactory = writerFactory;
      this.schemaFactory = schemaFactory;
      this.dynamicDestinations = dynamicDestinations;
    }

    AvroRowWriterFactory<ElementT, AvroT, DestinationT> prepare(
        DynamicDestinations<?, DestinationT> dynamicDestinations,
        SerializableFunction<TableSchema, Schema> schemaFactory) {
      return new AvroRowWriterFactory<>(toAvro, writerFactory, schemaFactory, dynamicDestinations);
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
      return new AvroRowWriter<>(tempFilePrefix, avroSchema, toAvro, writerFactory);
    }

    @Override
    String getSourceFormat() {
      return "AVRO";
    }
  }
}
