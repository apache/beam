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

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

abstract class BigQueryReaderFactory<T> implements BigQueryStorageReaderFactory<T>, Serializable {

  abstract TypeDescriptor<T> getOutputTypeDescriptor();

  abstract BoundedSource<T> getSource(
      MatchResult.Metadata metadata, TableSchema tableSchema, Coder<T> coder);

  abstract BoundedSource<T> getSource(
      String fileNameOrPattern, TableSchema tableSchema, Coder<T> coder);

  static <AvroT, T> BigQueryReaderFactory<T> avro(
      SerializableFunction<TableSchema, org.apache.avro.Schema> schemaFactory,
      AvroSource.DatumReaderFactory<AvroT> readerFactory,
      SerializableFunction<AvroT, T> fromAvro) {
    return new BigQueryAvroReaderFactory<>(schemaFactory, readerFactory, fromAvro);
  }

  static <T> BigQueryReaderFactory<T> arrow(
      SerializableFunction<TableSchema, Schema> schemaFactory,
      SerializableFunction<Row, T> fromArrow) {
    return new BigQueryArrowReaderFactory<>(schemaFactory, fromArrow);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Avro
  /////////////////////////////////////////////////////////////////////////////
  static class BigQueryAvroReaderFactory<AvroT, T> extends BigQueryReaderFactory<T> {
    private final SerializableFunction<TableSchema, org.apache.avro.Schema> schemaFactory;
    private final AvroSource.DatumReaderFactory<AvroT> readerFactory;
    private final SerializableFunction<AvroT, T> fromAvro;

    BigQueryAvroReaderFactory(
        SerializableFunction<TableSchema, org.apache.avro.Schema> schemaFactory,
        AvroSource.DatumReaderFactory<AvroT> readerFactory,
        SerializableFunction<AvroT, T> fromAvro) {
      this.schemaFactory = schemaFactory;
      this.readerFactory = readerFactory;
      this.fromAvro = fromAvro;
    }

    @Override
    TypeDescriptor<T> getOutputTypeDescriptor() {
      return TypeDescriptors.outputOf(fromAvro);
    }

    @Override
    public AvroSource<T> getSource(
        MatchResult.Metadata metadata, TableSchema tableSchema, Coder<T> coder) {
      return getSource(AvroSource.from(metadata), tableSchema, coder);
    }

    @Override
    public AvroSource<T> getSource(
        String fileNameOrPattern, TableSchema tableSchema, Coder<T> coder) {
      return getSource(AvroSource.from(fileNameOrPattern), tableSchema, coder);
    }

    private AvroSource<T> getSource(
        AvroSource<GenericRecord> source, TableSchema tableSchema, Coder<T> coder) {
      return source
          .withSchema(schemaFactory.apply(tableSchema))
          .withDatumReaderFactory(readerFactory)
          .withParseFn((SerializableFunction<GenericRecord, T>) fromAvro, coder);
    }

    @Override
    public BigQueryStorageAvroReader<AvroT, T> getReader(Table table, ReadSession readSession)
        throws IOException {
      org.apache.avro.Schema writerSchema =
          new org.apache.avro.Schema.Parser().parse(readSession.getAvroSchema().getSchema());
      TableSchema tableSchema =
          BigQueryAvroUtils.trimBigQueryTableSchema(table.getSchema(), writerSchema);
      return getReader(writerSchema, tableSchema);
    }

    @Override
    public BigQueryStorageAvroReader<AvroT, T> getReader(
        TableSchema tableSchema, ReadSession readSession) throws IOException {
      org.apache.avro.Schema writerSchema =
          new org.apache.avro.Schema.Parser().parse(readSession.getAvroSchema().getSchema());
      return getReader(writerSchema, tableSchema);
    }

    private BigQueryStorageAvroReader<AvroT, T> getReader(
        org.apache.avro.Schema writerSchema, TableSchema tableSchema) {
      org.apache.avro.Schema readerSchema = schemaFactory.apply(tableSchema);
      DatumReader<AvroT> datumReader = readerFactory.apply(writerSchema, readerSchema);
      return new BigQueryStorageAvroReader<>(datumReader, fromAvro);
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Arrow
  /////////////////////////////////////////////////////////////////////////////
  static class BigQueryArrowReaderFactory<T> extends BigQueryReaderFactory<T> {
    private final SerializableFunction<TableSchema, Schema> schemaFactory;
    private final SerializableFunction<Row, T> parseFn;

    BigQueryArrowReaderFactory(
        SerializableFunction<TableSchema, Schema> schemaFactory,
        SerializableFunction<Row, T> parseFn) {
      this.schemaFactory = schemaFactory;
      this.parseFn = parseFn;
    }

    @Override
    TypeDescriptor<T> getOutputTypeDescriptor() {
      return TypeDescriptors.outputOf(parseFn);
    }

    @Override
    BoundedSource<T> getSource(
        MatchResult.Metadata metadata, TableSchema tableSchema, Coder<T> coder) {
      throw new UnsupportedOperationException("Arrow file source not supported");
    }

    @Override
    BoundedSource<T> getSource(String fileNameOrPattern, TableSchema tableSchema, Coder<T> coder) {
      throw new UnsupportedOperationException("Arrow file source not supported");
    }

    @Override
    public BigQueryStorageArrowReader<T> getReader(Table table, ReadSession readSession)
        throws IOException {
      // TODO trim table schema
      return getReader(table.getSchema(), readSession);
    }

    @Override
    public BigQueryStorageArrowReader<T> getReader(TableSchema tableSchema, ReadSession readSession)
        throws IOException {
      try (InputStream input = readSession.getArrowSchema().getSerializedSchema().newInput()) {
        org.apache.arrow.vector.types.pojo.Schema arrowSchema =
            ArrowConversion.arrowSchemaFromInput(input);
        Schema schema = schemaFactory.apply(tableSchema);
        return new BigQueryStorageArrowReader<>(arrowSchema, schema, parseFn);
      }
    }
  }
}
