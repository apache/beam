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

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

abstract class BigQueryReaderFactory<T> implements BigQueryStorageReaderFactory<T>, Serializable {

  abstract BoundedSource<T> getSource(
      MatchResult.Metadata metadata, TableSchema tableSchema, Coder<T> coder);

  abstract BoundedSource<T> getSource(
      String fileNameOrPattern, TableSchema tableSchema, Coder<T> coder);

  static <AvroT, T> BigQueryReaderFactory<T> avro(
      org.apache.avro.@Nullable Schema schema,
      boolean extractWithLogicalTypes,
      AvroSource.DatumReaderFactory<AvroT> readerFactory,
      SerializableBiFunction<TableSchema, AvroT, T> fromAvro) {
    return new BigQueryAvroReaderFactory<>(
        schema, extractWithLogicalTypes, readerFactory, fromAvro);
  }

  static <T> BigQueryReaderFactory<T> arrow(
      @Nullable Schema schema, SerializableBiFunction<TableSchema, Row, T> fromArrow) {
    return new BigQueryArrowReaderFactory<>(schema, fromArrow);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Avro
  /////////////////////////////////////////////////////////////////////////////
  private static class SerializableSchemaSupplier
      implements SerializableSupplier<org.apache.avro.Schema> {
    private transient org.apache.avro.Schema schema;
    private final String jsonSchema;

    SerializableSchemaSupplier(org.apache.avro.Schema schema) {
      this.schema = schema;
      this.jsonSchema = schema.toString();
    }

    @Override
    public org.apache.avro.Schema get() {
      if (schema == null) {
        schema = new org.apache.avro.Schema.Parser().parse(jsonSchema);
      }
      return schema;
    }
  }

  static class BigQueryAvroReaderFactory<AvroT, T> extends BigQueryReaderFactory<T> {
    private final @Nullable SerializableSchemaSupplier
        schemaSupplier; // avro 1.8 schema is not serializable
    private final boolean extractWithLogicalTypes;
    private final AvroSource.DatumReaderFactory<AvroT> readerFactory;
    private final SerializableBiFunction<TableSchema, AvroT, T> fromAvro;

    BigQueryAvroReaderFactory(
        org.apache.avro.@Nullable Schema schema,
        boolean extractWithLogicalTypes,
        AvroSource.DatumReaderFactory<AvroT> readerFactory,
        SerializableBiFunction<TableSchema, AvroT, T> fromAvro) {

      this.schemaSupplier = schema == null ? null : new SerializableSchemaSupplier(schema);
      this.extractWithLogicalTypes = extractWithLogicalTypes;
      this.readerFactory = readerFactory;
      this.fromAvro = fromAvro;
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
      org.apache.avro.Schema readerSchema;
      if (schemaSupplier != null) {
        readerSchema = schemaSupplier.get();
      } else {
        readerSchema = BigQueryUtils.toGenericAvroSchema(tableSchema, extractWithLogicalTypes);
      }
      SerializableFunction<GenericRecord, T> parseFn =
          (r) -> fromAvro.apply(tableSchema, (AvroT) r);
      return source
          .withSchema(readerSchema)
          .withDatumReaderFactory(readerFactory)
          .withParseFn(parseFn, coder);
    }

    @Override
    public BigQueryStorageAvroReader<AvroT, T> getReader(
        TableSchema tableSchema, ReadSession readSession) throws IOException {
      org.apache.avro.Schema writerSchema =
          new org.apache.avro.Schema.Parser().parse(readSession.getAvroSchema().getSchema());
      org.apache.avro.Schema readerSchema;
      if (schemaSupplier != null) {
        readerSchema = schemaSupplier.get();
      } else {
        // BQ storage always uses logical-types
        readerSchema = BigQueryUtils.toGenericAvroSchema(tableSchema, true);
      }
      SerializableFunction<AvroT, T> fromAvroRecord = (r) -> fromAvro.apply(tableSchema, r);
      return new BigQueryStorageAvroReader<>(
          writerSchema, readerSchema, readerFactory, fromAvroRecord);
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Arrow
  /////////////////////////////////////////////////////////////////////////////
  static class BigQueryArrowReaderFactory<T> extends BigQueryReaderFactory<T> {
    private final SerializableFunction<TableSchema, Schema> schemaFactory;
    private final SerializableBiFunction<TableSchema, Row, T> parseFn;

    BigQueryArrowReaderFactory(
        @Nullable Schema schema, SerializableBiFunction<TableSchema, Row, T> parseFn) {
      this.parseFn = parseFn;
      if (schema == null) {
        this.schemaFactory = BigQueryUtils::fromTableSchema;
      } else {
        this.schemaFactory = tableSchema -> schema;
      }
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
    public BigQueryStorageArrowReader<T> getReader(TableSchema tableSchema, ReadSession readSession)
        throws IOException {
      try (InputStream input = readSession.getArrowSchema().getSerializedSchema().newInput()) {
        org.apache.arrow.vector.types.pojo.Schema writerSchema =
            ArrowConversion.arrowSchemaFromInput(input);
        Schema readerSchema = schemaFactory.apply(tableSchema);
        SerializableFunction<Row, T> fromRow = (r) -> parseFn.apply(tableSchema, r);
        return new BigQueryStorageArrowReader<>(writerSchema, readerSchema, fromRow);
      }
    }
  }
}
