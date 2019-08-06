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
package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.SerializableAvroCodecFactory;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;

/** {@link org.apache.beam.sdk.extensions.smb.FileOperations} implementation for Avro files. */
class AvroFileOperations<ValueT> extends FileOperations<ValueT> {
  private static final CodecFactory DEFAULT_CODEC = CodecFactory.snappyCodec();

  private final Class<ValueT> recordClass;
  private final SerializableSchemaSupplier schemaSupplier;
  private final SerializableAvroCodecFactory codec;

  private AvroFileOperations(Class<ValueT> recordClass, Schema schema, CodecFactory codec) {
    super(Compression.UNCOMPRESSED, MimeTypes.BINARY); // Avro has its own compression via codec
    this.recordClass = recordClass;
    this.schemaSupplier = new SerializableSchemaSupplier(schema);
    this.codec = new SerializableAvroCodecFactory(codec);
  }

  public static <V extends GenericRecord> AvroFileOperations<V> of(Schema schema) {
    return of(schema, DEFAULT_CODEC);
  }

  public static <V extends GenericRecord> AvroFileOperations<V> of(
      Schema schema, CodecFactory codec) {
    return new AvroFileOperations<>(null, schema, codec);
  }

  public static <V extends SpecificRecordBase> AvroFileOperations<V> of(Class<V> recordClass) {
    return of(recordClass, DEFAULT_CODEC);
  }

  public static <V extends SpecificRecordBase> AvroFileOperations<V> of(
      Class<V> recordClass, CodecFactory codec) {
    // Use reflection to get SR schema
    final Schema schema = new ReflectData(recordClass.getClassLoader()).getSchema(recordClass);
    return new AvroFileOperations<>(recordClass, schema, codec);
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("codecFactory", codec.getCodec().getClass()));
    builder.add(DisplayData.item("schema", schemaSupplier.schema.getFullName()));
  }

  @Override
  protected Reader<ValueT> createReader() {
    return new AvroReader<>(recordClass, schemaSupplier);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected FileIO.Sink<ValueT> createSink() {
    final AvroIO.Sink<ValueT> sink =
        recordClass == null
            ? (AvroIO.Sink<ValueT>) AvroIO.sink(getSchema())
            : AvroIO.sink(recordClass);
    return sink.withCodec(codec.getCodec());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Coder<ValueT> getCoder() {
    return recordClass == null
        ? (AvroCoder<ValueT>) AvroCoder.of(getSchema())
        : AvroCoder.of(recordClass);
  }

  Schema getSchema() {
    return schemaSupplier.get();
  }

  Class<ValueT> getRecordClass() {
    return recordClass;
  }

  private static class SerializableSchemaString implements Serializable {
    private final String schema;

    private SerializableSchemaString(String schema) {
      this.schema = schema;
    }

    private Object readResolve() throws IOException, ClassNotFoundException {
      return new SerializableSchemaSupplier(new Schema.Parser().parse(schema));
    }
  }

  static class SerializableSchemaSupplier implements Serializable, Supplier<Schema> {
    private transient Schema schema;

    private SerializableSchemaSupplier(Schema schema) {
      this.schema = schema;
    }

    private Object writeReplace() {
      return new SerializableSchemaString(schema.toString());
    }

    @Override
    public Schema get() {
      return schema;
    }
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class AvroReader<ValueT> extends FileOperations.Reader<ValueT> {
    private Class<ValueT> recordClass;
    private SerializableSchemaSupplier schemaSupplier;
    private transient DataFileStream<ValueT> reader;

    AvroReader(Class<ValueT> recordClass, SerializableSchemaSupplier schemaSupplier) {
      this.recordClass = recordClass;
      this.schemaSupplier = schemaSupplier;
    }

    @Override
    public void prepareRead(ReadableByteChannel channel) throws IOException {
      final Schema schema = schemaSupplier.get();

      DatumReader<ValueT> datumReader =
          recordClass == null
              ? new GenericDatumReader<>(schema)
              : new ReflectDatumReader<>(recordClass);

      reader = new DataFileStream<>(Channels.newInputStream(channel), datumReader);
    }

    @Override
    ValueT readNext() {
      return reader.next();
    }

    @Override
    boolean hasNextElement() {
      return reader.hasNext();
    }

    @Override
    public void finishRead() throws IOException {
      reader.close();
    }
  }
}
