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
package org.apache.beam.sdk.extensions.smb.avro;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.extensions.smb.FileOperations;
import org.apache.beam.sdk.io.SerializableAvroCodecFactory;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Supplier;

/** {@link FileOperations} implementation for Avro records. */
// TODO: figure out generic vs reflect, recordClass vs schema, compression, meta, etc.
public class AvroFileOperations<ValueT> extends FileOperations<ValueT> {
  private final Class<ValueT> recordClass;
  private final SerializableSchemaSupplier schemaSupplier;
  private final SerializableAvroCodecFactory codec;

  private static final CodecFactory DEFAULT_CODEC = CodecFactory.snappyCodec();

  private AvroFileOperations(Class<ValueT> recordClass, Schema schema, CodecFactory codec) {
    this.recordClass = recordClass;
    this.schemaSupplier = new SerializableSchemaSupplier(schema);
    this.codec = new SerializableAvroCodecFactory(codec);
  }

  public static AvroFileOperations<GenericRecord> of(Schema schema) {
    return new AvroFileOperations<>(null, schema, DEFAULT_CODEC);
  }

  public static <V extends SpecificRecordBase> AvroFileOperations<V> of(Class<V> recordClass) {
    // Use reflection to get SR schema
    final Schema schema = new ReflectData(recordClass.getClassLoader()).getSchema(recordClass);
    return new AvroFileOperations<>(recordClass, schema, DEFAULT_CODEC);
  }

  @Override
  public Reader<ValueT> createReader() {
    return new AvroReader<>(recordClass, schemaSupplier);
  }

  @Override
  public Writer<ValueT> createWriter() {
    return new AvroWriter<>(recordClass, schemaSupplier, codec);
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
    public void prepareRead(ReadableByteChannel channel) throws Exception {
      final Schema schema = schemaSupplier.get();

      DatumReader<ValueT> datumReader =
          recordClass == null
              ? new GenericDatumReader<>(schema)
              : new ReflectDatumReader<>(recordClass);

      reader = new DataFileStream<>(Channels.newInputStream(channel), datumReader);
    }

    @Override
    public ValueT read() throws Exception {
      return reader.hasNext() ? reader.next() : null;
    }

    @Override
    public void finishRead() throws Exception {
      reader.close();
    }
  }

  ////////////////////////////////////////
  // Writer
  ////////////////////////////////////////

  private static class AvroWriter<ValueT> extends FileOperations.Writer<ValueT> {

    private final Class<ValueT> recordClass;
    private final SerializableSchemaSupplier schemaSupplier;
    private final SerializableAvroCodecFactory codec;
    private transient DataFileWriter<ValueT> writer;

    AvroWriter(
        Class<ValueT> recordClass,
        SerializableSchemaSupplier schemaSupplier,
        SerializableAvroCodecFactory codec) {
      this.recordClass = recordClass;
      this.schemaSupplier = schemaSupplier;
      this.codec = codec;
    }

    @Override
    public String getMimeType() {
      return MimeTypes.BINARY;
    }

    @Override
    public void prepareWrite(WritableByteChannel channel) throws Exception {
      final Schema schema = schemaSupplier.get();
      DatumWriter<ValueT> datumWriter =
          recordClass == null
              ? new GenericDatumWriter<>(schema)
              : new ReflectDatumWriter<>(recordClass);
      writer = new DataFileWriter<>(datumWriter).setCodec(codec.getCodec());
      writer.create(schema, Channels.newOutputStream(channel));
    }

    @Override
    public void write(ValueT value) throws Exception {
      writer.append(value);
    }

    @Override
    public void close() throws Exception {
      writer.close();
    }
  }
}
