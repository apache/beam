package org.apache.beam.sdk.extensions.smb.avro;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Supplier;

// @Todo: figure out generic vs reflect, recordClass vs schema
public class AvroSortedBucketFile<ValueT> extends SortedBucketFile<ValueT> {
  private final Class<ValueT> recordClass;
  private final SerializableSchemaSupplier schemaSupplier;

  public AvroSortedBucketFile(Class<ValueT> recordClass, Schema schema) {
    this.recordClass = recordClass;
    this.schemaSupplier = new SerializableSchemaSupplier(schema);
  }

  @Override
  public Reader<ValueT> createReader() {
    return new AvroReader<>(recordClass, schemaSupplier);
  }

  @Override
  public Writer<ValueT> createWriter() {
    return new AvroWriter<>(recordClass, schemaSupplier);
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

  private static class SerializableSchemaSupplier implements Serializable, Supplier<Schema> {
    private final Schema schema;

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

  static class AvroReader<ValueT> extends SortedBucketFile.Reader<ValueT> implements Serializable {
    private Class<ValueT> recordClass;
    private SerializableSchemaSupplier schemaSupplier;
    private transient DataFileStream<ValueT> reader;

    AvroReader(Class<ValueT> recordClass, SerializableSchemaSupplier schemaSupplier) {
      this.recordClass = recordClass;
      this.schemaSupplier = schemaSupplier;
    }

    @Override
    public Coder<? extends Reader> coder() {
      return SerializableCoder.of(AvroReader.class);
    }

    @Override
    public void prepareRead(ReadableByteChannel channel) throws Exception {
      final Schema schema = schemaSupplier.get();

      DatumReader<ValueT> datumReader = recordClass == null
              ? new GenericDatumReader<>(schema)
              : new ReflectDatumReader<>(schema);
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

  private static class AvroWriter<ValueT> extends SortedBucketFile.Writer<ValueT> {

    private final Class<ValueT> recordClass;
    private final SerializableSchemaSupplier schemaSupplier;
    private DataFileWriter<ValueT> writer;

    AvroWriter(Class<ValueT> recordClass, SerializableSchemaSupplier schemaSupplier) {
      this.recordClass = recordClass;
      this.schemaSupplier = schemaSupplier;
    }

    @Override
    public String getMimeType() {
      return MimeTypes.BINARY;
    }

    @Override
    public void prepareWrite(WritableByteChannel channel) throws Exception {
      final Schema schema = schemaSupplier.get();
      DatumWriter<ValueT> datumWriter = recordClass == null
              ? new GenericDatumWriter<>(schema)
              : new ReflectDatumWriter<>(schema);
      writer = new DataFileWriter<>(datumWriter);
      writer.create(schema, Channels.newOutputStream(channel));
    }

    @Override
    public void write(ValueT value) throws Exception {
      writer.append(value);
    }

    @Override
    public void finishWrite() throws Exception {
      writer.close();
    }
  }
}
