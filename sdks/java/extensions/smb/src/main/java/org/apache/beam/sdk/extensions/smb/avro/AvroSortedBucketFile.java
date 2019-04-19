package org.apache.beam.sdk.extensions.smb.avro;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile;
import org.apache.beam.sdk.util.MimeTypes;

import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

// @Todo: figure out generic vs reflect, recordClass vs schema
public class AvroSortedBucketFile<ValueT> extends SortedBucketFile<ValueT> {

  private final Class<ValueT> recordClass;
  private final Supplier<Schema> schemaSupplier;

  public AvroSortedBucketFile(Class<ValueT> recordClass, Schema schema) {
    this.recordClass = recordClass;
    this.schemaSupplier = serializableSchemaSupplier(schema.toString());
  }

  @Override
  public Reader<ValueT> createReader() {
    return new AvroReader<ValueT>(recordClass, schemaSupplier.get());
  }

  @Override
  public Writer<ValueT> createWriter() {
    return new AvroWriter<ValueT>(recordClass, schemaSupplier.get());
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class AvroReader<ValueT> extends SortedBucketFile.Reader<ValueT> {

    private final Class<ValueT> recordClass;
    private final Schema schema;
    private DataFileStream<ValueT> reader;

    AvroReader(Class<ValueT> recordClass, Schema schema) {
      this.recordClass = recordClass;
      this.schema = schema;
    }

    @Override
    public void prepareRead(ReadableByteChannel channel) throws Exception {
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
    private final Schema schema;
    private DataFileWriter<ValueT> writer;

    AvroWriter(Class<ValueT> recordClass, Schema schema) {
      this.recordClass = recordClass;
      this.schema = schema;
    }

    @Override
    public String getMimeType() {
      return MimeTypes.BINARY;
    }

    @Override
    public void prepareWrite(WritableByteChannel channel) throws Exception {
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

  // copied from org.apache.beam.sdk.io.AvroUtils
  private static Supplier<Schema> serializableSchemaSupplier(String jsonSchema) {
    return Suppliers.memoize(
            Suppliers.compose(new JsonToSchema(), Suppliers.ofInstance(jsonSchema)));
  }
  private static class JsonToSchema implements Function<String, Schema>, Serializable {
    @Override
    public Schema apply(String input) {
      return new Schema.Parser().parse(input);
    }
  }
}
