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
package org.apache.beam.sdk.io;

import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link FileBasedSink} for Avro files. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class AvroSink<UserT, DestinationT, OutputT>
    extends FileBasedSink<UserT, DestinationT, OutputT> {
  private final Class<OutputT> type;

  @FunctionalInterface
  public interface DatumWriterFactory<T> extends Serializable {
    DatumWriter<T> apply(Schema writer);
  }

  // Keep this logic in sync with AvroGenericCoder.createWriter
  public static final DatumWriterFactory<GenericRecord> GENERIC_DATUM_WRITER_FACTORY =
      GenericDatumWriter::new;

  // Keep this logic in sync with AvroSpecificCoder.createWriter
  public static class SpecificDatumWriterFactory<T> implements DatumWriterFactory<T> {

    private final Class<T> type;

    public SpecificDatumWriterFactory(Class<T> type) {
      this.type = type;
    }

    @Override
    public DatumWriter<T> apply(Schema writer) {
      // create the datum writer using the Class<T> api.
      // avro will load the proper class loader and when using avro 1.9
      // the proper data with conversions (SpecificData.getForClass)
      SpecificDatumWriter<T> datumWriter = new SpecificDatumWriter<>(type);
      datumWriter.setSchema(writer);
      return datumWriter;
    }
  }

  // Keep this logic in sync with AvroReflectCoder.createWriter
  public static class ReflectDatumWriterFactory<T> implements DatumWriterFactory<T> {

    private final Class<T> type;

    public ReflectDatumWriterFactory(Class<T> type) {
      this.type = type;
    }

    @Override
    public DatumWriter<T> apply(Schema writer) {
      ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>(type);
      datumWriter.setSchema(writer);
      return datumWriter;
    }
  }

  // Keep this logic in sync with AvroCoder.of
  public static <T> DatumWriterFactory<T> defaultWriterFactory(Class<T> type) {
    return defaultWriterFactory(type, true);
  }

  // Keep this logic in sync with AvroCoder.of
  public static <T> DatumWriterFactory<T> defaultWriterFactory(
      Class<T> type, boolean useReflectApi) {
    if (GenericRecord.class.equals(type)) {
      return (DatumWriterFactory<T>) GENERIC_DATUM_WRITER_FACTORY;
    } else if (SpecificRecord.class.isAssignableFrom(type) && !useReflectApi) {
      return new SpecificDatumWriterFactory<>(type);
    } else {
      return new ReflectDatumWriterFactory<>(type);
    }
  }

  AvroSink(
      Class<OutputT> type,
      ValueProvider<ResourceId> outputPrefix,
      DynamicAvroDestinations<UserT, DestinationT, OutputT> dynamicDestinations) {
    // Avro handles compression internally using the codec.
    super(outputPrefix, dynamicDestinations, Compression.UNCOMPRESSED);
    this.type = type;
  }

  @Override
  public DynamicAvroDestinations<UserT, DestinationT, OutputT> getDynamicDestinations() {
    return (DynamicAvroDestinations<UserT, DestinationT, OutputT>) super.getDynamicDestinations();
  }

  @Override
  public WriteOperation<DestinationT, OutputT> createWriteOperation() {
    return new AvroWriteOperation<>(type, this);
  }

  /** A {@link WriteOperation WriteOperation} for Avro files. */
  private static class AvroWriteOperation<DestinationT, OutputT>
      extends WriteOperation<DestinationT, OutputT> {
    private final Class<OutputT> type;
    private final DynamicAvroDestinations<?, DestinationT, OutputT> dynamicDestinations;

    private AvroWriteOperation(Class<OutputT> type, AvroSink<?, DestinationT, OutputT> sink) {
      super(sink);
      this.dynamicDestinations = sink.getDynamicDestinations();
      this.type = type;
    }

    @Override
    public Writer<DestinationT, OutputT> createWriter() throws Exception {
      return new AvroWriter<>(type, this, dynamicDestinations);
    }
  }

  /** A {@link Writer Writer} for Avro files. */
  private static class AvroWriter<DestinationT, OutputT> extends Writer<DestinationT, OutputT> {

    // Initialized in prepareWrite
    private @Nullable DataFileWriter<OutputT> dataFileWriter;

    private final Class<OutputT> type;
    private final DynamicAvroDestinations<?, DestinationT, OutputT> dynamicDestinations;

    public AvroWriter(
        Class<OutputT> type,
        WriteOperation<DestinationT, OutputT> writeOperation,
        DynamicAvroDestinations<?, DestinationT, OutputT> dynamicDestinations) {
      super(writeOperation, MimeTypes.BINARY);
      this.type = type;
      this.dynamicDestinations = dynamicDestinations;
    }

    @SuppressWarnings("deprecation") // uses internal test functionality.
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      DestinationT destination = getDestination();
      CodecFactory codec = dynamicDestinations.getCodec(destination);
      Schema schema = dynamicDestinations.getSchema(destination);
      Map<String, Object> metadata = dynamicDestinations.getMetadata(destination);
      DatumWriter<OutputT> datumWriter =
          Optional.ofNullable(dynamicDestinations.getDatumWriterFactory(destination))
              .orElse(defaultWriterFactory(type))
              .apply(schema);
      dataFileWriter = new DataFileWriter<>(datumWriter).setCodec(codec);
      for (Map.Entry<String, Object> entry : metadata.entrySet()) {
        Object v = entry.getValue();
        if (v instanceof String) {
          dataFileWriter.setMeta(entry.getKey(), (String) v);
        } else if (v instanceof Long) {
          dataFileWriter.setMeta(entry.getKey(), (Long) v);
        } else if (v instanceof byte[]) {
          dataFileWriter.setMeta(entry.getKey(), (byte[]) v);
        } else {
          throw new IllegalStateException(
              "Metadata value type must be one of String, Long, or byte[]. Found "
                  + v.getClass().getSimpleName());
        }
      }
      dataFileWriter.create(schema, Channels.newOutputStream(channel));
    }

    @Override
    public void write(OutputT value) throws Exception {
      dataFileWriter.append(value);
    }

    @Override
    protected void finishWrite() throws Exception {
      dataFileWriter.flush();
    }
  }
}
