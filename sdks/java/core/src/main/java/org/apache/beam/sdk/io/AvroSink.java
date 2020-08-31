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
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link FileBasedSink} for Avro files. */
public class AvroSink<UserT, DestinationT, OutputT>
    extends FileBasedSink<UserT, DestinationT, OutputT> {
  private final boolean genericRecords;

  @FunctionalInterface
  public interface DatumWriterFactory<T> extends Serializable {
    DatumWriter<T> apply(Schema writer);
  }

  AvroSink(
      ValueProvider<ResourceId> outputPrefix,
      DynamicAvroDestinations<UserT, DestinationT, OutputT> dynamicDestinations,
      boolean genericRecords) {
    // Avro handles compression internally using the codec.
    super(outputPrefix, dynamicDestinations, Compression.UNCOMPRESSED);
    this.genericRecords = genericRecords;
  }

  @Override
  public DynamicAvroDestinations<UserT, DestinationT, OutputT> getDynamicDestinations() {
    return (DynamicAvroDestinations<UserT, DestinationT, OutputT>) super.getDynamicDestinations();
  }

  @Override
  public WriteOperation<DestinationT, OutputT> createWriteOperation() {
    return new AvroWriteOperation<>(this, genericRecords);
  }

  /** A {@link WriteOperation WriteOperation} for Avro files. */
  private static class AvroWriteOperation<DestinationT, OutputT>
      extends WriteOperation<DestinationT, OutputT> {
    private final DynamicAvroDestinations<?, DestinationT, OutputT> dynamicDestinations;
    private final boolean genericRecords;

    private AvroWriteOperation(AvroSink<?, DestinationT, OutputT> sink, boolean genericRecords) {
      super(sink);
      this.dynamicDestinations = sink.getDynamicDestinations();
      this.genericRecords = genericRecords;
    }

    @Override
    public Writer<DestinationT, OutputT> createWriter() throws Exception {
      return new AvroWriter<>(this, dynamicDestinations, genericRecords);
    }
  }

  /** A {@link Writer Writer} for Avro files. */
  private static class AvroWriter<DestinationT, OutputT> extends Writer<DestinationT, OutputT> {

    // Initialized in prepareWrite
    private @Nullable DataFileWriter<OutputT> dataFileWriter;

    private final DynamicAvroDestinations<?, DestinationT, OutputT> dynamicDestinations;
    private final boolean genericRecords;

    public AvroWriter(
        WriteOperation<DestinationT, OutputT> writeOperation,
        DynamicAvroDestinations<?, DestinationT, OutputT> dynamicDestinations,
        boolean genericRecords) {
      super(writeOperation, MimeTypes.BINARY);
      this.dynamicDestinations = dynamicDestinations;
      this.genericRecords = genericRecords;
    }

    @SuppressWarnings("deprecation") // uses internal test functionality.
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      DestinationT destination = getDestination();
      CodecFactory codec = dynamicDestinations.getCodec(destination);
      Schema schema = dynamicDestinations.getSchema(destination);
      Map<String, Object> metadata = dynamicDestinations.getMetadata(destination);
      DatumWriter<OutputT> datumWriter;
      DatumWriterFactory<OutputT> datumWriterFactory =
          dynamicDestinations.getDatumWriterFactory(destination);

      if (datumWriterFactory == null) {
        datumWriter =
            genericRecords ? new GenericDatumWriter<>(schema) : new ReflectDatumWriter<>(schema);
      } else {
        datumWriter = datumWriterFactory.apply(schema);
      }

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
