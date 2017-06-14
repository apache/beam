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

import com.google.common.collect.ImmutableMap;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;

/** A {@link FileBasedSink} for Avro files. */
class AvroSink<T, DestinationT> extends FileBasedSink<T, DestinationT> {
  private final AvroCoder<T> coder;
  private final SerializableAvroCodecFactory codec;
  private final ImmutableMap<String, Object> metadata;

  AvroSink(
      ValueProvider<ResourceId> outputPrefix,
      DynamicDestinations<T, DestinationT> dynamicDestinations,
      AvroCoder<T> coder,
      SerializableAvroCodecFactory codec,
      ImmutableMap<String, Object> metadata) {
    // Avro handle compression internally using the codec.
    super(outputPrefix, dynamicDestinations, CompressionType.UNCOMPRESSED);
    this.coder = coder;
    this.codec = codec;
    this.metadata = metadata;
  }

  @Override
  public WriteOperation<T, DestinationT> createWriteOperation() {
    return new AvroWriteOperation<>(this, coder, codec, metadata);
  }

  /** A {@link WriteOperation WriteOperation} for Avro files. */
  private static class AvroWriteOperation<T, DestinationT> extends WriteOperation<T, DestinationT> {
    private final AvroCoder<T> coder;
    private final SerializableAvroCodecFactory codec;
    private final ImmutableMap<String, Object> metadata;

    private AvroWriteOperation(AvroSink<T, DestinationT> sink,
                               AvroCoder<T> coder,
                               SerializableAvroCodecFactory codec,
                               ImmutableMap<String, Object> metadata) {
      super(sink);
      this.coder = coder;
      this.codec = codec;
      this.metadata = metadata;
    }

    @Override
    public Writer<T, DestinationT> createWriter() throws Exception {
      return new AvroWriter<>(this, coder, codec, metadata);
    }
  }

  /** A {@link Writer Writer} for Avro files. */
  private static class AvroWriter<T, DestinationT> extends Writer<T, DestinationT> {
    private final AvroCoder<T> coder;
    private DataFileWriter<T> dataFileWriter;
    private SerializableAvroCodecFactory codec;
    private final ImmutableMap<String, Object> metadata;

    public AvroWriter(WriteOperation<T, DestinationT> writeOperation,
                      AvroCoder<T> coder,
                      SerializableAvroCodecFactory codec,
                      ImmutableMap<String, Object> metadata) {
      super(writeOperation, MimeTypes.BINARY);
      this.coder = coder;
      this.codec = codec;
      this.metadata = metadata;
    }

    @SuppressWarnings("deprecation") // uses internal test functionality.
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      DatumWriter<T> datumWriter = coder.getType().equals(GenericRecord.class)
          ? new GenericDatumWriter<T>(coder.getSchema())
          : new ReflectDatumWriter<T>(coder.getSchema());

      dataFileWriter = new DataFileWriter<>(datumWriter).setCodec(codec.getCodec());
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
      dataFileWriter.create(coder.getSchema(), Channels.newOutputStream(channel));
    }

    @Override
    public void write(T value) throws Exception {
      dataFileWriter.append(value);
    }

    @Override
    protected void finishWrite() throws Exception {
      dataFileWriter.flush();
    }
  }
}
