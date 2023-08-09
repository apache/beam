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
package org.apache.beam.runners.dataflow.worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * A sink that writes Avro files. Records are written to the Avro file as a series of byte arrays.
 * The coder provided is used to serialize each record into a byte array.
 *
 * @param <T> the type of the elements written to the sink
 */
public class AvroByteSink<T> extends Sink<T> {

  final Coder<T> coder;
  @VisibleForTesting final ResourceId resourceId;
  private final Schema schema = Schema.create(Schema.Type.BYTES);

  public AvroByteSink(ResourceId resourceId, Coder<T> coder) {
    this.coder = coder;
    this.resourceId = resourceId;
  }

  @Override
  public SinkWriter<T> writer() throws IOException {
    return new AvroByteFileWriter();
  }

  class AvroByteFileWriter implements SinkWriter<T> {
    DataFileWriter<ByteBuffer> fileWriter;

    public AvroByteFileWriter() throws IOException {
      WritableByteChannel writer = FileSystems.create(resourceId, MimeTypes.BINARY);

      fileWriter = new DataFileWriter<>(new GenericDatumWriter<ByteBuffer>(schema));
      fileWriter.create(schema, Channels.newOutputStream(writer));
    }

    @Override
    public long add(T value) throws IOException {
      byte[] encodedElem = CoderUtils.encodeToByteArray(coder, value);
      ByteBuffer encodedBuffer = ByteBuffer.wrap(encodedElem);
      fileWriter.append(encodedBuffer);
      return encodedElem.length;
    }

    @Override
    public void close() throws IOException {
      fileWriter.close();
    }

    @Override
    public void abort() throws IOException {
      // DataFileWriter implements Closeable and is idempotent.
      fileWriter.close();
    }
  }
}
