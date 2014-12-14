/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A sink that writes Avro files. Records are written to the Avro file as a
 * series of byte arrays. The coder provided is used to serialize each record
 * into a byte array.
 *
 * @param <T> the type of the elements written to the sink
 */
public class AvroByteSink<T> extends Sink<T> {

  final AvroSink<ByteBuffer> avroSink;
  final Coder<T> coder;
  private final Schema schema = Schema.create(Schema.Type.BYTES);

  public AvroByteSink(String filenamePrefix, Coder<T> coder) {
    this(filenamePrefix, "", "", 1, coder);
  }

  public AvroByteSink(String filenamePrefix, String shardFormat, String filenameSuffix,
                      int shardCount, Coder<T> coder) {
    this.coder = coder;
    avroSink = new AvroSink<>(
        filenamePrefix, shardFormat, filenameSuffix, shardCount,
        WindowedValue.getValueOnlyCoder(AvroCoder.of(ByteBuffer.class, schema)));
  }

  @Override
  public SinkWriter<T> writer() throws IOException {
    return new AvroByteFileWriter();
  }

  /** The SinkWriter for an AvroByteSink. */
  class AvroByteFileWriter implements SinkWriter<T> {

    private final SinkWriter<WindowedValue<ByteBuffer>> avroFileWriter;

    public AvroByteFileWriter() throws IOException {
      avroFileWriter = avroSink.writer(new GenericDatumWriter<ByteBuffer>(schema));
    }

    @Override
    public long add(T value) throws IOException {
      byte[] encodedElem = CoderUtils.encodeToByteArray(coder, value);
      ByteBuffer encodedBuffer = ByteBuffer.wrap(encodedElem);
      avroFileWriter.add(WindowedValue.valueInGlobalWindow(encodedBuffer));
      return encodedElem.length;
    }

    @Override
    public void close() throws IOException {
      avroFileWriter.close();
    }
  }
}
