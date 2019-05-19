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
package org.apache.beam.sdk.extensions.smb.tensorflow;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.extensions.smb.FileOperations;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.util.MimeTypes;
import org.tensorflow.example.Example;

/**
 * {@link FileOperations} implementation for TensorFlow TFRecord files with TensorFlow {@link
 * Example} records.
 */
public class TensorFlowFileOperations extends FileOperations<Example> {

  @Override
  public Reader<Example> createReader() {
    return new TfReader();
  }

  @Override
  public Writer<Example> createWriter() {
    return new TfWriter();
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class TfReader extends Reader<Example> {

    private transient TFRecordIO.TFRecordCodec codec;
    private transient ReadableByteChannel channel;

    @Override
    public void prepareRead(ReadableByteChannel channel) throws Exception {
      this.codec = new TFRecordIO.TFRecordCodec();
      this.channel = channel;
    }

    @Override
    public Example read() throws Exception {
      final byte[] bytes = codec.read(channel);
      return bytes == null ? null : Example.parseFrom(bytes);
    }

    @Override
    public void finishRead() throws Exception {
      channel.close();
    }
  }

  ////////////////////////////////////////
  // Writer
  ////////////////////////////////////////

  private static class TfWriter extends Writer<Example> {

    private transient TFRecordIO.TFRecordCodec codec;
    private transient WritableByteChannel channel;

    @Override
    public String getMimeType() {
      return MimeTypes.BINARY;
    }

    @Override
    public void prepareWrite(WritableByteChannel channel) throws Exception {
      this.codec = new TFRecordIO.TFRecordCodec();
      this.channel = channel;
    }

    @Override
    public void write(Example value) throws Exception {
      codec.write(channel, value.toByteArray());
    }

    @Override
    public void close() throws Exception {
      channel.close();
    }
  }
}
