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

import static com.google.cloud.dataflow.sdk.util.WindowedValue.ValueOnlyWindowedValueCoder;
import static com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.MimeTypes;
import com.google.cloud.dataflow.sdk.util.ShardingWritableByteChannel;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Random;

/**
 * A sink that writes Avro files.
 *
 * @param <T> the type of the elements written to the sink
 */
public class AvroSink<T> extends Sink<WindowedValue<T>> {

  final String filenamePrefix;
  final String shardFormat;
  final String filenameSuffix;
  final int shardCount;
  final AvroCoder<T> avroCoder;
  final Schema schema;

  public AvroSink(String filename, WindowedValueCoder<T> coder) {
    this(filename, "", "", 1, coder);
  }

  public AvroSink(String filenamePrefix, String shardFormat, String filenameSuffix, int shardCount,
                  WindowedValueCoder<T> coder) {
    if (!(coder instanceof ValueOnlyWindowedValueCoder)) {
      throw new IllegalArgumentException("Expected ValueOnlyWindowedValueCoder");
    }

    if (!(coder.getValueCoder() instanceof AvroCoder)) {
      throw new IllegalArgumentException("AvroSink requires an AvroCoder");
    }

    this.filenamePrefix = filenamePrefix;
    this.shardFormat = shardFormat;
    this.filenameSuffix = filenameSuffix;
    this.shardCount = shardCount;
    this.avroCoder = (AvroCoder<T>) coder.getValueCoder();
    this.schema = this.avroCoder.getSchema();
  }

  public SinkWriter<WindowedValue<T>> writer(DatumWriter<T> datumWriter) throws IOException {
    WritableByteChannel writer = IOChannelUtils.create(
        filenamePrefix, shardFormat, filenameSuffix, shardCount, MimeTypes.BINARY);

    if (writer instanceof ShardingWritableByteChannel) {
      return new AvroShardingFileWriter(datumWriter, (ShardingWritableByteChannel) writer);
    } else {
      return new AvroFileWriter(datumWriter, writer);
    }
  }

  @Override
  public SinkWriter<WindowedValue<T>> writer() throws IOException {
    return writer(avroCoder.createDatumWriter());
  }

  /** The SinkWriter for an AvroSink. */
  class AvroFileWriter implements SinkWriter<WindowedValue<T>> {
    DataFileWriter<T> fileWriter;

    public AvroFileWriter(DatumWriter<T> datumWriter, WritableByteChannel outputChannel)
        throws IOException {
      fileWriter = new DataFileWriter<>(datumWriter);
      fileWriter.create(schema, Channels.newOutputStream(outputChannel));
    }

    @Override
    public long add(WindowedValue<T> value) throws IOException {
      fileWriter.append(value.getValue());
      // DataFileWriter doesn't support returning the length written. Use the
      // coder instead.
      return CoderUtils.encodeToByteArray(avroCoder, value.getValue()).length;
    }

    @Override
    public void close() throws IOException {
      fileWriter.close();
    }
  }

  /** The SinkWriter for an AvroSink, which supports sharding. */
  class AvroShardingFileWriter implements SinkWriter<WindowedValue<T>> {
    private ArrayList<AvroFileWriter> fileWriters = new ArrayList<>();
    private final Random random = new Random();

    public AvroShardingFileWriter(
        DatumWriter<T> datumWriter, ShardingWritableByteChannel outputChannel) throws IOException {
      for (int i = 0; i < outputChannel.getNumShards(); i++) {
        fileWriters.add(new AvroFileWriter(datumWriter, outputChannel.getChannel(i)));
      }
    }

    @Override
    public long add(WindowedValue<T> value) throws IOException {
      return fileWriters.get(random.nextInt(fileWriters.size())).add(value);
    }

    @Override
    public void close() throws IOException {
      for (AvroFileWriter fileWriter : fileWriters) {
        fileWriter.close();
      }
    }
  }
}
