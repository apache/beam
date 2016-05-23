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

package org.apache.beam.runners.flink.translation.wrappers;

import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.AbstractID;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Wrapper for executing a {@link Sink} on Flink as an {@link OutputFormat}.
 *
 * @param <T> The type of the incoming records.
 */
public class SinkOutputFormat<T> implements OutputFormat<WindowedValue<T>> {

  private final Sink<T> sink;

  private final SerializedPipelineOptions serializedOptions;

  private Sink.WriteOperation<T, ?> writeOperation;
  private Sink.Writer<T, ?> writer;

  private AbstractID uid = new AbstractID();

  public SinkOutputFormat(Write.Bound<T> transform, PipelineOptions pipelineOptions) {
    this.sink = transform.getSink();
    this.serializedOptions = new SerializedPipelineOptions(pipelineOptions);
  }

  @Override
  public void configure(Configuration configuration) {
    writeOperation = sink.createWriteOperation(serializedOptions.getPipelineOptions());
    try {
      writeOperation.initialize(serializedOptions.getPipelineOptions());
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize the write operation.", e);
    }
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    try {
      writer = writeOperation.createWriter(serializedOptions.getPipelineOptions());
    } catch (Exception e) {
      throw new IOException("Couldn't create writer.", e);
    }
    try {
      writer.open(uid + "-" + String.valueOf(taskNumber));
    } catch (Exception e) {
      throw new IOException("Couldn't open writer.", e);
    }
  }

  @Override
  public void writeRecord(WindowedValue<T> record) throws IOException {
    try {
      writer.write(record.getValue());
    } catch (Exception e) {
      throw new IOException("Couldn't write record.", e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      writer.close();
    } catch (Exception e) {
      throw new IOException("Couldn't close writer.", e);
    }
  }

}
