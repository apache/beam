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

package org.apache.beam.runners.gearpump.translators.io;

import java.io.IOException;
import java.time.Instant;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.gearpump.DefaultMessage;
import org.apache.gearpump.Message;
import org.apache.gearpump.streaming.source.DataSource;
import org.apache.gearpump.streaming.source.Watermark;
import org.apache.gearpump.streaming.task.TaskContext;

/**
 * common methods for {@link BoundedSourceWrapper} and {@link UnboundedSourceWrapper}.
 */
public abstract class GearpumpSource<T> implements DataSource {

  private final SerializablePipelineOptions serializedOptions;

  private Source.Reader<T> reader;
  private boolean available = false;

  GearpumpSource(PipelineOptions options) {
    this.serializedOptions = new SerializablePipelineOptions(options);
  }

  protected abstract Source.Reader<T> createReader(PipelineOptions options) throws IOException;

  @Override
  public void open(TaskContext context, Instant startTime) {
    try {
      PipelineOptions options = serializedOptions.get();
      this.reader = createReader(options);
      this.available = reader.start();
    } catch (Exception e) {
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  public Message read() {
    Message message = null;
    try {
      if (available) {
        T data = reader.getCurrent();
        org.joda.time.Instant timestamp = reader.getCurrentTimestamp();
        message = new DefaultMessage(
            WindowedValue.timestampedValueInGlobalWindow(data, timestamp),
            timestamp.getMillis());
      }
      available = reader.advance();
    } catch (Exception e) {
      close();
      throw new RuntimeException(e);
    }
    return message;
  }

  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Instant getWatermark() {
    if (reader instanceof UnboundedSource.UnboundedReader) {
      org.joda.time.Instant watermark =
          ((UnboundedSource.UnboundedReader) reader).getWatermark();
      if (watermark == BoundedWindow.TIMESTAMP_MAX_VALUE) {
        return Watermark.MAX();
      } else {
        return TranslatorUtils.jodaTimeToJava8Time(watermark);
      }
    } else {
      if (available) {
        return Watermark.MIN();
      } else {
        return Watermark.MAX();
      }
    }
  }
}
