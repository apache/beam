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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * A wrapper for Beam's unbounded sources. This class wraps around a source implementing the
 * {@link com.google.cloud.dataflow.sdk.io.Read.Unbounded}  interface.
 *
 * For now we support non-parallel sources, checkpointing is WIP.
 * */
public class UnboundedSourceWrapper<T> extends RichSourceFunction<WindowedValue<T>> implements Triggerable {

  private final String name;
  private final UnboundedSource<T, ?> source;

  private StreamingRuntimeContext runtime = null;
  private StreamSource.ManualWatermarkContext<WindowedValue<T>> context = null;

  private volatile boolean isRunning = false;

  /** Serialized using custom Java serialization via Jackson */
  private transient PipelineOptions pipelineOptions;

  /** Instantiated during runtime **/
  private transient UnboundedSource.UnboundedReader<T> reader;

  public UnboundedSourceWrapper(PipelineOptions pipelineOptions, Read.Unbounded<T> transform) {
    this.name = transform.getName();
    this.pipelineOptions = pipelineOptions;
    this.source = transform.getSource();
  }

  public String getName() {
    return this.name;
  }

  WindowedValue<T> makeWindowedValue(T output, Instant timestamp) {
    if (timestamp == null) {
      timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
    return WindowedValue.of(output, timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
  }

  @Override
  public void run(SourceContext<WindowedValue<T>> ctx) throws Exception {
    if (!(ctx instanceof StreamSource.ManualWatermarkContext)) {
      throw new RuntimeException(
          "We assume that all sources in Dataflow are EventTimeSourceFunction. " +
              "Apparently " + this.name + " is not. " +
              "Probably you should consider writing your own Wrapper for this source.");
    }

    context = (StreamSource.ManualWatermarkContext<WindowedValue<T>>) ctx;
    runtime = (StreamingRuntimeContext) getRuntimeContext();

    isRunning = true;

    reader = source.createReader(pipelineOptions, null);

    boolean inputAvailable = reader.start();

    setNextWatermarkTimer(this.runtime);


    try {

      while (isRunning) {

        if (!inputAvailable && isRunning) {
          // wait a bit until we retry to pull more records
          Thread.sleep(50);
          inputAvailable = reader.advance();
        }

        if (inputAvailable) {

          // get it and its timestamp from the source
          T item = reader.getCurrent();
          Instant timestamp = reader.getCurrentTimestamp();

          // write it to the output collector
          synchronized (ctx.getCheckpointLock()) {
            context.collectWithTimestamp(makeWindowedValue(item, timestamp), timestamp.getMillis());
          }

          inputAvailable = reader.advance();
        }
      }

    } finally {
      reader.close();
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  @Override
  public void trigger(long timestamp) throws Exception {
    if (this.isRunning) {
      synchronized (context.getCheckpointLock()) {
        long watermarkMillis = this.reader.getWatermark().getMillis();
        context.emitWatermark(new Watermark(watermarkMillis));
      }
      setNextWatermarkTimer(this.runtime);
    }
  }

  private void setNextWatermarkTimer(StreamingRuntimeContext runtime) {
    if (this.isRunning) {
      long watermarkInterval =  runtime.getExecutionConfig().getAutoWatermarkInterval();
      long timeToNextWatermark = getTimeToNextWaternark(watermarkInterval);
      runtime.registerTimer(timeToNextWatermark, this);
    }
  }

  private long getTimeToNextWaternark(long watermarkInterval) {
    return System.currentTimeMillis() + watermarkInterval;
  }


  // Special serialization of the PipelineOptions necessary to instantiate the reader.
  private void writeObject(ObjectOutputStream out) throws IOException, ClassNotFoundException {
    out.defaultWriteObject();
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(out, pipelineOptions);
  }

  // Special deserialization of the PipelineOptions necessary to instantiate the reader.
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    ObjectMapper mapper = new ObjectMapper();
    pipelineOptions = mapper.readValue(in, PipelineOptions.class);
  }
}
