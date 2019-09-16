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
package org.apache.beam.runners.apex.translation.operators;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import java.io.IOException;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.translation.utils.ApexStreamTuple;
import org.apache.beam.runners.apex.translation.utils.ApexStreamTuple.DataTuple;
import org.apache.beam.runners.apex.translation.utils.ValuesSource;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Apex input operator that wraps Beam {@link UnboundedSource}. */
public class ApexReadUnboundedInputOperator<
        OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    implements InputOperator {
  private static final Logger LOG = LoggerFactory.getLogger(ApexReadUnboundedInputOperator.class);
  private boolean traceTuples = false;
  private long outputWatermark = 0;

  @Bind(JavaSerializer.class)
  private final SerializablePipelineOptions pipelineOptions;

  @Bind(JavaSerializer.class)
  private final UnboundedSource<OutputT, CheckpointMarkT> source;

  private final boolean isBoundedSource;
  private transient UnboundedSource.UnboundedReader<OutputT> reader;
  private transient boolean available = false;

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<ApexStreamTuple<WindowedValue<OutputT>>> output =
      new DefaultOutputPort<>();

  public ApexReadUnboundedInputOperator(
      UnboundedSource<OutputT, CheckpointMarkT> source, ApexPipelineOptions options) {
    this.pipelineOptions = new SerializablePipelineOptions(options);
    this.source = source;
    this.isBoundedSource = false;
  }

  public ApexReadUnboundedInputOperator(
      UnboundedSource<OutputT, CheckpointMarkT> source,
      boolean isBoundedSource,
      ApexPipelineOptions options) {
    this.pipelineOptions = new SerializablePipelineOptions(options);
    this.source = source;
    this.isBoundedSource = isBoundedSource;
  }

  @SuppressWarnings("unused") // for Kryo
  private ApexReadUnboundedInputOperator() {
    this.pipelineOptions = null;
    this.source = null;
    this.isBoundedSource = false;
  }

  @Override
  public void beginWindow(long windowId) {
    if (!available && (isBoundedSource || source instanceof ValuesSource)) {
      // if it's a Create and the input was consumed, emit final watermark
      emitWatermarkIfNecessary(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
    } else {
      emitWatermarkIfNecessary(reader.getWatermark().getMillis());
    }
  }

  private void emitWatermarkIfNecessary(long mark) {
    if (mark > outputWatermark) {
      outputWatermark = mark;
      if (traceTuples) {
        LOG.debug("\nemitting watermark {}\n", mark);
      }
      output.emit(ApexStreamTuple.WatermarkTuple.of(mark));
    }
  }

  @Override
  public void endWindow() {
    if (outputWatermark >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      // terminate the stream
      if (traceTuples) {
        LOG.debug("terminating input after final watermark");
      }
      try {
        // see BEAM-1140 for why the delay after mark was emitted
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
      BaseOperator.shutdown();
    }
  }

  @Override
  public void setup(OperatorContext context) {
    this.traceTuples =
        ApexStreamTuple.Logging.isDebugEnabled(
            pipelineOptions.get().as(ApexPipelineOptions.class), this);
    try {
      reader = source.createReader(this.pipelineOptions.get(), null);
      available = reader.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown() {
    try {
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void emitTuples() {
    try {
      if (!available) {
        available = reader.advance();
      }
      if (available) {
        OutputT data = reader.getCurrent();
        Instant timestamp = reader.getCurrentTimestamp();
        available = reader.advance();
        if (traceTuples) {
          LOG.debug("\nemitting '{}' timestamp {}\n", data, timestamp);
        }
        output.emit(
            DataTuple.of(
                WindowedValue.of(data, timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING)));
      }
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }
}
