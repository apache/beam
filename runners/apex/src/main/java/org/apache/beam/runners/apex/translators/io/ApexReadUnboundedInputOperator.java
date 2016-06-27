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

package org.apache.beam.runners.apex.translators.io;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.translators.utils.ApexStreamTuple;
import org.apache.beam.runners.apex.translators.utils.ApexStreamTuple.DataTuple;
import org.apache.beam.runners.apex.translators.utils.SerializablePipelineOptions;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;

import org.joda.time.Instant;

import com.datatorrent.api.Context.OperatorContext;
import com.google.common.base.Throwables;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.IOException;

/**
 * Apex input operator that wraps Beam UnboundedSource.
 */
public class ApexReadUnboundedInputOperator<OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    implements InputOperator {

  @Bind(JavaSerializer.class)
  private final SerializablePipelineOptions pipelineOptions;
  @Bind(JavaSerializer.class)
  private final UnboundedSource<OutputT, CheckpointMarkT> source;
  private transient UnboundedSource.UnboundedReader<OutputT> reader;
  private transient boolean available = false;
  public final transient DefaultOutputPort<ApexStreamTuple<WindowedValue<OutputT>>> output = new DefaultOutputPort<>();

  public ApexReadUnboundedInputOperator(UnboundedSource<OutputT, CheckpointMarkT> source, ApexPipelineOptions options) {
    this.pipelineOptions = new SerializablePipelineOptions(options);
    this.source = source;
  }

  @SuppressWarnings("unused") // for Kryo
  private ApexReadUnboundedInputOperator() {
    this.pipelineOptions = null; this.source = null;
  }

  @Override
  public void beginWindow(long windowId)
  {
    Instant mark = reader.getWatermark();
    output.emit(ApexStreamTuple.WatermarkTuple.<WindowedValue<OutputT>>of(mark.getMillis()));
    if (!available && source instanceof ValuesSource) {
      // if it's a Create transformation and the input was consumed,
      // terminate the stream (allows tests to finish faster)
      BaseOperator.shutdown();
    }
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      reader = source.createReader(this.pipelineOptions.get(), null);
      available = reader.start();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public void teardown()
  {
    try {
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public void emitTuples()
  {
    try {
      if (!available) {
        available = reader.advance();
      }
      if (available) {
        OutputT data = reader.getCurrent();
        Instant timestamp = reader.getCurrentTimestamp();
        available = reader.advance();
        output.emit(DataTuple.of(WindowedValue.of(
            data, timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING)));
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

}
