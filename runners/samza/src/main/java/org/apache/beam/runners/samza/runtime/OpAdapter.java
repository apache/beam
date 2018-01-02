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

package org.apache.beam.runners.samza.runtime;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.samza.SamzaExecutionContext;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.task.TaskContext;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adaptor class that runs a Samza {@link Op} for BEAM in the Samza {@link FlatMapFunction}.
 */
public class OpAdapter<InT, OutT>
    implements FlatMapFunction<OpMessage<InT>, OpMessage<OutT>>,
      WatermarkFunction<OpMessage<OutT>>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(OpAdapter.class);

  private final Op<InT, OutT> op;
  private transient List<OpMessage<OutT>> outputList;
  private transient Instant outputWatermark;
  private transient OpEmitter<OutT> emitter;

  public static <InT, OutT> FlatMapFunction<OpMessage<InT>, OpMessage<OutT>>
  adapt(Op<InT, OutT> op) {
    return new OpAdapter<>(op);
  }

  private OpAdapter(Op<InT, OutT> op) {
    this.op = op;
  }

  @Override
  public final void init(Config config, TaskContext taskContext) {
    outputList = new ArrayList<>();
    emitter = new OpEmitterImpl();

    final SamzaExecutionContext executionContext =
        (SamzaExecutionContext) taskContext.getUserContext();

    op.open(config, taskContext, executionContext, emitter);
  }

  @Override
  public Collection<OpMessage<OutT>> apply(OpMessage<InT> message) {
    assert outputList.isEmpty();

    try {
      switch (message.getType()) {
        case ELEMENT:
          op.processElement(message.getElement(), emitter);
          break;
        case SIDE_INPUT:
          op.processSideInput(message.getViewId(), message.getViewElements(), emitter);
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unexpected input type: %s", message.getType()));
      }
    } catch (Exception e) {
      LOG.error("Op {} threw an exception during processing",
          this.getClass().getName(),
          e);
      throw UserCodeException.wrap(e);
    }

    final List<OpMessage<OutT>> results = new ArrayList<>(outputList);
    outputList.clear();
    return results;

  }

  @Override
  public Collection<OpMessage<OutT>> processWatermark(long time) {
    assert outputList.isEmpty();

    try {
      op.processWatermark(new Instant(time), emitter);
    } catch (Exception e) {
      LOG.error("Op {} threw an exception during processing watermark",
          this.getClass().getName(),
          e);
      throw UserCodeException.wrap(e);
    }

    final List<OpMessage<OutT>> results = new ArrayList<>(outputList);
    outputList.clear();
    return results;
  }

  @Override
  public Long getOutputWatermark() {
    return outputWatermark != null
      ? outputWatermark.getMillis()
      : null;
  }

  @Override
  public void close() {
    op.close();
  }

  private class OpEmitterImpl implements OpEmitter<OutT> {
    @Override
    public void emitElement(WindowedValue<OutT> element) {
      outputList.add(OpMessage.ofElement(element));
    }

    @Override
    public void emitWatermark(Instant watermark) {
      outputWatermark = watermark;
    }

    @Override
    public <T> void emitView(String id, WindowedValue<Iterable<T>> elements) {
      outputList.add(OpMessage.ofSideInput(id, elements));
    }
  }
}
