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
import java.util.Queue;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.samza.SamzaPipelineExceptionContext;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.translation.TranslationContext;
import org.apache.beam.runners.samza.util.FutureUtils;
import org.apache.beam.runners.samza.util.SamzaPipelineExceptionListener;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.operators.Scheduler;
import org.apache.samza.operators.functions.AsyncFlatMapFunction;
import org.apache.samza.operators.functions.ScheduledFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adaptor class that runs a Samza {@link Op} for BEAM in the Samza {@link AsyncFlatMapFunction}.
 * This class is initialized once for each Op within a Task for each Task.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class OpAdapter<InT, OutT, K>
    implements AsyncFlatMapFunction<OpMessage<InT>, OpMessage<OutT>>,
        WatermarkFunction<OpMessage<OutT>>,
        ScheduledFunction<KeyedTimerData<K>, OpMessage<OutT>>,
        Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(OpAdapter.class);

  private final Op<InT, OutT, K> op;
  private final String transformFullName;
  private final transient SamzaPipelineOptions samzaPipelineOptions;
  private transient OpEmitter<OutT> emitter;
  private transient Config config;
  private transient Context context;
  private transient List<SamzaPipelineExceptionListener.Registrar> exceptionListeners;

  public static <InT, OutT, K> AsyncFlatMapFunction<OpMessage<InT>, OpMessage<OutT>> adapt(
      Op<InT, OutT, K> op, TranslationContext ctx) {
    return new OpAdapter<>(op, ctx.getTransformFullName(), ctx.getPipelineOptions());
  }

  private OpAdapter(
      Op<InT, OutT, K> op, String transformFullName, SamzaPipelineOptions samzaPipelineOptions) {
    this.op = op;
    this.transformFullName = transformFullName;
    this.samzaPipelineOptions = samzaPipelineOptions;
  }

  @Override
  public final void init(Context context) {
    this.emitter = new OpEmitterImpl<>();
    this.config = context.getJobContext().getConfig();
    this.context = context;
    this.exceptionListeners =
        StreamSupport.stream(
                ServiceLoader.load(SamzaPipelineExceptionListener.Registrar.class).spliterator(),
                false)
            .collect(Collectors.toList());
  }

  @Override
  public final void schedule(Scheduler<KeyedTimerData<K>> timerRegistry) {
    assert context != null;

    op.open(config, context, timerRegistry, emitter);
  }

  @Override
  public synchronized CompletionStage<Collection<OpMessage<OutT>>> apply(OpMessage<InT> message) {
    try {
      switch (message.getType()) {
        case ELEMENT:
          op.processElement(message.getElement(), emitter);
          break;
        case SIDE_INPUT:
          op.processSideInput(message.getViewId(), message.getViewElements(), emitter);
          break;
        case SIDE_INPUT_WATERMARK:
          op.processSideInputWatermark(message.getSideInputWatermark(), emitter);
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unexpected input type: %s", message.getType()));
      }
    } catch (Exception e) {
      LOG.error("Exception happened in transform: {}", transformFullName, e);
      notifyExceptionListeners(transformFullName, e, samzaPipelineOptions);
      throw UserCodeException.wrap(e);
    }

    CompletionStage<Collection<OpMessage<OutT>>> resultFuture =
        CompletableFuture.completedFuture(emitter.collectOutput());

    return FutureUtils.combineFutures(resultFuture, emitter.collectFuture());
  }

  @Override
  public synchronized Collection<OpMessage<OutT>> processWatermark(long time) {
    try {
      op.processWatermark(new Instant(time), emitter);
    } catch (Exception e) {
      LOG.error(
          "Op {} threw an exception during processing watermark", this.getClass().getName(), e);
      throw UserCodeException.wrap(e);
    }

    return emitter.collectOutput();
  }

  @Override
  public synchronized Long getOutputWatermark() {
    return emitter.collectWatermark();
  }

  @Override
  public synchronized Collection<OpMessage<OutT>> onCallback(
      KeyedTimerData<K> keyedTimerData, long time) {
    try {
      op.processTimer(keyedTimerData, emitter);
    } catch (Exception e) {
      LOG.error("Op {} threw an exception during processing timer", this.getClass().getName(), e);
      throw UserCodeException.wrap(e);
    }

    return emitter.collectOutput();
  }

  @Override
  public void close() {
    op.close();
  }

  static class OpEmitterImpl<OutT> implements OpEmitter<OutT> {
    private final Queue<OpMessage<OutT>> outputQueue;
    private CompletionStage<Collection<OpMessage<OutT>>> outputFuture;
    private Instant outputWatermark;

    OpEmitterImpl() {
      outputQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void emitElement(WindowedValue<OutT> element) {
      outputQueue.add(OpMessage.ofElement(element));
    }

    @Override
    public void emitFuture(CompletionStage<Collection<WindowedValue<OutT>>> resultFuture) {
      final CompletionStage<Collection<OpMessage<OutT>>> resultFutureWrapped =
          resultFuture.thenApply(
              res -> res.stream().map(OpMessage::ofElement).collect(Collectors.toList()));

      outputFuture = FutureUtils.combineFutures(outputFuture, resultFutureWrapped);
    }

    @Override
    public void emitWatermark(Instant watermark) {
      outputWatermark = watermark;
    }

    @Override
    public <T> void emitView(String id, WindowedValue<Iterable<T>> elements) {
      outputQueue.add(OpMessage.ofSideInput(id, elements));
    }

    @Override
    public Collection<OpMessage<OutT>> collectOutput() {
      final List<OpMessage<OutT>> outputList = new ArrayList<>();
      OpMessage<OutT> output;
      while ((output = outputQueue.poll()) != null) {
        outputList.add(output);
      }
      return outputList;
    }

    @Override
    public CompletionStage<Collection<OpMessage<OutT>>> collectFuture() {
      final CompletionStage<Collection<OpMessage<OutT>>> future = outputFuture;
      outputFuture = null;
      return future;
    }

    @Override
    public Long collectWatermark() {
      final Instant watermark = outputWatermark;
      outputWatermark = null;
      return watermark == null ? null : watermark.getMillis();
    }
  }

  private void notifyExceptionListeners(
      String transformFullName, Exception e, SamzaPipelineOptions samzaPipelineOptions) {
    try {
      exceptionListeners.forEach(
          listener -> {
            listener
                .getExceptionListener(samzaPipelineOptions)
                .onException(new SamzaPipelineExceptionContext(transformFullName, e));
          });
    } catch (Exception t) {
      // ignore exception/interruption by listeners
    }
  }
}
