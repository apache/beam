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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.translation.utils.ApexStreamTuple;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apex operator for simple native map operations.
 */
public class ApexProcessFnOperator<InputT> extends BaseOperator {

  private static final Logger LOG = LoggerFactory.getLogger(ApexProcessFnOperator.class);
  private boolean traceTuples = false;
  @Bind(JavaSerializer.class)
  private final ApexOperatorFn<InputT> fn;

  public ApexProcessFnOperator(ApexOperatorFn<InputT> fn, boolean traceTuples) {
    super();
    this.traceTuples = traceTuples;
    this.fn = fn;
  }

  @SuppressWarnings("unused")
  private ApexProcessFnOperator() {
    // for Kryo
    fn = null;
  }

  private final transient OutputEmitter<ApexStreamTuple<? extends WindowedValue<?>>> outputEmitter =
      new OutputEmitter<ApexStreamTuple<? extends WindowedValue<?>>>() {
    @Override
    public void emit(ApexStreamTuple<? extends WindowedValue<?>> tuple) {
      if (traceTuples) {
        LOG.debug("\nemitting {}\n", tuple);
      }
      outputPort.emit(tuple);
    }
  };

  /**
   * Something that emits results.
   */
  public interface OutputEmitter<T> {
    void emit(T tuple);
  }

  /**
   * The processing logic for this operator.
   */
  public interface ApexOperatorFn<InputT> extends Serializable {
    void process(ApexStreamTuple<WindowedValue<InputT>> input,
        OutputEmitter<ApexStreamTuple<? extends WindowedValue<?>>> outputEmitter) throws Exception;
  }

  /**
   * Convert {@link KV} into {@link KeyedWorkItem}s.
   */
  public static <K, V> ApexProcessFnOperator<KV<K, V>> toKeyedWorkItems(
      ApexPipelineOptions options) {
    ApexOperatorFn<KV<K, V>> fn = new ToKeyedWorkItems<>();
    return new ApexProcessFnOperator<>(fn, options.isTupleTracingEnabled());
  }

  private static class ToKeyedWorkItems<K, V> implements ApexOperatorFn<KV<K, V>> {
    @Override
    public final void process(ApexStreamTuple<WindowedValue<KV<K, V>>> tuple,
        OutputEmitter<ApexStreamTuple<? extends WindowedValue<?>>> outputEmitter) {

      if (tuple instanceof ApexStreamTuple.WatermarkTuple) {
        outputEmitter.emit(tuple);
      } else {
        for (WindowedValue<KV<K, V>> in : tuple.getValue().explodeWindows()) {
          KeyedWorkItem<K, V> kwi = KeyedWorkItems.elementsWorkItem(in.getValue().getKey(),
              Collections.singletonList(in.withValue(in.getValue().getValue())));
          outputEmitter.emit(ApexStreamTuple.DataTuple.of(in.withValue(kwi)));
        }
      }
    }
  }

  public static <T, W extends BoundedWindow> ApexProcessFnOperator<T> assignWindows(
      WindowFn<T, W> windowFn, ApexPipelineOptions options) {
    ApexOperatorFn<T> fn = new AssignWindows<>(windowFn);
    return new ApexProcessFnOperator<>(fn, options.isTupleTracingEnabled());
  }

  /**
   * Function for implementing {@link org.apache.beam.sdk.transforms.windowing.Window.Assign}.
   */
  private static class AssignWindows<T, W extends BoundedWindow> implements ApexOperatorFn<T> {
    private final WindowFn<T, W> windowFn;

    private AssignWindows(WindowFn<T, W> windowFn) {
      this.windowFn = windowFn;
    }

    @Override
    public final void process(ApexStreamTuple<WindowedValue<T>> tuple,
        OutputEmitter<ApexStreamTuple<? extends WindowedValue<?>>> outputEmitter) throws Exception {
      if (tuple instanceof ApexStreamTuple.WatermarkTuple) {
        outputEmitter.emit(tuple);
      } else {
        final WindowedValue<T> input = tuple.getValue();
        Collection<W> windows =
            (windowFn).assignWindows(
                (windowFn).new AssignContext() {
                    @Override
                    public T element() {
                      return input.getValue();
                    }

                    @Override
                    public Instant timestamp() {
                      return input.getTimestamp();
                    }

                    @Override
                    public BoundedWindow window() {
                      return Iterables.getOnlyElement(input.getWindows());
                    }
                  });
        for (W w: windows) {
          WindowedValue<T> wv = WindowedValue.of(input.getValue(), input.getTimestamp(),
              w, input.getPane());
          outputEmitter.emit(ApexStreamTuple.DataTuple.of(wv));
        }
      }
    }
  }

  /**
   * Input port.
   */
  public final transient DefaultInputPort<ApexStreamTuple<WindowedValue<InputT>>> inputPort =
      new DefaultInputPort<ApexStreamTuple<WindowedValue<InputT>>>() {
    @Override
    public void process(ApexStreamTuple<WindowedValue<InputT>> tuple) {
      try {
        fn.process(tuple, outputEmitter);
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }
  };

  /**
   * Output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<ApexStreamTuple<? extends WindowedValue<?>>>
    outputPort = new DefaultOutputPort<>();

}
