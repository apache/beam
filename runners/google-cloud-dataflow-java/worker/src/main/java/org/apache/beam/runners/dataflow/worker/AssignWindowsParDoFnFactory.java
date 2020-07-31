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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.util.Structs.getBytes;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link ParDoFnFactory} to create instances of AssignWindowsDoFn according to specifications
 * from the Dataflow service.
 */
class AssignWindowsParDoFnFactory implements ParDoFnFactory {
  @Override
  public ParDoFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      List<SideInputInfo> sideInputInfos,
      TupleTag<?> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    byte[] encodedWindowingStrategy = getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN);

    WindowingStrategy<?, ?> deserializedWindowingStrategy =
        GroupAlsoByWindowParDoFnFactory.deserializeWindowingStrategy(encodedWindowingStrategy);

    @SuppressWarnings("unchecked")
    WindowingStrategy<Object, BoundedWindow> windowingStrategy =
        (WindowingStrategy<Object, BoundedWindow>) deserializedWindowingStrategy;

    return new AssignWindowsParDoFn<>(
        windowingStrategy.getWindowFn(), executionContext.getStepContext(operationContext));
  }

  private static class AssignWindowsParDoFn<T, W extends BoundedWindow> implements ParDoFn {
    private final DataflowExecutionContext.DataflowStepContext stepContext;
    private final WindowFn<T, W> windowFn;

    private @Nullable Receiver receiver;

    AssignWindowsParDoFn(
        WindowFn<T, W> windowFn, DataflowExecutionContext.DataflowStepContext stepContext) {
      this.stepContext = stepContext;
      this.windowFn = windowFn;
    }

    @Override
    public void startBundle(Receiver... receivers) throws Exception {
      checkState(
          receivers.length == 1,
          "%s.startBundle() called with %s receivers, expected exactly 1. "
              + "This is a bug in the Dataflow service",
          getClass().getSimpleName(),
          receivers.length);
      receiver = receivers[0];
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processElement(Object untypedElem) throws Exception {
      WindowedValue<T> elem = (WindowedValue<T>) untypedElem;

      Collection<W> windows =
          windowFn.assignWindows(
              windowFn.new AssignContext() {
                @Override
                public T element() {
                  return elem.getValue();
                }

                @Override
                public Instant timestamp() {
                  return elem.getTimestamp();
                }

                @Override
                public BoundedWindow window() {
                  return Iterables.getOnlyElement(elem.getWindows());
                }
              });

      WindowedValue<T> res =
          WindowedValue.of(elem.getValue(), elem.getTimestamp(), windows, elem.getPane());
      receiver.process(res);
    }

    @Override
    public void processTimers() throws Exception {
      // Nothing.
    }

    @Override
    public void finishBundle() throws Exception {
      receiver = null;
    }

    @Override
    public void abort() throws Exception {
      receiver = null;
    }
  }
}
