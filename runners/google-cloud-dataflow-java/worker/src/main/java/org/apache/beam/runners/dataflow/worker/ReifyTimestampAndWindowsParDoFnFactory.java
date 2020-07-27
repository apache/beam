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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link ParDoFnFactory} to create instances of {@link ReifyTimestampAndWindowsParDoFn}. */
class ReifyTimestampAndWindowsParDoFnFactory implements ParDoFnFactory {
  @Override
  public ParDoFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      @Nullable List<SideInputInfo> sideInputInfos,
      TupleTag<?> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    return new ReifyTimestampAndWindowsParDoFn();
  }

  private static class ReifyTimestampAndWindowsParDoFn implements ParDoFn {

    private Receiver receiver;

    @Override
    public void startBundle(Receiver... receivers) throws Exception {
      checkState(
          receivers.length == 1,
          "%s.startBundle() called with %s receivers, expected exactly 1. "
              + "This is a bug in the Dataflow service",
          getClass().getSimpleName(),
          receivers.length);
      this.receiver = receivers[0];
    }

    @Override
    public void processElement(Object untypedElem) throws Exception {
      WindowedValue<KV<?, ?>> typedElem = (WindowedValue<KV<?, ?>>) untypedElem;

      receiver.process(
          WindowedValue.of(
              KV.of(
                  typedElem.getValue().getKey(),
                  WindowedValue.of(
                      typedElem.getValue().getValue(),
                      typedElem.getTimestamp(),
                      typedElem.getWindows(),
                      typedElem.getPane())),
              typedElem.getTimestamp(),
              typedElem.getWindows(),
              typedElem.getPane()));
    }

    @Override
    public void processTimers() {}

    @Override
    public void finishBundle() throws Exception {}

    @Override
    public void abort() throws Exception {}
  }
}
