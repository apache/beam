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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * A {@link ParDoFn} that writes side input data using {@link
 * StreamingModeExecutionContext.StreamingModeStepContext#writePCollectionViewData}.
 */
@SuppressWarnings({"keyfor", "nullness"}) // TODO(https://github.com/apache/beam/issues/20497)
public class StreamingPCollectionViewWriterParDoFn implements ParDoFn {

  private final StreamingModeExecutionContext.StreamingModeStepContext stepContext;
  private final TupleTag<?> viewTag;
  private final Coder<Object> elemCoder;
  private final Coder<BoundedWindow> windowCoder;

  public StreamingPCollectionViewWriterParDoFn(
      StreamingModeExecutionContext.StreamingModeStepContext stepContext,
      TupleTag<?> viewTag,
      Coder<Object> elemCoder,
      Coder<BoundedWindow> windowCoder) {
    this.stepContext = stepContext;
    this.viewTag = viewTag;
    this.elemCoder = elemCoder;
    this.windowCoder = windowCoder;
  }

  @Override
  public void startBundle(Receiver... receivers) throws Exception {
    checkState(
        receivers.length == 1,
        "%s.startBundle() called with %s receivers, expected exactly 1. "
            + "This is a bug in the Dataflow service",
        getClass().getSimpleName(),
        receivers.length);
  }

  @Override
  public void processElement(Object element) throws Exception {
    WindowedValue<Iterable<Object>> elemsToWrite = (WindowedValue<Iterable<Object>>) element;
    BoundedWindow window = Iterables.getOnlyElement(elemsToWrite.getWindows());

    stepContext.writePCollectionViewData(
        viewTag, elemsToWrite.getValue(), IterableCoder.of(elemCoder), window, windowCoder);
  }

  @Override
  public void processTimers() {}

  @Override
  public void finishBundle() throws Exception {}

  @Override
  public void abort() throws Exception {
    // no-op as long as it remains stateless
  }
}
