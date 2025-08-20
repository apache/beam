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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.OutputBuilder;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValues;

/** Implementations of {@link OutputBuilderSupplier}. */
@Internal
public class OutputBuilderSuppliers {
  private OutputBuilderSuppliers() {}

  public static OutputBuilderSupplier forReceiver(WindowedValueMultiReceiver receiver) {
    return new OutputBuilderSupplierForReceiver(receiver);
  }

  public static OutputBuilderSupplier forFinishBundle(DoFn<?, ?>.FinishBundleContext context) {
    return new OutputBuilderSupplierForFinishBundle(context);
  }

  public static OutputBuilderSupplier forProcessContext(DoFn<?, ?>.ProcessContext context) {
    return new OutputBuilderSupplierForProcessContext(context);
  }

  public static OutputBuilderSupplier forWindowedContext(DoFn<?, ?>.WindowedContext context) {
    return new OutputBuilderSupplierForWindowedContext(context);
  }

  static class OutputBuilderSupplierForFinishBundle implements OutputBuilderSupplier {

    private final DoFn<?, ?>.FinishBundleContext context;

    OutputBuilderSupplierForFinishBundle(DoFn<?, ?>.FinishBundleContext context) {
      this.context = context;
    }

    @Override
    public <OutputT> OutputBuilder<OutputT> builder(TupleTag<OutputT> tag) {
      return WindowedValues.<OutputT>builder()
          .setReceiver(
              windowedValue -> {
                for (BoundedWindow window : windowedValue.getWindows()) {
                  context.output(
                      tag, windowedValue.getValue(), windowedValue.getTimestamp(), window);
                }
              });
    }
  }

  /**
   * Implementation of {@link OutputBuilderSupplier} that will always give an empty {@link
   * OutputBuilder} with method {@link OutputBuilder#output} connected to the given {@link
   * WindowedValueReceiver}.
   */
  private static class OutputBuilderSupplierForReceiver implements OutputBuilderSupplier {
    private final WindowedValueMultiReceiver receiver;

    OutputBuilderSupplierForReceiver(WindowedValueMultiReceiver receiver) {
      this.receiver = receiver;
    }

    @Override
    public <OutputT> OutputBuilder<OutputT> builder(TupleTag<OutputT> tag) {
      return WindowedValues.<OutputT>builder()
          .setReceiver(windowedValue -> receiver.output(tag, windowedValue));
    }
  }

  private static class OutputBuilderSupplierForWindowedContext implements OutputBuilderSupplier {
    private final DoFn<?, ?>.WindowedContext context;

    private OutputBuilderSupplierForWindowedContext(DoFn<?, ?>.WindowedContext context) {
      this.context = context;
    }

    @Override
    public <OutputT> OutputBuilder<OutputT> builder(TupleTag<OutputT> tag) {
      return WindowedValues.<OutputT>builder()
          .setReceiver(
              windowedValue ->
                  context.outputWindowedValue(
                      tag,
                      windowedValue.getValue(),
                      windowedValue.getTimestamp(),
                      windowedValue.getWindows(),
                      windowedValue.getPaneInfo()));
    }
  }

  private static class OutputBuilderSupplierForProcessContext implements OutputBuilderSupplier {
    private final DoFn<?, ?>.ProcessContext context;

    private OutputBuilderSupplierForProcessContext(DoFn<?, ?>.ProcessContext context) {
      this.context = context;
    }

    @Override
    public <OutputT> OutputBuilder<OutputT> builder(TupleTag<OutputT> tag) {
      return WindowedValues.<OutputT>builder()
          .setReceiver(
              windowedValue ->
                  context.outputWindowedValue(
                      tag,
                      windowedValue.getValue(),
                      windowedValue.getTimestamp(),
                      windowedValue.getWindows(),
                      windowedValue.getPaneInfo()));
    }
  }
}
