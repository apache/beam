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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.RequiresWindowAccess;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Accumulates all panes that are output to the input {@link PCollection}, reifying the contents of
 * those panes and emitting exactly one output pane.
 */
public class GatherAllPanes<T>
    extends PTransform<PCollection<T>, PCollection<Iterable<WindowedValue<T>>>> {
  public static <T> GatherAllPanes<T> create() {
    return new GatherAllPanes<>();
  }

  private GatherAllPanes() {}

  private static class ReifiyWindowAndPaneDoFn<ElemT> extends DoFn<ElemT, WindowedValue<ElemT>>
      implements RequiresWindowAccess {
    @Override
    public void processElement(DoFn<ElemT, WindowedValue<ElemT>>.ProcessContext c)
        throws Exception {
      c.output(WindowedValue.of(c.element(), c.timestamp(), c.window(), c.pane()));
    }
  }

  @Override
  public PCollection<Iterable<WindowedValue<T>>> apply(PCollection<T> input) {
    WindowFn<?, ?> originalWindowFn = input.getWindowingStrategy().getWindowFn();

    PCollection<WindowedValue<T>> reifiedWindows = input.apply(new ReifiyWindowedValue<T>());
    return reifiedWindows
        .apply(
            WithKeys.<Void, WindowedValue<T>>of((Void) null)
                .withKeyType(new TypeDescriptor<Void>() {}))
        .apply(
            Window.into(
                    new IdentityWindowFn<KV<Void, WindowedValue<T>>>(
                        originalWindowFn.windowCoder(),
                        input.getWindowingStrategy().getWindowFn().assignsToSingleWindow()))
                .triggering(Never.ever()))
        .apply(GroupByKey.<Void, WindowedValue<T>>create())
        .apply(Values.<Iterable<WindowedValue<T>>>create())
        .setWindowingStrategyInternal(input.getWindowingStrategy());
  }

  private static class ReifiyWindowedValue<ElemT>
      extends PTransform<PCollection<ElemT>, PCollection<WindowedValue<ElemT>>> {
    @Override
    public PCollection<WindowedValue<ElemT>> apply(PCollection<ElemT> input) {
      Coder<WindowedValue<ElemT>> windowedValueCoder =
          FullWindowedValueCoder.of(
              input.getCoder(), input.getWindowingStrategy().getWindowFn().windowCoder());

      return input
          .apply("ReifiyWindowedValues", ParDo.of(new ReifiyWindowAndPaneDoFn<ElemT>()))
          .setCoder(windowedValueCoder);
    }
  }
}
