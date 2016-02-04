/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.getString;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.IsmRecord;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.IsmRecordCoder;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

/**
 * Creates an {@link IsmSink} from a {@link CloudObject} spec. Note that it is invalid to use a
 * non {@link IsmRecordCoder} with this sink factory.
 */
public class IsmSinkFactory {
  // Do not instantiate.
  private IsmSinkFactory() {}

  @SuppressWarnings("unused")
  public static <V, T> Sink<WindowedValue<IsmRecord<V>>> create(
      PipelineOptions options,
      CloudObject spec,
      Coder<WindowedValue<IsmRecord<V>>> coder,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator) throws Exception {
    return create(spec, coder);
  }

  static <V> Sink<WindowedValue<IsmRecord<V>>> create(
      CloudObject spec, Coder<WindowedValue<IsmRecord<V>>> coder) throws Exception {
    String filename = getString(spec, PropertyNames.FILENAME);

    checkArgument(coder instanceof WindowedValueCoder,
        "%s only supports using %s but got %s.", IsmSink.class, WindowedValueCoder.class, coder);
    WindowedValueCoder<IsmRecord<V>> windowedCoder =
        (WindowedValueCoder<IsmRecord<V>>) coder;

    checkArgument(windowedCoder.getValueCoder() instanceof IsmRecordCoder,
        "%s only supports using %s but got %s.",
        IsmSink.class, IsmRecordCoder.class, windowedCoder.getValueCoder());
    @SuppressWarnings("unchecked")
    IsmRecordCoder<V> ismCoder =
        (IsmRecordCoder<V>) windowedCoder.getValueCoder();

    return new IsmSink<>(filename, ismCoder);
  }
}
