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

import static com.google.api.client.util.Base64.decodeBase64;
import static com.google.cloud.dataflow.sdk.runners.worker.ShuffleSink.parseShuffleKind;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import javax.annotation.Nullable;

/**
 * Creates a {@link ShuffleSink} from a {@link CloudObject} spec.
 */
public class ShuffleSinkFactory implements SinkFactory {

  @Override
  public ShuffleSink<?> create(
      CloudObject spec,
      Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable ExecutionContext executionContext,
      @Nullable CounterSet.AddCounterMutator addCounterMutator)
          throws Exception {

    @SuppressWarnings("unchecked")
    Coder<WindowedValue<Object>> typedCoder =
        (Coder<WindowedValue<Object>>) coder;

    return new ShuffleSink<>(
        options,
        decodeBase64(getString(spec, PropertyNames.SHUFFLE_WRITER_CONFIG, null)),
        parseShuffleKind(getString(spec, PropertyNames.SHUFFLE_KIND)),
        typedCoder,
        addCounterMutator);
  }
}
