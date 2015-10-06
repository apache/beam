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

import static com.google.cloud.dataflow.sdk.util.Structs.getLong;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue.ValueOnlyWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import javax.annotation.Nullable;

/**
 * Creates an AvroReader from a CloudObject spec.
 */
public class AvroReaderFactory implements ReaderFactory {

  public AvroReaderFactory() {}

  @Override
  public Reader<?> create(
      CloudObject spec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable ExecutionContext executionContext,
      @Nullable CounterSet.AddCounterMutator addCounterMutator,
      @Nullable String operationName)
          throws Exception {
    return create(spec, coder, options);
  }

  Reader<?> create(CloudObject spec, Coder<?> coder, PipelineOptions options) throws Exception {
    String filename = getString(spec, PropertyNames.FILENAME);
    Long startOffset = getLong(spec, PropertyNames.START_OFFSET, null);
    Long endOffset = getLong(spec, PropertyNames.END_OFFSET, null);

    // If the coder is a ValueOnlyWindowedValueCoder, the source is a user source. Otherwise,
    // the coder is a FullWindowedValueCoder and the source is a materialized PCollection.
    if (coder instanceof ValueOnlyWindowedValueCoder) {
      ValueOnlyWindowedValueCoder<?> valueCoder = (ValueOnlyWindowedValueCoder<?>) coder;
      if (!(valueCoder.getValueCoder() instanceof AvroCoder)) {
        throw new IllegalArgumentException(
            "AvroReader requires an AvroCoder, but the instance given was "
            + valueCoder.getValueCoder());
      }
      return new AvroReader<>(
          filename, startOffset, endOffset, (AvroCoder<?>) valueCoder.getValueCoder(), options);
    } else {
      return new AvroByteReader<>(filename, startOffset, endOffset, coder, options);
    }
  }
}
