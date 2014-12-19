/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

/**
 * Creates an AvroSink from a CloudObject spec.
 */
@SuppressWarnings("rawtypes")
public final class AvroSinkFactory {
  // Do not instantiate.
  private AvroSinkFactory() {}

  public static <T> Sink<T> create(PipelineOptions options,
                                   CloudObject spec,
                                   Coder<T> coder,
                                   ExecutionContext executionContext)
      throws Exception {
    return create(spec, coder);
  }

  static <T> Sink<T> create(CloudObject spec, Coder<T> coder)
      throws Exception {
    String filename = getString(spec, PropertyNames.FILENAME);

    if (!(coder instanceof WindowedValueCoder)) {
      return new AvroByteSink<>(filename, coder);
      //throw new IllegalArgumentException("Expected WindowedValueCoder");
    }

    WindowedValueCoder windowedCoder = (WindowedValueCoder) coder;
    if (windowedCoder.getValueCoder() instanceof AvroCoder) {
      return new AvroSink(filename, windowedCoder);
    } else {
      return new AvroByteSink<>(filename, windowedCoder);
    }
  }
}
