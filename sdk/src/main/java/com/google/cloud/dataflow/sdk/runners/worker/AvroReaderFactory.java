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

import static com.google.cloud.dataflow.sdk.util.Structs.getLong;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

/**
 * Creates an AvroReader from a CloudObject spec.
 */
@SuppressWarnings("rawtypes")
public class AvroReaderFactory {
  // Do not instantiate.
  private AvroReaderFactory() {}

  public static <T> Reader<T> create(PipelineOptions options, CloudObject spec, Coder<T> coder,
      ExecutionContext executionContext) throws Exception {
    return create(spec, coder);
  }

  static <T> Reader<T> create(CloudObject spec, Coder<T> coder) throws Exception {
    String filename = getString(spec, PropertyNames.FILENAME);
    Long startOffset = getLong(spec, PropertyNames.START_OFFSET, null);
    Long endOffset = getLong(spec, PropertyNames.END_OFFSET, null);

    if (!(coder instanceof WindowedValueCoder)) {
      return new AvroByteReader<>(filename, startOffset, endOffset, coder);
      //throw new IllegalArgumentException("Expected WindowedValueCoder");
    }

    WindowedValueCoder windowedCoder = (WindowedValueCoder) coder;
    if (windowedCoder.getValueCoder() instanceof AvroCoder) {
      return new AvroReader(filename, startOffset, endOffset, windowedCoder);
    } else {
      return new AvroByteReader<>(filename, startOffset, endOffset, windowedCoder);
    }
  }
}
