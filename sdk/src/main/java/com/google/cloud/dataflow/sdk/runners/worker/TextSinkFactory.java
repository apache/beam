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

import static com.google.cloud.dataflow.sdk.util.Structs.getBoolean;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;

/**
 * Creates a TextSink from a CloudObject spec.
 */
public final class TextSinkFactory {
  // Do not instantiate.
  private TextSinkFactory() {}

  public static <T> TextSink<T> create(PipelineOptions options,
                                       CloudObject spec,
                                       Coder<T> coder,
                                       ExecutionContext executionContext)
      throws Exception {
    return create(spec, coder);
  }

  static <T> TextSink<T> create(CloudObject spec, Coder<T> coder)
      throws Exception {
    return TextSink.create(
        getString(spec, PropertyNames.FILENAME),
        "",  // No shard template
        "",  // No suffix
        1,   // Exactly one output file
        getBoolean(spec, PropertyNames.APPEND_TRAILING_NEWLINES, true),
        getString(spec, PropertyNames.HEADER, null),
        getString(spec, PropertyNames.FOOTER, null),
        coder);
  }
}
