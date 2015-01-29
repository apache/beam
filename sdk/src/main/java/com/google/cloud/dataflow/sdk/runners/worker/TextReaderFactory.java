/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.getBoolean;
import static com.google.cloud.dataflow.sdk.util.Structs.getLong;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.Serializer;

/**
 * Creates a TextReader from a CloudObject spec.
 */
public class TextReaderFactory {
  // Do not instantiate.
  private TextReaderFactory() {}

  public static <T> TextReader<T> create(PipelineOptions options, CloudObject spec, Coder<T> coder,
      ExecutionContext executionContext) throws Exception {
    return create(spec, coder);
  }

  static <T> TextReader<T> create(CloudObject spec, Coder<T> coder) throws Exception {
    return create(spec, coder, true);
  }

  public static <T> TextReader<T> create(Source spec) throws Exception {
    return create(
        CloudObject.fromSpec(spec.getSpec()), Serializer.deserialize(spec.getCodec(), Coder.class));
  }

  static <T> TextReader<T> create(CloudObject spec, Coder<T> coder, boolean useDefaultBufferSize)
      throws Exception {
    String filenameOrPattern = getString(spec, PropertyNames.FILENAME, null);
    if (filenameOrPattern == null) {
      filenameOrPattern = getString(spec, PropertyNames.FILEPATTERN, null);
    }
    return new TextReader<>(filenameOrPattern,
        getBoolean(spec, PropertyNames.STRIP_TRAILING_NEWLINES, true),
        getLong(spec, PropertyNames.START_OFFSET, null),
        getLong(spec, PropertyNames.END_OFFSET, null), coder,
        useDefaultBufferSize,
        Enum.valueOf(TextIO.CompressionType.class,
            getString(spec, PropertyNames.COMPRESSION_TYPE, "AUTO")));
  }
}
