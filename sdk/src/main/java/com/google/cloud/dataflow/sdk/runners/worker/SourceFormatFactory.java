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

import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.worker.SourceFormat;

import java.util.Map;

/**
 * Creates {@code SourceFormat} objects from {@code Source}.
 */
public class SourceFormatFactory {
  private SourceFormatFactory() {}

  public static SourceFormat create(PipelineOptions options, Source source) throws Exception {
    Map<String, Object> spec = source.getSpec();

    try {
      return InstanceBuilder.ofType(SourceFormat.class)
          .fromClassName(getString(spec, PropertyNames.OBJECT_TYPE_NAME))
          .withArg(PipelineOptions.class, options)
          .build();

    } catch (ClassNotFoundException exn) {
      throw new Exception(
          "unable to create a source format from " + source, exn);
    }
  }
}
