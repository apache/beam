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

package org.apache.beam.runners.core.construction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/** Utilities for going to/from Runner API pipeline options. */
public class PipelineOptionsTranslation {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  /** Converts the provided {@link PipelineOptions} to a {@link Struct}. */
  public static Struct toProto(PipelineOptions options) {
    Struct.Builder builder = Struct.newBuilder();
    try {
      JsonFormat.parser().merge(MAPPER.writeValueAsString(options), builder);
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Converts the provided {@link Struct} into {@link PipelineOptions}. */
  public static PipelineOptions fromProto(Struct protoOptions) throws IOException {
    return MAPPER.readValue(JsonFormat.printer().print(protoOptions), PipelineOptions.class);
  }
}
