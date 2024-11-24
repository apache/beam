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
package org.apache.beam.sdk.util.construction;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Utilities for going to/from Runner API pipeline options.
 *
 * <p>TODO: Make this the default to/from translation for PipelineOptions.
 */
public class PipelineOptionsTranslation {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  public static final String PIPELINE_OPTIONS_URN_PREFIX = "beam:option:";
  public static final String PIPELINE_OPTIONS_URN_SUFFIX = ":v1";

  /** Converts the provided {@link PipelineOptions} to a {@link Struct}. */
  public static Struct toProto(PipelineOptions options) {
    Struct.Builder builder = Struct.newBuilder();

    try {
      // TODO: Officially define URNs for options and their scheme.
      JsonNode treeNode = MAPPER.valueToTree(options);
      JsonNode rootOptions = treeNode.get("options");
      Iterator<Map.Entry<String, JsonNode>> optionsEntries = rootOptions.fields();

      if (!optionsEntries.hasNext()) {
        // Due to mandatory options there is no way this map can be empty.
        // If it is, then fail fast as it is due to incompatible jackson-core in the classpath.
        // (observed with version 2.2.3)
        throw new RuntimeException(
            "Unable to convert pipeline options, please check for outdated jackson-core version in the classpath.");
      }

      Map<String, TreeNode> optionsUsingUrns = new HashMap<>();
      while (optionsEntries.hasNext()) {
        Map.Entry<String, JsonNode> entry = optionsEntries.next();
        optionsUsingUrns.put(
            PIPELINE_OPTIONS_URN_PREFIX
                + CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey())
                + PIPELINE_OPTIONS_URN_SUFFIX,
            entry.getValue());
      }

      // The JSON format of a Protobuf Struct is the JSON object that is equivalent to that struct
      // (with values encoded in a standard json-codeable manner). See Beam PR 3719 for more.
      JsonFormat.parser().merge(MAPPER.writeValueAsString(optionsUsingUrns), builder);
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert PipelineOptions to Protocol", e);
    }
  }

  /** Converts the provided {@link Struct} into {@link PipelineOptions}. */
  public static PipelineOptions fromProto(Struct protoOptions) {
    try {
      Map<String, TreeNode> mapWithoutUrns = new HashMap<>();
      TreeNode rootOptions = MAPPER.readTree(JsonFormat.printer().print(protoOptions));
      Iterator<String> optionsKeys = rootOptions.fieldNames();
      while (optionsKeys.hasNext()) {
        String optionKey = optionsKeys.next();
        TreeNode optionValue = rootOptions.get(optionKey);
        mapWithoutUrns.put(
            CaseFormat.LOWER_UNDERSCORE.to(
                CaseFormat.LOWER_CAMEL,
                optionKey.substring(
                    PIPELINE_OPTIONS_URN_PREFIX.length(),
                    optionKey.length() - PIPELINE_OPTIONS_URN_SUFFIX.length())),
            optionValue);
      }
      return MAPPER.readValue(
          MAPPER.writeValueAsString(ImmutableMap.of("options", mapWithoutUrns)),
          PipelineOptions.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read PipelineOptions from Protocol", e);
    }
  }

  /** Converts the provided Json{@link String} into {@link PipelineOptions}. */
  public static PipelineOptions fromJson(String optionsJson) {
    try {
      Map<String, Object> probingOptionsMap =
          MAPPER.readValue(optionsJson, new TypeReference<Map<String, Object>>() {});
      if (probingOptionsMap.containsKey("options")) {
        // Legacy options.
        return MAPPER.readValue(optionsJson, PipelineOptions.class);
      } else {
        // Fn Options with namespace and version.
        Struct.Builder builder = Struct.newBuilder();
        JsonFormat.parser().merge(optionsJson, builder);
        return fromProto(builder.build());
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read PipelineOptions from JSON", e);
    }
  }

  /** Converts the provided {@link PipelineOptions} into Json{@link String}. */
  public static String toJson(PipelineOptions options) {
    try {
      return JsonFormat.printer().print(toProto(options));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to convert PipelineOptions to JSON", e);
    }
  }
}
