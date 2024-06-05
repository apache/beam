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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardDisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Utilities for going to/from DisplayData protos. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DisplayDataTranslation {
  public static final String LABELLED = "beam:display_data:labelled:v1";

  static {
    checkState(LABELLED.equals(BeamUrns.getUrn(StandardDisplayData.DisplayData.LABELLED)));
  }

  private static final Map<String, Function<DisplayData.Item, ByteString>>
      WELL_KNOWN_URN_TRANSLATORS = ImmutableMap.of(LABELLED, DisplayDataTranslation::translate);

  public static List<RunnerApi.DisplayData> toProto(DisplayData displayData) {
    ImmutableList.Builder<RunnerApi.DisplayData> builder = ImmutableList.builder();
    for (DisplayData.Item item : displayData.items()) {
      Function<DisplayData.Item, ByteString> translator =
          WELL_KNOWN_URN_TRANSLATORS.get(item.getKey());
      String urn;
      if (translator != null) {
        urn = item.getKey();
      } else {
        urn = LABELLED;
        translator = DisplayDataTranslation::translate;
      }
      builder.add(
          RunnerApi.DisplayData.newBuilder()
              .setUrn(urn)
              .setPayload(translator.apply(item))
              .build());
    }
    return builder.build();
  }

  private static ByteString translate(DisplayData.Item item) {
    String label = item.getLabel() == null ? item.getKey() : item.getLabel();
    String namespace = item.getNamespace() == null ? "" : item.getNamespace().getName();
    RunnerApi.LabelledPayload.Builder builder =
        RunnerApi.LabelledPayload.newBuilder()
            .setKey(item.getKey())
            .setLabel(label)
            .setNamespace(namespace);
    Object valueObj = item.getValue() == null ? item.getShortValue() : item.getValue();
    if (valueObj instanceof Boolean) {
      builder.setBoolValue((Boolean) valueObj);
    } else if (valueObj instanceof Integer || valueObj instanceof Long) {
      builder.setIntValue((Long) valueObj);
    } else if (valueObj instanceof Number) {
      builder.setDoubleValue(((Number) valueObj).doubleValue());
    } else {
      builder.setStringValue(String.valueOf(valueObj));
    }
    return builder.build().toByteString();
  }
}
