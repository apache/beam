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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardDisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Utilities for going to/from DisplayData protos. */
public class DisplayDataTranslation {
  public static final String LABELLED_STRING = "beam:display_data:labelled_string:v1";

  static {
    checkState(
        LABELLED_STRING.equals(BeamUrns.getUrn(StandardDisplayData.DisplayData.LABELLED_STRING)));
  }

  private static final Map<String, Function<DisplayData.Item, ByteString>>
      WELL_KNOWN_URN_TRANSLATORS =
          ImmutableMap.of(LABELLED_STRING, DisplayDataTranslation::translateStringUtf8);

  public static List<RunnerApi.DisplayData> toProto(DisplayData displayData) {
    ImmutableList.Builder<RunnerApi.DisplayData> builder = ImmutableList.builder();
    for (DisplayData.Item item : displayData.items()) {
      Function<DisplayData.Item, ByteString> translator =
          WELL_KNOWN_URN_TRANSLATORS.get(item.getKey());
      String urn;
      if (translator != null) {
        urn = item.getKey();
      } else {
        urn = LABELLED_STRING;
        translator = DisplayDataTranslation::translateStringUtf8;
      }
      builder.add(
          RunnerApi.DisplayData.newBuilder()
              .setUrn(urn)
              .setPayload(translator.apply(item))
              .build());
    }
    return builder.build();
  }

  private static ByteString translateStringUtf8(DisplayData.Item item) {
    String value = String.valueOf(item.getValue() == null ? item.getShortValue() : item.getValue());
    String label = item.getLabel() == null ? item.getKey() : item.getLabel();
    return RunnerApi.LabelledStringPayload.newBuilder()
        .setLabel(label)
        .setValue(value)
        .build()
        .toByteString();
  }
}
