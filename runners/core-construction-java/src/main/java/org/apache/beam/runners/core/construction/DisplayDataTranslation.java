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

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.transforms.display.DisplayData;

/** Utilities for going to/from Runner API display data. */
public class DisplayDataTranslation {

  public static RunnerApi.DisplayData.Path toProto(DisplayData.Path path) {
    return RunnerApi.DisplayData.Path.newBuilder().addAllComponents(path.getComponents()).build();
  }

  public static DisplayData.Path fromProto(RunnerApi.DisplayData.Path path) {
    List<String> pathComponents = path.getComponentsList();
    return DisplayData.Path.absolute(
        pathComponents.get(0),
        pathComponents
            .subList(1, pathComponents.size())
            .toArray(new String[pathComponents.size() - 1]));
  }

  public static RunnerApi.DisplayData.Item toProto(DisplayData.Item item) {
    RunnerApi.DisplayData.Item.Builder builder = RunnerApi.DisplayData.Item.newBuilder()
        .setPath(toProto(item.getPath()))
        .setKey(item.getKey())
        // TODO: use something cross-language; probably requires SDK adjustment
        .setNamespace(item.getNamespace().getName())
        .setValue(item.getValue().toString());

    if (item.getLabel() != null) {
      builder.setLabel(item.getLabel());
    }

    if (item.getLinkUrl() != null) {
      builder.setLinkUrl(item.getLinkUrl());
    }

    if (item.getShortValue() != null) {
      builder.setShortValue(item.getShortValue().toString());
    }

    return builder.build();
  }

  public static DisplayData.Item fromProto(final RunnerApi.DisplayData.Item protoItem) {
    return DisplayData.Item.raw(
        fromProto(protoItem.getPath()),
        protoItem.getKey(),
        protoItem.getValue(),
        protoItem.getShortValue(),
        protoItem.getLabel(),
        protoItem.getLinkUrl());
  }

  public static DisplayData fromProto(final RunnerApi.DisplayData protoDisplayData) {
    List<DisplayData.Item> items = new ArrayList<>();
    for (RunnerApi.DisplayData.Item protoItem : protoDisplayData.getItemsList()) {
      items.add(fromProto(protoItem));
    }
    return DisplayData.fromItems(items);
  }

  public static RunnerApi.DisplayData toProto(final DisplayData displayData) {
    RunnerApi.DisplayData.Builder builder = RunnerApi.DisplayData.newBuilder();
    for (DisplayData.Item item : displayData.items()) {
      builder.addItems(toProto(item));
    }
    return builder.build();
  }
}
