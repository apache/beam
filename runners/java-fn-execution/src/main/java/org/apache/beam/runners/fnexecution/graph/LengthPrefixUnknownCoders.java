/*
 * Copyright (C) 2017 Google Inc.
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

package org.apache.beam.runners.fnexecution.graph;

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;

/**
 * Utilities for replacing or wrapping unknown coders with {@link LengthPrefixCoder}.
 *
 * <p>TODO: Support a dynamic list of well known coders using either registration or manual listing,
 * possibly from ModelCoderRegistrar.
 */
public class LengthPrefixUnknownCoders {
  private static final String BYTES_CODER_TYPE = ModelCoders.BYTES_CODER_URN;
  private static final String LENGTH_PREFIX_CODER_TYPE = ModelCoders.LENGTH_PREFIX_CODER_URN;

  /**
   * Recursively traverse the coder tree and wrap the first unknown coder in every branch with a
   * {@link LengthPrefixCoder} unless an ancestor coder is itself a {@link LengthPrefixCoder}. If
   * {@code replaceWithByteArrayCoder} is set, then replace that unknown coder with a
   * {@link ByteArrayCoder}. Note that no ids that are generated will collide with the ids supplied
   * within the {@link Components#getCodersMap() coder map} key space.
   *
   * @param coderId The root coder contained within {@code coders} to start the recursive descent
   * from.
   * @param components Contains the root coder and all component coders.
   * @param replaceWithByteArrayCoder whether to replace an unknown coder with a
   * {@link ByteArrayCoder}.
   * @return A {@link MessageWithComponents} with the
   * {@link MessageWithComponents#getCoder() root coder} and its component coders. Note that no ids
   * that are generated will collide with the ids supplied within the
   * {@link Components#getCodersMap() coder map} key space.
   */
  public static RunnerApi.MessageWithComponents forCoder(
      String coderId,
      RunnerApi.Components components,
      boolean replaceWithByteArrayCoder) {

    RunnerApi.Coder currentCoder = components.getCodersOrThrow(coderId);

    // We handle three cases:
    //  1) the requested coder is already a length prefix coder. In this case we just honor the
    //     request to replace the coder with a byte array coder.
    //  2) the requested coder is a known coder but not a length prefix coder. In this case we
    //     rebuild the coder by recursively length prefixing any unknown component coders.
    //  3) the requested coder is an unknown coder. In this case we either wrap the requested coder
    //     with a length prefix coder or replace it with a length prefix byte array coder.
    if (LENGTH_PREFIX_CODER_TYPE.equals(currentCoder.getSpec().getSpec().getUrn())) {
      if (replaceWithByteArrayCoder) {
        return createLengthPrefixByteArrayCoder(coderId, components);
      }

      MessageWithComponents.Builder rvalBuilder = MessageWithComponents.newBuilder();
      rvalBuilder.setCoder(currentCoder);
      rvalBuilder.setComponents(components);
      return rvalBuilder.build();
    } else if (ModelCoders.urns().contains(currentCoder.getSpec().getSpec().getUrn())) {
      return lengthPrefixUnknownComponentCoders(coderId, components, replaceWithByteArrayCoder);
    } else {
      return lengthPrefixUnknownCoder(coderId, components, replaceWithByteArrayCoder);
    }
  }

  private static MessageWithComponents lengthPrefixUnknownComponentCoders(
      String coderId,
      RunnerApi.Components components,
      boolean replaceWithByteArrayCoder) {

    MessageWithComponents.Builder rvalBuilder = MessageWithComponents.newBuilder();
    RunnerApi.Coder currentCoder = components.getCodersOrThrow(coderId);
    RunnerApi.Coder.Builder updatedCoder = currentCoder.toBuilder();
    // Rebuild the component coder ids to handle if any of the component coders changed.
    updatedCoder.clearComponentCoderIds();
    for (final String componentCoderId : currentCoder.getComponentCoderIdsList()) {
      MessageWithComponents componentCoder =
          forCoder(componentCoderId, components, replaceWithByteArrayCoder);
      String newComponentCoderId = componentCoderId;
      if (!components.getCodersOrThrow(componentCoderId).equals(componentCoder.getCoder())) {
        // Generate a new id if the component coder changed.
        newComponentCoderId = generateUniqueId(
            coderId + "-length_prefix",
            Sets.union(components.getCodersMap().keySet(),
                rvalBuilder.getComponents().getCodersMap().keySet()));
      }
      updatedCoder.addComponentCoderIds(newComponentCoderId);
      rvalBuilder.getComponentsBuilder().putCoders(newComponentCoderId, componentCoder.getCoder());
      // Insert all component coders of the component coder.
      rvalBuilder.getComponentsBuilder().putAllCoders(
          componentCoder.getComponents().getCodersMap());
    }
    rvalBuilder.setCoder(updatedCoder);

    return rvalBuilder.build();
  }

  // If we are handling an unknown URN then we need to wrap it with a length prefix coder.
  // If requested we also replace the unknown coder with a byte array coder.
  private static MessageWithComponents lengthPrefixUnknownCoder(
      String coderId,
      RunnerApi.Components components,
      boolean replaceWithByteArrayCoder) {
    MessageWithComponents.Builder rvalBuilder = MessageWithComponents.newBuilder();
    RunnerApi.Coder currentCoder = components.getCodersOrThrow(coderId);

    String lengthPrefixComponentCoderId = coderId;
    if (replaceWithByteArrayCoder) {
      return createLengthPrefixByteArrayCoder(coderId, components);
    } else {
      rvalBuilder.getComponentsBuilder().putCoders(coderId, currentCoder);
    }

    rvalBuilder.getCoderBuilder()
        .addComponentCoderIds(lengthPrefixComponentCoderId)
        .getSpecBuilder()
        .getSpecBuilder()
        .setUrn(LENGTH_PREFIX_CODER_TYPE);
    return rvalBuilder.build();
  }

  private static MessageWithComponents createLengthPrefixByteArrayCoder(
      String coderId,
      RunnerApi.Components components) {
    MessageWithComponents.Builder rvalBuilder = MessageWithComponents.newBuilder();

    String byteArrayCoderId = generateUniqueId(
        coderId + "-byte_array",
        Sets.union(components.getCodersMap().keySet(),
            rvalBuilder.getComponents().getCodersMap().keySet()));
    Coder.Builder byteArrayCoder = Coder.newBuilder();
    byteArrayCoder.getSpecBuilder().getSpecBuilder().setUrn(BYTES_CODER_TYPE);
    rvalBuilder.getComponentsBuilder().putCoders(byteArrayCoderId,
        byteArrayCoder.build());
    rvalBuilder.getCoderBuilder()
        .addComponentCoderIds(byteArrayCoderId)
        .getSpecBuilder()
        .getSpecBuilder()
        .setUrn(LENGTH_PREFIX_CODER_TYPE);

    return rvalBuilder.build();
  }

  /**
   * Generates a unique id given a prefix and the set of existing ids.
   */
  static String generateUniqueId(String prefix, Set<String> existingIds) {
    int i = 0;
    while (existingIds.contains(prefix + i)) {
      i += 1;
    }
    return prefix + i;
  }
}
