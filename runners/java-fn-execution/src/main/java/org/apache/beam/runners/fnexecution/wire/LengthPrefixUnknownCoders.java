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

package org.apache.beam.runners.fnexecution.wire;

import java.util.function.Predicate;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;

/** Utilities for replacing or wrapping unknown coders with {@link LengthPrefixCoder}. */
public class LengthPrefixUnknownCoders {
  /**
   * Recursively traverse the coder tree and wrap the first unknown coder in every branch with a
   * {@link LengthPrefixCoder} unless an ancestor coder is itself a {@link LengthPrefixCoder}. If
   * {@code replaceWithByteArrayCoder} is set, then replace that unknown coder with a {@link
   * ByteArrayCoder}. Note that no ids that are generated will collide with the ids supplied within
   * the {@link Components#getCodersMap() coder map} key space.
   *
   * @param coderId The root coder contained within {@code coders} to start the recursive descent
   *     from.
   * @param components Contains the root coder and all component coders.
   * @param replaceWithByteArrayCoder whether to replace an unknown coder with a {@link
   *     ByteArrayCoder}.
   * @return Id of the original coder (if unchanged) or the newly generated length-prefixed coder.
   */
  public static String addLengthPrefixedCoder(
      String coderId, RunnerApi.Components.Builder components, boolean replaceWithByteArrayCoder) {
    RunnerApi.Coder currentCoder = components.getCodersOrThrow(coderId);
    RunnerApi.Coder newCoder;

    Coder lengthPrefixedByteArrayCoder = addLengthPrefixByteArrayCoder(components);

    // We handle three cases:
    //  1) the requested coder is already a length prefix coder. In this case we just honor the
    //     request to replace the coder with a byte array coder.
    //  2) the requested coder is a known coder but not a length prefix coder. In this case we
    //     rebuild the coder by recursively length prefixing any unknown component coders.
    //  3) the requested coder is an unknown coder. In this case we either wrap the requested coder
    //     with a length prefix coder or replace it with a length prefix byte array coder.
    if (ModelCoders.LENGTH_PREFIX_CODER_URN.equals(currentCoder.getSpec().getSpec().getUrn())) {
      newCoder = replaceWithByteArrayCoder ? lengthPrefixedByteArrayCoder : currentCoder;
    } else if (ModelCoders.urns().contains(currentCoder.getSpec().getSpec().getUrn())) {
      newCoder = addForModelCoder(currentCoder, components, replaceWithByteArrayCoder);
    } else {
      newCoder =
          replaceWithByteArrayCoder
              ? lengthPrefixedByteArrayCoder
              : wrapWithLengthPrefixCoder(coderId);
    }

    if (newCoder.equals(currentCoder)) {
      return coderId;
    }
    String newCoderId = generateUniqueId(coderId + "-length_prefix", components::containsCoders);
    components.putCoders(newCoderId, newCoder);
    return newCoderId;
  }

  private static Coder addForModelCoder(
      Coder coder, RunnerApi.Components.Builder components, boolean replaceWithByteArrayCoder) {
    RunnerApi.Coder.Builder builder = coder.toBuilder().clearComponentCoderIds();
    for (String componentCoderId : coder.getComponentCoderIdsList()) {
      builder.addComponentCoderIds(
          addLengthPrefixedCoder(componentCoderId, components, replaceWithByteArrayCoder));
    }
    return builder.build();
  }

  // If we are handling an unknown URN then we need to wrap it with a length prefix coder.
  // If requested we also replace the unknown coder with a byte array coder.
  private static Coder wrapWithLengthPrefixCoder(String coderId) {
    Coder.Builder lengthPrefixed = Coder.newBuilder().addComponentCoderIds(coderId);
    lengthPrefixed
        .getSpecBuilder()
        .getSpecBuilder()
        .setUrn(ModelCoders.LENGTH_PREFIX_CODER_URN)
        .build();
    return lengthPrefixed.build();
  }

  /** Adds the (singleton) length-prefixed byte array coder. */
  private static Coder addLengthPrefixByteArrayCoder(RunnerApi.Components.Builder components) {
    // Add byte array coder
    String byteArrayCoderId = generateUniqueId("byte_array", components::containsCoders);
    Coder.Builder byteArrayCoder = Coder.newBuilder();
    byteArrayCoder.getSpecBuilder().getSpecBuilder().setUrn(ModelCoders.BYTES_CODER_URN);
    components.putCoders(byteArrayCoderId, byteArrayCoder.build());

    // Wrap it into length-prefixed coder
    Coder.Builder lengthPrefixByteArrayCoder = Coder.newBuilder();
    lengthPrefixByteArrayCoder
        .addComponentCoderIds(byteArrayCoderId)
        .getSpecBuilder()
        .getSpecBuilder()
        .setUrn(ModelCoders.LENGTH_PREFIX_CODER_URN);

    return lengthPrefixByteArrayCoder.build();
  }

  /** Generates a unique id given a prefix and the set of existing ids. */
  static String generateUniqueId(String prefix, Predicate<String> isExistingId) {
    int i = 0;
    while (isExistingId.test(prefix + i)) {
      i += 1;
    }
    return prefix + i;
  }
}
