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
package org.apache.beam.runners.fnexecution.wire;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.util.construction.ModelCoders;

/** Utilities for replacing or wrapping unknown coders with {@link LengthPrefixCoder}. */
public class LengthPrefixUnknownCoders {
  private static Set<String> otherKnownCoderUrns = new HashSet<>();

  /**
   * Registers a coder as being of known type and as such not meriting length prefixing.
   *
   * @param urn The urn of the coder that should not be length prefixed.
   */
  public static void addKnownCoderUrn(String urn) {
    otherKnownCoderUrns.add(urn);
  }

  /**
   * Recursively traverses the coder tree and wraps the first unknown coder in every branch with a
   * {@link LengthPrefixCoder} unless an ancestor coder is itself a {@link LengthPrefixCoder}. If
   * {@code replaceWithByteArrayCoder} is set, then replaces that unknown coder with a {@link
   * ByteArrayCoder}. Registers the new coders in the given {@link Components.Builder}. Note that no
   * ids that are generated will collide with the ids supplied within the {@link
   * Components#getCodersMap() coder map} key space.
   *
   * @param coderId The root coder contained within {@code coders} to start the recursive descent
   *     from.
   * @param components Components builder that initially contains the root coder and all component
   *     coders, and will be modified to contain all the necessary additional coders (including the
   *     resulting coder).
   * @param replaceWithByteArrayCoder whether to replace an unknown coder with a {@link
   *     ByteArrayCoder}.
   * @return Id of the original coder (if unchanged) or the newly generated length-prefixed coder.
   */
  public static String addLengthPrefixedCoder(
      String coderId, RunnerApi.Components.Builder components, boolean replaceWithByteArrayCoder) {
    String lengthPrefixedByteArrayCoderId = addLengthPrefixByteArrayCoder(components);
    String urn = components.getCodersOrThrow(coderId).getSpec().getUrn();

    // We handle three cases:
    //  1) the requested coder is already a length prefix coder. In this case we just honor the
    //     request to replace the coder with a byte array coder.
    //  2) the requested coder is a known coder but not a length prefix coder. In this case we
    //     rebuild the coder by recursively length prefixing any unknown component coders.
    //  3) the requested coder is an unknown coder. In this case we either wrap the requested coder
    //     with a length prefix coder or replace it with a length prefix byte array coder.
    if (ModelCoders.LENGTH_PREFIX_CODER_URN.equals(urn)) {
      return replaceWithByteArrayCoder ? lengthPrefixedByteArrayCoderId : coderId;
    } else if (ModelCoders.urns().contains(urn) || otherKnownCoderUrns.contains(urn)) {
      return addForModelCoder(coderId, components, replaceWithByteArrayCoder);
    } else {
      return replaceWithByteArrayCoder
          ? lengthPrefixedByteArrayCoderId
          : addWrappedWithLengthPrefixCoder(coderId, components);
    }
  }

  private static String addForModelCoder(
      String coderId, RunnerApi.Components.Builder components, boolean replaceWithByteArrayCoder) {
    Coder coder = components.getCodersOrThrow(coderId);
    if (coder.getComponentCoderIdsCount() == 0) {
      return coderId;
    }
    RunnerApi.Coder.Builder builder = coder.toBuilder().clearComponentCoderIds();
    for (String componentCoderId : coder.getComponentCoderIdsList()) {
      builder.addComponentCoderIds(
          addLengthPrefixedCoder(componentCoderId, components, replaceWithByteArrayCoder));
    }
    return addCoder(builder.build(), components, coderId + "-length_prefix");
  }

  // If we are handling an unknown URN then we need to wrap it with a length prefix coder.
  // If requested we also replace the unknown coder with a byte array coder.
  private static String addWrappedWithLengthPrefixCoder(
      String coderId, RunnerApi.Components.Builder components) {
    Coder.Builder lengthPrefixed = Coder.newBuilder().addComponentCoderIds(coderId);
    lengthPrefixed.getSpecBuilder().setUrn(ModelCoders.LENGTH_PREFIX_CODER_URN).build();
    return addCoder(lengthPrefixed.build(), components, coderId + "-length_prefix");
  }

  /** Adds the (singleton) length-prefixed byte array coder. */
  private static String addLengthPrefixByteArrayCoder(RunnerApi.Components.Builder components) {
    // Add byte array coder
    Coder.Builder byteArrayCoder = Coder.newBuilder();
    byteArrayCoder.getSpecBuilder().setUrn(ModelCoders.BYTES_CODER_URN);
    String byteArrayCoderId = addCoder(byteArrayCoder.build(), components, "byte_array");

    // Wrap it into length-prefixed coder
    Coder.Builder lengthPrefixByteArrayCoder = Coder.newBuilder();
    lengthPrefixByteArrayCoder
        .addComponentCoderIds(byteArrayCoderId)
        .getSpecBuilder()
        .setUrn(ModelCoders.LENGTH_PREFIX_CODER_URN);

    return addCoder(lengthPrefixByteArrayCoder.build(), components, "length_prefix_byte_array");
  }

  private static String addCoder(
      RunnerApi.Coder coder, RunnerApi.Components.Builder components, String uniqueIdPrefix) {
    for (Entry<String, Coder> entry : components.getCodersMap().entrySet()) {
      if (entry.getValue().equals(coder)) {
        return entry.getKey();
      }
    }
    String id = generateUniqueId(uniqueIdPrefix, components::containsCoders);
    components.putCoders(id, coder);
    return id;
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
