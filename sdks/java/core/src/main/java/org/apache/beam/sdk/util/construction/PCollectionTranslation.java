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

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;

/**
 * Utility methods for translating {@link PCollection PCollections} to and from Runner API protos.
 */
public class PCollectionTranslation {
  private PCollectionTranslation() {}

  public static RunnerApi.PCollection toProto(PCollection<?> pCollection, SdkComponents components)
      throws IOException {
    String coderId = components.registerCoder(pCollection.getCoder());
    String windowingStrategyId =
        components.registerWindowingStrategy(pCollection.getWindowingStrategy());

    return RunnerApi.PCollection.newBuilder()
        .setUniqueName(pCollection.getName())
        .setCoderId(coderId)
        .setIsBounded(toProto(pCollection.isBounded()))
        .setWindowingStrategyId(windowingStrategyId)
        .build();
  }

  public static PCollection<?> fromProto(
      RunnerApi.PCollection pCollection, Pipeline pipeline, RehydratedComponents components)
      throws IOException {

    Coder<?> coder = components.getCoder(pCollection.getCoderId());
    return PCollection.createPrimitiveOutputInternal(
        pipeline,
        components.getWindowingStrategy(pCollection.getWindowingStrategyId()),
        fromProto(pCollection.getIsBounded()),
        (Coder) coder);
  }

  public static IsBounded isBounded(RunnerApi.PCollection pCollection) {
    return fromProto(pCollection.getIsBounded());
  }

  static RunnerApi.IsBounded.Enum toProto(IsBounded bounded) {
    switch (bounded) {
      case BOUNDED:
        return RunnerApi.IsBounded.Enum.BOUNDED;
      case UNBOUNDED:
        return RunnerApi.IsBounded.Enum.UNBOUNDED;
      default:
        throw new IllegalArgumentException(
            String.format("Unknown %s %s", IsBounded.class.getSimpleName(), bounded));
    }
  }

  static IsBounded fromProto(RunnerApi.IsBounded.Enum isBounded) {
    switch (isBounded) {
      case BOUNDED:
        return IsBounded.BOUNDED;
      case UNBOUNDED:
        return IsBounded.UNBOUNDED;
      case UNRECOGNIZED:
      default:
        // Whether or not this enum cannot be recognized by the proto (due to the version of the
        // generated code we link to) or the switch hasn't been updated to handle it,
        // the situation is the same: we don't know what this IsBounded means
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert unknown %s to %s: %s",
                RunnerApi.IsBounded.class.getCanonicalName(),
                IsBounded.class.getCanonicalName(),
                isBounded));
    }
  }
}
