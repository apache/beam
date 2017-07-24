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

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Utility methods for translating {@link PCollection PCollections} to and from Runner API protos.
 */
public class PCollections {
  private PCollections() {}

  public static RunnerApi.PCollection toProto(PCollection<?> pCollection, SdkComponents components)
      throws IOException {
    String coderId = components.registerCoder(pCollection.getCoder());
    String windowingStrategyId =
        components.registerWindowingStrategy(pCollection.getWindowingStrategy());
    // TODO: Display Data

    return RunnerApi.PCollection.newBuilder()
        .setUniqueName(pCollection.getName())
        .setCoderId(coderId)
        .setIsBounded(toProto(pCollection.isBounded()))
        .setWindowingStrategyId(windowingStrategyId)
        .build();
  }

  public static IsBounded isBounded(RunnerApi.PCollection pCollection) {
    return fromProto(pCollection.getIsBounded());
  }

  public static Coder<?> getCoder(
      RunnerApi.PCollection pCollection, RunnerApi.Components components) throws IOException {
    return Coders.fromProto(components.getCodersOrThrow(pCollection.getCoderId()), components);
  }

  public static WindowingStrategy<?, ?> getWindowingStrategy(
      RunnerApi.PCollection pCollection, RunnerApi.Components components)
      throws InvalidProtocolBufferException {
    return WindowingStrategies.fromProto(
        components.getWindowingStrategiesOrThrow(pCollection.getWindowingStrategyId()), components);
  }

  private static RunnerApi.IsBounded toProto(IsBounded bounded) {
    switch (bounded) {
      case BOUNDED:
        return RunnerApi.IsBounded.BOUNDED;
      case UNBOUNDED:
        return RunnerApi.IsBounded.UNBOUNDED;
      default:
        throw new IllegalArgumentException(
            String.format("Unknown %s %s", IsBounded.class.getSimpleName(), bounded));
    }
  }

  private static IsBounded fromProto(RunnerApi.IsBounded isBounded) {
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
