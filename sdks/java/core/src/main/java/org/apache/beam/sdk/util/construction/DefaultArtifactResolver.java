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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

/**
 * A default artifact resolver. This resolver applies {@link ResolutionFn} in the reversed order
 * they registered i.e. the function registered later overrides the earlier one if they resolve the
 * same artifact.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DefaultArtifactResolver implements ArtifactResolver {
  public static final ArtifactResolver INSTANCE = new DefaultArtifactResolver();

  // Not threadsafe, access through register() and regesteredFns().
  private List<ResolutionFn> fns =
      Lists.newArrayList(
          (info) -> {
            if (BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE).equals(info.getTypeUrn())) {
              return Optional.of(ImmutableList.of(info));
            } else {
              return Optional.empty();
            }
          });

  private synchronized List<ResolutionFn> regesteredFns() {
    return ImmutableList.copyOf(fns);
  }

  private Function<RunnerApi.ArtifactInformation, Stream<RunnerApi.ArtifactInformation>> resolver =
      (info) -> {
        for (ResolutionFn fn : Lists.reverse(regesteredFns())) {
          Optional<List<RunnerApi.ArtifactInformation>> resolved = fn.resolve(info);
          if (resolved.isPresent()) {
            return resolved.get().stream();
          }
        }
        throw new RuntimeException(String.format("Cannot resolve artifact information: %s", info));
      };

  @Override
  public synchronized void register(ResolutionFn fn) {
    fns.add(fn);
  }

  @Override
  public List<RunnerApi.ArtifactInformation> resolveArtifacts(
      List<RunnerApi.ArtifactInformation> artifacts) {
    for (ResolutionFn fn : Lists.reverse(regesteredFns())) {
      List<RunnerApi.ArtifactInformation> moreResolved = new ArrayList<>();
      for (RunnerApi.ArtifactInformation artifact : artifacts) {
        Optional<List<RunnerApi.ArtifactInformation>> resolved = fn.resolve(artifact);
        if (resolved.isPresent()) {
          moreResolved.addAll(resolved.get());
        } else {
          moreResolved.add(artifact);
        }
      }
      artifacts = moreResolved;
    }
    return artifacts;
  }

  @Override
  public RunnerApi.Pipeline resolveArtifacts(RunnerApi.Pipeline pipeline) {
    ImmutableMap.Builder<String, RunnerApi.Environment> environmentMapBuilder =
        ImmutableMap.builder();
    for (Map.Entry<String, RunnerApi.Environment> entry :
        pipeline.getComponents().getEnvironmentsMap().entrySet()) {
      List<RunnerApi.ArtifactInformation> resolvedDependencies =
          entry
              .getValue()
              .getDependenciesList()
              .parallelStream()
              .flatMap(resolver)
              .collect(Collectors.toList());
      environmentMapBuilder.put(
          entry.getKey(),
          entry
              .getValue()
              .toBuilder()
              .clearDependencies()
              .addAllDependencies(resolvedDependencies)
              .build());
    }
    return pipeline
        .toBuilder()
        .setComponents(
            pipeline.getComponents().toBuilder().putAllEnvironments(environmentMapBuilder.build()))
        .build();
  }
}
