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

import java.util.List;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.RunnerApi;

/**
 * An interface for artifact resolvers. Artifact resolvers transform {@link
 * RunnerApi.ArtifactInformation} from one type to another typically from remote or deferred
 * artifacts to locally accessible file artifacts.
 */
public interface ArtifactResolver {

  /**
   * Register {@link ResolutionFn}. When multiple {@link ResolutionFn}s are capable of resolving the
   * same {@link RunnerApi.ArtifactInformation}, the application order is up to the implementing
   * class of this interface.
   *
   * @param fn a resolution function to be registered
   */
  void register(ResolutionFn fn);

  /** Updating pipeline proto by applying registered {@link ResolutionFn}s. */
  RunnerApi.Pipeline resolveArtifacts(RunnerApi.Pipeline pipeline);

  /** Resolves a list of artifacts by applying registered {@link ResolutionFn}s. */
  List<RunnerApi.ArtifactInformation> resolveArtifacts(
      List<RunnerApi.ArtifactInformation> artifacts);

  /**
   * A lazy transformer for resolving {@link RunnerApi.ArtifactInformation}. Note that {@link
   * ResolutionFn} postpones any side-effect until {@link
   * ArtifactResolver#resolveArtifacts(RunnerApi.Pipeline)} is called.
   */
  interface ResolutionFn {
    Optional<List<RunnerApi.ArtifactInformation>> resolve(RunnerApi.ArtifactInformation info);
  }
}
