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
package org.apache.beam.runners.prism;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.construction.DefaultArtifactResolver;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;

/**
 * The {@link PrismArtifactResolver} converts a {@link Pipeline} to a {@link RunnerApi.Pipeline} via
 * resolving {@link RunnerApi.ArtifactInformation}.
 */
@AutoValue
abstract class PrismArtifactResolver {

  /**
   * Instantiates a {@link PrismArtifactResolver} from the {@param pipeline}, applying defaults to
   * the remaining dependencies.
   */
  static PrismArtifactResolver of(Pipeline pipeline) {
    return PrismArtifactResolver.builder().setPipeline(pipeline).build();
  }

  static Builder builder() {
    return new AutoValue_PrismArtifactResolver.Builder();
  }

  /**
   * Converts the {@link #getPipeline()} using {@link PipelineTranslation#toProto} and {@link
   * #getDelegate()}'s {@link
   * org.apache.beam.sdk.util.construction.ArtifactResolver#resolveArtifacts}.
   */
  RunnerApi.Pipeline resolvePipelineProto() {
    RunnerApi.Pipeline result = PipelineTranslation.toProto(getPipeline(), getSdkComponents());
    return getDelegate().resolveArtifacts(result);
  }

  /**
   * {@link PrismArtifactResolver} delegates to {@link
   * org.apache.beam.sdk.util.construction.ArtifactResolver} to transform {@link
   * RunnerApi.ArtifactInformation}. Defaults to {@link DefaultArtifactResolver#INSTANCE} if not
   * set.
   */
  abstract org.apache.beam.sdk.util.construction.ArtifactResolver getDelegate();

  /** The {@link Pipeline} from which {@link PrismArtifactResolver#resolvePipelineProto()}. */
  abstract Pipeline getPipeline();

  /**
   * SDK objects that will be represented by {@link
   * org.apache.beam.model.pipeline.v1.RunnerApi.Components}. Instantiated via {@link
   * SdkComponents#create(PipelineOptions)} by default, where {@link PipelineOptions} are acquired
   * from {@link #getPipeline}'s {@link Pipeline#getOptions}.
   */
  abstract SdkComponents getSdkComponents();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setDelegate(
        org.apache.beam.sdk.util.construction.ArtifactResolver artifactResolver);

    abstract Optional<org.apache.beam.sdk.util.construction.ArtifactResolver> getDelegate();

    abstract Builder setSdkComponents(SdkComponents sdkComponents);

    abstract Optional<SdkComponents> getSdkComponents();

    abstract Builder setPipeline(Pipeline pipeline);

    abstract Optional<Pipeline> getPipeline();

    abstract PrismArtifactResolver autoBuild();

    final PrismArtifactResolver build() {
      if (!getDelegate().isPresent()) {
        setDelegate(DefaultArtifactResolver.INSTANCE);
      }

      if (!getSdkComponents().isPresent()) {
        checkState(getPipeline().isPresent());
        setSdkComponents(SdkComponents.create(getPipeline().get().getOptions()));
      }

      return autoBuild();
    }
  }
}
