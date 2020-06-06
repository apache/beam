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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Optional;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DefaultArtifactResolverTest {
  private RunnerApi.Pipeline createEmptyPipeline(
      Iterable<RunnerApi.ArtifactInformation> dependencies) {
    return RunnerApi.Pipeline.newBuilder()
        .setComponents(
            RunnerApi.Components.newBuilder()
                .putEnvironments(
                    "env",
                    RunnerApi.Environment.newBuilder().addAllDependencies(dependencies).build()))
        .build();
  }

  // Testing artifacts
  private RunnerApi.ArtifactInformation fooMavenArtifact =
      RunnerApi.ArtifactInformation.newBuilder()
          .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.MAVEN))
          .setTypePayload(
              RunnerApi.MavenPayload.newBuilder()
                  .setArtifact("org.apache:foo:1.0")
                  .build()
                  .toByteString())
          .build();

  private RunnerApi.ArtifactInformation fooFileArtifact =
      RunnerApi.ArtifactInformation.newBuilder()
          .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE))
          .setTypePayload(
              RunnerApi.ArtifactFilePayload.newBuilder().setPath("foo.jar").build().toByteString())
          .build();

  private RunnerApi.ArtifactInformation barFileArtifact =
      RunnerApi.ArtifactInformation.newBuilder()
          .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE))
          .setTypePayload(
              RunnerApi.ArtifactFilePayload.newBuilder().setPath("bar.jar").build().toByteString())
          .build();

  private RunnerApi.ArtifactInformation ambientArtifact =
      RunnerApi.ArtifactInformation.newBuilder()
          .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.DEFERRED))
          .setTypePayload(
              RunnerApi.DeferredArtifactPayload.newBuilder()
                  .setKey("test-key")
                  .build()
                  .toByteString())
          .build();

  // Testing Resolvers
  private ArtifactResolver.ResolutionFn dummyMavenFooResolver =
      info -> {
        if (info.equals(fooMavenArtifact)) {
          return Optional.of(ImmutableList.of(fooFileArtifact));
        } else {
          return Optional.empty();
        }
      };

  private ArtifactResolver.ResolutionFn dummyAmbientFooBarResolver =
      info -> {
        if (info.equals(ambientArtifact)) {
          return Optional.of(ImmutableList.of(fooFileArtifact, barFileArtifact));
        } else {
          return Optional.empty();
        }
      };

  private ArtifactResolver.ResolutionFn dummyAmbientBarFooResolver =
      info -> {
        if (info.equals(ambientArtifact)) {
          return Optional.of(ImmutableList.of(fooFileArtifact, barFileArtifact));
        } else {
          return Optional.empty();
        }
      };

  @Test
  public void testOverridesResolver() {
    ArtifactResolver resolver = new DefaultArtifactResolver();
    resolver.register(dummyAmbientBarFooResolver);
    resolver.register(dummyAmbientFooBarResolver);
    RunnerApi.Pipeline pipeline =
        resolver.resolveArtifacts(createEmptyPipeline(ImmutableList.of(ambientArtifact)));
    assertThat(
        createEmptyPipeline(ImmutableList.of(fooFileArtifact, barFileArtifact)), equalTo(pipeline));
  }

  @Test
  public void testUnknownArtifactInformation() {
    ArtifactResolver resolver = new DefaultArtifactResolver();
    resolver.register(dummyMavenFooResolver);
    try {
      resolver.resolveArtifacts(
          createEmptyPipeline(ImmutableList.of(fooMavenArtifact, ambientArtifact)));
      fail();
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), startsWith("Cannot resolve artifact information:"));
    }
  }
}
