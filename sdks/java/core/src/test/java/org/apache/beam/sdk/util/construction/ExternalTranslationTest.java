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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;

import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ExternalTranslation}. */
@RunWith(JUnit4.class)
public class ExternalTranslationTest {
  @Test
  public void testTranslation() {
    Pipeline p = TestPipeline.create();
    TestExpansionServiceClientFactory clientFactory = new TestExpansionServiceClientFactory();
    p.apply(External.of("", new byte[] {}, "", clientFactory));
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    assertThat(
        pipelineProto.getRequirementsList(),
        containsInAnyOrder(clientFactory.response.getRequirementsList().toArray()));
    assertThat(
        pipelineProto.getComponents().getPcollectionsMap().keySet(),
        containsInAnyOrder(
            clientFactory.response.getComponents().getPcollectionsMap().keySet().toArray()));
    assertThat(
        pipelineProto.getComponents().getTransformsMap().keySet(),
        hasItems(
            clientFactory
                .response
                .getComponents()
                .getTransformsMap()
                .keySet()
                .toArray(new String[0])));
  }

  static class TestExpansionServiceClientFactory implements ExpansionServiceClientFactory {
    ExpansionApi.ExpansionResponse response;

    @Override
    public ExpansionServiceClient getExpansionServiceClient(
        Endpoints.ApiServiceDescriptor endpoint) {
      return new ExpansionServiceClient() {
        @Override
        public ExpansionApi.ExpansionResponse expand(ExpansionApi.ExpansionRequest request) {
          Pipeline p = TestPipeline.create();
          p.apply(Create.of(1, 2, 3));
          SdkComponents sdkComponents =
              SdkComponents.create(p.getOptions()).withNewIdPrefix(request.getNamespace());
          RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p, sdkComponents);
          String transformId = Iterables.getOnlyElement(pipelineProto.getRootTransformIdsList());
          RunnerApi.Components components = pipelineProto.getComponents();
          ImmutableList.Builder<String> requirementsBuilder = ImmutableList.builder();
          requirementsBuilder.addAll(pipelineProto.getRequirementsList());
          requirementsBuilder.add("ExternalTranslationTest_Requirement_URN");
          response =
              ExpansionApi.ExpansionResponse.newBuilder()
                  .setComponents(components)
                  .setTransform(
                      components
                          .getTransformsOrThrow(transformId)
                          .toBuilder()
                          .setUniqueName(transformId))
                  .addAllRequirements(requirementsBuilder.build())
                  .build();
          return response;
        }

        @Override
        public ExpansionApi.DiscoverSchemaTransformResponse discover(
            ExpansionApi.DiscoverSchemaTransformRequest request) {
          return null;
        }

        @Override
        public void close() throws Exception {
          // do nothing
        }
      };
    }

    @Override
    public void close() throws Exception {
      // do nothing
    }
  }
}
