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
package org.apache.beam.runners.fnexecution.expansion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.junit.Test;

/** Tests for {@link org.apache.beam.runners.fnexecution.expansion.ExpansionService}. */
public class ExpansionServiceTest {

  private static final String TEST_URN = "test:beam:transforms:count";

  private static final String TEST_NAME = "TestName";

  private static final String TEST_NAMESPACE = "namespace";

  private ExpansionService expansionService = new ExpansionService();

  /** Registers a single test transformation. */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class TestTransforms implements ExpansionService.ExpansionServiceRegistrar {
    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      return ImmutableMap.of(TEST_URN, spec -> Count.perElement());
    }
  }

  @Test
  public void testConstruct() {
    Pipeline p = Pipeline.create();
    p.apply(Impulse.create());
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    String inputPcollId =
        Iterables.getOnlyElement(
            Iterables.getOnlyElement(pipelineProto.getComponents().getTransformsMap().values())
                .getOutputsMap()
                .values());
    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(pipelineProto.getComponents())
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName(TEST_NAME)
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(TEST_URN))
                    .putInputs("input", inputPcollId))
            .setNamespace(TEST_NAMESPACE)
            .build();
    ExpansionApi.ExpansionResponse response = expansionService.expand(request);
    RunnerApi.PTransform expandedTransform = response.getTransform();
    assertEquals(TEST_NAME, expandedTransform.getUniqueName());
    // Verify it has the right input.
    assertEquals(inputPcollId, Iterables.getOnlyElement(expandedTransform.getInputsMap().values()));
    // Loose check that it's composite, and its children are represented.
    assertNotEquals(expandedTransform.getSubtransformsCount(), 0);
    for (String subtransform : expandedTransform.getSubtransformsList()) {
      assertTrue(response.getComponents().containsTransforms(subtransform));
    }
    // Check that any newly generated components are properly namespaced.
    Set<String> originalIds = allIds(request.getComponents());
    for (String id : allIds(response.getComponents())) {
      assertTrue(id, id.startsWith(TEST_NAMESPACE) || originalIds.contains(id));
    }
  }

  public Set<String> allIds(RunnerApi.Components components) {
    Set<String> all = new HashSet<>();
    all.addAll(components.getTransformsMap().keySet());
    all.addAll(components.getPcollectionsMap().keySet());
    all.addAll(components.getCodersMap().keySet());
    all.addAll(components.getWindowingStrategiesMap().keySet());
    all.addAll(components.getEnvironmentsMap().keySet());
    return all;
  }
}
