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
package org.apache.beam.runners.core.construction.graph;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.AccumulationMode;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ComponentsOrBuilder;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides.TransformReplacement;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ProtoOverrides}. */
@RunWith(JUnit4.class)
public class ProtoOverridesTest {
  @Test
  public void replacesOnlyMatching() {
    RunnerApi.Pipeline p =
        Pipeline.newBuilder()
            .addAllRootTransformIds(ImmutableList.of("first", "second"))
            .setComponents(
                Components.newBuilder()
                    .putTransforms(
                        "first",
                        PTransform.newBuilder()
                            .setSpec(FunctionSpec.newBuilder().setUrn("beam:first"))
                            .build())
                    .putTransforms(
                        "second",
                        PTransform.newBuilder()
                            .setSpec(FunctionSpec.newBuilder().setUrn("beam:second"))
                            .build())
                    .putPcollections(
                        "intermediatePc",
                        PCollection.newBuilder().setUniqueName("intermediate").build())
                    .putCoders(
                        "coder",
                        Coder.newBuilder().setSpec(FunctionSpec.getDefaultInstance()).build()))
            .build();

    PTransform secondReplacement =
        PTransform.newBuilder()
            .addSubtransforms("second_sub")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn("beam:second:replacement")
                    .setPayload(
                        ByteString.copyFrom("foo-bar-baz".getBytes(StandardCharsets.UTF_8))))
            .build();
    WindowingStrategy introducedWS =
        WindowingStrategy.newBuilder()
            .setAccumulationMode(AccumulationMode.Enum.ACCUMULATING)
            .build();
    RunnerApi.Components extraComponents =
        Components.newBuilder()
            .putPcollections(
                "intermediatePc",
                PCollection.newBuilder().setUniqueName("intermediate_replacement").build())
            .putWindowingStrategies("new_ws", introducedWS)
            .putTransforms("second_sub", PTransform.getDefaultInstance())
            .build();

    Pipeline updated =
        ProtoOverrides.updateTransform(
            "beam:second", p, new TestReplacer(secondReplacement, extraComponents));
    PTransform updatedSecond = updated.getComponents().getTransformsOrThrow("second");

    assertThat(updatedSecond, equalTo(secondReplacement));
    assertThat(
        updated.getComponents().getWindowingStrategiesOrThrow("new_ws"), equalTo(introducedWS));
    assertThat(
        updated.getComponents().getTransformsOrThrow("second_sub"),
        equalTo(PTransform.getDefaultInstance()));

    // TODO: This might not be appropriate. Merging in the other direction might force that callers
    // are well behaved.
    assertThat(
        updated.getComponents().getPcollectionsOrThrow("intermediatePc").getUniqueName(),
        equalTo("intermediate_replacement"));

    // Assert that the untouched components are unchanged.
    assertThat(
        updated.getComponents().getTransformsOrThrow("first"),
        equalTo(p.getComponents().getTransformsOrThrow("first")));
    assertThat(
        updated.getComponents().getCodersOrThrow("coder"),
        equalTo(p.getComponents().getCodersOrThrow("coder")));
    assertThat(updated.getRootTransformIdsList(), equalTo(p.getRootTransformIdsList()));
  }

  @Test
  public void replacesMultiple() {
    RunnerApi.Pipeline p =
        Pipeline.newBuilder()
            .addAllRootTransformIds(ImmutableList.of("first", "second"))
            .setComponents(
                Components.newBuilder()
                    .putTransforms(
                        "first",
                        PTransform.newBuilder()
                            .setSpec(FunctionSpec.newBuilder().setUrn("beam:first"))
                            .build())
                    .putTransforms(
                        "second",
                        PTransform.newBuilder()
                            .setSpec(FunctionSpec.newBuilder().setUrn("beam:repeated"))
                            .build())
                    .putTransforms(
                        "third",
                        PTransform.newBuilder()
                            .setSpec(FunctionSpec.newBuilder().setUrn("beam:repeated"))
                            .build())
                    .putPcollections(
                        "intermediatePc",
                        PCollection.newBuilder().setUniqueName("intermediate").build())
                    .putCoders(
                        "coder",
                        Coder.newBuilder().setSpec(FunctionSpec.getDefaultInstance()).build()))
            .build();

    ByteString newPayload = ByteString.copyFrom("foo-bar-baz".getBytes(StandardCharsets.UTF_8));
    Pipeline updated =
        ProtoOverrides.updateTransform(
            "beam:repeated",
            p,
            (transformId, existingComponents) -> {
              String subtransform = String.format("%s_sub", transformId);
              return MessageWithComponents.newBuilder()
                  .setPtransform(
                      PTransform.newBuilder()
                          .setSpec(
                              FunctionSpec.newBuilder()
                                  .setUrn("beam:repeated:replacement")
                                  .setPayload(newPayload))
                          .addSubtransforms(subtransform))
                  .setComponents(
                      Components.newBuilder()
                          .putTransforms(
                              subtransform,
                              PTransform.newBuilder().setUniqueName(subtransform).build()))
                  .build();
            });
    PTransform updatedSecond = updated.getComponents().getTransformsOrThrow("second");
    PTransform updatedThird = updated.getComponents().getTransformsOrThrow("third");

    assertThat(updatedSecond, not(equalTo(p.getComponents().getTransformsOrThrow("second"))));
    assertThat(updatedThird, not(equalTo(p.getComponents().getTransformsOrThrow("third"))));
    assertThat(updatedSecond.getSubtransformsList(), contains("second_sub"));
    assertThat(updatedSecond.getSpec().getPayload(), equalTo(newPayload));
    assertThat(updatedThird.getSubtransformsList(), contains("third_sub"));
    assertThat(updatedThird.getSpec().getPayload(), equalTo(newPayload));

    assertThat(updated.getComponents().getTransformsMap(), hasKey("second_sub"));
    assertThat(updated.getComponents().getTransformsMap(), hasKey("third_sub"));
    assertThat(
        updated.getComponents().getTransformsOrThrow("second_sub").getUniqueName(),
        equalTo("second_sub"));
    assertThat(
        updated.getComponents().getTransformsOrThrow("third_sub").getUniqueName(),
        equalTo("third_sub"));
  }

  @Test
  public void replaceExistingCompositeSucceeds() {
    Pipeline p =
        Pipeline.newBuilder()
            .addRootTransformIds("root")
            .setComponents(
                Components.newBuilder()
                    .putTransforms(
                        "root",
                        PTransform.newBuilder()
                            .addSubtransforms("sub_first")
                            .setSpec(FunctionSpec.newBuilder().setUrn("beam:composite"))
                            .build())
                    .putTransforms(
                        "sub_first",
                        PTransform.newBuilder()
                            .setSpec(FunctionSpec.newBuilder().setUrn("beam:inner"))
                            .build()))
            .build();

    Pipeline pipeline =
        ProtoOverrides.updateTransform(
            "beam:composite",
            p,
            new TestReplacer(
                PTransform.newBuilder()
                    .addSubtransforms("foo")
                    .addSubtransforms("bar")
                    .setSpec(
                        FunctionSpec.getDefaultInstance()
                            .newBuilderForType()
                            .setUrn("beam:composite"))
                    .build(),
                Components.getDefaultInstance()));
    assertThat(
        pipeline.getComponents().getTransformsOrThrow("root").getSpec().getUrn(),
        equalTo("beam:composite"));
    assertThat(
        pipeline.getComponents().getTransformsOrThrow("root").getSubtransformsList(),
        contains("foo", "bar"));
  }

  private static class TestReplacer implements TransformReplacement {
    private final PTransform extraTransform;
    private final Components extraComponents;

    private TestReplacer(PTransform extraTransform, Components extraComponents) {
      this.extraTransform = extraTransform;
      this.extraComponents = extraComponents;
    }

    @Override
    public MessageWithComponents getReplacement(
        String transformId, ComponentsOrBuilder existingComponents) {
      return MessageWithComponents.newBuilder()
          .setPtransform(extraTransform)
          .setComponents(extraComponents)
          .build();
    }
  }
}
