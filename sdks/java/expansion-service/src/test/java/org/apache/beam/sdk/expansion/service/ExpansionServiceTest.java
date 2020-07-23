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
package org.apache.beam.sdk.expansion.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.junit.Test;

/** Tests for {@link ExpansionService}. */
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
    assertEquals(TEST_NAMESPACE + TEST_NAME, expandedTransform.getUniqueName());

    // Verify it has the right input.
    assertThat(expandedTransform.getInputsMap().values(), contains(inputPcollId));

    // Verify it has the right output.
    assertThat(expandedTransform.getOutputsMap().keySet(), contains("output"));

    // Loose check that it's composite, and its children are represented.
    assertThat(expandedTransform.getSubtransformsCount(), greaterThan(0));
    for (String subtransform : expandedTransform.getSubtransformsList()) {
      assertTrue(response.getComponents().containsTransforms(subtransform));
    }
    // Check that any newly generated components are properly namespaced.
    Set<String> originalIds = allIds(request.getComponents());
    for (String id : allIds(response.getComponents())) {
      assertTrue(id, id.startsWith(TEST_NAMESPACE) || originalIds.contains(id));
    }
  }

  @Test
  public void testConstructGenerateSequence() {
    ExternalTransforms.ExternalConfigurationPayload payload =
        ExternalTransforms.ExternalConfigurationPayload.newBuilder()
            .putConfiguration(
                "start",
                ExternalTransforms.ConfigValue.newBuilder()
                    .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.VARINT))
                    .setPayload(ByteString.copyFrom(new byte[] {0}))
                    .build())
            .putConfiguration(
                "stop",
                ExternalTransforms.ConfigValue.newBuilder()
                    .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.VARINT))
                    .setPayload(ByteString.copyFrom(new byte[] {1}))
                    .build())
            .build();
    Pipeline p = Pipeline.create();
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(pipelineProto.getComponents())
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName(TEST_NAME)
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn(GenerateSequence.External.URN)
                            .setPayload(payload.toByteString())))
            .setNamespace(TEST_NAMESPACE)
            .build();
    ExpansionApi.ExpansionResponse response = expansionService.expand(request);
    RunnerApi.PTransform expandedTransform = response.getTransform();
    assertEquals(TEST_NAMESPACE + TEST_NAME, expandedTransform.getUniqueName());
    assertThat(expandedTransform.getInputsCount(), Matchers.is(0));
    assertThat(expandedTransform.getOutputsCount(), Matchers.is(1));
    assertThat(expandedTransform.getSubtransformsCount(), greaterThan(0));
  }

  @Test
  public void testCompoundCodersForExternalConfiguration() throws Exception {
    ExternalTransforms.ExternalConfigurationPayload.Builder builder =
        ExternalTransforms.ExternalConfigurationPayload.newBuilder();

    builder.putConfiguration(
        "config_key1",
        ExternalTransforms.ConfigValue.newBuilder()
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.VARINT))
            .setPayload(ByteString.copyFrom(new byte[] {1}))
            .build());

    List<byte[]> byteList =
        ImmutableList.of("testing", "compound", "coders").stream()
            .map(str -> str.getBytes(Charsets.UTF_8))
            .collect(Collectors.toList());
    IterableCoder<byte[]> compoundCoder = IterableCoder.of(ByteArrayCoder.of());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    compoundCoder.encode(byteList, baos);

    builder.putConfiguration(
        "config_key2",
        ExternalTransforms.ConfigValue.newBuilder()
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.ITERABLE))
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.BYTES))
            .setPayload(ByteString.copyFrom(baos.toByteArray()))
            .build());

    List<KV<byte[], Long>> byteKvList =
        ImmutableList.of("testing", "compound", "coders").stream()
            .map(str -> KV.of(str.getBytes(Charsets.UTF_8), (long) str.length()))
            .collect(Collectors.toList());
    IterableCoder<KV<byte[], Long>> compoundCoder2 =
        IterableCoder.of(KvCoder.of(ByteArrayCoder.of(), VarLongCoder.of()));
    ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    compoundCoder2.encode(byteKvList, baos2);

    builder.putConfiguration(
        "config_key3",
        ExternalTransforms.ConfigValue.newBuilder()
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.ITERABLE))
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.KV))
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.BYTES))
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.VARINT))
            .setPayload(ByteString.copyFrom(baos2.toByteArray()))
            .build());

    List<KV<List<Long>, byte[]>> byteKvListWithListKey =
        ImmutableList.of("testing", "compound", "coders").stream()
            .map(
                str ->
                    KV.of(
                        Collections.singletonList((long) str.length()),
                        str.getBytes(Charsets.UTF_8)))
            .collect(Collectors.toList());
    Coder compoundCoder3 =
        IterableCoder.of(KvCoder.of(IterableCoder.of(VarLongCoder.of()), ByteArrayCoder.of()));
    ByteArrayOutputStream baos3 = new ByteArrayOutputStream();
    compoundCoder3.encode(byteKvListWithListKey, baos3);

    builder.putConfiguration(
        "config_key4",
        ExternalTransforms.ConfigValue.newBuilder()
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.ITERABLE))
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.KV))
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.ITERABLE))
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.VARINT))
            .addCoderUrn(BeamUrns.getUrn(RunnerApi.StandardCoders.Enum.BYTES))
            .setPayload(ByteString.copyFrom(baos3.toByteArray()))
            .build());

    ExternalTransforms.ExternalConfigurationPayload externalConfig = builder.build();
    TestConfig config = new TestConfig();
    ExpansionService.ExternalTransformRegistrarLoader.populateConfiguration(config, externalConfig);

    assertThat(config.configKey1, Matchers.is(1L));
    assertThat(config.configKey2, contains(byteList.toArray()));
    assertThat(config.configKey3, contains(byteKvList.toArray()));
    assertThat(config.configKey4, contains(byteKvListWithListKey.toArray()));
  }

  private static class TestConfig {

    private @Nullable Long configKey1 = null;
    private @Nullable Iterable<byte[]> configKey2 = null;
    private @Nullable Iterable<KV<byte[], Long>> configKey3 = null;
    private @Nullable Iterable<KV<Iterable<Long>, byte[]>> configKey4 = null;

    public void setConfigKey1(@Nullable Long configKey1) {
      this.configKey1 = configKey1;
    }

    public void setConfigKey2(@Nullable Iterable<byte[]> configKey2) {
      this.configKey2 = configKey2;
    }

    public void setConfigKey3(@Nullable Iterable<KV<byte[], Long>> configKey3) {
      this.configKey3 = configKey3;
    }

    public void setConfigKey4(@Nullable Iterable<KV<Iterable<Long>, byte[]>> configKey4) {
      this.configKey4 = configKey4;
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
