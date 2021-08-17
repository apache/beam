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

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.BuilderMethod;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.PayloadTypeUrns;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.junit.Test;

/** Tests for {@link ExpansionService}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class ExpansionServiceTest {

  private static final String TEST_URN = "test:beam:transforms:count";

  private static final String TEST_NAME = "TestName";

  private static final String TEST_NAMESPACE = "namespace";

  private ExpansionService expansionService = new ExpansionService();
  public static final List<byte[]> BYTE_LIST =
      ImmutableList.of("testing", "compound", "coders").stream()
          .map(str -> str.getBytes(Charsets.UTF_8))
          .collect(Collectors.toList());
  public static final Map<String, Long> BYTE_KV_LIST =
      ImmutableList.of("testing", "compound", "coders").stream()
          .collect(Collectors.toMap((str) -> str, (str) -> (long) str.length()));
  public static final Map<String, List<Long>> BYTE_KV_LIST_WITH_LIST_VALUE =
      ImmutableList.of("testing", "compound", "coders").stream()
          .collect(
              Collectors.toMap(str -> str, str -> Collections.singletonList((long) str.length())));

  /** Registers a single test transformation. */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class TestTransformRegistrar implements ExpansionService.ExpansionServiceRegistrar {

    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      return ImmutableMap.of(TEST_URN, spec -> Count.perElement());
    }
  }

  public static class DummyTransform extends PTransform<PBegin, PCollection<String>> {

    String strField1;
    String strField2;
    int intField1;

    @Override
    public PCollection<String> expand(PBegin input) {
      return input
          .apply(Create.of("aaa", "bbb", "ccc"))
          .apply(
              ParDo.of(
                  new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      c.output(c.element() + strField1);
                    }
                  }));
    }
  }

  public static class DummyTransformWithConstructor extends DummyTransform {

    public DummyTransformWithConstructor(String strField1) {
      this.strField1 = strField1;
    }
  }

  public static class DummyTransformWithConstructorAndBuilderMethods extends DummyTransform {

    public DummyTransformWithConstructorAndBuilderMethods(String strField1) {
      this.strField1 = strField1;
    }

    public DummyTransformWithConstructorAndBuilderMethods withStrField2(String strField2) {
      this.strField2 = strField2;
      return this;
    }

    public DummyTransformWithConstructorAndBuilderMethods withIntField1(int intField1) {
      this.intField1 = intField1;
      return this;
    }
  }

  public static class DummyTransformWithConstructorMethod extends DummyTransform {

    public static DummyTransformWithConstructorMethod from(String strField1) {
      DummyTransformWithConstructorMethod transform = new DummyTransformWithConstructorMethod();
      transform.strField1 = strField1;
      return transform;
    }
  }

  public static class DummyTransformWithConstructorMethodAndBuilderMethods extends DummyTransform {

    public static DummyTransformWithConstructorMethodAndBuilderMethods from(String strField1) {
      DummyTransformWithConstructorMethodAndBuilderMethods transform =
          new DummyTransformWithConstructorMethodAndBuilderMethods();
      transform.strField1 = strField1;
      return transform;
    }

    public DummyTransformWithConstructorMethodAndBuilderMethods withStrField2(String strField2) {
      this.strField2 = strField2;
      return this;
    }

    public DummyTransformWithConstructorMethodAndBuilderMethods withIntField1(int intField1) {
      this.intField1 = intField1;
      return this;
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
  public void testConstructGenerateSequenceWithRegistration() {
    ExternalTransforms.ExternalConfigurationPayload payload =
        encodeRowIntoExternalConfigurationPayload(
            Row.withSchema(
                    Schema.of(
                        Field.of("start", FieldType.INT64),
                        Field.nullable("stop", FieldType.INT64)))
                .withFieldValue("start", 0L)
                .withFieldValue("stop", 1L)
                .build());

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

  void testClassLookupExpansionRequestConstruction(
      ExternalTransforms.JavaClassLookupPayload payloaad) {
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
                            .setUrn(getUrn(PayloadTypeUrns.Enum.JAVA_CLASS_LOOKUP))
                            .setPayload(payloaad.toByteString())))
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
  public void testJavaClassLookupWithConstructor() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.ExpansionServiceTest$DummyTransformWithConstructor");

    payloadBuilder.addConstructorParameters(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    testClassLookupExpansionRequestConstruction(payloadBuilder.build());
  }

  @Test
  public void testJavaClassLookupWithConstructorMethod() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.ExpansionServiceTest$DummyTransformWithConstructorMethod");

    payloadBuilder.setConstructorMethod("from");
    payloadBuilder.addConstructorParameters(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    testClassLookupExpansionRequestConstruction(payloadBuilder.build());
  }

  @Test
  public void testJavaClassLookupWithConstructorAndBuilderMethods() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.ExpansionServiceTest$DummyTransformWithConstructorAndBuilderMethods");

    payloadBuilder.addConstructorParameters(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrField2");
    builderMethodBuilder.addParameter(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField2", FieldType.STRING)))
                .withFieldValue("strField2", "test_str_2")
                .build(),
            "strField2"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withIntField1");
    builderMethodBuilder.addParameter(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("intField1", FieldType.INT32)))
                .withFieldValue("intField1", 10)
                .build(),
            "intField1"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(payloadBuilder.build());
  }

  @Test
  public void testJavaClassLookupWithConstructorMethodAndBuilderMethods() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.ExpansionServiceTest$DummyTransformWithConstructorMethodAndBuilderMethods");
    payloadBuilder.setConstructorMethod("from");

    payloadBuilder.addConstructorParameters(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrField2");
    builderMethodBuilder.addParameter(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField2", FieldType.STRING)))
                .withFieldValue("strField2", "test_str_2")
                .build(),
            "strField2"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withIntField1");
    builderMethodBuilder.addParameter(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("intField1", FieldType.INT32)))
                .withFieldValue("intField1", 10)
                .build(),
            "intField1"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

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
                            .setUrn(getUrn(PayloadTypeUrns.Enum.JAVA_CLASS_LOOKUP))
                            .setPayload(payloadBuilder.build().toByteString())))
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
  public void testJavaClassLookupClassNotAvailable() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.ExpansionServiceTest$UnavailableClass");

    payloadBuilder.addConstructorParameters(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> testClassLookupExpansionRequestConstruction(payloadBuilder.build()));
    assertTrue(thrown.getMessage().contains("Could not find class"));
  }

  @Test
  public void testJavaClassLookupIncorrectConstructionParameter() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.ExpansionServiceTest$DummyTransformWithConstructor");

    payloadBuilder.addConstructorParameters(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("incorrectField", FieldType.STRING)))
                .withFieldValue("incorrectField", "test_str_1")
                .build(),
            "incorrectField"));

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> testClassLookupExpansionRequestConstruction(payloadBuilder.build()));
    assertTrue(thrown.getMessage().contains("Expected to find a single mapping constructor"));
  }

  @Test
  public void testJavaClassLookupIncorrectBuilderMethodParameter() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.ExpansionServiceTest$DummyTransformWithConstructorAndBuilderMethods");

    payloadBuilder.addConstructorParameters(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrField2");
    builderMethodBuilder.addParameter(
        encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("incorrectParam", FieldType.STRING)))
                .withFieldValue("incorrectParam", "test_str_2")
                .build(),
            "incorrectParam"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> testClassLookupExpansionRequestConstruction(payloadBuilder.build()));
    assertTrue(thrown.getMessage().contains("Expected to find exact one matching method"));
  }

  @Test
  public void testCompoundCodersForExternalConfiguration_setters() throws Exception {
    ExternalTransforms.ExternalConfigurationPayload externalConfig =
        encodeRowIntoExternalConfigurationPayload(
            Row.withSchema(
                    Schema.of(
                        Field.nullable("config_key1", FieldType.INT64),
                        Field.nullable("config_key2", FieldType.iterable(FieldType.BYTES)),
                        Field.of("config_key3", FieldType.map(FieldType.STRING, FieldType.INT64)),
                        Field.of(
                            "config_key4",
                            FieldType.map(FieldType.STRING, FieldType.array(FieldType.INT64)))))
                .withFieldValue("config_key1", 1L)
                .withFieldValue("config_key2", BYTE_LIST)
                .withFieldValue("config_key3", BYTE_KV_LIST)
                .withFieldValue("config_key4", BYTE_KV_LIST_WITH_LIST_VALUE)
                .build());

    TestConfigSetters config =
        ExpansionService.ExternalTransformRegistrarLoader.payloadToConfig(
            externalConfig, TestConfigSetters.class);

    assertThat(config.configKey1, Matchers.is(1L));
    assertThat(config.configKey2, contains(BYTE_LIST.toArray()));
    assertThat(config.configKey3, is(notNullValue()));
    // no-op for checker framework
    if (config.configKey3 != null) {
      assertThat(
          config.configKey3.entrySet(),
          containsInAnyOrder(
              BYTE_KV_LIST.entrySet().stream()
                  .map(
                      (entry) ->
                          allOf(
                              hasProperty("key", equalTo(entry.getKey())),
                              hasProperty("value", equalTo(entry.getValue()))))
                  .collect(Collectors.toList())));
    }
    assertThat(config.configKey4, is(notNullValue()));
    // no-op for checker framework
    if (config.configKey4 != null) {
      assertThat(
          config.configKey4.entrySet(),
          containsInAnyOrder(
              BYTE_KV_LIST_WITH_LIST_VALUE.entrySet().stream()
                  .map(
                      (entry) ->
                          allOf(
                              hasProperty("key", equalTo(entry.getKey())),
                              hasProperty("value", equalTo(entry.getValue()))))
                  .collect(Collectors.toList())));
    }
  }

  private static class TestConfigSetters {
    private @Nullable Long configKey1 = null;
    private @Nullable Iterable<byte[]> configKey2 = null;
    private @Nullable Map<byte[], Long> configKey3 = null;
    private @Nullable Map<byte[], List<Long>> configKey4 = null;

    public void setConfigKey1(@Nullable Long configKey1) {
      this.configKey1 = configKey1;
    }

    public void setConfigKey2(@Nullable Iterable<byte[]> configKey2) {
      this.configKey2 = configKey2;
    }

    public void setConfigKey3(@Nullable Map<byte[], Long> configKey3) {
      this.configKey3 = configKey3;
    }

    public void setConfigKey4(@Nullable Map<byte[], List<Long>> configKey4) {
      this.configKey4 = configKey4;
    }
  }

  @Test
  public void testCompoundCodersForExternalConfiguration_schemas() throws Exception {
    ExternalTransforms.ExternalConfigurationPayload externalConfig =
        encodeRowIntoExternalConfigurationPayload(
            Row.withSchema(
                    Schema.of(
                        Field.nullable("configKey1", FieldType.INT64),
                        Field.nullable("configKey2", FieldType.iterable(FieldType.BYTES)),
                        Field.of("configKey3", FieldType.map(FieldType.STRING, FieldType.INT64)),
                        Field.of(
                            "configKey4",
                            FieldType.map(FieldType.STRING, FieldType.array(FieldType.INT64)))))
                .withFieldValue("configKey1", 1L)
                .withFieldValue("configKey2", BYTE_LIST)
                .withFieldValue("configKey3", BYTE_KV_LIST)
                .withFieldValue("configKey4", BYTE_KV_LIST_WITH_LIST_VALUE)
                .build());
    TestConfigSchema config =
        ExpansionService.ExternalTransformRegistrarLoader.payloadToConfig(
            externalConfig, TestConfigSchema.class);

    assertThat(config.getConfigKey1(), Matchers.is(1L));
    assertThat(config.getConfigKey2(), contains(BYTE_LIST.toArray()));
    Map<String, Long> configKey3 = config.getConfigKey3();
    assertThat(configKey3, is(notNullValue()));
    // no-op for checker framework
    if (configKey3 != null) {
      assertThat(
          configKey3.entrySet(),
          containsInAnyOrder(
              BYTE_KV_LIST.entrySet().stream()
                  .map(
                      (entry) ->
                          allOf(
                              hasProperty("key", equalTo(entry.getKey())),
                              hasProperty("value", equalTo(entry.getValue()))))
                  .collect(Collectors.toList())));
    }
    Map<String, List<Long>> configKey4 = config.getConfigKey4();
    assertThat(configKey4, is(notNullValue()));
    // no-op for checker framework
    if (configKey4 != null) {
      assertThat(
          configKey4.entrySet(),
          containsInAnyOrder(
              BYTE_KV_LIST_WITH_LIST_VALUE.entrySet().stream()
                  .map(
                      (entry) ->
                          allOf(
                              hasProperty("key", equalTo(entry.getKey())),
                              hasProperty("value", equalTo(entry.getValue()))))
                  .collect(Collectors.toList())));
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class TestConfigSchema {
    abstract @Nullable Long getConfigKey1();

    abstract @Nullable Iterable<byte[]> getConfigKey2();

    abstract @Nullable Map<String, Long> getConfigKey3();

    abstract @Nullable Map<String, List<Long>> getConfigKey4();
  }

  @Test
  public void testExternalConfiguration_simpleSchema() throws Exception {
    ExternalTransforms.ExternalConfigurationPayload externalConfig =
        encodeRowIntoExternalConfigurationPayload(
            Row.withSchema(
                    Schema.of(
                        Field.of("bar", FieldType.STRING),
                        Field.of("foo", FieldType.INT64),
                        Field.of("list", FieldType.array(FieldType.STRING))))
                .withFieldValue("foo", 1L)
                .withFieldValue("bar", "test string")
                .withFieldValue("list", ImmutableList.of("abc", "123"))
                .build());

    TestConfigSimpleSchema config =
        ExpansionService.ExternalTransformRegistrarLoader.payloadToConfig(
            externalConfig, TestConfigSimpleSchema.class);

    assertThat(config.getFoo(), Matchers.is(1L));
    assertThat(config.getBar(), Matchers.is("test string"));
    assertThat(config.getList(), Matchers.is(ImmutableList.of("abc", "123")));
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class TestConfigSimpleSchema {
    abstract Long getFoo();

    abstract String getBar();

    abstract List<String> getList();
  }

  private static ExternalTransforms.ExternalConfigurationPayload
      encodeRowIntoExternalConfigurationPayload(Row row) {
    ByteString.Output outputStream = ByteString.newOutput();
    try {
      SchemaCoder.of(row.getSchema()).encode(row, outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return ExternalTransforms.ExternalConfigurationPayload.newBuilder()
        .setSchema(SchemaTranslation.schemaToProto(row.getSchema(), true))
        .setPayload(outputStream.toByteString())
        .build();
  }

  private static ExternalTransforms.Parameter encodeRowIntoParameter(
      Row row, @Nullable String name) {
    ByteString.Output outputStream = ByteString.newOutput();
    try {
      SchemaCoder.of(row.getSchema()).encode(row, outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return ExternalTransforms.Parameter.newBuilder()
        .setName(name)
        .setSchema(SchemaTranslation.schemaToProto(row.getSchema(), true))
        .setPayload(outputStream.toByteString())
        .build();
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
