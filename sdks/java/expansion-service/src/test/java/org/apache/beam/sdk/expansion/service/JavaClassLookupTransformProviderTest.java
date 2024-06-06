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

import static org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProvider.ALLOW_LIST_VERSION;
import static org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProvider.AllowList;
import static org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProvider.AllowedClass;
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.BuilderMethod;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Resources;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JavaClassLookupTransformProvider}. */
@RunWith(JUnit4.class)
public class JavaClassLookupTransformProviderTest {

  private static final String TEST_NAME = "TestName";

  private static final String TEST_NAMESPACE = "namespace";

  private static ExpansionService expansionService;

  @BeforeClass
  public static void setupExpansionService() {
    PipelineOptionsFactory.register(ExpansionServiceOptions.class);
    URL allowListFile = Resources.getResource("./test_allowlist.yaml");
    expansionService =
        new ExpansionService(
            new String[] {"--javaClassLookupAllowlistFile=" + allowListFile.getPath()});
  }

  static class DummyDoFn extends DoFn<String, String> {
    String strField1;
    String strField2;
    int intField1;
    Double doubleWrapperField;
    String[] strArrayField;
    DummyComplexType complexTypeField;
    DummyComplexType[] complexTypeArrayField;
    List<String> strListField;
    List<DummyComplexType> complexTypeListField;

    private DummyDoFn(
        String strField1,
        String strField2,
        int intField1,
        Double doubleWrapperField,
        String[] strArrayField,
        DummyComplexType complexTypeField,
        DummyComplexType[] complexTypeArrayField,
        List<String> strListField,
        List<DummyComplexType> complexTypeListField) {
      this.intField1 = intField1;
      this.strField1 = strField1;
      this.strField2 = strField2;
      this.doubleWrapperField = doubleWrapperField;
      this.strArrayField = strArrayField;
      this.complexTypeField = complexTypeField;
      this.complexTypeArrayField = complexTypeArrayField;
      this.strListField = strListField;
      this.complexTypeListField = complexTypeListField;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  public static class DummyComplexType implements Serializable {
    String complexTypeStrField;
    int complexTypeIntField;

    public DummyComplexType() {}

    public DummyComplexType(String complexTypeStrField, int complexTypeIntField) {
      this.complexTypeStrField = complexTypeStrField;
      this.complexTypeIntField = complexTypeIntField;
    }

    @Override
    public int hashCode() {
      return this.complexTypeStrField.hashCode() + this.complexTypeIntField * 31;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DummyComplexType)) {
        return false;
      }
      DummyComplexType toCompare = (DummyComplexType) obj;
      return (this.complexTypeIntField == toCompare.complexTypeIntField)
          && this.complexTypeStrField.equals(toCompare.complexTypeStrField);
    }
  }

  public static class DummyTransform extends PTransform<PBegin, PCollection<String>> {
    String strField1;
    String strField2;
    int intField1;
    Double doubleWrapperField;
    String[] strArrayField;
    DummyComplexType complexTypeField;
    DummyComplexType[] complexTypeArrayField;
    List<String> strListField;
    List<DummyComplexType> complexTypeListField;

    @Override
    public PCollection<String> expand(PBegin input) {
      return input
          .apply("MyCreateTransform", Create.of("aaa", "bbb", "ccc"))
          .apply(
              "MyParDoTransform",
              ParDo.of(
                  new DummyDoFn(
                      this.strField1,
                      this.strField2,
                      this.intField1,
                      this.doubleWrapperField,
                      this.strArrayField,
                      this.complexTypeField,
                      this.complexTypeArrayField,
                      this.strListField,
                      this.complexTypeListField)));
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

  public static class DummyTransformWithMultiArgumentBuilderMethod extends DummyTransform {

    public DummyTransformWithMultiArgumentBuilderMethod(String strField1) {
      this.strField1 = strField1;
    }

    public DummyTransformWithMultiArgumentBuilderMethod withFields(
        String strField2, int intField1) {
      this.strField2 = strField2;
      this.intField1 = intField1;
      return this;
    }
  }

  public static class DummyTransformWithMultiArgumentConstructor extends DummyTransform {

    public DummyTransformWithMultiArgumentConstructor(String strField1, String strField2) {
      this.strField1 = strField1;
      this.strField2 = strField2;
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

  public static class DummyTransformWithMultiLanguageAnnotations extends DummyTransform {

    @MultiLanguageConstructorMethod(name = "create_transform")
    public static DummyTransformWithMultiLanguageAnnotations from(String strField1) {
      DummyTransformWithMultiLanguageAnnotations transform =
          new DummyTransformWithMultiLanguageAnnotations();
      transform.strField1 = strField1;
      return transform;
    }

    @MultiLanguageBuilderMethod(name = "abc")
    public DummyTransformWithMultiLanguageAnnotations withStrField2(String strField2) {
      this.strField2 = strField2;
      return this;
    }

    @MultiLanguageBuilderMethod(name = "xyz")
    public DummyTransformWithMultiLanguageAnnotations withIntField1(int intField1) {
      this.intField1 = intField1;
      return this;
    }
  }

  public static class DummyTransformWithWrapperTypes extends DummyTransform {
    public DummyTransformWithWrapperTypes(String strField1) {
      this.strField1 = strField1;
    }

    public DummyTransformWithWrapperTypes withDoubleWrapperField(Double doubleWrapperField) {
      this.doubleWrapperField = doubleWrapperField;
      return this;
    }
  }

  public static class DummyTransformWithComplexTypes extends DummyTransform {
    public DummyTransformWithComplexTypes(String strField1) {
      this.strField1 = strField1;
    }

    public DummyTransformWithComplexTypes withComplexTypeField(DummyComplexType complexTypeField) {
      this.complexTypeField = complexTypeField;
      return this;
    }
  }

  public static class DummyTransformWithArray extends DummyTransform {
    public DummyTransformWithArray(String strField1) {
      this.strField1 = strField1;
    }

    public DummyTransformWithArray withStrArrayField(String[] strArrayField) {
      this.strArrayField = strArrayField;
      return this;
    }
  }

  public static class DummyTransformWithList extends DummyTransform {
    public DummyTransformWithList(String strField1) {
      this.strField1 = strField1;
    }

    public DummyTransformWithList withStrListField(List<String> strListField) {
      this.strListField = strListField;
      return this;
    }
  }

  public static class DummyTransformWithComplexTypeArray extends DummyTransform {
    public DummyTransformWithComplexTypeArray(String strField1) {
      this.strField1 = strField1;
    }

    public DummyTransformWithComplexTypeArray withComplexTypeArrayField(
        DummyComplexType[] complexTypeArrayField) {
      this.complexTypeArrayField = complexTypeArrayField;
      return this;
    }
  }

  public static class DummyTransformWithComplexTypeList extends DummyTransform {
    public DummyTransformWithComplexTypeList(String strField1) {
      this.strField1 = strField1;
    }

    public DummyTransformWithComplexTypeList withComplexTypeListField(
        List<DummyComplexType> complexTypeListField) {
      this.complexTypeListField = complexTypeListField;
      return this;
    }
  }

  void testClassLookupExpansionRequestConstruction(
      ExternalTransforms.JavaClassLookupPayload payload, Map<String, Object> fieldsToVerify) {
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
                            .setUrn(getUrn(ExpansionMethods.Enum.JAVA_CLASS_LOOKUP))
                            .setPayload(payload.toByteString())))
            .setNamespace(TEST_NAMESPACE)
            .build();
    ExpansionApi.ExpansionResponse response = expansionService.expand(request);
    RunnerApi.PTransform expandedTransform = response.getTransform();
    assertEquals(TEST_NAMESPACE + TEST_NAME, expandedTransform.getUniqueName());
    assertThat(expandedTransform.getInputsCount(), Matchers.is(0));
    assertThat(expandedTransform.getOutputsCount(), Matchers.is(1));
    assertEquals(2, expandedTransform.getSubtransformsCount());
    assertEquals(2, expandedTransform.getSubtransformsCount());
    assertThat(
        expandedTransform.getSubtransforms(0),
        anyOf(containsString("MyCreateTransform"), containsString("MyParDoTransform")));
    assertThat(
        expandedTransform.getSubtransforms(1),
        anyOf(containsString("MyCreateTransform"), containsString("MyParDoTransform")));

    org.apache.beam.model.pipeline.v1.RunnerApi.PTransform userParDoTransform = null;
    for (String transformId : response.getComponents().getTransformsMap().keySet()) {
      if (transformId.contains("ParMultiDo-Dummy-")) {
        userParDoTransform = response.getComponents().getTransformsMap().get(transformId);
      }
    }
    assertNotNull(userParDoTransform);
    ParDoPayload parDoPayload = null;
    try {
      parDoPayload = ParDoPayload.parseFrom(userParDoTransform.getSpec().getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    assertNotNull(parDoPayload);
    DummyDoFn doFn =
        (DummyDoFn)
            ParDoTranslation.doFnWithExecutionInformationFromProto(parDoPayload.getDoFn())
                .getDoFn();
    System.out.println("DoFn" + doFn);

    List<String> verifiedFields = new ArrayList<>();
    if (fieldsToVerify.keySet().contains("strField1")) {
      assertEquals(doFn.strField1, fieldsToVerify.get("strField1"));
      verifiedFields.add("strField1");
    }
    if (fieldsToVerify.keySet().contains("strField2")) {
      assertEquals(doFn.strField2, fieldsToVerify.get("strField2"));
      verifiedFields.add("strField2");
    }
    if (fieldsToVerify.keySet().contains("intField1")) {
      assertEquals(doFn.intField1, fieldsToVerify.get("intField1"));
      verifiedFields.add("intField1");
    }
    if (fieldsToVerify.keySet().contains("doubleWrapperField")) {
      assertEquals(doFn.doubleWrapperField, fieldsToVerify.get("doubleWrapperField"));
      verifiedFields.add("doubleWrapperField");
    }
    if (fieldsToVerify.containsKey("complexTypeStrField")) {
      assertEquals(
          doFn.complexTypeField.complexTypeStrField, fieldsToVerify.get("complexTypeStrField"));
      verifiedFields.add("complexTypeStrField");
    }
    if (fieldsToVerify.containsKey("complexTypeIntField")) {
      assertEquals(
          doFn.complexTypeField.complexTypeIntField, fieldsToVerify.get("complexTypeIntField"));
      verifiedFields.add("complexTypeIntField");
    }

    if (fieldsToVerify.keySet().contains("strArrayField")) {
      assertArrayEquals(doFn.strArrayField, (String[]) fieldsToVerify.get("strArrayField"));
      verifiedFields.add("strArrayField");
    }

    if (fieldsToVerify.keySet().contains("strListField")) {
      assertEquals(doFn.strListField, (List) fieldsToVerify.get("strListField"));
      verifiedFields.add("strListField");
    }

    if (fieldsToVerify.keySet().contains("complexTypeArrayField")) {
      assertArrayEquals(
          doFn.complexTypeArrayField,
          (DummyComplexType[]) fieldsToVerify.get("complexTypeArrayField"));
      verifiedFields.add("complexTypeArrayField");
    }

    if (fieldsToVerify.keySet().contains("complexTypeListField")) {
      assertEquals(doFn.complexTypeListField, (List) fieldsToVerify.get("complexTypeListField"));
      verifiedFields.add("complexTypeListField");
    }

    List<String> unverifiedFields = new ArrayList<>(fieldsToVerify.keySet());
    unverifiedFields.removeAll(verifiedFields);
    if (!unverifiedFields.isEmpty()) {
      throw new RuntimeException("Failed to verify some fields: " + unverifiedFields);
    }
  }

  @Test
  public void testJavaClassLookupWithConstructor() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithConstructor");

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(), ImmutableMap.of("strField1", "test_str_1"));
  }

  @Test
  public void testJavaClassLookupWithConstructorMethod() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithConstructorMethod");

    payloadBuilder.setConstructorMethod("from");
    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(), ImmutableMap.of("strField1", "test_str_1"));
  }

  @Test
  public void testJavaClassLookupWithConstructorAndBuilderMethods() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithConstructorAndBuilderMethods");

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrField2");
    Row builderMethodRow =
        Row.withSchema(Schema.of(Field.of("strField2", FieldType.STRING)))
            .withFieldValue("strField2", "test_str_2")
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withIntField1");
    builderMethodRow =
        Row.withSchema(Schema.of(Field.of("intField1", FieldType.INT32)))
            .withFieldValue("intField1", 10)
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(),
        ImmutableMap.of("strField1", "test_str_1", "strField2", "test_str_2", "intField1", 10));
  }

  @Test
  public void testJavaClassLookupRespectsIgnoreFields() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithConstructorAndBuilderMethods");

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("ignore123", FieldType.STRING)))
            .withFieldValue("ignore123", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrField2");
    Row builderMethodRow =
        Row.withSchema(Schema.of(Field.of("ignore456", FieldType.STRING)))
            .withFieldValue("ignore456", "test_str_2")
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withIntField1");
    builderMethodRow =
        Row.withSchema(Schema.of(Field.of("ignore789", FieldType.INT32)))
            .withFieldValue("ignore789", 10)
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(),
        ImmutableMap.of("strField1", "test_str_1", "strField2", "test_str_2", "intField1", 10));
  }

  @Test
  public void testJavaClassLookupWithMultiArgumentConstructor() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithMultiArgumentConstructor");

    Row constructorRow =
        Row.withSchema(
                Schema.of(
                    Field.of("strField1", FieldType.STRING),
                    Field.of("strField2", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .withFieldValue("strField2", "test_str_2")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(),
        ImmutableMap.of("strField1", "test_str_1", "strField2", "test_str_2"));
  }

  @Test
  public void testJavaClassLookupWithMultiArgumentBuilderMethod() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithMultiArgumentBuilderMethod");

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withFields");
    Row builderMethodRow =
        Row.withSchema(
                Schema.of(
                    Field.of("strField2", FieldType.STRING),
                    Field.of("intField1", FieldType.INT32)))
            .withFieldValue("strField2", "test_str_2")
            .withFieldValue("intField1", 10)
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(),
        ImmutableMap.of("strField1", "test_str_1", "strField2", "test_str_2", "intField1", 10));
  }

  @Test
  public void testJavaClassLookupWithWrapperTypes() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithWrapperTypes");

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withDoubleWrapperField");
    Row builderMethodRow =
        Row.withSchema(Schema.of(Field.of("doubleWrapperField", FieldType.DOUBLE)))
            .withFieldValue("doubleWrapperField", 123.56)
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(), ImmutableMap.of("doubleWrapperField", 123.56));
  }

  @Test
  public void testJavaClassLookupWithComplexTypes() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithComplexTypes");

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();
    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    Schema complexTypeSchema =
        Schema.builder()
            .addStringField("complexTypeStrField")
            .addInt32Field("complexTypeIntField")
            .build();

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withComplexTypeField");

    Row builderMethodParamRow =
        Row.withSchema(complexTypeSchema)
            .withFieldValue("complexTypeStrField", "complex_type_str_1")
            .withFieldValue("complexTypeIntField", 123)
            .build();

    Schema builderMethodSchema =
        Schema.builder().addRowField("complexTypeField", complexTypeSchema).build();
    Row builderMethodRow =
        Row.withSchema(builderMethodSchema)
            .withFieldValue("complexTypeField", builderMethodParamRow)
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(),
        ImmutableMap.of("complexTypeStrField", "complex_type_str_1", "complexTypeIntField", 123));
  }

  @Test
  public void testJavaClassLookupWithSimpleArrayType() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithArray");

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrArrayField");

    Schema builderMethodSchema =
        Schema.builder().addArrayField("strArrayField", FieldType.STRING).build();

    Row builderMethodRow =
        Row.withSchema(builderMethodSchema)
            .withFieldValue(
                "strArrayField", ImmutableList.of("test_str_1", "test_str_2", "test_str_3"))
            .build();

    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    String[] resultArray = {"test_str_1", "test_str_2", "test_str_3"};
    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(), ImmutableMap.of("strArrayField", resultArray));
  }

  @Test
  public void testJavaClassLookupWithSimpleListType() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithList");

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrListField");

    Schema builderMethodSchema =
        Schema.builder().addIterableField("strListField", FieldType.STRING).build();

    Row builderMethodRow =
        Row.withSchema(builderMethodSchema)
            .withFieldValue(
                "strListField", ImmutableList.of("test_str_1", "test_str_2", "test_str_3"))
            .build();

    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    List<String> resultList = new ArrayList<>();
    resultList.add("test_str_1");
    resultList.add("test_str_2");
    resultList.add("test_str_3");
    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(), ImmutableMap.of("strListField", resultList));
  }

  @Test
  public void testJavaClassLookupWithComplexArrayType() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithComplexTypeArray");

    Schema complexTypeSchema =
        Schema.builder()
            .addStringField("complexTypeStrField")
            .addInt32Field("complexTypeIntField")
            .build();

    Schema builderMethodSchema =
        Schema.builder()
            .addArrayField("complexTypeArrayField", FieldType.row(complexTypeSchema))
            .build();

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    List<Row> complexTypeList = new ArrayList<>();
    complexTypeList.add(
        Row.withSchema(complexTypeSchema)
            .withFieldValue("complexTypeStrField", "complex_type_str_1")
            .withFieldValue("complexTypeIntField", 123)
            .build());
    complexTypeList.add(
        Row.withSchema(complexTypeSchema)
            .withFieldValue("complexTypeStrField", "complex_type_str_2")
            .withFieldValue("complexTypeIntField", 456)
            .build());
    complexTypeList.add(
        Row.withSchema(complexTypeSchema)
            .withFieldValue("complexTypeStrField", "complex_type_str_3")
            .withFieldValue("complexTypeIntField", 789)
            .build());

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withComplexTypeArrayField");

    Row builderMethodRow =
        Row.withSchema(builderMethodSchema)
            .withFieldValue("complexTypeArrayField", complexTypeList)
            .build();

    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    ArrayList<DummyComplexType> resultList = new ArrayList<>();
    resultList.add(new DummyComplexType("complex_type_str_1", 123));
    resultList.add(new DummyComplexType("complex_type_str_2", 456));
    resultList.add(new DummyComplexType("complex_type_str_3", 789));

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(),
        ImmutableMap.of("complexTypeArrayField", resultList.toArray(new DummyComplexType[0])));
  }

  @Test
  public void testJavaClassLookupWithComplexListType() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithComplexTypeList");

    Schema complexTypeSchema =
        Schema.builder()
            .addStringField("complexTypeStrField")
            .addInt32Field("complexTypeIntField")
            .build();

    Schema builderMethodSchema =
        Schema.builder()
            .addIterableField("complexTypeListField", FieldType.row(complexTypeSchema))
            .build();

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    List<Row> complexTypeList = new ArrayList<>();
    complexTypeList.add(
        Row.withSchema(complexTypeSchema)
            .withFieldValue("complexTypeStrField", "complex_type_str_1")
            .withFieldValue("complexTypeIntField", 123)
            .build());
    complexTypeList.add(
        Row.withSchema(complexTypeSchema)
            .withFieldValue("complexTypeStrField", "complex_type_str_2")
            .withFieldValue("complexTypeIntField", 456)
            .build());
    complexTypeList.add(
        Row.withSchema(complexTypeSchema)
            .withFieldValue("complexTypeStrField", "complex_type_str_3")
            .withFieldValue("complexTypeIntField", 789)
            .build());

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withComplexTypeListField");

    Row builderMethodRow =
        Row.withSchema(builderMethodSchema)
            .withFieldValue("complexTypeListField", complexTypeList)
            .build();

    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    ArrayList<DummyComplexType> resultList = new ArrayList<>();
    resultList.add(new DummyComplexType("complex_type_str_1", 123));
    resultList.add(new DummyComplexType("complex_type_str_2", 456));
    resultList.add(new DummyComplexType("complex_type_str_3", 789));

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(), ImmutableMap.of("complexTypeListField", resultList));
  }

  @Test
  public void testJavaClassLookupWithConstructorMethodAndBuilderMethods() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithConstructorMethodAndBuilderMethods");
    payloadBuilder.setConstructorMethod("from");

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrField2");

    Row builderMethodRow =
        Row.withSchema(Schema.of(Field.of("strField2", FieldType.STRING)))
            .withFieldValue("strField2", "test_str_2")
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withIntField1");

    builderMethodRow =
        Row.withSchema(Schema.of(Field.of("intField1", FieldType.INT32)))
            .withFieldValue("intField1", 10)
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(),
        ImmutableMap.of("strField1", "test_str_1", "strField2", "test_str_2", "intField1", 10));
  }

  @Test
  public void testJavaClassLookupWithSimplifiedBuilderMethodNames() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithConstructorMethodAndBuilderMethods");
    payloadBuilder.setConstructorMethod("from");

    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("strField2");
    Row builderMethodRow =
        Row.withSchema(Schema.of(Field.of("strField2", FieldType.STRING)))
            .withFieldValue("strField2", "test_str_2")
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("intField1");
    builderMethodRow =
        Row.withSchema(Schema.of(Field.of("intField1", FieldType.INT32)))
            .withFieldValue("intField1", 10)
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(),
        ImmutableMap.of("strField1", "test_str_1", "strField2", "test_str_2", "intField1", 10));
  }

  @Test
  public void testJavaClassLookupWithAnnotations() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithMultiLanguageAnnotations");
    payloadBuilder.setConstructorMethod("create_transform");
    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("abc");
    Row builderMethodRow =
        Row.withSchema(Schema.of(Field.of("strField2", FieldType.STRING)))
            .withFieldValue("strField2", "test_str_2")
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("xyz");
    builderMethodRow =
        Row.withSchema(Schema.of(Field.of("intField1", FieldType.INT32)))
            .withFieldValue("intField1", 10)
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(
        payloadBuilder.build(),
        ImmutableMap.of("strField1", "test_str_1", "strField2", "test_str_2", "intField1", 10));
  }

  @Test
  public void testJavaClassLookupClassNotAvailable() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$UnavailableClass");
    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                testClassLookupExpansionRequestConstruction(
                    payloadBuilder.build(), ImmutableMap.of()));
    assertTrue(thrown.getMessage().contains("does not enable"));
  }

  @Test
  public void testJavaClassLookupIncorrectConstructionParameter() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithConstructor");
    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("incorrectField", FieldType.STRING)))
            .withFieldValue("incorrectField", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                testClassLookupExpansionRequestConstruction(
                    payloadBuilder.build(), ImmutableMap.of()));
    assertTrue(thrown.getMessage().contains("Could not find a matching constructor"));
  }

  @Test
  public void testJavaClassLookupIncorrectBuilderMethodParameter() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaClassLookupTransformProviderTest$DummyTransformWithConstructorAndBuilderMethods");
    Row constructorRow =
        Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
            .withFieldValue("strField1", "test_str_1")
            .build();

    payloadBuilder.setConstructorSchema(getProtoSchemaFromRow(constructorRow));
    payloadBuilder.setConstructorPayload(getProtoPayloadFromRow(constructorRow));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrField2");
    Row builderMethodRow =
        Row.withSchema(Schema.of(Field.of("incorrectParam", FieldType.STRING)))
            .withFieldValue("incorrectParam", "test_str_2")
            .build();
    builderMethodBuilder.setSchema(getProtoSchemaFromRow(builderMethodRow));
    builderMethodBuilder.setPayload(getProtoPayloadFromRow(builderMethodRow));

    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                testClassLookupExpansionRequestConstruction(
                    payloadBuilder.build(), ImmutableMap.of()));
    assertTrue(thrown.getMessage().contains("Could not find a matching method"));
  }

  private SchemaApi.Schema getProtoSchemaFromRow(Row row) {
    return SchemaTranslation.schemaToProto(row.getSchema(), true);
  }

  private ByteString getProtoPayloadFromRow(Row row) {
    ByteStringOutputStream outputStream = new ByteStringOutputStream();
    try {
      SchemaCoder.of(row.getSchema()).encode(row, outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return outputStream.toByteString();
  }

  @Test
  public void testNothingAllowList() {
    AllowList nothing = AllowList.nothing();
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> nothing.getAllowedClass("org.apache.beam.sdk.transforms.KvSwap"));
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("allow list does not enable"));
    assertTrue(
        thrown.getMessage(), thrown.getMessage().contains("org.apache.beam.sdk.transforms.KvSwap"));
  }

  @Test
  public void testEverythingAllowList() {
    AllowList everything = AllowList.everything();
    AllowedClass allowedClass = everything.getAllowedClass("org.apache.beam.sdk.transforms.KvSwap");
    assertTrue(allowedClass.isAllowedBuilderMethod("builder"));
    assertTrue(allowedClass.isAllowedConstructorMethod("constructor"));
  }

  @Test
  public void testPackageAllowList() {
    AllowList allowList =
        AllowList.create(
            ALLOW_LIST_VERSION,
            Collections.singletonList(
                AllowedClass.create(
                    "good.package.*",
                    Collections.singletonList("goodBuilder"),
                    AllowedClass.WILDCARD)));
    assertThrows(RuntimeException.class, () -> allowList.getAllowedClass("bad.package.Transform"));
    AllowedClass allowedClass = allowList.getAllowedClass("good.package.Transform");
    assertTrue(allowedClass.isAllowedBuilderMethod("goodBuilder"));
    assertFalse(allowedClass.isAllowedBuilderMethod("badBuilder"));
    assertTrue(allowedClass.isAllowedConstructorMethod("anyConstructor"));
  }
}
