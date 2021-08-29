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
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.BuilderMethod;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JavaCLassLookupTransformProvider}. */
@RunWith(JUnit4.class)
public class JavaCLassLookupTransformProviderTest {

  private static final String TEST_URN = "test:beam:transforms:count";

  private static final String TEST_NAME = "TestName";

  private static final String TEST_NAMESPACE = "namespace";

  private static ExpansionService expansionService;

  @BeforeClass
  public static void setupExpansionService() {
    PipelineOptionsFactory.register(ExpansionServiceOptions.class);
    URL allowListFile = Resources.getResource("./test_allowlist.yaml");
    System.out.println("Exists: " + new File(allowListFile.getPath()).exists());
    expansionService =
        new ExpansionService(
            new String[] {"--javaClassLookupAllowlistFile=" + allowListFile.getPath()});
  }

  public static class DummyTransform extends PTransform<PBegin, PCollection<String>> {

    String strField1;
    String strField2;
    int intField1;

    @Override
    public PCollection<String> expand(PBegin input) {
      return input
          .apply("MyCreateTransform", Create.of("aaa", "bbb", "ccc"))
          .apply(
              "MyParDoTransform",
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
                            .setUrn(getUrn(ExpansionMethods.Enum.JAVA_CLASS_LOOKUP))
                            .setPayload(payloaad.toByteString())))
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
  }

  @Test
  public void testJavaClassLookupWithConstructor() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaCLassLookupTransformProviderTest$DummyTransformWithConstructor");

    payloadBuilder.addConstructorParameters(
        ExpansionServiceTest.encodeRowIntoParameter(
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
        "org.apache.beam.sdk.expansion.service.JavaCLassLookupTransformProviderTest$DummyTransformWithConstructorMethod");

    payloadBuilder.setConstructorMethod("from");
    payloadBuilder.addConstructorParameters(
        ExpansionServiceTest.encodeRowIntoParameter(
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
        "org.apache.beam.sdk.expansion.service.JavaCLassLookupTransformProviderTest$DummyTransformWithConstructorAndBuilderMethods");

    payloadBuilder.addConstructorParameters(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrField2");
    builderMethodBuilder.addParameter(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField2", FieldType.STRING)))
                .withFieldValue("strField2", "test_str_2")
                .build(),
            "strField2"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withIntField1");
    builderMethodBuilder.addParameter(
        ExpansionServiceTest.encodeRowIntoParameter(
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
        "org.apache.beam.sdk.expansion.service.JavaCLassLookupTransformProviderTest$DummyTransformWithConstructorMethodAndBuilderMethods");
    payloadBuilder.setConstructorMethod("from");

    payloadBuilder.addConstructorParameters(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrField2");
    builderMethodBuilder.addParameter(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField2", FieldType.STRING)))
                .withFieldValue("strField2", "test_str_2")
                .build(),
            "strField2"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withIntField1");
    builderMethodBuilder.addParameter(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("intField1", FieldType.INT32)))
                .withFieldValue("intField1", 10)
                .build(),
            "intField1"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(payloadBuilder.build());
  }

  @Test
  public void testJavaClassLookupWithSimplifiedBuilderMethodNames() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaCLassLookupTransformProviderTest$DummyTransformWithConstructorMethodAndBuilderMethods");
    payloadBuilder.setConstructorMethod("from");

    payloadBuilder.addConstructorParameters(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("strField2");
    builderMethodBuilder.addParameter(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField2", FieldType.STRING)))
                .withFieldValue("strField2", "test_str_2")
                .build(),
            "strField2"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("intField1");
    builderMethodBuilder.addParameter(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("intField1", FieldType.INT32)))
                .withFieldValue("intField1", 10)
                .build(),
            "intField1"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(payloadBuilder.build());
  }

  @Test
  public void testJavaClassLookupWithAnnotations() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaCLassLookupTransformProviderTest$DummyTransformWithMultiLanguageAnnotations");
    payloadBuilder.setConstructorMethod("create_transform");

    payloadBuilder.addConstructorParameters(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("abc");
    builderMethodBuilder.addParameter(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField2", FieldType.STRING)))
                .withFieldValue("strField2", "test_str_2")
                .build(),
            "strField2"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("xyz");
    builderMethodBuilder.addParameter(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("intField1", FieldType.INT32)))
                .withFieldValue("intField1", 10)
                .build(),
            "intField1"));
    payloadBuilder.addBuilderMethods(builderMethodBuilder);

    testClassLookupExpansionRequestConstruction(payloadBuilder.build());
  }

  @Test
  public void testJavaClassLookupClassNotAvailable() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaCLassLookupTransformProviderTest$UnavailableClass");

    payloadBuilder.addConstructorParameters(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> testClassLookupExpansionRequestConstruction(payloadBuilder.build()));
    assertTrue(thrown.getMessage().contains("not allowed"));
  }

  @Test
  public void testJavaClassLookupIncorrectConstructionParameter() {
    ExternalTransforms.JavaClassLookupPayload.Builder payloadBuilder =
        ExternalTransforms.JavaClassLookupPayload.newBuilder();
    payloadBuilder.setClassName(
        "org.apache.beam.sdk.expansion.service.JavaCLassLookupTransformProviderTest$DummyTransformWithConstructor");

    payloadBuilder.addConstructorParameters(
        ExpansionServiceTest.encodeRowIntoParameter(
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
        "org.apache.beam.sdk.expansion.service.JavaCLassLookupTransformProviderTest$DummyTransformWithConstructorAndBuilderMethods");

    payloadBuilder.addConstructorParameters(
        ExpansionServiceTest.encodeRowIntoParameter(
            Row.withSchema(Schema.of(Field.of("strField1", FieldType.STRING)))
                .withFieldValue("strField1", "test_str_1")
                .build(),
            "strField1"));

    BuilderMethod.Builder builderMethodBuilder = BuilderMethod.newBuilder();
    builderMethodBuilder.setName("withStrField2");
    builderMethodBuilder.addParameter(
        ExpansionServiceTest.encodeRowIntoParameter(
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
}
