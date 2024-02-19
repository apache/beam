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

import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;

/** Tests for {@link ExpansionServiceSchemaTransformProvider}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class ExpansionServiceSchemaTransformProviderTest {

  private static final String TEST_NAME = "TestName";

  private static final String TEST_NAMESPACE = "namespace";

  private static final Schema TEST_SCHEMATRANSFORM_CONFIG_SCHEMA =
      Schema.of(
          Field.of("int1", FieldType.INT32),
          Field.of("int2", FieldType.INT32),
          Field.of("str1", FieldType.STRING),
          Field.of("str2", FieldType.STRING));

  private static final Schema TEST_SCHEMATRANSFORM_EQUIVALENT_CONFIG_SCHEMA =
      Schema.of(
          Field.of("str2", FieldType.STRING),
          Field.of("str1", FieldType.STRING),
          Field.of("int2", FieldType.INT32),
          Field.of("int1", FieldType.INT32));

  private ExpansionService expansionService = new ExpansionService();

  @DefaultSchema(JavaFieldSchema.class)
  public static class TestSchemaTransformConfiguration {

    public final String str1;
    public final String str2;
    public final Integer int1;
    public final Integer int2;

    @SchemaCreate
    public TestSchemaTransformConfiguration(String str1, String str2, Integer int1, Integer int2) {
      this.str1 = str1;
      this.str2 = str2;
      this.int1 = int1;
      this.int2 = int2;
    }
  }

  /** Registers a SchemaTransform. */
  @AutoService(SchemaTransformProvider.class)
  public static class TestSchemaTransformProvider
      extends TypedSchemaTransformProvider<TestSchemaTransformConfiguration> {

    @Override
    protected Class<TestSchemaTransformConfiguration> configurationClass() {
      return TestSchemaTransformConfiguration.class;
    }

    @Override
    protected SchemaTransform from(TestSchemaTransformConfiguration configuration) {
      return new TestSchemaTransform(
          configuration.str1, configuration.str2, configuration.int1, configuration.int2);
    }

    @Override
    public String identifier() {
      return "dummy_id";
    }

    @Override
    public List<String> inputCollectionNames() {
      return ImmutableList.of("input1");
    }

    @Override
    public List<String> outputCollectionNames() {
      return ImmutableList.of("output1");
    }
  }

  public static class TestDoFn extends DoFn<String, String> {

    public String str1;
    public String str2;
    public int int1;
    public int int2;

    public TestDoFn(String str1, String str2, Integer int1, Integer int2) {
      this.str1 = str1;
      this.str2 = str2;
      this.int1 = int1;
      this.int2 = int2;
    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      receiver.output(element);
    }
  }

  public static class TestSchemaTransform extends SchemaTransform {

    private String str1;
    private String str2;
    private Integer int1;
    private Integer int2;

    public TestSchemaTransform(String str1, String str2, Integer int1, Integer int2) {
      this.str1 = str1;
      this.str2 = str2;
      this.int1 = int1;
      this.int2 = int2;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> outputPC =
          input
              .getAll()
              .values()
              .iterator()
              .next()
              .apply(
                  MapElements.via(
                      new InferableFunction<Row, String>() {
                        @Override
                        public String apply(Row input) throws Exception {
                          return input.getString("in_str");
                        }
                      }))
              .apply(ParDo.of(new TestDoFn(this.str1, this.str2, this.int1, this.int2)))
              .apply(
                  MapElements.via(
                      new InferableFunction<String, Row>() {
                        @Override
                        public Row apply(String input) throws Exception {
                          return Row.withSchema(Schema.of(Field.of("out_str", FieldType.STRING)))
                              .withFieldValue("out_str", input)
                              .build();
                        }
                      }))
              .setRowSchema(Schema.of(Field.of("out_str", FieldType.STRING)));
      return PCollectionRowTuple.of("output1", outputPC);
    }
  }

  /** Registers a SchemaTransform. */
  @AutoService(SchemaTransformProvider.class)
  public static class TestSchemaTransformProviderMultiInputMultiOutput
      extends TypedSchemaTransformProvider<TestSchemaTransformConfiguration> {

    @Override
    protected Class<TestSchemaTransformConfiguration> configurationClass() {
      return TestSchemaTransformConfiguration.class;
    }

    @Override
    protected SchemaTransform from(TestSchemaTransformConfiguration configuration) {
      return new TestSchemaTransformMultiInputOutput(
          configuration.str1, configuration.str2, configuration.int1, configuration.int2);
    }

    @Override
    public String identifier() {
      return "dummy_id_multi_input_multi_output";
    }

    @Override
    public List<String> inputCollectionNames() {
      return ImmutableList.of("input1", "input2");
    }

    @Override
    public List<String> outputCollectionNames() {
      return ImmutableList.of("output1", "output2");
    }
  }

  public static class TestSchemaTransformMultiInputOutput extends SchemaTransform {

    private String str1;
    private String str2;
    private Integer int1;
    private Integer int2;

    public TestSchemaTransformMultiInputOutput(
        String str1, String str2, Integer int1, Integer int2) {
      this.str1 = str1;
      this.str2 = str2;
      this.int1 = int1;
      this.int2 = int2;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> outputPC1 =
          input
              .get("input1")
              .apply(
                  MapElements.via(
                      new InferableFunction<Row, String>() {
                        @Override
                        public String apply(Row input) throws Exception {
                          return input.getString("in_str");
                        }
                      }))
              .apply(ParDo.of(new TestDoFn(this.str1, this.str2, this.int1, this.int2)))
              .apply(
                  MapElements.via(
                      new InferableFunction<String, Row>() {
                        @Override
                        public Row apply(String input) throws Exception {
                          return Row.withSchema(Schema.of(Field.of("out_str", FieldType.STRING)))
                              .withFieldValue("out_str", input)
                              .build();
                        }
                      }))
              .setRowSchema(Schema.of(Field.of("out_str", FieldType.STRING)));
      PCollection<Row> outputPC2 =
          input
              .get("input2")
              .apply(
                  MapElements.via(
                      new InferableFunction<Row, String>() {
                        @Override
                        public String apply(Row input) throws Exception {
                          return input.getString("in_str");
                        }
                      }))
              .apply(ParDo.of(new TestDoFn(this.str1, this.str2, this.int1, this.int2)))
              .apply(
                  MapElements.via(
                      new InferableFunction<String, Row>() {
                        @Override
                        public Row apply(String input) throws Exception {
                          return Row.withSchema(Schema.of(Field.of("out_str", FieldType.STRING)))
                              .withFieldValue("out_str", input)
                              .build();
                        }
                      }))
              .setRowSchema(Schema.of(Field.of("out_str", FieldType.STRING)));
      return PCollectionRowTuple.of("output1", outputPC1, "output2", outputPC2);
    }
  }

  @Test
  public void testSchemaTransformDiscovery() {
    ExpansionApi.DiscoverSchemaTransformRequest discoverRequest =
        ExpansionApi.DiscoverSchemaTransformRequest.newBuilder().build();
    ExpansionApi.DiscoverSchemaTransformResponse response =
        expansionService.discover(discoverRequest);
    assertTrue(response.getSchemaTransformConfigsCount() >= 2);
  }

  private void verifyLeafTransforms(ExpansionApi.ExpansionResponse response, int count) {

    int leafTransformCount = 0;
    for (RunnerApi.PTransform transform : response.getComponents().getTransformsMap().values()) {
      if (transform.getSpec().getUrn().equals(PTransformTranslation.PAR_DO_TRANSFORM_URN)) {
        RunnerApi.ParDoPayload parDoPayload;
        try {
          parDoPayload = RunnerApi.ParDoPayload.parseFrom(transform.getSpec().getPayload());
          DoFn doFn = ParDoTranslation.getDoFn(parDoPayload);
          if (!(doFn instanceof TestDoFn)) {
            continue;
          }
          TestDoFn testDoFn = (TestDoFn) doFn;
          assertEquals("aaa", testDoFn.str1);
          assertEquals("bbb", testDoFn.str2);
          assertEquals(111, testDoFn.int1);
          assertEquals(222, testDoFn.int2);
          leafTransformCount++;
        } catch (InvalidProtocolBufferException exc) {
          throw new RuntimeException(exc);
        }
      }
    }
    assertEquals(count, leafTransformCount);
  }

  @Test
  public void testSchemaTransformExpansion() {
    Pipeline p = Pipeline.create();
    p.apply(Impulse.create());
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);

    String inputPcollId =
        Iterables.getOnlyElement(
            Iterables.getOnlyElement(pipelineProto.getComponents().getTransformsMap().values())
                .getOutputsMap()
                .values());
    Row configRow =
        Row.withSchema(TEST_SCHEMATRANSFORM_CONFIG_SCHEMA)
            .withFieldValue("int1", 111)
            .withFieldValue("int2", 222)
            .withFieldValue("str1", "aaa")
            .withFieldValue("str2", "bbb")
            .build();

    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(pipelineProto.getComponents())
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName(TEST_NAME)
                    .setSpec(createSpec("dummy_id", configRow))
                    .putInputs("input1", inputPcollId))
            .setNamespace(TEST_NAMESPACE)
            .build();
    ExpansionApi.ExpansionResponse response = expansionService.expand(request);
    RunnerApi.PTransform expandedTransform = response.getTransform();

    assertEquals(3, expandedTransform.getSubtransformsCount());
    assertEquals(1, expandedTransform.getInputsCount());
    assertEquals(1, expandedTransform.getOutputsCount());
    verifyLeafTransforms(response, 1);
  }

  @Test
  public void testSchemaTransformExpansionMultiInputMultiOutput() {
    Pipeline p = Pipeline.create();
    p.apply(Impulse.create());
    p.apply(Impulse.create());
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);

    List<String> inputPcollIds = new ArrayList<>();
    for (RunnerApi.PTransform transform :
        pipelineProto.getComponents().getTransformsMap().values()) {
      inputPcollIds.add(Iterables.getOnlyElement(transform.getOutputsMap().values()));
    }
    assertEquals(2, inputPcollIds.size());

    Row configRow =
        Row.withSchema(TEST_SCHEMATRANSFORM_CONFIG_SCHEMA)
            .withFieldValue("int1", 111)
            .withFieldValue("int2", 222)
            .withFieldValue("str1", "aaa")
            .withFieldValue("str2", "bbb")
            .build();

    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(pipelineProto.getComponents())
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName(TEST_NAME)
                    .setSpec(createSpec("dummy_id_multi_input_multi_output", configRow))
                    .putInputs("input1", inputPcollIds.get(0))
                    .putInputs("input2", inputPcollIds.get(1)))
            .setNamespace(TEST_NAMESPACE)
            .build();

    ExpansionApi.ExpansionResponse response = expansionService.expand(request);
    RunnerApi.PTransform expandedTransform = response.getTransform();

    assertEquals(6, expandedTransform.getSubtransformsCount());
    assertEquals(2, expandedTransform.getInputsCount());
    assertEquals(2, expandedTransform.getOutputsCount());
    verifyLeafTransforms(response, 2);
  }

  @Test
  public void testSchematransformEquivalentConfigSchema() throws CoderException {
    Row configRow =
        Row.withSchema(TEST_SCHEMATRANSFORM_CONFIG_SCHEMA)
            .withFieldValue("int1", 111)
            .withFieldValue("int2", 222)
            .withFieldValue("str1", "aaa")
            .withFieldValue("str2", "bbb")
            .build();

    RunnerApi.FunctionSpec spec = createSpec("dummy_id", configRow);

    Row equivalentConfigRow =
        Row.withSchema(TEST_SCHEMATRANSFORM_EQUIVALENT_CONFIG_SCHEMA)
            .withFieldValue("int1", 111)
            .withFieldValue("int2", 222)
            .withFieldValue("str1", "aaa")
            .withFieldValue("str2", "bbb")
            .build();

    RunnerApi.FunctionSpec equivalentSpec = createSpec("dummy_id", equivalentConfigRow);

    assertNotEquals(spec.getPayload(), equivalentSpec.getPayload());

    TestSchemaTransform transform =
        (TestSchemaTransform)
            ExpansionServiceSchemaTransformProvider.of()
                .getTransform(spec, PipelineOptionsFactory.create());
    TestSchemaTransform equivalentTransform =
        (TestSchemaTransform)
            ExpansionServiceSchemaTransformProvider.of()
                .getTransform(equivalentSpec, PipelineOptionsFactory.create());

    assertEquals(transform.int1, equivalentTransform.int1);
    assertEquals(transform.int2, equivalentTransform.int2);
    assertEquals(transform.str1, equivalentTransform.str1);
    assertEquals(transform.str2, equivalentTransform.str2);
  }

  private RunnerApi.FunctionSpec createSpec(String identifier, Row configRow) {
    byte[] encodedRow;
    try {
      encodedRow = CoderUtils.encodeToByteArray(SchemaCoder.of(configRow.getSchema()), configRow);
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }

    ExternalTransforms.SchemaTransformPayload payload =
        ExternalTransforms.SchemaTransformPayload.newBuilder()
            .setIdentifier(identifier)
            .setConfigurationRow(ByteString.copyFrom(encodedRow))
            .setConfigurationSchema(SchemaTranslation.schemaToProto(configRow.getSchema(), true))
            .build();

    return RunnerApi.FunctionSpec.newBuilder()
        .setUrn(getUrn(ExpansionMethods.Enum.SCHEMA_TRANSFORM))
        .setPayload(payload.toByteString())
        .build();
  }
}
