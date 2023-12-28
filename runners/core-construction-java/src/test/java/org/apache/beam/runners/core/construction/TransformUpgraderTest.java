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

import static org.junit.Assert.assertEquals;

import com.google.auto.service.AutoService;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for TransformServiceBasedOverride. */
@RunWith(JUnit4.class)
public class TransformUpgraderTest {
  static class TestTransform extends PTransform<PCollection<Integer>, PCollection<Integer>> {
    private int testParam;

    public TestTransform(int testParam) {
      this.testParam = testParam;
    }

    @Override
    public PCollection<Integer> expand(PCollection<Integer> input) {
      return input.apply(
          MapElements.via(
              new SimpleFunction<Integer, Integer>() {
                @Override
                public Integer apply(Integer input) {
                  return input * testParam;
                }
              }));
    }

    public Integer getTestParam() {
      return testParam;
    }
  }

  static class TestTransformPayloadTranslator
      implements PTransformTranslation.TransformPayloadTranslator<TestTransform> {

    static final String URN = "beam:transform:test:transform_to_update";

    Schema configRowSchema = Schema.builder().addInt32Field("multiplier").build();

    @Override
    public String getUrn() {
      return URN;
    }

    @Override
    public TestTransform fromConfigRow(Row configRow) {
      return new TestTransform(configRow.getInt32("multiplier"));
    }

    @Override
    public Row toConfigRow(TestTransform transform) {
      return Row.withSchema(configRowSchema)
          .withFieldValue("multiplier", transform.getTestParam())
          .build();
    }

    @Override
    public RunnerApi.@Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, TestTransform> application, SdkComponents components)
        throws IOException {

      int testParam = application.getTransform().getTestParam();

      FunctionSpec.Builder specBuilder = FunctionSpec.newBuilder();
      specBuilder.setUrn(getUrn());

      ByteStringOutputStream byteStringOut = new ByteStringOutputStream();
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteStringOut);
      objectOutputStream.writeObject(testParam);
      objectOutputStream.flush();
      specBuilder.setPayload(byteStringOut.toByteString());

      return specBuilder.build();
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<Class<TestTransform>, TestTransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(TestTransform.class, new TestTransformPayloadTranslator());
    }
  }

  static class TestTransform2 extends TestTransform {
    public TestTransform2(int testParam) {
      super(testParam);
    }
  }

  static class TestTransformPayloadTranslator2 extends TestTransformPayloadTranslator {
    static final String URN = "beam:transform:test:transform_to_update2";

    @Override
    public String getUrn() {
      return URN;
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar2 implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<Class<TestTransform2>, TestTransformPayloadTranslator2>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(TestTransform2.class, new TestTransformPayloadTranslator2());
    }
  }

  static class TestExpansionServiceClientFactory implements ExpansionServiceClientFactory {
    ExpansionApi.ExpansionResponse response;

    @Override
    public ExpansionServiceClient getExpansionServiceClient(
        Endpoints.ApiServiceDescriptor endpoint) {
      return new ExpansionServiceClient() {
        @Override
        public ExpansionApi.ExpansionResponse expand(ExpansionApi.ExpansionRequest request) {
          RunnerApi.Components.Builder responseComponents = request.getComponents().toBuilder();
          RunnerApi.PTransform transformToUpgrade =
              request.getComponents().getTransformsMap().get("TransformUpgraderTest-TestTransform");
          ByteString alreadyUpgraded = ByteString.empty();
          try {
            alreadyUpgraded = transformToUpgrade.getAnnotationsOrThrow("already_upgraded");
          } catch (Exception e) {
            // Ignore
          }
          if (!alreadyUpgraded.isEmpty()) {
            transformToUpgrade =
                request
                    .getComponents()
                    .getTransformsMap()
                    .get("TransformUpgraderTest-TestTransform2");
          }
          if (!transformToUpgrade
              .getSpec()
              .getUrn()
              .equals(request.getTransform().getSpec().getUrn())) {
            throw new RuntimeException("Could not find a valid transform to upgrade");
          }

          Integer oldParam;
          try {
            ByteArrayInputStream byteArrayInputStream =
                new ByteArrayInputStream(transformToUpgrade.getSpec().getPayload().toByteArray());
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            oldParam = (Integer) objectInputStream.readObject();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          RunnerApi.PTransform.Builder upgradedTransform = transformToUpgrade.toBuilder();
          FunctionSpec.Builder specBuilder = upgradedTransform.getSpecBuilder();

          ByteStringOutputStream byteStringOutputStream = new ByteStringOutputStream();
          try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteStringOutputStream);
            objectOutputStream.writeObject(oldParam * 2);
            objectOutputStream.flush();
            specBuilder.setPayload(byteStringOutputStream.toByteString());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }

          upgradedTransform.setSpec(specBuilder.build());
          upgradedTransform.putAnnotations(
              "already_upgraded",
              ByteString.copyFrom("dummyvalue".getBytes(Charset.defaultCharset())));

          response =
              ExpansionApi.ExpansionResponse.newBuilder()
                  .setComponents(responseComponents.build())
                  .setTransform(upgradedTransform.build())
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

  private void validateTestParam(RunnerApi.PTransform updatedTestTransform, Integer expectedValue) {
    Integer updatedParam;
    try {
      ByteArrayInputStream byteArrayInputStream =
          new ByteArrayInputStream(updatedTestTransform.getSpec().getPayload().toByteArray());
      ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
      updatedParam = (Integer) objectInputStream.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertEquals(Integer.valueOf(expectedValue), updatedParam);
  }

  @Test
  public void testTransformUpgrade() throws Exception {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of(1, 2, 3))
        .apply(new TestTransform(2))
        .apply(ToString.elements())
        .apply(TextIO.write().to("dummyfilename"));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, false);
    ExternalTranslationOptions options =
        PipelineOptionsFactory.create().as(ExternalTranslationOptions.class);
    List<String> urnsToOverride = ImmutableList.of(TestTransformPayloadTranslator.URN);
    options.setTransformsToOverride(urnsToOverride);
    options.setTransformServiceAddress("dummyaddress");

    RunnerApi.Pipeline upgradedPipelineProto =
        TransformUpgrader.of(new TestExpansionServiceClientFactory())
            .upgradeTransformsViaTransformService(pipelineProto, urnsToOverride, options);

    RunnerApi.PTransform upgradedTransform =
        upgradedPipelineProto
            .getComponents()
            .getTransformsMap()
            .get("TransformUpgraderTest-TestTransform");

    validateTestParam(upgradedTransform, 4);
  }

  @Test
  public void testTransformUpgradeMultipleOccurrences() throws Exception {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of(1, 2, 3))
        .apply(new TestTransform(2))
        .apply(ToString.elements())
        .apply(TextIO.write().to("dummyfilename"));
    pipeline
        .apply(Create.of(1, 2, 3))
        .apply(new TestTransform(2))
        .apply(ToString.elements())
        .apply(TextIO.write().to("dummyfilename"));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, false);
    ExternalTranslationOptions options =
        PipelineOptionsFactory.create().as(ExternalTranslationOptions.class);
    List<String> urnsToOverride = ImmutableList.of(TestTransformPayloadTranslator.URN);
    options.setTransformsToOverride(urnsToOverride);
    options.setTransformServiceAddress("dummyaddress");

    RunnerApi.Pipeline upgradedPipelineProto =
        TransformUpgrader.of(new TestExpansionServiceClientFactory())
            .upgradeTransformsViaTransformService(pipelineProto, urnsToOverride, options);

    RunnerApi.PTransform upgradedTransform1 =
        upgradedPipelineProto
            .getComponents()
            .getTransformsMap()
            .get("TransformUpgraderTest-TestTransform");
    validateTestParam(upgradedTransform1, 4);

    RunnerApi.PTransform upgradedTransform2 =
        upgradedPipelineProto
            .getComponents()
            .getTransformsMap()
            .get("TransformUpgraderTest-TestTransform2");
    validateTestParam(upgradedTransform2, 4);
  }

  @Test
  public void testTransformUpgradeMultipleURNs() throws Exception {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of(1, 2, 3))
        .apply(new TestTransform(2))
        .apply(ToString.elements())
        .apply(TextIO.write().to("dummyfilename"));
    pipeline
        .apply(Create.of(1, 2, 3))
        .apply(new TestTransform2(2))
        .apply(ToString.elements())
        .apply(TextIO.write().to("dummyfilename"));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, false);
    ExternalTranslationOptions options =
        PipelineOptionsFactory.create().as(ExternalTranslationOptions.class);
    List<String> urnsToOverride =
        ImmutableList.of(TestTransformPayloadTranslator.URN, TestTransformPayloadTranslator2.URN);
    options.setTransformsToOverride(urnsToOverride);
    options.setTransformServiceAddress("dummyaddress");

    RunnerApi.Pipeline upgradedPipelineProto =
        TransformUpgrader.of(new TestExpansionServiceClientFactory())
            .upgradeTransformsViaTransformService(pipelineProto, urnsToOverride, options);

    RunnerApi.PTransform upgradedTransform1 =
        upgradedPipelineProto
            .getComponents()
            .getTransformsMap()
            .get("TransformUpgraderTest-TestTransform");
    validateTestParam(upgradedTransform1, 4);

    RunnerApi.PTransform upgradedTransform2 =
        upgradedPipelineProto
            .getComponents()
            .getTransformsMap()
            .get("TransformUpgraderTest-TestTransform2");
    validateTestParam(upgradedTransform2, 4);
  }
}
