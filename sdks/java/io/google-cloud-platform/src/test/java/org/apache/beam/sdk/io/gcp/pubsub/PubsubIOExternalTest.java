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
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.powermock.reflect.Whitebox;

/** Tests for building {@link PubsubIO} externally via the ExpansionService. */
@RunWith(JUnit4.class)
public class PubsubIOExternalTest {
  @Test
  public void testConstructPubsubRead() throws Exception {
    String topic = "projects/project-1234/topics/topic_name";
    String idAttribute = "id_foo";
    Boolean needsAttributes = true;

    ExternalTransforms.ExternalConfigurationPayload payload =
        encodeRow(
            Row.withSchema(
                    Schema.of(
                        Field.of("topic", FieldType.STRING),
                        Field.of("id_label", FieldType.STRING),
                        Field.of("with_attributes", FieldType.BOOLEAN)))
                .withFieldValue("topic", topic)
                .withFieldValue("id_label", idAttribute)
                .withFieldValue("with_attributes", needsAttributes)
                .build());

    RunnerApi.Components defaultInstance = RunnerApi.Components.getDefaultInstance();
    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(defaultInstance)
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName("test")
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:external:java:pubsub:read:v1")
                            .setPayload(payload.toByteString())))
            .setNamespace("test_namespace")
            .build();

    ExpansionService expansionService = new ExpansionService();
    TestStreamObserver<ExpansionApi.ExpansionResponse> observer = new TestStreamObserver<>();
    expansionService.expand(request, observer);

    ExpansionApi.ExpansionResponse result = observer.result;
    RunnerApi.PTransform transform = result.getTransform();
    assertThat(
        transform.getSubtransformsList(),
        Matchers.contains(
            "test_namespacetest/PubsubUnboundedSource", "test_namespacetest/MapElements"));
    assertThat(transform.getInputsCount(), Matchers.is(0));
    assertThat(transform.getOutputsCount(), Matchers.is(1));

    RunnerApi.PTransform pubsubComposite =
        result.getComponents().getTransformsOrThrow(transform.getSubtransforms(0));
    RunnerApi.PTransform pubsubRead =
        result.getComponents().getTransformsOrThrow(pubsubComposite.getSubtransforms(0));
    RunnerApi.ReadPayload readPayload =
        RunnerApi.ReadPayload.parseFrom(pubsubRead.getSpec().getPayload());
    PubsubUnboundedSource.PubsubSource source =
        (PubsubUnboundedSource.PubsubSource) ReadTranslation.unboundedSourceFromProto(readPayload);
    PubsubUnboundedSource spec = source.outer;

    assertThat(
        spec.getTopicProvider() == null ? null : String.valueOf(spec.getTopicProvider()),
        Matchers.is(topic));
    assertThat(spec.getIdAttribute(), Matchers.is(idAttribute));
    assertThat(spec.getNeedsAttributes(), Matchers.is(true));
  }

  @Test
  public void testConstructPubsubWrite() throws Exception {
    String topic = "projects/project-1234/topics/topic_name";
    String idAttribute = "id_foo";

    ExternalTransforms.ExternalConfigurationPayload payload =
        encodeRow(
            Row.withSchema(
                    Schema.of(
                        Field.of("topic", FieldType.STRING),
                        Field.of("id_label", FieldType.STRING)))
                .withFieldValue("topic", topic)
                .withFieldValue("id_label", idAttribute)
                .build());

    Pipeline p = Pipeline.create();
    p.apply("unbounded", Create.of(1, 2, 3)).setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    String inputPCollection =
        Iterables.getOnlyElement(
            Iterables.getLast(pipelineProto.getComponents().getTransformsMap().values())
                .getOutputsMap()
                .values());

    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(pipelineProto.getComponents())
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName("test")
                    .putInputs("input", inputPCollection)
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn("beam:external:java:pubsub:write:v1")
                            .setPayload(payload.toByteString())))
            .setNamespace("test_namespace")
            .build();

    ExpansionService expansionService = new ExpansionService();
    TestStreamObserver<ExpansionApi.ExpansionResponse> observer = new TestStreamObserver<>();
    expansionService.expand(request, observer);

    ExpansionApi.ExpansionResponse result = observer.result;

    RunnerApi.PTransform transform = result.getTransform();
    assertThat(
        transform.getSubtransformsList(),
        Matchers.contains(
            "test_namespacetest/MapElements", "test_namespacetest/PubsubUnboundedSink"));
    assertThat(transform.getInputsCount(), Matchers.is(1));
    assertThat(transform.getOutputsCount(), Matchers.is(0));

    // test_namespacetest/PubsubUnboundedSink
    RunnerApi.PTransform writeComposite =
        result.getComponents().getTransformsOrThrow(transform.getSubtransforms(1));

    // test_namespacetest/PubsubUnboundedSink/PubsubUnboundedSink.Writer
    RunnerApi.PTransform writeComposite2 =
        result.getComponents().getTransformsOrThrow(writeComposite.getSubtransforms(3));

    // test_namespacetest/PubsubUnboundedSink/PubsubUnboundedSink.Writer/ParMultiDo(Writer)
    RunnerApi.PTransform writeParDo =
        result.getComponents().getTransformsOrThrow(writeComposite2.getSubtransforms(0));

    RunnerApi.ParDoPayload parDoPayload =
        RunnerApi.ParDoPayload.parseFrom(writeParDo.getSpec().getPayload());
    DoFn<?, ?> pubsubWriter = ParDoTranslation.getDoFn(parDoPayload);

    String idAttributeActual = (String) Whitebox.getInternalState(pubsubWriter, "idAttribute");

    ValueProvider<PubsubClient.TopicPath> topicActual =
        (ValueProvider<PubsubClient.TopicPath>) Whitebox.getInternalState(pubsubWriter, "topic");

    assertThat(topicActual == null ? null : String.valueOf(topicActual), Matchers.is(topic));
    assertThat(idAttributeActual, Matchers.is(idAttribute));
  }

  private static byte[] encodeString(String str) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StringUtf8Coder.of().encode(str, baos);
    return baos.toByteArray();
  }

  private static byte[] encodeBoolean(Boolean value) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BooleanCoder.of().encode(value, baos);
    return baos.toByteArray();
  }

  private static @Nullable String getTopic(@Nullable ValueProvider<PubsubIO.PubsubTopic> value) {
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
  }

  private static class TestStreamObserver<T> implements StreamObserver<T> {

    private T result;

    @Override
    public void onNext(T t) {
      result = t;
    }

    @Override
    public void onError(Throwable throwable) {
      throw new RuntimeException("Should not happen", throwable);
    }

    @Override
    public void onCompleted() {}
  }

  private static ExternalTransforms.ExternalConfigurationPayload encodeRow(Row row) {
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
}
