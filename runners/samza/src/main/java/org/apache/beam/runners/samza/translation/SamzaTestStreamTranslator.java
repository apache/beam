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
package org.apache.beam.runners.samza.translation;

import java.io.IOException;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.runners.samza.util.SamzaPipelineTranslatorUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.TestStreamTranslation;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;

/**
 * Translate {@link org.apache.beam.sdk.testing.TestStream} to a samza message stream produced by
 * {@link SamzaTestStreamSystemFactory.SamzaTestStreamSystemConsumer}.
 */
@SuppressWarnings({"rawtypes"})
public class SamzaTestStreamTranslator<T> implements TransformTranslator<TestStream<T>> {
  public static final String ENCODED_TEST_STREAM = "encodedTestStream";
  public static final String TEST_STREAM_DECODER = "testStreamDecoder";

  @Override
  public void translate(
      TestStream<T> testStream, TransformHierarchy.Node node, TranslationContext ctx) {
    final PCollection<T> output = ctx.getOutput(testStream);
    final String outputId = ctx.getIdForPValue(output);
    final Coder<T> valueCoder = testStream.getValueCoder();
    final TestStream.TestStreamCoder<T> testStreamCoder = TestStream.TestStreamCoder.of(valueCoder);

    // encode testStream as a string
    final String encodedTestStream;
    try {
      encodedTestStream = CoderUtils.encodeToBase64(testStreamCoder, testStream);
    } catch (CoderException e) {
      throw new RuntimeException("Could not encode TestStream.", e);
    }

    // the decoder for encodedTestStream
    SerializableFunction<String, TestStream<T>> testStreamDecoder =
        string -> {
          try {
            return CoderUtils.decodeFromBase64(TestStream.TestStreamCoder.of(valueCoder), string);
          } catch (CoderException e) {
            throw new RuntimeException("Could not decode TestStream.", e);
          }
        };

    ctx.registerInputMessageStream(
        output, createInputDescriptor(outputId, encodedTestStream, testStreamDecoder));
  }

  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    final ByteString bytes = transform.getTransform().getSpec().getPayload();
    final SerializableFunction<String, TestStream<T>> testStreamDecoder =
        createTestStreamDecoder(pipeline.getComponents(), bytes);

    final String outputId = ctx.getOutputId(transform);
    final String escapedOutputId = SamzaPipelineTranslatorUtils.escape(outputId);

    ctx.registerInputMessageStream(
        outputId,
        createInputDescriptor(
            escapedOutputId, Base64Serializer.serializeUnchecked(bytes), testStreamDecoder));
  }

  @SuppressWarnings("unchecked")
  private static <T> GenericInputDescriptor<KV<?, OpMessage<T>>> createInputDescriptor(
      String id,
      String encodedTestStream,
      SerializableFunction<String, TestStream<T>> testStreamDecoder) {
    final Map<String, String> systemConfig =
        ImmutableMap.of(
            ENCODED_TEST_STREAM,
            encodedTestStream,
            TEST_STREAM_DECODER,
            Base64Serializer.serializeUnchecked(testStreamDecoder));
    final GenericSystemDescriptor systemDescriptor =
        new GenericSystemDescriptor(id, SamzaTestStreamSystemFactory.class.getName())
            .withSystemConfigs(systemConfig);

    // The KvCoder is needed here for Samza not to crop the key.
    final Serde<KV<?, OpMessage<T>>> kvSerde = KVSerde.of(new NoOpSerde(), new NoOpSerde<>());
    return systemDescriptor.getInputDescriptor(id, kvSerde);
  }

  @SuppressWarnings("unchecked")
  private static <T> SerializableFunction<String, TestStream<T>> createTestStreamDecoder(
      RunnerApi.Components components, ByteString payload) {
    Coder<T> coder;
    try {
      coder =
          (Coder<T>)
              RehydratedComponents.forComponents(components)
                  .getCoder(RunnerApi.TestStreamPayload.parseFrom(payload).getCoderId());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // the decoder for encodedTestStream
    return encodedTestStream -> {
      try {
        return TestStreamTranslation.testStreamFromProtoPayload(
            RunnerApi.TestStreamPayload.parseFrom(
                Base64Serializer.deserializeUnchecked(encodedTestStream, ByteString.class)),
            coder);
      } catch (IOException e) {
        throw new RuntimeException("Could not decode TestStream.", e);
      }
    };
  }
}
