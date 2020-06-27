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

import java.util.Map;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;

/**
 * Translate {@link org.apache.beam.sdk.testing.TestStream} to a samza message stream produced by
 * {@link
 * org.apache.beam.runners.samza.translation.SamzaTestStreamSystemFactory.SmazaTestStreamSystemConsumer}.
 */
public class SamzaTestStreamTranslator<T> implements TransformTranslator<TestStream<T>> {

  @Override
  public void translate(
      TestStream<T> testStream, TransformHierarchy.Node node, TranslationContext ctx) {
    final PCollection<T> output = ctx.getOutput(testStream);
    final String outputId = ctx.getIdForPValue(output);
    final Coder<T> valueCoder = testStream.getValueCoder();
    final TestStream.TestStreamCoder<T> testStreamCoder = TestStream.TestStreamCoder.of(valueCoder);
    final GenericSystemDescriptor systemDescriptor =
        new GenericSystemDescriptor(outputId, SamzaTestStreamSystemFactory.class.getName());

    // encode testStream as a string
    final String encodedTestStream;
    try {
      encodedTestStream = CoderUtils.encodeToBase64(testStreamCoder, testStream);
    } catch (CoderException e) {
      throw new SamzaException("Could not encode TestStream.", e);
    }

    // the decoder for encodedTestStream
    SerializableFunction<String, TestStream<T>> testStreamDecoder =
        string -> {
          try {
            return CoderUtils.decodeFromBase64(TestStream.TestStreamCoder.of(valueCoder), string);
          } catch (CoderException e) {
            throw new SamzaException("Could not decode TestStream.", e);
          }
        };

    final Map<String, String> systemConfig =
        ImmutableMap.of(
            "encodedTestStream",
            encodedTestStream,
            "testStreamDecoder",
            Base64Serializer.serializeUnchecked(testStreamDecoder));
    systemDescriptor.withSystemConfigs(systemConfig);

    // The KvCoder is needed here for Samza not to crop the key.
    final Serde<KV<?, OpMessage<byte[]>>> kvSerde = KVSerde.of(new NoOpSerde(), new NoOpSerde<>());
    final GenericInputDescriptor<KV<?, OpMessage<byte[]>>> inputDescriptor =
        systemDescriptor.getInputDescriptor(outputId, kvSerde);

    ctx.registerInputMessageStream(output, inputDescriptor);
  }

  @Override
  public void translatePortable(
      PipelineNode.PTransformNode transform,
      QueryablePipeline pipeline,
      PortableTranslationContext ctx) {
    throw new SamzaException("TestStream is not supported in portable by Samza runner");
  }
}
