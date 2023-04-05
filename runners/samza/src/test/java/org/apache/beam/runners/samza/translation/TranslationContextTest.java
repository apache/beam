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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.application.descriptors.StreamApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.junit.Test;

@SuppressWarnings({"rawtypes"})
public class TranslationContextTest {
  private final GenericInputDescriptor testInputDescriptor =
      new GenericSystemDescriptor("mockSystem", "mockFactoryClassName")
          .getInputDescriptor("test-input-1", mock(Serde.class));
  MapFunction<Object, String> keyFn = m -> m.toString();
  MapFunction<Object, Object> valueFn = m -> m;
  private final String streamName = "testStream";
  KVSerde<Object, Object> serde = KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
  StreamApplicationDescriptor streamApplicationDescriptor =
      new StreamApplicationDescriptorImpl(
          appDesc -> {
            MessageStream inputStream = appDesc.getInputStream(testInputDescriptor);
            inputStream.partitionBy(keyFn, valueFn, serde, streamName);
          },
          getConfig());
  Map<PValue, String> idMap = new HashMap<>();
  Set<String> nonUniqueStateIds = new HashSet<>();
  TranslationContext translationContext =
      new TranslationContext(
          streamApplicationDescriptor, idMap, nonUniqueStateIds, mock(SamzaPipelineOptions.class));

  @Test
  public void testRegisterInputMessageStreams() {
    final PCollection output = mock(PCollection.class);
    List<String> topics = Arrays.asList("stream1", "stream2");
    List inputDescriptors =
        topics.stream()
            .map(topicName -> createSamzaInputDescriptor(topicName, topicName))
            .collect(Collectors.toList());

    translationContext.registerInputMessageStreams(output, inputDescriptors);

    assertNotNull(translationContext.getMessageStream(output));
  }

  public GenericInputDescriptor<KV<String, OpMessage<?>>> createSamzaInputDescriptor(
      String systemName, String streamId) {
    final Serde<KV<String, OpMessage<?>>> kvSerde =
        KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>());
    return new GenericSystemDescriptor(systemName, "factoryClass")
        .getInputDescriptor(streamId, kvSerde);
  }

  private static Config getConfig() {
    HashMap<String, String> configMap = new HashMap<>();
    configMap.put("job.name", "testJobName");
    configMap.put("job.id", "testJobId");
    return new MapConfig(configMap);
  }
}
