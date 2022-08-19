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
package org.apache.beam.sdk.io.kafka;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;

@AutoService(SchemaTransformProvider.class)
public class KafkaWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<KafkaWriteSchemaTransformConfiguration> {

  static final String INPUT_TAG = "INPUT";

  @Override
  protected Class<KafkaWriteSchemaTransformConfiguration> configurationClass() {
    return KafkaWriteSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(KafkaWriteSchemaTransformConfiguration configuration) {
    return new KafkaWriteSchemaTransform(configuration);
  }

  @Override
  public Schema configurationSchema() {
    return Schema.builder()
        .addStringField("bootstrapServers")
        .addStringField("topic")
        .addStringField("keySerializer")
        .addStringField("valueSerializer")
        .build();
  }

  @Override
  public String identifier() {
    return "kafkaio:write";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }
}
