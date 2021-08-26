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
package org.apache.beam.examples.complete.kafkatopubsub.avro;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

/** Example of custom AVRO Deserialize. */
public class AvroDataClassKafkaAvroDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<AvroDataClass> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(new KafkaAvroDeserializerConfig(configs));
  }

  @Override
  public AvroDataClass deserialize(String s, byte[] bytes) {
    return (AvroDataClass) this.deserialize(bytes);
  }

  @Override
  public void close() {}
}
