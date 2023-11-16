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
package org.apache.beam.sdk.schemas.io;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.AvroPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AvroPayloadSerializerProviderTest {
  private static final Schema SCHEMA =
      Schema.builder().addInt64Field("abc").addStringField("xyz").build();
  private static final org.apache.avro.Schema AVRO_SCHEMA = AvroUtils.toAvroSchema(SCHEMA);
  private static final AvroCoder<GenericRecord> AVRO_CODER = AvroCoder.of(AVRO_SCHEMA);
  private static final Row DESERIALIZED =
      Row.withSchema(SCHEMA).withFieldValue("abc", 3L).withFieldValue("xyz", "qqq").build();
  private static final GenericRecord SERIALIZED =
      new GenericRecordBuilder(AVRO_SCHEMA).set("abc", 3L).set("xyz", "qqq").build();

  private final AvroPayloadSerializerProvider provider = new AvroPayloadSerializerProvider();

  @Test
  public void serialize() throws Exception {
    byte[] bytes = provider.getSerializer(SCHEMA, ImmutableMap.of()).serialize(DESERIALIZED);
    GenericRecord record = AVRO_CODER.decode(new ByteArrayInputStream(bytes));
    assertEquals(3L, record.get("abc"));
    assertEquals("qqq", record.get("xyz").toString());
  }

  @Test
  public void deserialize() throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    AVRO_CODER.encode(SERIALIZED, os);
    Row row = provider.getSerializer(SCHEMA, ImmutableMap.of()).deserialize(os.toByteArray());
    assertEquals(DESERIALIZED, row);
  }
}
