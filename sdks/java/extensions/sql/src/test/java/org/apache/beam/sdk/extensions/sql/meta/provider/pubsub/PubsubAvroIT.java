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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.Matcher;
import org.joda.time.Instant;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for querying Pubsub AVRO messages with SQL. */
@RunWith(JUnit4.class)
public class PubsubAvroIT extends PubsubTableProviderIT {
  private static final Schema NAME_HEIGHT_KNOWS_JS_SCHEMA =
      Schema.builder()
          .addNullableField("name", Schema.FieldType.STRING)
          .addNullableField("height", Schema.FieldType.INT32)
          .addNullableField("knowsJavascript", Schema.FieldType.BOOLEAN)
          .build();

  private static final Schema NAME_HEIGHT_SCHEMA =
      Schema.builder()
          .addNullableField("name", Schema.FieldType.STRING)
          .addNullableField("height", Schema.FieldType.INT32)
          .build();

  @Override
  protected String getPayloadFormat() {
    return "avro";
  }

  @Override
  protected PubsubMessage messageIdName(Instant timestamp, int id, String name) throws IOException {
    byte[] encodedRecord = createEncodedGenericRecord(PAYLOAD_SCHEMA, ImmutableList.of(id, name));
    return message(timestamp, encodedRecord);
  }

  @Override
  protected Matcher<PubsubMessage> matcherNames(String name) throws IOException {
    Schema schema = Schema.builder().addStringField("name").build();
    byte[] encodedRecord = createEncodedGenericRecord(schema, ImmutableList.of(name));
    return hasProperty("payload", equalTo(encodedRecord));
  }

  @Override
  protected Matcher<PubsubMessage> matcherNameHeight(String name, int height) throws IOException {
    byte[] encodedRecord =
        createEncodedGenericRecord(NAME_HEIGHT_SCHEMA, ImmutableList.of(name, height));
    return hasProperty("payload", equalTo(encodedRecord));
  }

  @Override
  protected Matcher<PubsubMessage> matcherNameHeightKnowsJS(
      String name, int height, boolean knowsJS) throws IOException {
    byte[] encodedRecord =
        createEncodedGenericRecord(
            NAME_HEIGHT_KNOWS_JS_SCHEMA, ImmutableList.of(name, height, knowsJS));
    return hasProperty("payload", equalTo(encodedRecord));
  }

  private byte[] createEncodedGenericRecord(Schema beamSchema, List<Object> values)
      throws IOException {
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    List<org.apache.avro.Schema.Field> fields = avroSchema.getFields();
    for (int i = 0; i < fields.size(); ++i) {
      builder.set(fields.get(i), values.get(i));
    }
    AvroCoder<GenericRecord> coder = AvroCoder.of(avroSchema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    coder.encode(builder.build(), out);
    return out.toByteArray();
  }
}
