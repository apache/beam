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
package org.apache.beam.sdk.io.gcp.bigquery;

import static java.util.Arrays.asList;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIOStorageReadTest.field;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryStorageReaderTest {

  private static final org.apache.arrow.vector.types.pojo.Schema ARROW_SCHEMA =
      new org.apache.arrow.vector.types.pojo.Schema(
          asList(
              field("name", new ArrowType.Utf8()), field("number", new ArrowType.Int(64, true))));
  private static final ReadSession ARROW_READ_SESSION =
      ReadSession.newBuilder()
          .setName("readSession")
          .setArrowSchema(
              ArrowSchema.newBuilder()
                  .setSerializedSchema(serializeArrowSchema(ARROW_SCHEMA))
                  .build())
          .build();
  private static final String AVRO_SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"RowRecord\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"number\", \"type\": \"long\"}\n"
          + " ]\n"
          + "}";
  private static final ReadSession AVRO_READ_SESSION =
      ReadSession.newBuilder()
          .setName("readSession")
          .setAvroSchema(AvroSchema.newBuilder().setSchema(AVRO_SCHEMA_STRING))
          .build();

  @Test
  public void bigQueryStorageReaderFactory_arrowReader() throws Exception {
    BigQueryStorageReader reader = BigQueryStorageReaderFactory.getReader(ARROW_READ_SESSION);
    assertThat(reader, instanceOf(BigQueryStorageArrowReader.class));
  }

  @Test
  public void bigQueryStorageReaderFactory_avroReader() throws Exception {
    BigQueryStorageReader reader = BigQueryStorageReaderFactory.getReader(AVRO_READ_SESSION);
    assertThat(reader, instanceOf(BigQueryStorageAvroReader.class));
  }

  private static ByteString serializeArrowSchema(
      org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    try {
      MessageSerializer.serialize(
          new WriteChannel(Channels.newChannel(byteOutputStream)), arrowSchema);
    } catch (IOException ex) {
      throw new RuntimeException("Failed to serialize arrow schema.", ex);
    }
    return ByteString.copyFrom(byteOutputStream.toByteArray());
  }
}
