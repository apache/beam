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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class BeamKafkaTableCSVTest extends BeamKafkaTableTest {
  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addInt64Field("f_long")
          .addInt32Field("f_int")
          .addInt16Field("f_short")
          .addByteField("f_byte")
          .addDoubleField("f_double")
          .addStringField("f_string")
          .build();

  @Override
  protected byte[] generateEncodedPayload(int i) {
    return createCsv(i).getBytes(UTF_8);
  }

  @Override
  protected Row generateRow(int i) {
    List<Object> values =
        ImmutableList.of((long) i, i, (short) i, (byte) i, (double) i, "csv_value" + i);
    return Row.withSchema(TEST_SCHEMA).attachValues(values);
  }

  @Override
  protected BeamKafkaTable getBeamKafkaTable() {
    return new BeamKafkaCSVTable(TEST_SCHEMA, "", ImmutableList.of("mytopic"));
  }

  private String createCsv(int i) {
    return String.format("%s,%s,%s,%s,%s,\"%s\"", i, i, i, i, (double) i, "csv_value" + i);
  }
}
