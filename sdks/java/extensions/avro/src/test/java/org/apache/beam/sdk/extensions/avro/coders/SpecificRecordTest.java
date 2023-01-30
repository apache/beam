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
package org.apache.beam.sdk.extensions.avro.coders;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.extensions.avro.schemas.TestAvro;
import org.apache.beam.sdk.extensions.avro.schemas.TestAvroNested;
import org.apache.beam.sdk.extensions.avro.schemas.TestEnum;
import org.apache.beam.sdk.extensions.avro.schemas.fixed4;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpecificRecordTest {
  private static final TestAvroNested AVRO_NESTED_SPECIFIC_RECORD = new TestAvroNested(true, 42);

  private static final TestAvro AVRO_SPECIFIC_RECORD =
      new TestAvro(
          true,
          43,
          44L,
          44.1f,
          44.2d,
          "mystring",
          ByteBuffer.wrap(new byte[] {1, 2, 3, 4}),
          new fixed4(new byte[] {1, 2, 3, 4}),
          new LocalDate(1979, 3, 14),
          new DateTime().withDate(1979, 3, 14).withTime(1, 2, 3, 4),
          TestEnum.abc,
          AVRO_NESTED_SPECIFIC_RECORD,
          ImmutableList.of(AVRO_NESTED_SPECIFIC_RECORD, AVRO_NESTED_SPECIFIC_RECORD),
          ImmutableMap.of("k1", AVRO_NESTED_SPECIFIC_RECORD, "k2", AVRO_NESTED_SPECIFIC_RECORD));

  @Test
  public void testSpecificRecordEncoding() throws Exception {
    AvroCoder<TestAvro> coder =
        AvroCoder.of(TestAvro.class, AVRO_SPECIFIC_RECORD.getSchema(), false);

    assertTrue(SpecificRecord.class.isAssignableFrom(coder.getType()));
    CoderProperties.coderDecodeEncodeEqual(coder, AVRO_SPECIFIC_RECORD);
  }

  @Test
  public void testReflectRecordEncoding() throws Exception {
    AvroCoder<TestAvro> coder = AvroCoder.of(TestAvro.class, true);
    AvroCoder<TestAvro> coderWithSchema =
        AvroCoder.of(TestAvro.class, AVRO_SPECIFIC_RECORD.getSchema(), true);

    assertTrue(SpecificRecord.class.isAssignableFrom(coder.getType()));
    assertTrue(SpecificRecord.class.isAssignableFrom(coderWithSchema.getType()));

    CoderProperties.coderDecodeEncodeEqual(coder, AVRO_SPECIFIC_RECORD);
    CoderProperties.coderDecodeEncodeEqual(coderWithSchema, AVRO_SPECIFIC_RECORD);
  }
}
