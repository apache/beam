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
package org.apache.beam.sdk.coders;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link StringUtf8Coder}. */
@RunWith(JUnit4.class)
public class AvroGenericCoderTest {
  private static final Schema SCHEMA =
      SchemaBuilder.record("test")
          .namespace("org.apache.beam")
          .fields()
          .name("name")
          .type()
          .stringType()
          .noDefault()
          .name("age")
          .type()
          .intType()
          .intDefault(0)
          .name("hometown")
          .type()
          .nullable()
          .stringType()
          .noDefault()
          .endRecord();

  private static final Coder<GenericRecord> TEST_CODER = AvroGenericCoder.of(SCHEMA);

  private static final List<GenericRecord> TEST_VALUES =
      Arrays.asList(
          new GenericRecordBuilder(SCHEMA)
              .set("name", "Jon Snow")
              .set("age", 23)
              .set("hometown", "キングズ・ランディング")
              .build(),
          new GenericRecordBuilder(SCHEMA)
              .set("name", "Daenerys targaryen")
              .set("age", 23)
              .set("hometown", null)
              .build(),
          new GenericRecordBuilder(SCHEMA)
              .set("name", "Sansa Stark")
              .set("hometown", "윈터펠")
              .build(),
          new GenericRecordBuilder(SCHEMA)
              .set("name", "Tyrion Lannister")
              .set("hometown", null)
              .build());

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (GenericRecord value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(
        TEST_CODER.getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(GenericRecord.class)));
  }
}
