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
package org.apache.beam.sdk.extensions.sbe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.Uint8;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SbeSchemaTest {

  private static final Field STRING_FIELD = Field.of("stringField", FieldType.STRING);
  private static final Field UINT_8_FIELD =
      Field.of("uint8Field", FieldType.logicalType(new Uint8()));

  @Test
  public void testNoSchemaGenerationValid() {
    SbeSchema sbeSchema =
        SbeSchema.builder().addField(STRING_FIELD).insertField(1, UINT_8_FIELD).build();

    Schema expectedSchema = Schema.builder().addFields(STRING_FIELD, UINT_8_FIELD).build();

    assertEquals(expectedSchema, sbeSchema.schema());
  }

  @Test
  public void testNoSchemaGenerationEmpty() {
    assertEquals(Schema.builder().build(), SbeSchema.builder().build().schema());
  }

  @Test
  public void testNoSchemaGenerationInvalid() {
    SbeSchema.Builder builder = SbeSchema.builder().insertField(1, STRING_FIELD);
    assertThrows(IllegalStateException.class, builder::build);
  }
}
