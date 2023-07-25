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

import org.apache.beam.sdk.extensions.sbe.SbeField.SbeFieldOptions;
import org.apache.beam.sdk.extensions.sbe.UnsignedOptions.Behavior;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import uk.co.real_logic.sbe.PrimitiveType;

@RunWith(JUnit4.class)
public final class PrimitiveSbeFieldTest {
  private static final String NAME = "test-name";

  @Test
  public void testAsBeamFieldRequired() {
    SbeField field =
        PrimitiveSbeField.builder()
            .setName(NAME)
            .setIsRequired(true)
            .setType(PrimitiveType.INT32)
            .build();

    Field actual =
        field.asBeamField(
            SbeFieldOptions.builder()
                .setUnsignedOptions(UnsignedOptions.usingSameBitSize())
                .build());

    Field expected = Field.of(NAME, FieldType.INT32);
    assertEquals(expected, actual);
  }

  @Test
  public void testAsBeamFieldOptional() {
    SbeField field =
        PrimitiveSbeField.builder()
            .setName(NAME)
            .setIsRequired(false)
            .setType(PrimitiveType.INT32)
            .build();

    Field actual =
        field.asBeamField(
            SbeFieldOptions.builder()
                .setUnsignedOptions(UnsignedOptions.usingSameBitSize())
                .build());

    Field expected = Field.nullable(NAME, FieldType.INT32);
    assertEquals(expected, actual);
  }

  @Test
  public void testAsBeamFieldSameBitCount() {
    SbeField field =
        PrimitiveSbeField.builder()
            .setName(NAME)
            .setIsRequired(true)
            .setType(PrimitiveType.UINT32)
            .build();

    Field actual =
        field.asBeamField(
            SbeFieldOptions.builder()
                .setUnsignedOptions(UnsignedOptions.usingSameBitSize())
                .build());

    Field expected = Field.of(NAME, FieldType.INT32);
    assertEquals(expected, actual);
  }

  @Test
  public void testAsBeamFieldHigherBitCount() {
    SbeField field =
        PrimitiveSbeField.builder()
            .setName(NAME)
            .setIsRequired(true)
            .setType(PrimitiveType.UINT32)
            .build();

    Field actual =
        field.asBeamField(
            SbeFieldOptions.builder()
                .setUnsignedOptions(UnsignedOptions.usingHigherBitSize(Behavior.CONVERT_TO_STRING))
                .build());

    Field expected = Field.of(NAME, FieldType.INT64);
    assertEquals(expected, actual);
  }
}
