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

import static org.apache.beam.sdk.extensions.sbe.SbeFieldUtils.signedSbePrimitiveToBeamPrimitive;
import static org.apache.beam.sdk.extensions.sbe.SbeFieldUtils.unsignedSbePrimitiveToBeamPrimitive;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.extensions.sbe.SbeSchema.UnsignedOptions;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import uk.co.real_logic.sbe.PrimitiveType;

@RunWith(JUnit4.class)
public final class SbeFieldUtilsTest {
  @Test
  public void testSignedConversion() {
    assertEquals(FieldType.BYTE, signedSbePrimitiveToBeamPrimitive(PrimitiveType.INT8));
    assertEquals(FieldType.INT16, signedSbePrimitiveToBeamPrimitive(PrimitiveType.INT16));
    assertEquals(FieldType.INT32, signedSbePrimitiveToBeamPrimitive(PrimitiveType.INT32));
    assertEquals(FieldType.INT64, signedSbePrimitiveToBeamPrimitive(PrimitiveType.INT64));
    assertEquals(FieldType.FLOAT, signedSbePrimitiveToBeamPrimitive(PrimitiveType.FLOAT));
    assertEquals(FieldType.DOUBLE, signedSbePrimitiveToBeamPrimitive(PrimitiveType.DOUBLE));

    assertThrows(
        IllegalArgumentException.class,
        () -> signedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT8));
    assertThrows(
        IllegalArgumentException.class,
        () -> signedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT16));
    assertThrows(
        IllegalArgumentException.class,
        () -> signedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT32));
    assertThrows(
        IllegalArgumentException.class,
        () -> signedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT64));
  }

  @Test
  public void testUnsignedConversion() {
    UnsignedOptions sameForAll = UnsignedOptions.usingSameBitSize();
    UnsignedOptions higherForAll = UnsignedOptions.usingHigherBitSize(true);

    assertEquals(
        FieldType.BYTE, unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT8, sameForAll));
    assertEquals(
        FieldType.INT16, unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT8, higherForAll));
    assertEquals(
        FieldType.INT16, unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT16, sameForAll));
    assertEquals(
        FieldType.INT32, unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT16, higherForAll));
    assertEquals(
        FieldType.INT32, unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT32, sameForAll));
    assertEquals(
        FieldType.INT64, unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT32, higherForAll));
    assertEquals(
        FieldType.INT64, unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT64, sameForAll));
    assertEquals(
        FieldType.STRING, unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.UINT64, higherForAll));

    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.INT8, sameForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.INT16, sameForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.INT32, sameForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.INT64, sameForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.FLOAT, sameForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.DOUBLE, sameForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.CHAR, higherForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.INT8, higherForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.INT16, higherForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.INT32, higherForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.INT64, higherForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.FLOAT, higherForAll));
    assertThrows(
        IllegalArgumentException.class,
        () -> unsignedSbePrimitiveToBeamPrimitive(PrimitiveType.DOUBLE, higherForAll));
  }
}
