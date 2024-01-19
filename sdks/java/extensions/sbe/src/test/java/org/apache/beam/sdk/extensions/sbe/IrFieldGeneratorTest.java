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

import static org.apache.beam.sdk.extensions.sbe.TestSchemas.getIr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.UUID;
import org.apache.beam.sdk.extensions.sbe.SbeSchema.IrOptions;
import org.apache.beam.sdk.extensions.sbe.TestSchemas.OnlyPrimitives;
import org.apache.beam.sdk.extensions.sbe.TestSchemas.OnlyPrimitivesMultiMessage;
import org.apache.beam.sdk.extensions.sbe.TestSchemas.OnlyPrimitivesMultiMessage.Primitives1;
import org.apache.beam.sdk.extensions.sbe.TestSchemas.OnlyPrimitivesMultiMessage.Primitives2;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import uk.co.real_logic.sbe.ir.Ir;

@RunWith(JUnit4.class)
public final class IrFieldGeneratorTest {
  @Test
  public void testGenerateFieldsAllPrimitive() throws Exception {
    Ir ir = getIr(OnlyPrimitives.RESOURCE_PATH);

    ImmutableList<SbeField> actual = IrFieldGenerator.generateFields(ir, IrOptions.DEFAULT);

    assertEquals(OnlyPrimitives.FIELDS, actual);
  }

  @Test
  public void testGenerateFieldsWithMessageName() throws Exception {
    Ir ir = getIr(OnlyPrimitivesMultiMessage.RESOURCE_PATH);
    IrOptions msg1Opts = IrOptions.builder().setMessageName(Primitives1.NAME).build();
    IrOptions msg2Opts = IrOptions.builder().setMessageName(Primitives2.NAME).build();

    ImmutableList<SbeField> actual1 = IrFieldGenerator.generateFields(ir, msg1Opts);
    ImmutableList<SbeField> actual2 = IrFieldGenerator.generateFields(ir, msg2Opts);

    assertEquals(Primitives1.FIELDS, actual1);
    assertEquals(Primitives2.FIELDS, actual2);
  }

  @Test
  public void testGenerateFieldsWithInvalidMessageName() throws Exception {
    Ir ir = getIr(OnlyPrimitives.RESOURCE_PATH);
    IrOptions options = IrOptions.builder().setMessageName(UUID.randomUUID().toString()).build();

    assertThrows(
        IllegalArgumentException.class, () -> IrFieldGenerator.generateFields(ir, options));
  }

  @Test
  public void testGenerateFieldsWithMessageId() throws Exception {
    Ir ir = getIr(OnlyPrimitivesMultiMessage.RESOURCE_PATH);
    IrOptions msg1Opts = IrOptions.builder().setMessageId(Primitives1.ID).build();
    IrOptions msg2Opts = IrOptions.builder().setMessageId(Primitives2.ID).build();

    ImmutableList<SbeField> actual1 = IrFieldGenerator.generateFields(ir, msg1Opts);
    ImmutableList<SbeField> actual2 = IrFieldGenerator.generateFields(ir, msg2Opts);

    assertEquals(Primitives1.FIELDS, actual1);
    assertEquals(Primitives2.FIELDS, actual2);
  }

  @Test
  public void testGenerateFieldsWithInvalidMessageId() throws Exception {
    Ir ir = getIr(OnlyPrimitives.RESOURCE_PATH);
    IrOptions options = IrOptions.builder().setMessageId(100L).build();

    assertThrows(
        IllegalArgumentException.class, () -> IrFieldGenerator.generateFields(ir, options));
  }

  @Test
  public void testGenerateFieldsNoSpecifiedMessage() throws Exception {
    Ir ir = getIr(OnlyPrimitivesMultiMessage.RESOURCE_PATH);

    assertThrows(
        IllegalArgumentException.class,
        () -> IrFieldGenerator.generateFields(ir, IrOptions.DEFAULT));
  }
}
