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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;

import java.util.UUID;
import org.apache.beam.sdk.extensions.sbe.SbeSchema.IrOptions;
import org.apache.beam.sdk.extensions.sbe.TestSchemas.OnlyPrimitives;
import org.apache.beam.sdk.extensions.sbe.TestSchemas.Person;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import uk.co.real_logic.sbe.ir.Ir;

@RunWith(JUnit4.class)
public final class SbeSchemaTest {
  @Test
  public void testFromIr() throws Exception {
    Ir ir = getIr(OnlyPrimitives.RESOURCE_PATH);

    SbeSchema actual = SbeSchema.fromIr(ir, IrOptions.DEFAULT);

    assertNotNull(actual.getIr());
    assertNotSame(ir, actual.getIr());
    assertEquals(IrOptions.DEFAULT, actual.getIrOptions());
    assertEquals(OnlyPrimitives.FIELDS, actual.getSbeFields());
  }

  @Test
  public void testIrOptionsBuildWithAmbiguousIdentifier() {
    IrOptions.Builder builder = IrOptions.builder().setMessageId(1L).setMessageName("msg");
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void testFromIrWithInvalidMessageId() {
    IrOptions options = IrOptions.builder().setMessageId(Long.MAX_VALUE).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> SbeSchema.fromIr(getIr(Person.RESOURCE_PATH), options));
  }

  @Test
  public void testFromIrWithInvalidMessageName() {
    IrOptions options = IrOptions.builder().setMessageName(UUID.randomUUID().toString()).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> SbeSchema.fromIr(getIr(Person.RESOURCE_PATH), options));
  }
}
