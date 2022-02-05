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

import static org.junit.Assert.*;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.beam.sdk.extensions.sbe.SbeSchema.IrOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.MessageSchema;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

@RunWith(JUnit4.class)
public final class SbeSchemaTest {
  private static final String PERSON_SCHEMA_PATH = Resources.getResource("person.xml").getPath();

  @Test
  public void testIrOptionsBuildWithAmbiguousIdentifier() {
    IrOptions.Builder builder = IrOptions.builder().setMessageId(1).setMessageName("msg");
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void testFromIrWithInvalidMessageId() {
    IrOptions options = IrOptions.builder().setMessageId(Integer.MAX_VALUE).build();
    assertThrows(IllegalArgumentException.class, () -> SbeSchema.fromIr(getIr(), options));
  }

  @Test
  public void testFromIrWithInvalidMessageName() {
    IrOptions options = IrOptions.builder().setMessageName(UUID.randomUUID().toString()).build();
    assertThrows(IllegalArgumentException.class, () -> SbeSchema.fromIr(getIr(), options));
  }

  private static Ir getIr() throws Exception {
    Path schemaPath = Paths.get(PERSON_SCHEMA_PATH);
    try (InputStream in = Files.newInputStream(schemaPath)) {
      MessageSchema schema = XmlSchemaParser.parse(in, ParserOptions.DEFAULT);
      return new IrGenerator().generate(schema);
    }
  }
}
