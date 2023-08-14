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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Resources;
import uk.co.real_logic.sbe.PrimitiveType;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.MessageSchema;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

/** Constants and utilities for working with the schemas under resources/. */
public final class TestSchemas {
  private TestSchemas() {}

  /** Returns the {@link Ir} for the schema located at {@code schemaFile}. */
  public static Ir getIr(String schemaFile) throws Exception {
    Path schemaPath = Paths.get(schemaFile);
    try (InputStream in = Files.newInputStream(schemaPath)) {
      MessageSchema schema = XmlSchemaParser.parse(in, ParserOptions.DEFAULT);
      return new IrGenerator().generate(schema);
    }
  }

  /** Info for person.xml. */
  public static final class Person {
    private Person() {}

    public static final String RESOURCE_PATH = Resources.getResource("person.xml").getPath();
  }

  /** Info for only-primitives.xml. */
  public static final class OnlyPrimitives {
    private OnlyPrimitives() {}

    public static final String RESOURCE_PATH =
        Resources.getResource("only-primitives.xml").getPath();

    public static final SbeField INT32_FIELD =
        PrimitiveSbeField.builder()
            .setName("int32Primitive")
            .setIsRequired(true)
            .setType(PrimitiveType.INT32)
            .build();
    public static final SbeField UINT32_FIELD =
        PrimitiveSbeField.builder()
            .setName("uint32Primitive")
            .setIsRequired(false)
            .setType(PrimitiveType.UINT32)
            .build();
    public static final SbeField DOUBLE_FIELD =
        PrimitiveSbeField.builder()
            .setName("doublePrimitive")
            .setIsRequired(true)
            .setType(PrimitiveType.DOUBLE)
            .build();
    public static final ImmutableList<SbeField> FIELDS =
        ImmutableList.of(INT32_FIELD, UINT32_FIELD, DOUBLE_FIELD);
  }

  /** Info for only-primitives-multi-message.xml. */
  public static final class OnlyPrimitivesMultiMessage {
    private OnlyPrimitivesMultiMessage() {}

    public static final String RESOURCE_PATH =
        Resources.getResource("only-primitives-multi-message.xml").getPath();

    /** Info for the Primitives1 message. */
    public static final class Primitives1 {
      private Primitives1() {}

      public static final long ID = 1;
      public static final String NAME = "Primitives1";

      public static final SbeField INT32_FIELD =
          PrimitiveSbeField.builder()
              .setName("int32Primitive")
              .setIsRequired(true)
              .setType(PrimitiveType.INT32)
              .build();
      public static final SbeField UINT32_FIELD =
          PrimitiveSbeField.builder()
              .setName("uint32Primitive")
              .setIsRequired(false)
              .setType(PrimitiveType.UINT32)
              .build();
      public static final SbeField DOUBLE_FIELD =
          PrimitiveSbeField.builder()
              .setName("doublePrimitive")
              .setIsRequired(true)
              .setType(PrimitiveType.DOUBLE)
              .build();
      public static final ImmutableList<SbeField> FIELDS =
          ImmutableList.of(INT32_FIELD, UINT32_FIELD, DOUBLE_FIELD);
    }

    /** Info for the Primitives2 message. */
    public static final class Primitives2 {
      private Primitives2() {}

      public static final long ID = 2;
      public static final String NAME = "Primitives2";

      public static final SbeField INT16_FIELD =
          PrimitiveSbeField.builder()
              .setName("int16Primitive")
              .setIsRequired(true)
              .setType(PrimitiveType.INT16)
              .build();
      public static final SbeField UINT16_FIELD =
          PrimitiveSbeField.builder()
              .setName("uint16Primitive")
              .setIsRequired(true)
              .setType(PrimitiveType.UINT16)
              .build();
      public static final SbeField FLOAT_FIELD =
          PrimitiveSbeField.builder()
              .setName("floatPrimitive")
              .setIsRequired(false)
              .setType(PrimitiveType.FLOAT)
              .build();
      public static final ImmutableList<SbeField> FIELDS =
          ImmutableList.of(INT16_FIELD, UINT16_FIELD, FLOAT_FIELD);
    }
  }
}
