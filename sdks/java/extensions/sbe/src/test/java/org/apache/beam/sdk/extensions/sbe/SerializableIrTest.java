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

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Resources;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrEncoder;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.MessageSchema;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

@RunWith(JUnit4.class)
public final class SerializableIrTest {
  private static final String PERSON_SCHEMA_PATH = Resources.getResource("person.xml").getPath();

  @Test
  public void testSerializeAndDeserialize() throws Exception {
    // Arrange
    Ir original = getIr();
    SerializableIr asSerializable = SerializableIr.fromIr(original);

    // Act
    byte[] asBytes;
    try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
      try (ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
        objectStream.writeObject(asSerializable);
        asBytes = byteStream.toByteArray();
      }
    }

    SerializableIr afterSerialization;
    try (ByteArrayInputStream byteStream = new ByteArrayInputStream(asBytes)) {
      try (ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
        afterSerialization = (SerializableIr) objectStream.readObject();
      }
    }

    // Assert
    byte[] actual = encodeIr(afterSerialization.ir());
    byte[] expected = encodeIr(original);
    assertArrayEquals(expected, actual);
  }

  private static Ir getIr() throws Exception {
    Path schemaPath = Paths.get(PERSON_SCHEMA_PATH);
    try (InputStream in = Files.newInputStream(schemaPath)) {
      MessageSchema schema = XmlSchemaParser.parse(in, ParserOptions.DEFAULT);
      return new IrGenerator().generate(schema);
    }
  }

  private static byte[] encodeIr(Ir ir) {
    byte[] arr = new byte[16 * 1024];
    ByteBuffer buffer = ByteBuffer.wrap(arr);
    int actualBytes;
    try (IrEncoder encoder = new IrEncoder(buffer, ir)) {
      actualBytes = encoder.encode();
    }

    return Arrays.copyOf(arr, actualBytes);
  }
}
