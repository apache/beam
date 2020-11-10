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
package org.apache.beam.sdk.extensions.schemaio.expansion;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.schemaio.expansion.ExternalSchemaIOTransformRegistrar.Configuration;
import org.apache.beam.sdk.extensions.schemaio.expansion.ExternalSchemaIOTransformRegistrar.ReaderBuilder;
import org.apache.beam.sdk.extensions.schemaio.expansion.ExternalSchemaIOTransformRegistrar.WriterBuilder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Unit tests for {@link ExternalSchemaIOTransformRegistrar}. */
@RunWith(JUnit4.class)
public class ExternalSchemaIOTransformRegistrarTest {
  String location = "test";
  Schema validDataSchema = Schema.builder().addStringField("dataField").build();
  Schema validConfigSchema = Schema.builder().addStringField("configField").build();
  Row validConfigRow = Row.withSchema(validConfigSchema).addValue("value").build();

  byte[] validSchemaBytes = SchemaTranslation.schemaToProto(validDataSchema, true).toByteArray();
  byte[] invalidBytes = "Nice try".getBytes(Charset.defaultCharset());

  SchemaIO schemaIO = Mockito.mock(SchemaIO.class);
  SchemaIOProvider schemaIOProvider = Mockito.mock(SchemaIOProvider.class);

  @Before
  public void setUp() {
    Mockito.doReturn(validConfigSchema).when(schemaIOProvider).configurationSchema();
    Mockito.doReturn(schemaIO)
        .when(schemaIOProvider)
        .from(location, validConfigRow, validDataSchema);
    Mockito.doReturn(schemaIO).when(schemaIOProvider).from(location, validConfigRow, null);
  }

  @Test
  public void testBuildWriter() {
    byte[] validConfigRowBytes = toByteArray(validConfigSchema, validConfigRow);
    WriterBuilder writerBuilder = new WriterBuilder(schemaIOProvider);

    writerBuilder.buildExternal(generateConfig(location, validConfigRowBytes, validSchemaBytes));

    verify(schemaIOProvider).from(location, validConfigRow, validDataSchema);
    when(schemaIOProvider.from(location, validConfigRow, validDataSchema)).thenReturn(schemaIO);
    verify(schemaIO).buildWriter();
  }

  @Test
  public void testBuildWriterNullDataSchema() {
    byte[] validConfigRowBytes = toByteArray(validConfigSchema, validConfigRow);
    WriterBuilder writerBuilder = new WriterBuilder(schemaIOProvider);

    writerBuilder.buildExternal(generateConfig(location, validConfigRowBytes, null));

    verify(schemaIOProvider).from(location, validConfigRow, null);
    when(schemaIOProvider.from(location, validConfigRow, null)).thenReturn(schemaIO);
    verify(schemaIO).buildWriter();
  }

  @Test
  public void testBuildWriterBadRowThrowsException() {
    byte[] validConfigRowBytes = toByteArray(validConfigSchema, validConfigRow);
    WriterBuilder writerBuilder = new WriterBuilder(schemaIOProvider);

    assertThrows(
        "Unable to infer data schema from configuration proto.",
        RuntimeException.class,
        () ->
            writerBuilder.buildExternal(
                generateConfig(location, validConfigRowBytes, invalidBytes)));
  }

  @Test
  public void testBuildWriterBadConfigRowThrowsException() {
    WriterBuilder writerBuilder = new WriterBuilder(schemaIOProvider);

    assertThrows(
        "Unable to infer configuration row from configuration proto and schema.",
        RuntimeException.class,
        () ->
            writerBuilder.buildExternal(generateConfig(location, invalidBytes, validSchemaBytes)));
  }

  @Test
  public void testBuildReader() {
    byte[] validConfigRowBytes = toByteArray(validConfigSchema, validConfigRow);
    ReaderBuilder readerBuilder = new ReaderBuilder(schemaIOProvider);

    readerBuilder.buildExternal(generateConfig(location, validConfigRowBytes, validSchemaBytes));

    verify(schemaIOProvider).from(location, validConfigRow, validDataSchema);
    when(schemaIOProvider.from(location, validConfigRow, validDataSchema)).thenReturn(schemaIO);
    verify(schemaIO).buildReader();
  }

  @Test
  public void testBuildReaderNullDataSchema() {
    byte[] validConfigRowBytes = toByteArray(validConfigSchema, validConfigRow);
    ReaderBuilder readerBuilder = new ReaderBuilder(schemaIOProvider);

    readerBuilder.buildExternal(generateConfig(location, validConfigRowBytes, null));

    verify(schemaIOProvider).from(location, validConfigRow, null);
    when(schemaIOProvider.from(location, validConfigRow, null)).thenReturn(schemaIO);
    verify(schemaIO).buildReader();
  }

  @Test
  public void testBuildReaderBadRowThrowsException() {
    byte[] validConfigRowBytes = toByteArray(validConfigSchema, validConfigRow);
    ReaderBuilder readerBuilder = new ReaderBuilder(schemaIOProvider);

    assertThrows(
        "Unable to infer data schema from configuration proto.",
        RuntimeException.class,
        () ->
            readerBuilder.buildExternal(
                generateConfig(location, validConfigRowBytes, invalidBytes)));
  }

  @Test
  public void testBuildReaderBadConfigRowThrowsException() {
    ReaderBuilder readerBuilder = new ReaderBuilder(schemaIOProvider);

    assertThrows(
        "Unable to infer configuration row from configuration proto and schema.",
        RuntimeException.class,
        () ->
            readerBuilder.buildExternal(generateConfig(location, invalidBytes, validSchemaBytes)));
  }

  private Configuration generateConfig(
      String location, byte[] rowBytes, @Nullable byte[] schemaBytes) {
    Configuration config = new Configuration();
    config.setLocation(location);
    config.setConfig(rowBytes);
    config.setDataSchema(schemaBytes);
    return config;
  }

  private byte[] toByteArray(Schema configSchema, Row configRow) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      RowCoder.of(configSchema).encode(configRow, outputStream);
      return outputStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Could not create test row bytes.");
    }
  }
}
