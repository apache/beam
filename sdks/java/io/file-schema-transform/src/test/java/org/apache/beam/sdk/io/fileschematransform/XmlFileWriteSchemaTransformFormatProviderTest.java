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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.XML;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link XmlWriteSchemaTransformFormatProvider}. */
@RunWith(JUnit4.class)
public class XmlFileWriteSchemaTransformFormatProviderTest
    extends FileWriteSchemaTransformFormatProviderTest {

  private static final String ROOT_ELEMENT = "rootElement";
  private static final String RECORD_ELEMENT = "row";

  @Override
  protected String getFormat() {
    return XML;
  }

  @Override
  protected String getFilenamePrefix() {
    return "";
  }

  @Override
  protected void assertFolderContainsInAnyOrder(String folder, List<Row> rows, Schema beamSchema) {
    List<XmlRowAdapter> expected =
        rows.stream()
            .map(
                (Row row) -> {
                  XmlRowAdapter result = new XmlRowAdapter();
                  result.wrapRow(row);
                  return result;
                })
            .collect(Collectors.toList());

    PCollection<XmlRowAdapter> actual =
        readPipeline.apply(
            XmlIO.<XmlRowAdapter>read()
                .from(folder + "/*")
                .withRecordClass(XmlRowAdapter.class)
                .withRootElement(ROOT_ELEMENT)
                .withRecordElement(RECORD_ELEMENT)
                .withCharset(Charset.defaultCharset()));

    PAssert.that(actual).containsInAnyOrder(expected);
  }

  @Override
  protected FileWriteSchemaTransformConfiguration buildConfiguration(String folder) {
    return FileWriteSchemaTransformConfiguration.builder()
        .setFormat(XML)
        .setXmlConfiguration(
            FileWriteSchemaTransformConfiguration.xmlConfigurationBuilder()
                .setRootElement(ROOT_ELEMENT)
                .build())
        .setFilenamePrefix(folder)
        .setNumShards(1)
        .build();
  }

  @Override
  protected Optional<String> expectedErrorWhenCompressionSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenParquetConfigurationSet() {
    return Optional.of(
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$ParquetConfiguration is not compatible with a xml format");
  }

  @Override
  protected Optional<String> expectedErrorWhenXmlConfigurationSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenNumShardsSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenShardNameTemplateSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenCsvConfigurationSet() {
    return Optional.of(
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$CsvConfiguration is not compatible with a xml format");
  }
}
