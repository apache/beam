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

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Optional;

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.CSV;

/** Tests for {@link CsvWriteSchemaTransformFormatProvider}. */
@RunWith(JUnit4.class)
public class CsvFileWriteSchemaTransformFormatProviderTest
    extends FileWriteSchemaTransformFormatProviderTest {
  @Override
  protected String getFormat() {
    return CSV;
  }

  @Override
  protected String getFilenamePrefix() {
    return "/out";
  }

  @Override
  protected void assertFolderContainsInAnyOrder(String folder, List<Row> rows, Schema beamSchema) {

  }

  @Override
  protected FileWriteSchemaTransformConfiguration buildConfiguration(String folder) {
    return defaultConfiguration(folder);
  }

  @Override
  protected Optional<String> expectedErrorWhenCompressionSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenParquetConfigurationSet() {
    return Optional.of(
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$ParquetConfiguration is not compatible with a csv format");
  }

  @Override
  protected Optional<String> expectedErrorWhenXmlConfigurationSet() {
    return Optional.of(
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$XmlConfiguration is not compatible with a csv format");
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
    return Optional.empty();
  }
}
