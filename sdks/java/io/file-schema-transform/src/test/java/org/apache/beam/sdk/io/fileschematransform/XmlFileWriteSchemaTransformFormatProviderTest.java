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

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.DOUBLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.SINGLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestHelpers.prefix;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.XML;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.Xml.RowToXmlFn;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileWriteSchemaTransformFormatProviders.Xml}. */
@RunWith(JUnit4.class)
public class XmlFileWriteSchemaTransformFormatProviderTest {
  private static final FileWriteSchemaTransformFormatProvider PROVIDER =
      FileWriteSchemaTransformFormatProviders.loadProviders().get(XML);

  private static final FileWriteSchemaTransformFormatProviderTestData DATA =
      new FileWriteSchemaTransformFormatProviderTestData();

  private static final String recordElement = "row";

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void allPrimitiveDataTypes() {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    String rootElement = "allPrimitiveDataTypes";
    List<XmlRowAdapter> expectedWrapped =
        DATA.allPrimitiveDataTypesRows.stream()
            .map(
                (Row row) -> {
                  XmlRowAdapter result = new XmlRowAdapter();
                  result.wrapRow(row);
                  return result;
                })
            .collect(Collectors.toList());

    String to = String.format("%s_%s", XML, rootElement);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.allPrimitiveDataTypesRows).withRowSchema(schema));
    input.apply(PROVIDER.buildTransform(configuration(prefixTo, rootElement), schema));

    writePipeline.run().waitUntilFinish();

    PCollection<XmlRowAdapter> actual =
        readPipeline.apply(
            XmlIO.<XmlRowAdapter>read()
                .from(prefixTo + "*")
                .withRecordClass(XmlRowAdapter.class)
                .withRootElement(rootElement)
                .withRecordElement(recordElement)
                .withCharset(Charset.defaultCharset()));

    PAssert.that(actual).containsInAnyOrder(expectedWrapped);
    readPipeline.run();
  }

  @Test
  public void nullableAllPrimitiveDataTypes() {
    Schema schema = NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    String rootElement = "nullableAllPrimitiveDataTypes";
    List<XmlRowAdapter> expectedWrapped =
        DATA.nullableAllPrimitiveDataTypesRows.stream()
            .map(
                (Row row) -> {
                  XmlRowAdapter result = new XmlRowAdapter();
                  result.wrapRow(row);
                  return result;
                })
            .collect(Collectors.toList());

    String to = String.format("%s_%s", XML, rootElement);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.nullableAllPrimitiveDataTypesRows).withRowSchema(schema));
    input.apply(PROVIDER.buildTransform(configuration(prefixTo, rootElement), schema));

    writePipeline.run().waitUntilFinish();

    PCollection<XmlRowAdapter> actual =
        readPipeline.apply(
            XmlIO.<XmlRowAdapter>read()
                .from(prefixTo + "*")
                .withRecordClass(XmlRowAdapter.class)
                .withRootElement(rootElement)
                .withRecordElement(recordElement)
                .withCharset(Charset.defaultCharset()));

    PAssert.that(actual).containsInAnyOrder(expectedWrapped);
    readPipeline.run();
  }

  @Test
  public void timeContaining() {
    Schema schema = TIME_CONTAINING_SCHEMA;
    String rootElement = "timeContaining";
    List<XmlRowAdapter> expectedWrapped =
        DATA.timeContainingRows.stream()
            .map(
                (Row row) -> {
                  XmlRowAdapter result = new XmlRowAdapter();
                  result.wrapRow(row);
                  return result;
                })
            .collect(Collectors.toList());

    String to = String.format("%s_%s", XML, rootElement);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.timeContainingRows).withRowSchema(schema));
    input.apply(PROVIDER.buildTransform(configuration(prefixTo, rootElement), schema));

    writePipeline.run().waitUntilFinish();

    PCollection<XmlRowAdapter> actual =
        readPipeline.apply(
            XmlIO.<XmlRowAdapter>read()
                .from(prefixTo + "*")
                .withRecordClass(XmlRowAdapter.class)
                .withRootElement(rootElement)
                .withRecordElement(recordElement)
                .withCharset(Charset.defaultCharset()));

    PAssert.that(actual).containsInAnyOrder(expectedWrapped);
    readPipeline.run();
  }

  @Test
  public void arrayPrimitiveDataTypes() {
    Schema schema = ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
    String rootElement = "arrayPrimitiveDataTypes";
    List<XmlRowAdapter> expectedWrapped =
        DATA.arrayPrimitiveDataTypesRows.stream()
            .map(
                (Row row) -> {
                  XmlRowAdapter result = new XmlRowAdapter();
                  result.wrapRow(row);
                  return result;
                })
            .collect(Collectors.toList());

    String to = String.format("%s_%s", XML, rootElement);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.arrayPrimitiveDataTypesRows).withRowSchema(schema));
    input.apply(PROVIDER.buildTransform(configuration(prefixTo, rootElement), schema));

    writePipeline.run().waitUntilFinish();

    PCollection<XmlRowAdapter> actual =
        readPipeline.apply(
            XmlIO.<XmlRowAdapter>read()
                .from(prefixTo + "*")
                .withRecordClass(XmlRowAdapter.class)
                .withRootElement(rootElement)
                .withRecordElement(recordElement)
                .withCharset(Charset.defaultCharset()));

    PAssert.that(actual).containsInAnyOrder(expectedWrapped);
    readPipeline.run();
  }

  @Test
  public void singlyNestedDataTypesNoRepeat() {
    Schema schema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    String rootElement = "singlyNestedDataTypesNoRepeat";
    List<XmlRowAdapter> expectedWrapped =
        DATA.singlyNestedDataTypesNoRepeatRows.stream()
            .map(
                (Row row) -> {
                  XmlRowAdapter result = new XmlRowAdapter();
                  result.wrapRow(row);
                  return result;
                })
            .collect(Collectors.toList());

    String to = String.format("%s_%s", XML, rootElement);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.singlyNestedDataTypesNoRepeatRows).withRowSchema(schema));
    input.apply(PROVIDER.buildTransform(configuration(prefixTo, rootElement), schema));

    writePipeline.run().waitUntilFinish();

    PCollection<XmlRowAdapter> actual =
        readPipeline.apply(
            XmlIO.<XmlRowAdapter>read()
                .from(prefixTo + "*")
                .withRecordClass(XmlRowAdapter.class)
                .withRootElement(rootElement)
                .withRecordElement(recordElement)
                .withCharset(Charset.defaultCharset()));

    PAssert.that(actual).containsInAnyOrder(expectedWrapped);
    readPipeline.run();
  }

  @Test
  public void doublyNestedDataTypesNoRepeat() {
    Schema schema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    String rootElement = "doublyNestedDataTypesNoRepeat";
    List<XmlRowAdapter> expectedWrapped =
        DATA.doublyNestedDataTypesNoRepeatRows.stream()
            .map(
                (Row row) -> {
                  XmlRowAdapter result = new XmlRowAdapter();
                  result.wrapRow(row);
                  return result;
                })
            .collect(Collectors.toList());

    String to = String.format("%s_%s", XML, rootElement);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.doublyNestedDataTypesNoRepeatRows).withRowSchema(schema));
    input.apply(PROVIDER.buildTransform(configuration(prefixTo, rootElement), schema));

    writePipeline.run().waitUntilFinish();

    PCollection<XmlRowAdapter> actual =
        readPipeline.apply(
            XmlIO.<XmlRowAdapter>read()
                .from(prefixTo + "*")
                .withRecordClass(XmlRowAdapter.class)
                .withRootElement(rootElement)
                .withRecordElement(recordElement)
                .withCharset(Charset.defaultCharset()));

    PAssert.that(actual).containsInAnyOrder(expectedWrapped);
    readPipeline.run();
  }

  @Test
  public void doublyNestedDataTypesRepeat() {
    Schema schema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    String rootElement = "doublyNestedDataTypesRepeat";
    List<XmlRowAdapter> expectedWrapped =
        DATA.doublyNestedDataTypesRepeatRows.stream()
            .map(
                (Row row) -> {
                  XmlRowAdapter result = new XmlRowAdapter();
                  result.wrapRow(row);
                  return result;
                })
            .collect(Collectors.toList());

    String to = String.format("%s_%s", XML, rootElement);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.doublyNestedDataTypesRepeatRows).withRowSchema(schema));
    input.apply(PROVIDER.buildTransform(configuration(prefixTo, rootElement), schema));

    writePipeline.run().waitUntilFinish();

    PCollection<XmlRowAdapter> actual =
        readPipeline.apply(
            XmlIO.<XmlRowAdapter>read()
                .from(prefixTo + "*")
                .withRecordClass(XmlRowAdapter.class)
                .withRootElement(rootElement)
                .withRecordElement(recordElement)
                .withCharset(Charset.defaultCharset()));

    PAssert.that(actual).containsInAnyOrder(expectedWrapped);
    readPipeline.run();
  }

  @Test
  public void testRowToXmlFn() {
    List<XmlRowAdapter> expected =
        DATA.allPrimitiveDataTypesRows.stream()
            .map(
                (Row row) -> {
                  XmlRowAdapter result = new XmlRowAdapter();
                  result.wrapRow(row);
                  return result;
                })
            .collect(Collectors.toList());

    PCollection<XmlRowAdapter> actual =
        writePipeline
            .apply(Create.of(DATA.allPrimitiveDataTypesRows))
            .apply(MapElements.into(TypeDescriptor.of(XmlRowAdapter.class)).via(new RowToXmlFn()));

    PAssert.that(actual).containsInAnyOrder(expected);

    writePipeline.run();
  }

  private static <T> FileWriteSchemaTransformConfiguration configuration(
      String to, String rootElement) {
    return FileWriteSchemaTransformConfiguration.builder()
        .setFormat(XML)
        .setXmlConfiguration(
            FileWriteSchemaTransformConfiguration.xmlConfigurationBuilder()
                .setRootElement(rootElement)
                .build())
        .setFilenamePrefix(to)
        .build();
  }
}
