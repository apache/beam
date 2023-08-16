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
package org.apache.beam.sdk.extensions.python;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.UsesPythonExpansionService;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PythonExternalTransformTest implements Serializable {

  @Test
  @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
  public void trivialPythonTransform() {
    Pipeline p = Pipeline.create();
    PCollection<String> output =
        p.apply(Create.of(KV.of("A", "x"), KV.of("A", "y"), KV.of("B", "z")))
            .apply(
                PythonExternalTransform
                    .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>
                        from("apache_beam.GroupByKey"))
            .apply(Keys.create());
    PAssert.that(output).containsInAnyOrder("A", "B");
    // TODO: Run this on a multi-language supporting runner.
  }

  @Test
  @Category({ValidatesRunner.class, UsesPythonExpansionService.class})
  public void pythonTransformWithDependencies() {
    Pipeline p = Pipeline.create();
    PCollection<String> output =
        p.apply(Create.of("elephant", "mouse", "sheep"))
            .apply(
                PythonExternalTransform.<PCollection<String>, PCollection<String>>from(
                        "apache_beam.Map")
                    .withArgs(PythonCallableSource.of("import inflection\ninflection.pluralize"))
                    .withExtraPackages(ImmutableList.of("inflection"))
                    .withOutputCoder(StringUtf8Coder.of()));
    PAssert.that(output).containsInAnyOrder("elephants", "mice", "sheep");
    // TODO: Run this on a multi-language supporting runner.
  }

  @Test
  public void generateArgsEmpty() {
    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform");

    Row receivedRow = transform.buildOrGetArgsRow();
    assertEquals(0, receivedRow.getFieldCount());
  }

  @Test
  public void generateArgsWithPrimitives() {
    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withArgs("aaa", "bbb", 11, 12L, 15.6, true);

    Schema expectedSchema =
        Schema.builder()
            .addStringField("field0")
            .addStringField("field1")
            .addInt32Field("field2")
            .addInt64Field("field3")
            .addDoubleField("field4")
            .addBooleanField("field5")
            .build();
    Row expectedRow =
        Row.withSchema(expectedSchema).addValues("aaa", "bbb", 11, 12L, 15.6, true).build();

    Row receivedRow = transform.buildOrGetArgsRow();
    assertEquals(expectedRow, receivedRow);
  }

  @Test
  public void generateArgsWithRow() {
    Schema subRowSchema1 =
        Schema.builder().addStringField("field0").addInt32Field("field1").build();
    Row rowField1 = Row.withSchema(subRowSchema1).addValues("xxx", 123).build();
    Schema subRowSchema2 =
        Schema.builder()
            .addDoubleField("field0")
            .addBooleanField("field1")
            .addStringField("field2")
            .build();
    Row rowField2 = Row.withSchema(subRowSchema2).addValues(12.5, true, "yyy").build();

    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withArgs(rowField1, rowField2);

    Schema expectedSchema =
        Schema.builder()
            .addRowField("field0", subRowSchema1)
            .addRowField("field1", subRowSchema2)
            .build();
    Row expectedRow = Row.withSchema(expectedSchema).addValues(rowField1, rowField2).build();

    Row receivedRow = transform.buildOrGetArgsRow();
    assertEquals(expectedRow, receivedRow);
  }

  @Test
  public void generatePayloadWithoutKwargs() throws Exception {
    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withArgs("aaa", "bbb", 11, 12L, 15.6, true);
    ExternalTransforms.ExternalConfigurationPayload payload = transform.generatePayload();

    Schema schema = SchemaTranslation.schemaFromProto(payload.getSchema());
    assertTrue(schema.hasField("args"));
    assertFalse(schema.hasField("kwargs"));
  }

  @Test
  public void generatePayloadWithoutArgs() {
    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withKwarg("stringField1", "aaa")
            .withKwarg("stringField2", "bbb")
            .withKwarg("intField", 11)
            .withKwarg("longField", 12L)
            .withKwarg("doubleField", 15.6)
            .withKwarg("boolField", true);
    ExternalTransforms.ExternalConfigurationPayload payload = transform.generatePayload();
    Schema schema = SchemaTranslation.schemaFromProto(payload.getSchema());
    assertFalse(schema.hasField("args"));
    assertTrue(schema.hasField("kwargs"));
  }

  static class CustomType {
    int intField;
    String strField;
  }

  @Test
  public void generateArgsWithCustomType() {
    CustomType customType1 = new CustomType();
    customType1.strField = "xxx";
    customType1.intField = 123;

    CustomType customType2 = new CustomType();
    customType2.strField = "yyy";
    customType2.intField = 456;

    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withArgs(customType1, customType2);

    Row receivedRow = transform.buildOrGetArgsRow();

    assertEquals("xxx", receivedRow.getRow("field0").getString("strField"));
    assertEquals(123, (int) receivedRow.getRow("field0").getInt32("intField"));

    assertEquals("yyy", receivedRow.getRow("field1").getString("strField"));
    assertEquals(456, (int) receivedRow.getRow("field1").getInt32("intField"));
  }

  @Test
  public void generateArgsWithPythonCallableSource() {
    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withArgs(PythonCallableSource.of("dummy data"));
    Row receivedRow = transform.buildOrGetArgsRow();
    assertTrue(receivedRow.getValue("field0") instanceof PythonCallableSource);
  }

  @Test
  public void generateArgsWithTypeHint() {
    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withArgs(Instant.ofEpochSecond(0))
            .withTypeHint(Instant.class, Schema.FieldType.logicalType(new MicrosInstant()));
    Row receivedRow = transform.buildOrGetArgsRow();
    assertTrue(receivedRow.getValue("field0") instanceof Instant);
  }

  @Test
  public void generateKwargsEmpty() {
    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform");

    Row receivedRow = transform.buildOrGetKwargsRow();
    assertEquals(0, receivedRow.getFieldCount());
  }

  @Test
  public void generateKwargsWithPrimitives() {
    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withKwarg("stringField1", "aaa")
            .withKwarg("stringField2", "bbb")
            .withKwarg("intField", 11)
            .withKwarg("longField", 12L)
            .withKwarg("doubleField", 15.6)
            .withKwarg("boolField", true);

    Row receivedRow = transform.buildOrGetKwargsRow();
    assertEquals("aaa", receivedRow.getString("stringField1"));
    assertEquals("bbb", receivedRow.getString("stringField2"));
    assertEquals(11, (int) receivedRow.getInt32("intField"));
    assertEquals(12L, (long) receivedRow.getInt64("longField"));
    assertEquals(15.6, (double) receivedRow.getDouble("doubleField"), 0);
    assertEquals(true, receivedRow.getBoolean("boolField"));
  }

  @Test
  public void generateKwargsRow() {
    Schema subRowSchema1 =
        Schema.builder().addStringField("field0").addInt32Field("field1").build();
    Row rowField1 = Row.withSchema(subRowSchema1).addValues("xxx", 123).build();
    Schema subRowSchema2 =
        Schema.builder()
            .addDoubleField("field0")
            .addBooleanField("field1")
            .addStringField("field2")
            .build();
    Row rowField2 = Row.withSchema(subRowSchema2).addValues(12.5, true, "yyy").build();

    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withKwarg("customField0", rowField1)
            .withKwarg("customField1", rowField2);

    Schema expectedSchema =
        Schema.builder()
            .addRowField("customField0", subRowSchema1)
            .addRowField("customField1", subRowSchema2)
            .build();
    Row expectedRow = Row.withSchema(expectedSchema).addValues(rowField1, rowField2).build();

    Row receivedRow = transform.buildOrGetKwargsRow();
    assertEquals(expectedRow, receivedRow);
  }

  @Test
  public void generateKwargsWithCustomType() {
    CustomType customType1 = new CustomType();
    customType1.strField = "xxx";
    customType1.intField = 123;

    CustomType customType2 = new CustomType();
    customType2.strField = "yyy";
    customType2.intField = 456;

    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withKwarg("customField0", customType1)
            .withKwarg("customField1", customType2);

    Row receivedRow = transform.buildOrGetKwargsRow();

    assertEquals("xxx", receivedRow.getRow("customField0").getString("strField"));
    assertEquals(123, (int) receivedRow.getRow("customField0").getInt32("intField"));

    assertEquals("yyy", receivedRow.getRow("customField1").getString("strField"));
    assertEquals(456, (int) receivedRow.getRow("customField1").getInt32("intField"));
  }

  @Test
  public void generateKwargsWithPythonCallableSource() {
    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withKwarg("customField0", PythonCallableSource.of("dummy data"));
    Row receivedRow = transform.buildOrGetKwargsRow();
    assertTrue(receivedRow.getValue("customField0") instanceof PythonCallableSource);
  }

  @Test
  public void generateKwargsWithTypeHint() {
    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withKwarg("customField0", Instant.ofEpochSecond(0))
            .withTypeHint(Instant.class, Schema.FieldType.logicalType(new MicrosInstant()));
    Row receivedRow = transform.buildOrGetKwargsRow();
    assertTrue(receivedRow.getValue("customField0") instanceof Instant);
  }

  @Test
  public void generateKwargsFromMap() {
    Map<String, Object> kwargsMap =
        ImmutableMap.of(
            "stringField1",
            "aaa",
            "stringField2",
            "bbb",
            "intField",
            Integer.valueOf(11),
            "longField",
            Long.valueOf(12L),
            "doubleField",
            Double.valueOf(15.6));

    PythonExternalTransform<?, ?> transform =
        PythonExternalTransform
            .<PCollection<KV<String, String>>, PCollection<KV<String, Iterable<String>>>>from(
                "DummyTransform")
            .withKwargs(kwargsMap);

    Row receivedRow = transform.buildOrGetKwargsRow();
    assertEquals("aaa", receivedRow.getString("stringField1"));
    assertEquals("bbb", receivedRow.getString("stringField2"));
    assertEquals(11, (int) receivedRow.getInt32("intField"));
    assertEquals(12L, (long) receivedRow.getInt64("longField"));
    assertEquals(15.6, (double) receivedRow.getDouble("doubleField"), 0);
  }
}
