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
package org.apache.beam.sdk.io.singlestore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.sql.ResultSet;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test Util. */
@RunWith(JUnit4.class)
public class UtilTest {
  private static final Logger LOG = LoggerFactory.getLogger(UtilTest.class);

  @Test
  public void testEscapeIdentifierEmpty() {
    assertEquals("``", Util.escapeIdentifier(""));
  }

  @Test
  public void testEscapeIdentifierNoSpecialCharacters() {
    assertEquals("`asdasd asd ad`", Util.escapeIdentifier("asdasd asd ad"));
  }

  @Test
  public void testEscapeIdentifierWithSpecialCharacters() {
    assertEquals("`a````sdasd`` asd`` ad```", Util.escapeIdentifier("a``sdasd` asd` ad`"));
  }

  @Test
  public void testEscapeStringEmpty() {
    assertEquals("''", Util.escapeString(""));
  }

  @Test
  public void testEscapeStringNoSpecialCharacters() {
    assertEquals("'asdasd asd ad'", Util.escapeString("asdasd asd ad"));
  }

  @Test
  public void testEscapeStringWithSpecialCharacters() {
    assertEquals(
        "'a\\'\\'sdasd\\' \\\\asd\\' \\\\ad\\''", Util.escapeString("a''sdasd' \\asd' \\ad'"));
  }

  private static class TestRowMapper implements RowMapper<TestRow> {
    @Override
    public TestRow mapRow(ResultSet resultSet) throws Exception {
      return TestRow.create(resultSet.getInt(1), resultSet.getString(2));
    }
  }

  @Test
  public void testInferCoderFromSchemaRegistry() {
    SchemaRegistry sr = SchemaRegistry.createDefault();
    CoderRegistry cr = CoderRegistry.createDefault();
    Coder<TestRow> c = SerializableCoder.of(TestRow.class);
    cr.registerCoderForClass(TestRow.class, c);

    assertEquals(c, Util.inferCoder(new TestRowMapper(), cr, sr, LOG));
  }

  @Test
  public void testInferCoderFromCoderRegistry() throws NoSuchSchemaException {
    SchemaRegistry sr = SchemaRegistry.createDefault();
    CoderRegistry cr = CoderRegistry.createDefault();
    sr.registerPOJO(TestRow.class);
    Coder<TestRow> c = sr.getSchemaCoder(TestRow.class);

    assertEquals(c, Util.inferCoder(new TestRowMapper(), cr, sr, LOG));
  }

  @Test
  public void testGetSelectQueryAllNulls() {
    String errorMessage = "One of withTable() or withQuery() is required";
    assertThrows(
        errorMessage, IllegalArgumentException.class, () -> Util.getSelectQuery(null, null));
    assertThrows(
        errorMessage,
        IllegalArgumentException.class,
        () ->
            Util.getSelectQuery(
                ValueProvider.StaticValueProvider.of(null),
                ValueProvider.StaticValueProvider.of(null)));
    assertThrows(
        errorMessage,
        IllegalArgumentException.class,
        () -> Util.getSelectQuery(null, ValueProvider.StaticValueProvider.of(null)));
    assertThrows(
        errorMessage,
        IllegalArgumentException.class,
        () -> Util.getSelectQuery(ValueProvider.StaticValueProvider.of(null), null));
  }

  @Test
  public void testGetSelectQueryNonNullQuery() {
    assertEquals(
        "SELECT * FROM table",
        Util.getSelectQuery(null, ValueProvider.StaticValueProvider.of("SELECT * FROM table")));
    assertEquals(
        "SELECT * FROM table",
        Util.getSelectQuery(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of("SELECT * FROM table")));
  }

  @Test
  public void testGetSelectQueryNonNullTable() {
    assertEquals(
        "SELECT * FROM `ta``ble`",
        Util.getSelectQuery(ValueProvider.StaticValueProvider.of("ta`ble"), null));
    assertEquals(
        "SELECT * FROM `tab``le`",
        Util.getSelectQuery(
            ValueProvider.StaticValueProvider.of("tab`le"),
            ValueProvider.StaticValueProvider.of(null)));
  }

  @Test
  public void testGetSelectQueryBothNonNulls() {
    assertThrows(
        "withTable() can not be used together with withQuery()",
        IllegalArgumentException.class,
        () ->
            Util.getSelectQuery(
                ValueProvider.StaticValueProvider.of("table"),
                ValueProvider.StaticValueProvider.of("SELECT * FROM table")));
  }

  @Test
  public void testGetRequiredArgumentError() {
    assertThrows(
        "ERROR!!!",
        IllegalArgumentException.class,
        () -> Util.getRequiredArgument(null, "ERROR!!!"));
    assertThrows(
        "ERROR!!!",
        IllegalArgumentException.class,
        () -> Util.getRequiredArgument(ValueProvider.StaticValueProvider.of(null), "ERROR!!!"));
  }

  @Test
  public void testGetRequiredArgument() {
    assertEquals("value", Util.getRequiredArgument("value", "ERROR!!!"));
    assertEquals(
        "value",
        Util.getRequiredArgument(ValueProvider.StaticValueProvider.of("value"), "ERROR!!!"));
  }

  @Test
  public void testGetArgumentWithDefaultReturnsDefault() {
    assertEquals("default", Util.getArgumentWithDefault(null, "default"));
    assertEquals(
        "default",
        Util.getArgumentWithDefault(ValueProvider.StaticValueProvider.of(null), "default"));
  }

  @Test
  public void testGetArgumentWithDefault() {
    assertEquals("value", Util.getArgumentWithDefault("value", "default"));
    assertEquals(
        "value",
        Util.getArgumentWithDefault(ValueProvider.StaticValueProvider.of("value"), "default"));
  }

  @Test
  public void testGetClassNameOrNullNull() {
    assertNull(Util.getClassNameOrNull(null));
  }

  @Test
  public void testGetClassNameOrNullClassName() {
    assertEquals("java.lang.String", Util.getClassNameOrNull("asd"));
  }
}
