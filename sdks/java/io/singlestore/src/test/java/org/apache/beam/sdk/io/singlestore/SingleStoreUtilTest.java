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
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test Util. */
@RunWith(JUnit4.class)
public class SingleStoreUtilTest {
  private static final Logger LOG = LoggerFactory.getLogger(SingleStoreUtilTest.class);

  @Test
  public void testEscapeIdentifierEmpty() {
    assertEquals("``", SingleStoreUtil.escapeIdentifier(""));
  }

  @Test
  public void testEscapeIdentifierNoSpecialCharacters() {
    assertEquals("`asdasd asd ad`", SingleStoreUtil.escapeIdentifier("asdasd asd ad"));
  }

  @Test
  public void testEscapeIdentifierWithSpecialCharacters() {
    assertEquals(
        "`a````sdasd`` asd`` ad```", SingleStoreUtil.escapeIdentifier("a``sdasd` asd` ad`"));
  }

  @Test
  public void testEscapeStringEmpty() {
    assertEquals("''", SingleStoreUtil.escapeString(""));
  }

  @Test
  public void testEscapeStringNoSpecialCharacters() {
    assertEquals("'asdasd asd ad'", SingleStoreUtil.escapeString("asdasd asd ad"));
  }

  @Test
  public void testEscapeStringWithSpecialCharacters() {
    assertEquals(
        "'a\\'\\'sdasd\\' \\\\asd\\' \\\\ad\\''",
        SingleStoreUtil.escapeString("a''sdasd' \\asd' \\ad'"));
  }

  private static class TestRowMapper implements SingleStoreIO.RowMapper<TestRow> {
    @Override
    public TestRow mapRow(ResultSet resultSet) throws Exception {
      return TestRow.create(resultSet.getInt(1), resultSet.getString(2));
    }
  }

  private static class TestRowMapperWithCoder extends TestRowMapper
      implements SingleStoreIO.RowMapperWithCoder<TestRow> {
    @Override
    public Coder<TestRow> getCoder() throws Exception {
      return SerializableCoder.of(TestRow.class);
    }
  }

  @Test
  public void testInferCoderFromRowMapper() {
    SchemaRegistry sr = SchemaRegistry.createDefault();
    CoderRegistry cr = CoderRegistry.createDefault(null);
    Coder<TestRow> c = SerializableCoder.of(TestRow.class);

    assertEquals(c, SingleStoreUtil.inferCoder(new TestRowMapperWithCoder(), cr, sr, LOG));
  }

  @Test
  public void testInferCoderFromSchemaRegistry() {
    SchemaRegistry sr = SchemaRegistry.createDefault();
    CoderRegistry cr = CoderRegistry.createDefault(null);
    Coder<TestRow> c = SerializableCoder.of(TestRow.class);
    cr.registerCoderForClass(TestRow.class, c);

    assertEquals(c, SingleStoreUtil.inferCoder(new TestRowMapper(), cr, sr, LOG));
  }

  @Test
  public void testInferCoderFromCoderRegistry() throws NoSuchSchemaException {
    SchemaRegistry sr = SchemaRegistry.createDefault();
    CoderRegistry cr = CoderRegistry.createDefault(null);
    sr.registerPOJO(TestRow.class);
    Coder<TestRow> c = sr.getSchemaCoder(TestRow.class);

    assertEquals(c, SingleStoreUtil.inferCoder(new TestRowMapper(), cr, sr, LOG));
  }

  @Test
  public void testGetSelectQueryAllNulls() {
    String errorMessage = "One of withTable() or withQuery() is required";
    assertThrows(
        errorMessage,
        IllegalArgumentException.class,
        () -> SingleStoreUtil.getSelectQuery(null, null));
    assertThrows(
        errorMessage,
        IllegalArgumentException.class,
        () -> SingleStoreUtil.getSelectQuery(null, null));
  }

  @Test
  public void testGetSelectQueryNonNullQuery() {
    assertEquals(
        "SELECT * FROM table", SingleStoreUtil.getSelectQuery(null, "SELECT * FROM table"));
  }

  @Test
  public void testGetSelectQueryNonNullTable() {
    assertEquals("SELECT * FROM `ta``ble`", SingleStoreUtil.getSelectQuery("ta`ble", null));
  }

  @Test
  public void testGetSelectQueryBothNonNulls() {
    assertThrows(
        "withTable() can not be used together with withQuery()",
        IllegalArgumentException.class,
        () -> SingleStoreUtil.getSelectQuery("table", "SELECT * FROM table"));
  }

  @Test
  public void testGetArgumentWithDefaultReturnsDefault() {
    assertEquals("default", SingleStoreUtil.getArgumentWithDefault(null, "default"));
  }

  @Test
  public void testGetArgumentWithDefault() {
    assertEquals("value", SingleStoreUtil.getArgumentWithDefault("value", "default"));
  }

  @Test
  public void testGetClassNameOrNullNull() {
    assertNull(SingleStoreUtil.getClassNameOrNull(null));
  }

  @Test
  public void testGetClassNameOrNullClassName() {
    assertEquals("java.lang.String", SingleStoreUtil.getClassNameOrNull("asd"));
  }
}
