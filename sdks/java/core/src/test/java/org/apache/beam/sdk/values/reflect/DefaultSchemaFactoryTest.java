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

package org.apache.beam.sdk.values.reflect;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link DefaultSchemaFactory}. */
public class DefaultSchemaFactoryTest {

  /** Test class without supported coder. */
  private static class UnsupportedClass {}

  private static final ImmutableList<FieldValueGetter> GETTERS =
      ImmutableList.<FieldValueGetter>builder()
          .add(getter("byteGetter", Byte.class))
          .add(getter("integerGetter", Integer.class))
          .add(getter("longGetter", Long.class))
          .add(getter("doubleGetter", Double.class))
          .add(getter("booleanGetter", Boolean.class))
          .add(getter("stringGetter", String.class))
          .build();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testContainsCorrectFields() throws Exception {
    DefaultSchemaFactory factory = new DefaultSchemaFactory();

    Schema schema = factory.createSchema(GETTERS);

    assertEquals(GETTERS.size(), schema.getFieldCount());
    assertEquals(
        Arrays.asList(
            "byteGetter",
            "integerGetter",
            "longGetter",
            "doubleGetter",
            "booleanGetter",
            "stringGetter"),
        schema.getFieldNames());
  }

  @Test
  public void testThrowsForUnsupportedTypes() throws Exception {
    thrown.expect(UnsupportedOperationException.class);

    DefaultSchemaFactory factory = new DefaultSchemaFactory();

    factory.createSchema(
        Arrays.<FieldValueGetter>asList(getter("unsupportedGetter", UnsupportedClass.class)));
  }

  private static FieldValueGetter getter(final String fieldName, final Class fieldType) {
    return new FieldValueGetter() {
      @Override
      public Object get(Object object) {
        return null;
      }

      @Override
      public String name() {
        return fieldName;
      }

      @Override
      public Class type() {
        return fieldType;
      }

      @Override
      public boolean isRecursive() {
        return false;
      }
    };
  }
}
