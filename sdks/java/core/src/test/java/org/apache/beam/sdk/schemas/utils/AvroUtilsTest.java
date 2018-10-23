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
package org.apache.beam.sdk.schemas.utils;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

import com.google.common.collect.Lists;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.util.List;
import java.util.function.Function;
import org.apache.avro.RandomData;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroGenerators.RecordSchemaGenerator;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Tests for conversion between AVRO records and Beam rows. */
@RunWith(JUnitQuickcheck.class)
public class AvroUtilsTest {

  private static final org.apache.avro.Schema NULL_SCHEMA =
      org.apache.avro.Schema.create(Type.NULL);

  @Property(trials = 1000)
  @SuppressWarnings("unchecked")
  public void supportsAnyAvroSchema(
      @From(RecordSchemaGenerator.class) org.apache.avro.Schema avroSchema) {
    // not everything is possible to translate
    assumeThat(avroSchema, not(containsField(AvroUtilsTest::hasArrayOrMapOfNullable)));
    assumeThat(avroSchema, not(containsField(AvroUtilsTest::hasNonNullUnion)));

    Schema schema = AvroUtils.toSchema(avroSchema);
    Iterable iterable = new RandomData(avroSchema, 10);
    List<GenericRecord> records = Lists.newArrayList((Iterable<GenericRecord>) iterable);

    for (GenericRecord record : records) {
      AvroUtils.toRowStrict(record, schema);
    }
  }

  @Test
  public void testUnwrapNullableSchema() {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createUnion(
        org.apache.avro.Schema.create(Type.NULL),
        org.apache.avro.Schema.create(Type.STRING));

    assertEquals(
        org.apache.avro.Schema.create(Type.STRING),
        AvroUtils.unwrapNullableSchema(avroSchema));
  }

  @Test
  public void testUnwrapNullableSchemaReordered() {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createUnion(
        org.apache.avro.Schema.create(Type.STRING),
        org.apache.avro.Schema.create(Type.NULL));

    assertEquals(
        org.apache.avro.Schema.create(Type.STRING),
        AvroUtils.unwrapNullableSchema(avroSchema));
  }

  @Test
  public void testUnwrapNullableSchemaToUnion() {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createUnion(
        org.apache.avro.Schema.create(Type.STRING),
        org.apache.avro.Schema.create(Type.LONG),
        org.apache.avro.Schema.create(Type.NULL));

    assertEquals(
        org.apache.avro.Schema.createUnion(
            org.apache.avro.Schema.create(Type.STRING),
            org.apache.avro.Schema.create(Type.LONG)),
        AvroUtils.unwrapNullableSchema(avroSchema));
  }

  public static ContainsField containsField(Function<org.apache.avro.Schema, Boolean> predicate) {
    return new ContainsField(predicate);
  }

  // doesn't work because Beam doesn't have unions, only nullable fields
  public static boolean hasNonNullUnion(org.apache.avro.Schema schema) {
    if (schema.getType() == Type.UNION) {
      final List<org.apache.avro.Schema> types = schema.getTypes();

      if (types.size() == 2) {
        return !types.contains(NULL_SCHEMA);
      } else {
        return true;
      }
    }

    return false;
  }

  // doesn't work because Beam doesn't support arrays and maps of nullable types
  public static boolean hasArrayOrMapOfNullable(org.apache.avro.Schema schema) {

    if (schema.getType() == Type.ARRAY) {
      org.apache.avro.Schema elementType = schema.getElementType();
      if (elementType.getType() == Type.UNION) {
        return elementType.getTypes().contains(NULL_SCHEMA);
      }
    }

    if (schema.getType() == Type.MAP) {
      org.apache.avro.Schema valueType = schema.getValueType();
      if (valueType.getType() == Type.UNION) {
        return valueType.getTypes().contains(NULL_SCHEMA);
      }
    }

    return false;
  }

  static class ContainsField extends BaseMatcher<org.apache.avro.Schema> {

    private final Function<org.apache.avro.Schema, Boolean> predicate;

    ContainsField(final Function<org.apache.avro.Schema, Boolean> predicate) {
      this.predicate = predicate;
    }

    @Override
    public boolean matches(final Object item0) {
      if (!(item0 instanceof org.apache.avro.Schema)) {
        return false;
      }

      org.apache.avro.Schema item = (org.apache.avro.Schema) item0;

      if (predicate.apply(item)) {
        return true;
      }

      switch (item.getType()) {
        case RECORD:
          return item.getFields().stream().anyMatch(x -> matches(x.schema()));

        case UNION:
          return item.getTypes().stream().anyMatch(this::matches);

        case ARRAY:
          return matches(item.getElementType());

        case MAP:
          return matches(item.getValueType());

        default:
          return false;
      }
    }

    @Override
    public void describeTo(final Description description) {}
  }
}
