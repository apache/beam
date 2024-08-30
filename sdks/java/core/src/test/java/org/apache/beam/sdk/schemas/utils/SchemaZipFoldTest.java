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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.junit.Test;

/** Tests for {@link SchemaZipFold} with examples. */
public class SchemaZipFoldTest {

  private static final Schema LEFT =
      Schema.of(
          Schema.Field.of("left", Schema.FieldType.INT32),
          Schema.Field.of("f0", Schema.FieldType.INT32),
          Schema.Field.of("f1", Schema.FieldType.INT32),
          Schema.Field.of("f2", Schema.FieldType.INT32),
          Schema.Field.of(
              "f3",
              Schema.FieldType.row(
                  Schema.of(
                      Schema.Field.of("inner_left", Schema.FieldType.INT32),
                      Schema.Field.of("f0", Schema.FieldType.INT32),
                      Schema.Field.of("f1", Schema.FieldType.INT32)))));

  private static final Schema RIGHT =
      Schema.of(
          Schema.Field.of("right", Schema.FieldType.INT32),
          Schema.Field.of("f0", Schema.FieldType.INT32),
          Schema.Field.of("f1", Schema.FieldType.INT32),
          Schema.Field.of("f2", Schema.FieldType.STRING),
          Schema.Field.of(
              "f3",
              Schema.FieldType.row(
                  Schema.of(
                      Schema.Field.of("inner_right", Schema.FieldType.INT32),
                      Schema.Field.of("f0", Schema.FieldType.INT32),
                      Schema.Field.of("f1", Schema.FieldType.STRING)))));

  @Test
  public void testCountCommonLeafs() {
    assertEquals(3, new CountCommonLeafs().apply(LEFT, RIGHT).intValue());
  }

  @Test
  public void testCountCommonFields() {
    assertEquals(6, new CountCommonFields().apply(LEFT, RIGHT).intValue());
  }

  @Test
  public void testCountMissingFields() {
    assertEquals(4, new CountMissingFields().apply(LEFT, RIGHT).intValue());
  }

  @Test
  public void testListCommonFields() {
    assertThat(
        new ListCommonFields().apply(LEFT, RIGHT),
        containsInAnyOrder("f0", "f1", "f2", "f3", "f3.f0", "f3.f1"));
  }

  static class CountCommonLeafs extends SchemaZipFold<Integer> {

    @Override
    public Integer accumulate(Integer left, Integer right) {
      return left + right;
    }

    @Override
    public Integer accept(Context context, Schema.FieldType left, Schema.FieldType right) {

      if (left.getTypeName() != right.getTypeName()) {
        return 0;
      }

      if (left.getTypeName() == Schema.TypeName.ROW) {
        return 0;
      }

      if (left.getTypeName() == Schema.TypeName.ARRAY) {
        return 0;
      }

      if (left.getTypeName() == Schema.TypeName.MAP) {
        return 0;
      }

      return 1;
    }

    @Override
    public Integer accept(
        Context context, Optional<Schema.Field> left, Optional<Schema.Field> right) {
      return 0;
    }
  }

  static class CountCommonFields extends SchemaZipFold<Integer> {

    @Override
    public Integer accumulate(Integer left, Integer right) {
      return left + right;
    }

    @Override
    public Integer accept(Context context, Schema.FieldType left, Schema.FieldType right) {

      return 0;
    }

    @Override
    public Integer accept(
        Context context, Optional<Schema.Field> left, Optional<Schema.Field> right) {
      if (left.isPresent() && right.isPresent()) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  static class CountMissingFields extends SchemaZipFold<Integer> {

    @Override
    public Integer accumulate(Integer left, Integer right) {
      return left + right;
    }

    @Override
    public Integer accept(Context context, Schema.FieldType left, Schema.FieldType right) {

      return 0;
    }

    @Override
    public Integer accept(
        Context context, Optional<Schema.Field> left, Optional<Schema.Field> right) {
      if (!left.isPresent() || !right.isPresent()) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  static class ListCommonFields extends SchemaZipFold<List<String>> {

    @Override
    public List<String> accumulate(List<String> left, List<String> right) {
      return Stream.concat(left.stream(), right.stream()).collect(Collectors.toList());
    }

    @Override
    public List<String> accept(Context context, Schema.FieldType left, Schema.FieldType right) {
      return Collections.emptyList();
    }

    @Override
    public List<String> accept(
        Context context, Optional<Schema.Field> left, Optional<Schema.Field> right) {
      if (left.isPresent() && right.isPresent()) {
        String pathStr = Joiner.on('.').join(context.path());
        return Collections.singletonList(pathStr);
      } else {
        return Collections.emptyList();
      }
    }
  }
}
