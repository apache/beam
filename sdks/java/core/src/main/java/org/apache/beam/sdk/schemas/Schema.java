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
package org.apache.beam.sdk.schemas;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.values.Row;

/**
 * {@link Schema} describes the fields in {@link Row}.
 *
 * <p>TODO: Currently this is a mpaping of field names to Coders. We want this to instead directly
 * represent a schema, with direct support for nested schemas.
 */
@Experimental
@AutoValue
public abstract class Schema implements Serializable{
  abstract List<String> fieldNames();
  abstract List<Coder> fieldCoders();

  /**
   * Field of a row.
   *
   * <p>Contains field name and its coder.
   */
  @AutoValue
  public abstract static class Field {
    abstract String name();
    abstract Coder coder();

    public static Field of(String name, Coder coder) {
      return new AutoValue_Schema_Field(name, coder);
    }
  }

  /**
   * Collects a stream of {@link Field}s into a {@link Schema}.
   */
  public static Collector<Field, List<Field>, Schema> toSchema() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        },
        Schema::fromFields);
  }

  private static Schema fromFields(List<Field> fields) {
    ImmutableList.Builder<String> names = ImmutableList.builder();
    ImmutableList.Builder<Coder> coders = ImmutableList.builder();

    for (Field field : fields) {
      names.add(field.name());
      coders.add(field.coder());
    }

    return fromNamesAndCoders(names.build(), coders.build());
  }

  /**
   * Creates a new {@link Field} with specified name and coder.
   */
  public static Field newField(String name, Coder coder) {
    return Field.of(name, coder);
  }

  public static Schema fromNamesAndCoders(
      List<String> fieldNames,
      List<Coder> fieldCoders) {

    if (fieldNames.size() != fieldCoders.size()) {
      throw new IllegalStateException(
          "the size of fieldNames and fieldCoders need to be the same.");
    }

    return new AutoValue_Schema(fieldNames, fieldCoders);
  }

  /**
   * Return the coder for {@link Row}, which wraps {@link #fieldCoders} for each field.
   */
  public RowCoder getRowCoder() {
    return RowCoder.of(this, fieldCoders());
  }

  /**
   * Return the field coder for {@code index}.
   */
  public Coder getFieldCoder(int index) {
    return fieldCoders().get(index);
  }

  /**
   * Returns an immutable list of field names.
   */
  public List<String> getFieldNames() {
    return ImmutableList.copyOf(fieldNames());
  }

  /**
   * Return the name of field by index.
   */
  public String getFieldName(int index) {
    return fieldNames().get(index);
  }

  /**
   * Find the index of a given field.
   */
  public int indexOf(String fieldName) {
    return fieldNames().indexOf(fieldName);
  }

  /**
   * Return the count of fields.
   */
  public int getFieldCount() {
    return fieldNames().size();
  }
}
