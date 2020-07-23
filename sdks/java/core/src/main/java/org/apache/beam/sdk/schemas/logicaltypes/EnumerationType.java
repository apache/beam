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
package org.apache.beam.sdk.schemas.logicaltypes;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType.Value;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBiMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** This {@link LogicalType} represent an enumeration over a fixed set of values. */
@Experimental(Kind.SCHEMAS)
public class EnumerationType implements LogicalType<Value, Integer> {
  public static final String IDENTIFIER = "Enum";
  final BiMap<String, Integer> enumValues = HashBiMap.create();
  final List<String> values;

  private EnumerationType(Map<String, Integer> enumValues) {
    this.enumValues.putAll(enumValues);
    values =
        enumValues.entrySet().stream()
            .sorted(Comparator.comparingInt(e -> e.getValue()))
            .map(Entry::getKey)
            .collect(Collectors.toList());
  }

  /** Create an enumeration type over a set of String->Integer values. */
  public static EnumerationType create(Map<String, Integer> enumValues) {
    return new EnumerationType(enumValues);
  }

  /**
   * Create an enumeration type from a fixed set of String values; integer values will be
   * automatically chosen.
   */
  public static EnumerationType create(List<String> enumValues) {
    return new EnumerationType(
        IntStream.range(0, enumValues.size())
            .boxed()
            .collect(Collectors.toMap(i -> enumValues.get(i), i -> i)));
  }

  /**
   * Create an enumeration type from a fixed set of String values; integer values will be
   * automatically chosen.
   */
  public static EnumerationType create(String... enumValues) {
    return create(Arrays.asList(enumValues));
  }
  /** Return an {@link Value} corresponding to one of the enumeration strings. */
  public Value valueOf(String stringValue) {
    return new Value(enumValues.get(stringValue));
  }

  /** Return an {@link Value} corresponding to one of the enumeration integer values. */
  public Value valueOf(int value) {
    return new Value(value);
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public FieldType getArgumentType() {
    return FieldType.map(FieldType.STRING, FieldType.INT32);
  }

  @Override
  public Map<String, Integer> getArgument() {
    return enumValues;
  }

  @Override
  public FieldType getBaseType() {
    return FieldType.INT32;
  }

  @Override
  public Integer toBaseType(Value input) {
    return input.getValue();
  }

  @Override
  public Value toInputType(Integer base) {
    return valueOf(base);
  }

  public Map<String, Integer> getValuesMap() {
    return enumValues;
  }

  public List<String> getValues() {
    return values;
  }

  public String toString(EnumerationType.Value value) {
    return enumValues.inverse().get(value.getValue());
  }

  @Override
  public String toString() {
    return "Enumeration: " + enumValues;
  }

  /**
   * This class represents a single enum value. It can be referenced as a String or as an integer.
   */
  public static class Value implements Serializable {
    private final int value;

    public Value(int value) {
      this.value = value;
    }

    /** Return the integer enum value. */
    public int getValue() {
      return value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Value enumValue = (Value) o;
      return value == enumValue.value;
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public String toString() {
      return "enum value: " + value;
    }
  }
}
