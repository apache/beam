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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType.Value;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** This {@link LogicalType} represent an enumeration over a fixed set of values. */
public class EnumerationType implements LogicalType<Value, Integer> {
  public static final String IDENTIFIER = "Enum";
  private static final int MAX_DENSE_ENUM_VALUE = 256;
  private static final FieldType ARGUMENT_TYPE = FieldType.map(FieldType.STRING, FieldType.INT32);

  final BiMap<String, Integer> enumValues;
  final List<String> values;
  private transient volatile @Nullable LookupCache lookupCache;

  private static final class LookupCache {
    final ImmutableMap<String, Value> nameToValue;
    final @Nullable Value @Nullable [] denseValues;
    final @Nullable String @Nullable [] denseNames;
    final @Nullable ImmutableMap<Integer, Value> sparseValues;

    LookupCache(BiMap<String, Integer> enumValues) {
      int min = Integer.MAX_VALUE;
      int max = Integer.MIN_VALUE;
      for (Integer v : enumValues.values()) {
        int iv = v;
        if (iv < min) {
          min = iv;
        }
        if (iv > max) {
          max = iv;
        }
      }
      boolean useDense =
          !enumValues.isEmpty()
              && min >= 0
              && max <= MAX_DENSE_ENUM_VALUE
              && max <= Math.max(enumValues.size() * 2, 64);
      @Nullable Value @Nullable [] dVals = useDense ? new @Nullable Value[max + 1] : null;
      @Nullable String @Nullable [] dNames = useDense ? new @Nullable String[max + 1] : null;
      ImmutableMap.Builder<String, Value> byName =
          ImmutableMap.builderWithExpectedSize(enumValues.size());
      ImmutableMap.Builder<Integer, Value> byId =
          useDense ? null : ImmutableMap.builderWithExpectedSize(enumValues.size());
      for (Entry<String, Integer> entry : enumValues.entrySet()) {
        int id = entry.getValue();
        Value cached = new Value(id);
        byName.put(entry.getKey(), cached);
        if (dVals != null && dNames != null) {
          dVals[id] = cached;
          dNames[id] = entry.getKey();
        } else if (byId != null) {
          byId.put(id, cached);
        }
      }
      this.nameToValue = byName.build();
      this.denseValues = dVals;
      this.denseNames = dNames;
      this.sparseValues = byId != null ? byId.build() : null;
    }
  }

  private LookupCache getLookupCache() {
    LookupCache cache = lookupCache;
    if (cache == null) {
      cache = new LookupCache(enumValues);
      lookupCache = cache;
    }
    return cache;
  }

  private EnumerationType(Map<String, Integer> enumValues) {
    this.enumValues = ImmutableBiMap.copyOf(enumValues);
    values =
        enumValues.entrySet().stream()
            .sorted(Comparator.comparingInt(e -> e.getValue()))
            .map(Entry::getKey)
            .collect(Collectors.toList());
    this.lookupCache = new LookupCache(this.enumValues);
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
    Value value = getLookupCache().nameToValue.get(stringValue);
    if (value == null) {
      throw new IllegalArgumentException(
          String.format("Unknown enumeration value %s", stringValue));
    }
    return value;
  }

  /** Return an {@link Value} corresponding to one of the enumeration integer values. */
  public Value valueOf(int value) {
    LookupCache cache = getLookupCache();
    @Nullable Value @Nullable [] dense = cache.denseValues;
    if (dense != null && value >= 0 && value < dense.length) {
      Value cached = dense[value];
      if (cached != null) {
        return cached;
      }
    } else if (cache.sparseValues != null) {
      Value cached = cache.sparseValues.get(value);
      if (cached != null) {
        return cached;
      }
    }
    return new Value(value);
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public FieldType getArgumentType() {
    return ARGUMENT_TYPE;
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

  public BiMap<String, Integer> getValuesMap() {
    return enumValues;
  }

  public @Nullable String getEnumName(int number) {
    @Nullable String @Nullable [] dense = getLookupCache().denseNames;
    if (dense != null) {
      return (number >= 0 && number < dense.length) ? dense[number] : null;
    }
    return enumValues.inverse().get(number);
  }

  public @Nullable Integer getEnumValue(String enumName) {
    return enumValues.get(enumName);
  }

  public List<String> getValues() {
    return values;
  }

  public String toString(EnumerationType.Value value) {
    String name = getEnumName(value.getValue());
    if (name == null) {
      throw new IllegalArgumentException(
          String.format("Unknown enumeration value %s", value.getValue()));
    }
    return name;
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
      if (!(o instanceof Value)) {
        return false;
      }
      Value enumValue = (Value) o;
      return value == enumValue.value;
    }

    @Override
    public int hashCode() {
      return 31 + value;
    }

    @Override
    public String toString() {
      return "enum value: " + value;
    }
  }
}
