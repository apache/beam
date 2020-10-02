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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/** Various Java POJOs and associated schemas used in tests. */
public class TestPOJOs {
  /** A POJO containing one nullable and one non-nullable type. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJOWithNullables {
    public @Nullable String str;
    public int anInt;

    public POJOWithNullables(@Nullable String str, int anInt) {
      this.str = str;
      this.anInt = anInt;
    }

    public POJOWithNullables() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJOWithNullables that = (POJOWithNullables) o;
      return anInt == that.anInt && Objects.equals(str, that.str);
    }

    @Override
    public int hashCode() {
      return Objects.hash(str, anInt);
    }
  }

  /** The schema for {@link POJOWithNullables}. * */
  public static final Schema NULLABLES_SCHEMA =
      Schema.builder().addNullableField("str", FieldType.STRING).addInt32Field("anInt").build();

  /** a POJO containing a nested nullable field. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJOWithNestedNullable {
    public @Nullable POJOWithNullables nested;

    public POJOWithNestedNullable(@Nullable POJOWithNullables nested) {
      this.nested = nested;
    }

    public POJOWithNestedNullable() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJOWithNestedNullable that = (POJOWithNestedNullable) o;
      return Objects.equals(nested, that.nested);
    }

    @Override
    public int hashCode() {
      return Objects.hash(nested);
    }
  }

  /** The schema for {@link POJOWithNestedNullable}. * */
  public static final Schema NESTED_NULLABLE_SCHEMA =
      Schema.builder().addNullableField("nested", FieldType.row(NULLABLES_SCHEMA)).build();

  /** A POJO for testing static factory methods. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class StaticCreationSimplePojo {
    public final String str;
    public final byte aByte;
    public final short aShort;
    public final int anInt;
    public final long aLong;
    public final boolean aBoolean;
    public final DateTime dateTime;
    public final Instant instant;
    public final byte[] bytes;
    public final ByteBuffer byteBuffer;
    public final BigDecimal bigDecimal;
    public final StringBuilder stringBuilder;

    private StaticCreationSimplePojo(
        String str,
        byte aByte,
        short aShort,
        int anInt,
        long aLong,
        boolean aBoolean,
        DateTime dateTime,
        Instant instant,
        byte[] bytes,
        ByteBuffer byteBuffer,
        BigDecimal bigDecimal,
        StringBuilder stringBuilder) {
      this.str = str;
      this.aByte = aByte;
      this.aShort = aShort;
      this.anInt = anInt;
      this.aLong = aLong;
      this.aBoolean = aBoolean;
      this.dateTime = dateTime;
      this.instant = instant;
      this.bytes = bytes;
      this.byteBuffer = byteBuffer;
      this.bigDecimal = bigDecimal;
      this.stringBuilder = stringBuilder;
    }

    @SchemaCreate
    public static StaticCreationSimplePojo of(
        String str,
        long aLong,
        byte aByte,
        short aShort,
        int anInt,
        boolean aBoolean,
        DateTime dateTime,
        ByteBuffer byteBuffer,
        Instant instant,
        byte[] bytes,
        BigDecimal bigDecimal,
        StringBuilder stringBuilder) {
      return new StaticCreationSimplePojo(
          str,
          aByte,
          aShort,
          anInt,
          aLong,
          aBoolean,
          dateTime,
          instant,
          bytes,
          byteBuffer,
          bigDecimal,
          stringBuilder);
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StaticCreationSimplePojo)) {
        return false;
      }
      StaticCreationSimplePojo that = (StaticCreationSimplePojo) o;
      return aByte == that.aByte
          && aShort == that.aShort
          && anInt == that.anInt
          && aLong == that.aLong
          && aBoolean == that.aBoolean
          && Objects.equals(str, that.str)
          && Objects.equals(dateTime, that.dateTime)
          && Objects.equals(instant, that.instant)
          && Arrays.equals(bytes, that.bytes)
          && Objects.equals(byteBuffer, that.byteBuffer)
          && Objects.equals(bigDecimal, that.bigDecimal)
          && Objects.equals(stringBuilder.toString(), that.stringBuilder.toString());
    }

    @Override
    public int hashCode() {
      int result =
          Objects.hash(
              str,
              aByte,
              aShort,
              anInt,
              aLong,
              aBoolean,
              dateTime,
              instant,
              byteBuffer,
              bigDecimal,
              stringBuilder);
      result = 31 * result + Arrays.hashCode(bytes);
      return result;
    }
  }

  /** A POJO for testing annotations. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class AnnotatedSimplePojo {
    public final String str;

    @SchemaFieldName("aByte")
    public final byte theByte;

    @SchemaFieldName("aShort")
    public final short theShort;

    public final int anInt;
    public final long aLong;
    public final boolean aBoolean;
    public final DateTime dateTime;
    public final Instant instant;
    public final byte[] bytes;
    public final ByteBuffer byteBuffer;
    public final BigDecimal bigDecimal;
    public final StringBuilder stringBuilder;
    @SchemaIgnore public final Integer pleaseIgnore;

    // Marked with SchemaCreate, so this will be called to construct instances.
    @SchemaCreate
    public AnnotatedSimplePojo(
        String str,
        byte theByte,
        long aLong,
        short theShort,
        int anInt,
        boolean aBoolean,
        DateTime dateTime,
        BigDecimal bigDecimal,
        Instant instant,
        byte[] bytes,
        ByteBuffer byteBuffer,
        StringBuilder stringBuilder) {
      this.str = str;
      this.theByte = theByte;
      this.theShort = theShort;
      this.anInt = anInt;
      this.aLong = aLong;
      this.aBoolean = aBoolean;
      this.dateTime = dateTime;
      this.instant = instant;
      this.bytes = bytes;
      this.byteBuffer = byteBuffer;
      this.bigDecimal = bigDecimal;
      this.stringBuilder = stringBuilder;
      this.pleaseIgnore = 42;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AnnotatedSimplePojo that = (AnnotatedSimplePojo) o;
      return theByte == that.theByte
          && theShort == that.theShort
          && anInt == that.anInt
          && aLong == that.aLong
          && aBoolean == that.aBoolean
          && Objects.equals(str, that.str)
          && Objects.equals(dateTime, that.dateTime)
          && Objects.equals(instant, that.instant)
          && Arrays.equals(bytes, that.bytes)
          && Objects.equals(byteBuffer, that.byteBuffer)
          && Objects.equals(bigDecimal, that.bigDecimal)
          && Objects.equals(stringBuilder.toString(), that.stringBuilder.toString());
    }

    @Override
    public int hashCode() {
      int result =
          Objects.hash(
              str,
              theByte,
              theShort,
              anInt,
              aLong,
              aBoolean,
              dateTime,
              instant,
              byteBuffer,
              bigDecimal,
              stringBuilder.toString());
      result = 31 * result + Arrays.hashCode(bytes);
      return result;
    }

    @Override
    public String toString() {
      return "AnnotatedSimplePojo{"
          + "str='"
          + str
          + '\''
          + ", theByte="
          + theByte
          + ", theShort="
          + theShort
          + ", anInt="
          + anInt
          + ", aLong="
          + aLong
          + ", aBoolean="
          + aBoolean
          + ", dateTime="
          + dateTime
          + ", instant="
          + instant
          + ", bytes="
          + Arrays.toString(bytes)
          + ", byteBuffer="
          + byteBuffer
          + ", bigDecimal="
          + bigDecimal
          + ", stringBuilder="
          + stringBuilder
          + ", pleaseIgnore="
          + pleaseIgnore
          + '}';
    }
  }

  /** A simple POJO containing basic types. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class SimplePOJO {
    public String str;
    public byte aByte;
    public short aShort;
    public int anInt;
    public long aLong;
    public boolean aBoolean;
    public DateTime dateTime;
    public Instant instant;
    public byte[] bytes;
    public ByteBuffer byteBuffer;
    public BigDecimal bigDecimal;
    public StringBuilder stringBuilder;

    public SimplePOJO() {}

    public SimplePOJO(
        String str,
        byte aByte,
        short aShort,
        int anInt,
        long aLong,
        boolean aBoolean,
        DateTime dateTime,
        Instant instant,
        byte[] bytes,
        ByteBuffer byteBuffer,
        BigDecimal bigDecimal,
        StringBuilder stringBuilder) {
      this.str = str;
      this.aByte = aByte;
      this.aShort = aShort;
      this.anInt = anInt;
      this.aLong = aLong;
      this.aBoolean = aBoolean;
      this.dateTime = dateTime;
      this.instant = instant;
      this.bytes = bytes;
      this.byteBuffer = byteBuffer;
      this.bigDecimal = bigDecimal;
      this.stringBuilder = stringBuilder;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SimplePOJO that = (SimplePOJO) o;
      return aByte == that.aByte
          && aShort == that.aShort
          && anInt == that.anInt
          && aLong == that.aLong
          && aBoolean == that.aBoolean
          && Objects.equals(str, that.str)
          && Objects.equals(dateTime, that.dateTime)
          && Objects.equals(instant, that.instant)
          && Arrays.equals(bytes, that.bytes)
          && Objects.equals(byteBuffer, that.byteBuffer)
          && Objects.equals(bigDecimal, that.bigDecimal)
          && Objects.equals(stringBuilder.toString(), that.stringBuilder.toString());
    }

    @Override
    public int hashCode() {
      int result =
          Objects.hash(
              str,
              aByte,
              aShort,
              anInt,
              aLong,
              aBoolean,
              dateTime,
              instant,
              byteBuffer,
              bigDecimal,
              stringBuilder);
      result = 31 * result + Arrays.hashCode(bytes);
      return result;
    }
  }

  /** The schema for {@link SimplePOJO}. * */
  public static final Schema SIMPLE_POJO_SCHEMA =
      Schema.builder()
          .addStringField("str")
          .addByteField("aByte")
          .addInt16Field("aShort")
          .addInt32Field("anInt")
          .addInt64Field("aLong")
          .addBooleanField("aBoolean")
          .addDateTimeField("dateTime")
          .addDateTimeField("instant")
          .addByteArrayField("bytes")
          .addByteArrayField("byteBuffer")
          .addDecimalField("bigDecimal")
          .addStringField("stringBuilder")
          .build();

  /** A POJO containing a nested class. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class NestedPOJO {
    public SimplePOJO nested;

    public NestedPOJO(SimplePOJO nested) {
      this.nested = nested;
    }

    public NestedPOJO() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedPOJO that = (NestedPOJO) o;
      return Objects.equals(nested, that.nested);
    }

    @Override
    public int hashCode() {
      return Objects.hash(nested);
    }
  }

  /** The schema for {@link NestedPOJO}. * */
  public static final Schema NESTED_POJO_SCHEMA =
      Schema.builder().addRowField("nested", SIMPLE_POJO_SCHEMA).build();

  /** A POJO containing arrays of primitive types. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class PrimitiveArrayPOJO {
    // Test every type of array parameter supported.
    public List<String> strings;
    public int[] integers;
    public Long[] longs;

    public PrimitiveArrayPOJO() {}

    public PrimitiveArrayPOJO(List<String> strings, int[] integers, Long[] longs) {
      this.strings = strings;
      this.integers = integers;
      this.longs = longs;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PrimitiveArrayPOJO that = (PrimitiveArrayPOJO) o;
      return Objects.equals(strings, that.strings)
          && Arrays.equals(integers, that.integers)
          && Arrays.equals(longs, that.longs);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(strings);
      result = 31 * result + Arrays.hashCode(integers);
      result = 31 * result + Arrays.hashCode(longs);
      return result;
    }
  }

  /** The schema for {@link PrimitiveArrayPOJO}. * */
  public static final Schema PRIMITIVE_ARRAY_POJO_SCHEMA =
      Schema.builder()
          .addArrayField("strings", FieldType.STRING)
          .addArrayField("integers", FieldType.INT32)
          .addArrayField("longs", FieldType.INT64)
          .build();

  /** A POJO containing arrays of complex classes. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class NestedArrayPOJO {
    public SimplePOJO[] pojos;

    public NestedArrayPOJO(SimplePOJO... pojos) {
      this.pojos = pojos;
    }

    public NestedArrayPOJO() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedArrayPOJO that = (NestedArrayPOJO) o;
      return Arrays.equals(pojos, that.pojos);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(pojos);
    }
  }

  /** The schema for {@link NestedArrayPOJO}. * */
  public static final Schema NESTED_ARRAY_POJO_SCHEMA =
      Schema.builder().addArrayField("pojos", FieldType.row(SIMPLE_POJO_SCHEMA)).build();

  /** A bean containing arrays of arrays. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class NestedArraysPOJO {
    public List<List<String>> lists;

    public NestedArraysPOJO(List<List<String>> lists) {
      this.lists = lists;
    }

    public NestedArraysPOJO() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedArraysPOJO that = (NestedArraysPOJO) o;
      return Objects.equals(lists, that.lists);
    }

    @Override
    public int hashCode() {
      return Objects.hash(lists);
    }
  }

  /** The schema for {@link NestedArraysPOJO}. * */
  public static final Schema NESTED_ARRAYS_POJO_SCHEMA =
      Schema.builder().addArrayField("lists", FieldType.array(FieldType.STRING)).build();

  /** A POJO containing a {@link List} of a complex type. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class NestedCollectionPOJO {
    public List<SimplePOJO> simples;
    public Iterable<SimplePOJO> iterableSimples;

    public NestedCollectionPOJO(List<SimplePOJO> simples) {
      this.simples = simples;
      this.iterableSimples = simples;
    }

    public NestedCollectionPOJO() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedCollectionPOJO that = (NestedCollectionPOJO) o;
      return Objects.equals(simples, that.simples)
          && Objects.equals(iterableSimples, that.iterableSimples);
    }

    @Override
    public int hashCode() {
      return Objects.hash(simples, iterableSimples);
    }
  }

  /** The schema for {@link NestedCollectionPOJO}. * */
  public static final Schema NESTED_COLLECTION_POJO_SCHEMA =
      Schema.builder()
          .addArrayField("simples", FieldType.row(SIMPLE_POJO_SCHEMA))
          .addIterableField("iterableSimples", FieldType.row(SIMPLE_POJO_SCHEMA))
          .build();

  /** A POJO containing a simple {@link Map}. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class PrimitiveMapPOJO {
    public Map<String, Integer> map;

    public PrimitiveMapPOJO(Map<String, Integer> map) {
      this.map = map;
    }

    public PrimitiveMapPOJO() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PrimitiveMapPOJO that = (PrimitiveMapPOJO) o;
      return Objects.equals(map, that.map);
    }

    @Override
    public int hashCode() {

      return Objects.hash(map);
    }
  }

  /** The schema for {@link PrimitiveMapPOJO}. * */
  public static final Schema PRIMITIVE_MAP_POJO_SCHEMA =
      Schema.builder().addMapField("map", FieldType.STRING, FieldType.INT32).build();

  /** A POJO containing a map of a complex type. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class NestedMapPOJO {
    public Map<String, SimplePOJO> map;

    public NestedMapPOJO(Map<String, SimplePOJO> map) {
      this.map = map;
    }

    public NestedMapPOJO() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedMapPOJO that = (NestedMapPOJO) o;
      return Objects.equals(map, that.map);
    }

    @Override
    public int hashCode() {
      return Objects.hash(map);
    }
  }

  /** The schema for {@link NestedMapPOJO}. * */
  public static final Schema NESTED_MAP_POJO_SCHEMA =
      Schema.builder()
          .addMapField("map", FieldType.STRING, FieldType.row(SIMPLE_POJO_SCHEMA))
          .build();

  /** A POJO containing the boxed version of primitive types. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJOWithBoxedFields {
    public Byte aByte;
    public Short aShort;
    public Integer anInt;
    public Long aLong;
    public Boolean aBoolean;

    public POJOWithBoxedFields(
        Byte aByte, Short aShort, Integer anInt, Long aLong, Boolean aBoolean) {
      this.aByte = aByte;
      this.aShort = aShort;
      this.anInt = anInt;
      this.aLong = aLong;
      this.aBoolean = aBoolean;
    }

    public POJOWithBoxedFields() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJOWithBoxedFields that = (POJOWithBoxedFields) o;
      return Objects.equals(aByte, that.aByte)
          && Objects.equals(aShort, that.aShort)
          && Objects.equals(anInt, that.anInt)
          && Objects.equals(aLong, that.aLong)
          && Objects.equals(aBoolean, that.aBoolean);
    }

    @Override
    public int hashCode() {
      return Objects.hash(aByte, aShort, anInt, aLong, aBoolean);
    }
  }

  /** The schema for {@link POJOWithBoxedFields}. * */
  public static final Schema POJO_WITH_BOXED_FIELDS_SCHEMA =
      Schema.builder()
          .addByteField("aByte")
          .addInt16Field("aShort")
          .addInt32Field("anInt")
          .addInt64Field("aLong")
          .addBooleanField("aBoolean")
          .build();

  /** A POJO containing byte arrays. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJOWithByteArray {
    public byte[] bytes1;
    public ByteBuffer bytes2;

    public POJOWithByteArray(byte[] bytes1, ByteBuffer bytes2) {
      this.bytes1 = bytes1;
      this.bytes2 = bytes2;
    }

    public POJOWithByteArray() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJOWithByteArray that = (POJOWithByteArray) o;
      return Arrays.equals(bytes1, that.bytes1) && Objects.equals(bytes2, that.bytes2);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(bytes2);
      result = 31 * result + Arrays.hashCode(bytes1);
      return result;
    }
  }

  /** The schema for {@link POJOWithByteArray}. */
  public static final Schema POJO_WITH_BYTE_ARRAY_SCHEMA =
      Schema.builder().addByteArrayField("bytes1").addByteArrayField("bytes2").build();

  /** A Pojo containing a doubly-nested array. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class PojoWithNestedArray {
    public final List<List<SimplePOJO>> pojos;

    @SchemaCreate
    public PojoWithNestedArray(List<List<SimplePOJO>> pojos) {
      this.pojos = pojos;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PojoWithNestedArray)) {
        return false;
      }
      PojoWithNestedArray that = (PojoWithNestedArray) o;
      return Objects.equals(pojos, that.pojos);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pojos);
    }
  }

  /** The schema for {@link PojoWithNestedArray}. */
  public static final Schema POJO_WITH_NESTED_ARRAY_SCHEMA =
      Schema.builder()
          .addArrayField("pojos", FieldType.array(FieldType.row(SIMPLE_POJO_SCHEMA)))
          .build();

  /** A Pojo containing an iterable. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class PojoWithIterable {
    public final Iterable<String> strings;

    @SchemaCreate
    public PojoWithIterable(Iterable<String> strings) {
      this.strings = strings;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PojoWithNestedArray)) {
        return false;
      }
      PojoWithIterable that = (PojoWithIterable) o;
      return Objects.equals(strings, that.strings);
    }

    @Override
    public int hashCode() {
      return Objects.hash(strings);
    }
  }

  /** The schema for {@link PojoWithNestedArray}. */
  public static final Schema POJO_WITH_ITERABLE =
      Schema.builder().addIterableField("strings", FieldType.STRING).build();

  /** A Pojo containing an enum type. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class PojoWithEnum {
    public enum Color {
      RED,
      GREEN,
      BLUE
    };

    public final Color color;
    public final List<Color> colors;

    @SchemaCreate
    public PojoWithEnum(Color color, List<Color> colors) {
      this.color = color;
      this.colors = colors;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PojoWithEnum that = (PojoWithEnum) o;
      return color == that.color && Objects.equals(colors, that.colors);
    }

    @Override
    public int hashCode() {
      return Objects.hash(color, colors);
    }
  }

  /** The schema for {@link PojoWithEnum}. */
  public static final EnumerationType ENUMERATION = EnumerationType.create("RED", "GREEN", "BLUE");

  public static final Schema POJO_WITH_ENUM_SCHEMA =
      Schema.builder()
          .addLogicalTypeField("color", ENUMERATION)
          .addArrayField("colors", FieldType.logicalType(ENUMERATION))
          .build();

  /** A simple POJO containing nullable basic types. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class NullablePOJO {
    @Nullable public String str;
    public @Nullable Byte aByte;
    public @Nullable Short aShort;
    public @Nullable Integer anInt;
    public @Nullable Long aLong;
    public @Nullable Boolean aBoolean;
    public @Nullable DateTime dateTime;
    public @Nullable Instant instant;
    public byte @Nullable [] bytes;
    public @Nullable ByteBuffer byteBuffer;
    public @Nullable BigDecimal bigDecimal;
    public @Nullable StringBuilder stringBuilder;

    public NullablePOJO() {}

    public NullablePOJO(
        String str,
        Byte aByte,
        Short aShort,
        Integer anInt,
        Long aLong,
        Boolean aBoolean,
        DateTime dateTime,
        Instant instant,
        byte[] bytes,
        ByteBuffer byteBuffer,
        BigDecimal bigDecimal,
        StringBuilder stringBuilder) {
      this.str = str;
      this.aByte = aByte;
      this.aShort = aShort;
      this.anInt = anInt;
      this.aLong = aLong;
      this.aBoolean = aBoolean;
      this.dateTime = dateTime;
      this.instant = instant;
      this.bytes = bytes;
      this.byteBuffer = byteBuffer;
      this.bigDecimal = bigDecimal;
      this.stringBuilder = stringBuilder;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NullablePOJO that = (NullablePOJO) o;
      return Objects.equals(aByte, that.aByte)
          && Objects.equals(aShort, that.aShort)
          && Objects.equals(anInt, that.anInt)
          && Objects.equals(aLong, that.aLong)
          && Objects.equals(aBoolean, that.aBoolean)
          && Objects.equals(str, that.str)
          && Objects.equals(dateTime, that.dateTime)
          && Objects.equals(instant, that.instant)
          && Arrays.equals(bytes, that.bytes)
          && Objects.equals(byteBuffer, that.byteBuffer)
          && Objects.equals(bigDecimal, that.bigDecimal)
          && Objects.equals(stringBuilder.toString(), that.stringBuilder.toString());
    }

    @Override
    public int hashCode() {
      int result =
          Objects.hash(
              str,
              aByte,
              aShort,
              anInt,
              aLong,
              aBoolean,
              dateTime,
              instant,
              byteBuffer,
              bigDecimal,
              stringBuilder);
      result = 31 * result + Arrays.hashCode(bytes);
      return result;
    }
  }

  /** The schema for {@link NullablePOJO}. * */
  public static final Schema NULLABLE_POJO_SCHEMA =
      Schema.builder()
          .addNullableField("str", FieldType.STRING)
          .addNullableField("aByte", FieldType.BYTE)
          .addNullableField("aShort", FieldType.INT16)
          .addNullableField("anInt", FieldType.INT32)
          .addNullableField("aLong", FieldType.INT64)
          .addNullableField("aBoolean", FieldType.BOOLEAN)
          .addNullableField("dateTime", FieldType.DATETIME)
          .addNullableField("instant", FieldType.DATETIME)
          .addNullableField("bytes", FieldType.BYTES)
          .addNullableField("byteBuffer", FieldType.BYTES)
          .addNullableField("bigDecimal", FieldType.DECIMAL)
          .addNullableField("stringBuilder", FieldType.STRING)
          .build();

  @DefaultSchema(JavaFieldSchema.class)
  @SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
  public static class PojoWithCaseFormat {
    public String user;
    public int ageInYears;

    @SchemaCaseFormat(CaseFormat.UPPER_CAMEL)
    public boolean knowsJavascript;

    @SchemaCreate
    public PojoWithCaseFormat(String user, int ageInYears, boolean knowsJavascript) {
      this.user = user;
      this.ageInYears = ageInYears;
      this.knowsJavascript = knowsJavascript;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PojoWithCaseFormat that = (PojoWithCaseFormat) o;
      return ageInYears == that.ageInYears
          && knowsJavascript == that.knowsJavascript
          && user.equals(that.user);
    }

    @Override
    public int hashCode() {
      return Objects.hash(user, ageInYears, knowsJavascript);
    }
  }

  /** The schema for {@link PojoWithCaseFormat}. * */
  public static final Schema CASE_FORMAT_POJO_SCHEMA =
      Schema.builder()
          .addField("user", FieldType.STRING)
          .addField("age_in_years", FieldType.INT32)
          .addField("KnowsJavascript", FieldType.BOOLEAN)
          .build();

  @DefaultSchema(JavaFieldSchema.class)
  public static class PojoNoCreateOption {
    public String user;

    // JavaFieldSchema will not try to use this constructor unless annotated with @SchemaCreate
    public PojoNoCreateOption(String user) {
      this.user = user;
    }
  }
}
