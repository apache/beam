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
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/** Various Java POJOs and associated schemas used in tests. */
public class TestPOJOs {
  /** A POJO containing one nullable and one non-nullable type. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJOWithNullables {
    @Nullable public String str;
    public int anInt;

    public POJOWithNullables(@Nullable String str, int anInt) {
      this.str = str;
      this.anInt = anInt;
    }

    public POJOWithNullables() {}

    @Override
    public boolean equals(Object o) {
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
    @Nullable public POJOWithNullables nested;

    public POJOWithNestedNullable(@Nullable POJOWithNullables nested) {
      this.nested = nested;
    }

    public POJOWithNestedNullable() {}

    @Override
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
          && Objects.equals(stringBuilder, that.stringBuilder);
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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

    public NestedCollectionPOJO(List<SimplePOJO> simples) {
      this.simples = simples;
    }

    public NestedCollectionPOJO() {}

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedCollectionPOJO that = (NestedCollectionPOJO) o;
      return Objects.equals(simples, that.simples);
    }

    @Override
    public int hashCode() {

      return Objects.hash(simples);
    }
  }

  /** The schema for {@link NestedCollectionPOJO}. * */
  public static final Schema NESTED_COLLECTION_POJO_SCHEMA =
      Schema.builder().addArrayField("simples", FieldType.row(SIMPLE_POJO_SCHEMA)).build();

  /** A POJO containing a simple {@link Map}. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class PrimitiveMapPOJO {
    public Map<String, Integer> map;

    public PrimitiveMapPOJO(Map<String, Integer> map) {
      this.map = map;
    }

    public PrimitiveMapPOJO() {}

    @Override
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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

  /** The schema for {@link POJOWithByteArray}. * */
  public static final Schema POJO_WITH_BYTE_ARRAY_SCHEMA =
      Schema.builder().addByteArrayField("bytes1").addByteArrayField("bytes2").build();
}
