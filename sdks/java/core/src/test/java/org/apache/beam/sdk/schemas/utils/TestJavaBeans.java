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
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/** Various Java Beans and associated schemas used in tests. */
public class TestJavaBeans {
  /** A Bean containing one nullable and one non-nullable type. */
  @DefaultSchema(JavaBeanSchema.class)
  public static class NullableBean {
    private @Nullable String str;
    private int anInt;

    public NullableBean() {}

    public @Nullable String getStr() {
      return str;
    }

    public void setStr(@Nullable String str) {
      this.str = str;
    }

    public int getAnInt() {
      return anInt;
    }

    public void setAnInt(int anInt) {
      this.anInt = anInt;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NullableBean that = (NullableBean) o;
      return anInt == that.anInt && Objects.equals(str, that.str);
    }

    @Override
    public int hashCode() {
      return Objects.hash(str, anInt);
    }
  }

  /** A Bean containing nullable getter but a non-nullable setter. */
  @DefaultSchema(JavaBeanSchema.class)
  public static class MismatchingNullableBean {
    private @Nullable String str;

    public MismatchingNullableBean() {}

    public @Nullable String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MismatchingNullableBean that = (MismatchingNullableBean) o;
      return Objects.equals(str, that.str);
    }

    @Override
    public int hashCode() {
      return Objects.hash(str);
    }
  }

  /** A simple Bean containing basic types. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class SimpleBean {
    private String str;
    private byte aByte;
    private short aShort;
    private int anInt;
    private long aLong;
    private boolean aBoolean;
    private DateTime dateTime;
    private Instant instant;
    private byte[] bytes;
    private ByteBuffer byteBuffer;
    private BigDecimal bigDecimal;
    private StringBuilder stringBuilder;

    public SimpleBean() {}

    public SimpleBean(
        String str,
        byte aByte,
        short aShort,
        int anInt,
        long aLong,
        boolean aBoolean,
        DateTime dateTime,
        Instant instant,
        byte[] bytes,
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
      this.byteBuffer = ByteBuffer.wrap(bytes);
      this.bigDecimal = bigDecimal;
      this.stringBuilder = stringBuilder;
    }

    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }

    public byte getaByte() {
      return aByte;
    }

    public void setaByte(byte aByte) {
      this.aByte = aByte;
    }

    public short getaShort() {
      return aShort;
    }

    public void setaShort(short aShort) {
      this.aShort = aShort;
    }

    public int getAnInt() {
      return anInt;
    }

    public void setAnInt(int anInt) {
      this.anInt = anInt;
    }

    public long getaLong() {
      return aLong;
    }

    public void setaLong(long aLong) {
      this.aLong = aLong;
    }

    public boolean isaBoolean() {
      return aBoolean;
    }

    public void setaBoolean(boolean aBoolean) {
      this.aBoolean = aBoolean;
    }

    public DateTime getDateTime() {
      return dateTime;
    }

    public void setDateTime(DateTime dateTime) {
      this.dateTime = dateTime;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
    }

    public ByteBuffer getByteBuffer() {
      return byteBuffer;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {
      this.byteBuffer = byteBuffer;
    }

    public Instant getInstant() {
      return instant;
    }

    public void setInstant(Instant instant) {
      this.instant = instant;
    }

    public BigDecimal getBigDecimal() {
      return bigDecimal;
    }

    public void setBigDecimal(BigDecimal bigDecimal) {
      this.bigDecimal = bigDecimal;
    }

    public StringBuilder getStringBuilder() {
      return stringBuilder;
    }

    public void setStringBuilder(StringBuilder stringBuilder) {
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
      SimpleBean that = (SimpleBean) o;
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

  /** The schema for {@link SimpleBean}. * */
  public static final Schema SIMPLE_BEAN_SCHEMA =
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

  /** A simple Bean containing basic nullable types. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class AllNullableBean {
    private @Nullable String str;
    private @Nullable Byte aByte;
    private @Nullable Short aShort;
    private @Nullable Integer anInt;
    private @Nullable Long aLong;
    private @Nullable Boolean aBoolean;
    private @Nullable DateTime dateTime;
    private @Nullable Instant instant;
    private byte @Nullable [] bytes;
    private @Nullable ByteBuffer byteBuffer;
    private @Nullable BigDecimal bigDecimal;
    private @Nullable StringBuilder stringBuilder;

    public AllNullableBean() {
      this.str = null;
      this.aByte = null;
      this.aShort = null;
      this.anInt = null;
      this.aLong = null;
      this.aBoolean = null;
      this.dateTime = null;
      this.instant = null;
      this.bytes = null;
      this.byteBuffer = null;
      this.bigDecimal = null;
      this.stringBuilder = null;
    }

    public @Nullable String getStr() {
      return str;
    }

    public void setStr(@Nullable String str) {
      this.str = str;
    }

    public @Nullable Byte getaByte() {
      return aByte;
    }

    public void setaByte(@Nullable Byte aByte) {
      this.aByte = aByte;
    }

    public @Nullable Short getaShort() {
      return aShort;
    }

    public void setaShort(@Nullable Short aShort) {
      this.aShort = aShort;
    }

    public @Nullable Integer getAnInt() {
      return anInt;
    }

    public void setAnInt(@Nullable Integer anInt) {
      this.anInt = anInt;
    }

    public @Nullable Long getaLong() {
      return aLong;
    }

    public void setaLong(@Nullable Long aLong) {
      this.aLong = aLong;
    }

    public @Nullable Boolean isaBoolean() {
      return aBoolean;
    }

    public void setaBoolean(@Nullable Boolean aBoolean) {
      this.aBoolean = aBoolean;
    }

    public @Nullable DateTime getDateTime() {
      return dateTime;
    }

    public void setDateTime(@Nullable DateTime dateTime) {
      this.dateTime = dateTime;
    }

    public byte @Nullable [] getBytes() {
      return bytes;
    }

    public void setBytes(byte @Nullable [] bytes) {
      this.bytes = bytes;
    }

    public @Nullable ByteBuffer getByteBuffer() {
      return byteBuffer;
    }

    public void setByteBuffer(@Nullable ByteBuffer byteBuffer) {
      this.byteBuffer = byteBuffer;
    }

    public @Nullable Instant getInstant() {
      return instant;
    }

    public void setInstant(@Nullable Instant instant) {
      this.instant = instant;
    }

    public @Nullable BigDecimal getBigDecimal() {
      return bigDecimal;
    }

    public void setBigDecimal(@Nullable BigDecimal bigDecimal) {
      this.bigDecimal = bigDecimal;
    }

    public @Nullable StringBuilder getStringBuilder() {
      return stringBuilder;
    }

    public void setStringBuilder(@Nullable StringBuilder stringBuilder) {
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
      SimpleBean that = (SimpleBean) o;
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

  /** The schema for {@link AllNullableBean}. * */
  public static final Schema ALL_NULLABLE_BEAN_SCHEMA =
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

  /** A simple Bean containing basic types. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class SimpleBeanWithAnnotations {
    private final String str;
    private final byte aByte;
    private final short aShort;
    private final int anInt;
    private final long aLong;
    private final boolean aBoolean;
    private final DateTime dateTime;
    private final Instant instant;
    private final byte[] bytes;
    private final ByteBuffer byteBuffer;
    private final BigDecimal bigDecimal;
    private final StringBuilder stringBuilder;

    @SchemaCreate
    public SimpleBeanWithAnnotations(
        String str,
        byte aByte,
        short aShort,
        int anInt,
        long aLong,
        boolean aBoolean,
        DateTime dateTime,
        Instant instant,
        byte[] bytes,
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
      this.byteBuffer = ByteBuffer.wrap(bytes);
      this.bigDecimal = bigDecimal;
      this.stringBuilder = stringBuilder;
    }

    @SchemaIgnore
    public String getUnknown() {
      return "";
    }

    public String getStr() {
      return str;
    }

    @SchemaFieldName("aByte")
    public byte getTheByteByte() {
      return aByte;
    }

    @SchemaFieldName("aShort")
    public short getNotAShort() {
      return aShort;
    }

    public int getAnInt() {
      return anInt;
    }

    public long getaLong() {
      return aLong;
    }

    public boolean isaBoolean() {
      return aBoolean;
    }

    public DateTime getDateTime() {
      return dateTime;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public ByteBuffer getByteBuffer() {
      return byteBuffer;
    }

    public Instant getInstant() {
      return instant;
    }

    public BigDecimal getBigDecimal() {
      return bigDecimal;
    }

    public StringBuilder getStringBuilder() {
      return stringBuilder;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SimpleBeanWithAnnotations that = (SimpleBeanWithAnnotations) o;
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
              stringBuilder.toString());
      result = 31 * result + Arrays.hashCode(bytes);
      return result;
    }
  }

  /** A Bean containing a nested class. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class NestedBean {
    private SimpleBean nested;

    public NestedBean(SimpleBean nested) {
      this.nested = nested;
    }

    public NestedBean() {}

    public SimpleBean getNested() {
      return nested;
    }

    public void setNested(SimpleBean nested) {
      this.nested = nested;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedBean that = (NestedBean) o;
      return Objects.equals(nested, that.nested);
    }

    @Override
    public int hashCode() {
      return Objects.hash(nested);
    }
  }

  /** The schema for {@link NestedBean}. * */
  public static final Schema NESTED_BEAN_SCHEMA =
      Schema.builder().addRowField("nested", SIMPLE_BEAN_SCHEMA).build();

  /** A Bean containing arrays of primitive types. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class PrimitiveArrayBean {
    // Test every type of array parameter supported.
    private List<String> strings;
    private int[] integers;
    private Long[] longs;

    public PrimitiveArrayBean() {}

    public PrimitiveArrayBean(List<String> strings, int[] integers, Long[] longs) {
      this.strings = strings;
      this.integers = integers;
      this.longs = longs;
    }

    public List<String> getStrings() {
      return strings;
    }

    public void setStrings(List<String> strings) {
      this.strings = strings;
    }

    public int[] getIntegers() {
      return integers;
    }

    public void setIntegers(int[] integers) {
      this.integers = integers;
    }

    public Long[] getLongs() {
      return longs;
    }

    public void setLongs(Long[] longs) {
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
      PrimitiveArrayBean that = (PrimitiveArrayBean) o;
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

  /** The schema for {@link PrimitiveArrayBean}. * */
  public static final Schema PRIMITIVE_ARRAY_BEAN_SCHEMA =
      Schema.builder()
          .addArrayField("strings", FieldType.STRING)
          .addArrayField("integers", FieldType.INT32)
          .addArrayField("longs", FieldType.INT64)
          .build();

  /** A Bean containing arrays of complex classes. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class NestedArrayBean {
    private SimpleBean[] beans;

    public NestedArrayBean(SimpleBean... beans) {
      this.beans = beans;
    }

    public NestedArrayBean() {}

    public SimpleBean[] getBeans() {
      return beans;
    }

    public void setBeans(SimpleBean[] beans) {
      this.beans = beans;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedArrayBean that = (NestedArrayBean) o;
      return Arrays.equals(beans, that.beans);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(beans);
    }
  }

  /** The schema for {@link NestedArrayBean}. * */
  public static final Schema NESTED_ARRAY_BEAN_SCHEMA =
      Schema.builder().addArrayField("beans", FieldType.row(SIMPLE_BEAN_SCHEMA)).build();

  /** A bean containing arrays of arrays. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class NestedArraysBean {
    private List<List<String>> lists;

    public NestedArraysBean(List<List<String>> lists) {
      this.lists = lists;
    }

    public NestedArraysBean() {}

    public List<List<String>> getLists() {
      return lists;
    }

    public void setLists(List<List<String>> lists) {
      this.lists = lists;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedArraysBean that = (NestedArraysBean) o;
      return Objects.equals(lists, that.lists);
    }

    @Override
    public int hashCode() {
      return Objects.hash(lists);
    }
  }

  /** The schema for {@link NestedArrayBean}. * */
  public static final Schema NESTED_ARRAYS_BEAM_SCHEMA =
      Schema.builder().addArrayField("lists", FieldType.array(FieldType.STRING)).build();

  /** A Bean containing a {@link List} of a complex type. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class NestedCollectionBean {
    private List<SimpleBean> simples;
    private Iterable<SimpleBean> iterableSimples;

    public NestedCollectionBean(List<SimpleBean> simples) {
      this.simples = simples;
      this.iterableSimples = simples;
    }

    public NestedCollectionBean() {}

    public List<SimpleBean> getSimples() {
      return simples;
    }

    public Iterable<SimpleBean> getIterableSimples() {
      return iterableSimples;
    }

    public void setSimples(List<SimpleBean> simples) {
      this.simples = simples;
    }

    public void setIterableSimples(Iterable<SimpleBean> iterableSimples) {
      this.iterableSimples = iterableSimples;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedCollectionBean that = (NestedCollectionBean) o;
      return Objects.equals(simples, that.simples)
          && Objects.equals(iterableSimples, that.iterableSimples);
    }

    @Override
    public int hashCode() {
      return Objects.hash(simples);
    }
  }

  /** The schema for {@link NestedCollectionBean}. * */
  public static final Schema NESTED_COLLECTION_BEAN_SCHEMA =
      Schema.builder()
          .addArrayField("simples", FieldType.row(SIMPLE_BEAN_SCHEMA))
          .addIterableField("iterableSimples", FieldType.row(SIMPLE_BEAN_SCHEMA))
          .build();

  /** A Bean containing a simple {@link Map}. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class PrimitiveMapBean {
    private Map<String, Integer> map;

    public PrimitiveMapBean(Map<String, Integer> map) {
      this.map = map;
    }

    public PrimitiveMapBean() {}

    public Map<String, Integer> getMap() {
      return map;
    }

    public void setMap(Map<String, Integer> map) {
      this.map = map;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PrimitiveMapBean that = (PrimitiveMapBean) o;
      return Objects.equals(map, that.map);
    }

    @Override
    public int hashCode() {
      return Objects.hash(map);
    }
  }

  /** The schema for {@link PrimitiveMapBean}. * */
  public static final Schema PRIMITIVE_MAP_BEAN_SCHEMA =
      Schema.builder().addMapField("map", FieldType.STRING, FieldType.INT32).build();

  /** A Bean containing a map of a complex type. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class NestedMapBean {
    private Map<String, SimpleBean> map;

    public NestedMapBean(Map<String, SimpleBean> map) {
      this.map = map;
    }

    public NestedMapBean() {}

    public Map<String, SimpleBean> getMap() {
      return map;
    }

    public void setMap(Map<String, SimpleBean> map) {
      this.map = map;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedMapBean that = (NestedMapBean) o;
      return Objects.equals(map, that.map);
    }

    @Override
    public int hashCode() {
      return Objects.hash(map);
    }
  }

  /** The schema for {@link NestedMapBean}. * */
  public static final Schema NESTED_MAP_BEAN_SCHEMA =
      Schema.builder()
          .addMapField("map", FieldType.STRING, FieldType.row(SIMPLE_BEAN_SCHEMA))
          .build();

  /** A Bean containing the boxed version of primitive types. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class BeanWithBoxedFields {
    private Byte aByte;
    private Short aShort;
    private Integer anInt;
    private Long aLong;
    private Boolean aBoolean;

    public BeanWithBoxedFields(
        Byte aByte, Short aShort, Integer anInt, Long aLong, Boolean aBoolean) {
      this.aByte = aByte;
      this.aShort = aShort;
      this.anInt = anInt;
      this.aLong = aLong;
      this.aBoolean = aBoolean;
    }

    public BeanWithBoxedFields() {}

    public Byte getaByte() {
      return aByte;
    }

    public void setaByte(Byte aByte) {
      this.aByte = aByte;
    }

    public Short getaShort() {
      return aShort;
    }

    public void setaShort(Short aShort) {
      this.aShort = aShort;
    }

    public Integer getAnInt() {
      return anInt;
    }

    public void setAnInt(Integer anInt) {
      this.anInt = anInt;
    }

    public Long getaLong() {
      return aLong;
    }

    public void setaLong(Long aLong) {
      this.aLong = aLong;
    }

    public Boolean getaBoolean() {
      return aBoolean;
    }

    public void setaBoolean(Boolean aBoolean) {
      this.aBoolean = aBoolean;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BeanWithBoxedFields that = (BeanWithBoxedFields) o;
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

  /** The schema for {@link BeanWithBoxedFields}. * */
  public static final Schema BEAN_WITH_BOXED_FIELDS_SCHEMA =
      Schema.builder()
          .addByteField("aByte")
          .addInt16Field("aShort")
          .addInt32Field("anInt")
          .addInt64Field("aLong")
          .addBooleanField("aBoolean")
          .build();

  /** A Bean containing byte arrays. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class BeanWithByteArray {
    private byte[] bytes1;
    private ByteBuffer bytes2;

    public BeanWithByteArray(byte[] bytes1, ByteBuffer bytes2) {
      this.bytes1 = bytes1;
      this.bytes2 = bytes2;
    }

    public BeanWithByteArray() {}

    public byte[] getBytes1() {
      return bytes1;
    }

    public void setBytes1(byte[] bytes1) {
      this.bytes1 = bytes1;
    }

    public ByteBuffer getBytes2() {
      return bytes2;
    }

    public void setBytes2(ByteBuffer bytes2) {
      this.bytes2 = bytes2;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BeanWithByteArray that = (BeanWithByteArray) o;
      return Arrays.equals(bytes1, that.bytes1) && Objects.equals(bytes2, that.bytes2);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(bytes2);
      result = 31 * result + Arrays.hashCode(bytes1);
      return result;
    }
  }

  /** The schema for {@link BeanWithByteArray}. * */
  public static final Schema BEAN_WITH_BYTE_ARRAY_SCHEMA =
      Schema.builder().addByteArrayField("bytes1").addByteArrayField("bytes2").build();

  /** A bean containing an Iterable. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class IterableBean {
    private Iterable<String> strings;

    public IterableBean(Iterable<String> strings) {
      this.strings = strings;
    }

    public IterableBean() {}

    public Iterable<String> getStrings() {
      return strings;
    }

    public void setStrings(Iterable<String> strings) {
      this.strings = strings;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IterableBean that = (IterableBean) o;
      return Objects.equals(strings, that.strings);
    }

    @Override
    public int hashCode() {
      return Objects.hash(strings);
    }
  }

  /** The schema for {@link NestedArrayBean}. * */
  public static final Schema ITERABLE_BEAM_SCHEMA =
      Schema.builder().addIterableField("strings", FieldType.STRING).build();

  /** A bean containing an Array of ByteArray. * */
  @DefaultSchema(JavaBeanSchema.class)
  public static class ArrayOfByteArray {
    private List<ByteBuffer> byteBuffers;

    public ArrayOfByteArray(List<ByteBuffer> byteBuffers) {
      this.byteBuffers = byteBuffers;
    }

    public ArrayOfByteArray() {}

    public List<ByteBuffer> getByteBuffers() {
      return byteBuffers;
    }

    public void setByteBuffers(List<ByteBuffer> byteBuffers) {
      this.byteBuffers = byteBuffers;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ArrayOfByteArray that = (ArrayOfByteArray) o;
      return Objects.equals(byteBuffers, that.byteBuffers);
    }

    @Override
    public int hashCode() {
      return Objects.hash(byteBuffers);
    }
  }

  /** The schema for {@link NestedArrayBean}. * */
  public static final Schema ARRAY_OF_BYTE_ARRAY_BEAM_SCHEMA =
      Schema.builder().addArrayField("byteBuffers", FieldType.BYTES).build();

  @DefaultSchema(JavaBeanSchema.class)
  @SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
  public static class BeanWithCaseFormat {
    private String user;
    private int ageInYears;
    private boolean knowsJavascript;

    @SchemaCreate
    public BeanWithCaseFormat(String user, int ageInYears, boolean knowsJavascript) {
      this.user = user;
      this.ageInYears = ageInYears;
      this.knowsJavascript = knowsJavascript;
    }

    public String getUser() {
      return user;
    }

    public int getAgeInYears() {
      return ageInYears;
    }

    @SchemaCaseFormat(CaseFormat.UPPER_CAMEL)
    public boolean getKnowsJavascript() {
      return knowsJavascript;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BeanWithCaseFormat that = (BeanWithCaseFormat) o;
      return ageInYears == that.ageInYears
          && knowsJavascript == that.knowsJavascript
          && Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
      return Objects.hash(user, ageInYears, knowsJavascript);
    }
  }

  public static final Schema CASE_FORMAT_BEAM_SCHEMA =
      Schema.builder()
          .addStringField("user")
          .addInt32Field("age_in_years")
          .addBooleanField("KnowsJavascript")
          .build();

  // A Bean that has no way for us to create an instance. Should throw an error during schema
  // generation.
  @DefaultSchema(JavaBeanSchema.class)
  public static class BeanWithNoCreateOption {
    private String str;

    public String getStr() {
      return str;
    }
  }

  // A Bean that has no @SchemaCreate, so it must be created with Setters.
  // It also renames fields, which has the potential to make us misidentify setters.
  @DefaultSchema(JavaBeanSchema.class)
  @SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
  public static class BeanWithRenamedFieldsAndSetters {
    private String user;
    private int ageInYears;
    private boolean knowsJavascript;

    @SchemaFieldName("username")
    public String getUser() {
      return user;
    }

    public int getAgeInYears() {
      return ageInYears;
    }

    @SchemaCaseFormat(CaseFormat.UPPER_CAMEL)
    public boolean getKnowsJavascript() {
      return knowsJavascript;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public void setAgeInYears(int ageInYears) {
      this.ageInYears = ageInYears;
    }

    public void setKnowsJavascript(boolean knowsJavascript) {
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
      BeanWithCaseFormat that = (BeanWithCaseFormat) o;
      return ageInYears == that.ageInYears
          && knowsJavascript == that.knowsJavascript
          && Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
      return Objects.hash(user, ageInYears, knowsJavascript);
    }
  }

  public static final Schema RENAMED_FIELDS_AND_SETTERS_BEAM_SCHEMA =
      Schema.builder()
          .addStringField("username")
          .addInt32Field("age_in_years")
          .addBooleanField("KnowsJavascript")
          .build();
}
