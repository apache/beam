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
import org.apache.beam.sdk.schemas.DefaultSchema;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.joda.time.DateTime;
import org.joda.time.Instant;

/** Various Java Beans and associated schemas used in tests. */
public class TestJavaBeans {
  /** A Bean containing one nullable and one non-nullable type. */
  @DefaultSchema(JavaBeanSchema.class)
  public static class NullableBean {
    @Nullable private String str;
    private int anInt;

    public NullableBean() {}

    @Nullable
    public String getStr() {
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
    public boolean equals(Object o) {
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
    @Nullable private String str;

    public MismatchingNullableBean() {}

    @Nullable
    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }

    @Override
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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

    public NestedCollectionBean(List<SimpleBean> simples) {
      this.simples = simples;
    }

    public NestedCollectionBean() {}

    public List<SimpleBean> getSimples() {
      return simples;
    }

    public void setSimples(List<SimpleBean> simples) {
      this.simples = simples;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NestedCollectionBean that = (NestedCollectionBean) o;
      return Objects.equals(simples, that.simples);
    }

    @Override
    public int hashCode() {
      return Objects.hash(simples);
    }
  }

  /** The schema for {@link NestedCollectionBean}. * */
  public static final Schema NESTED_COLLECTION_BEAN_SCHEMA =
      Schema.builder().addArrayField("simples", FieldType.row(SIMPLE_BEAN_SCHEMA)).build();

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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
    public boolean equals(Object o) {
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
}
