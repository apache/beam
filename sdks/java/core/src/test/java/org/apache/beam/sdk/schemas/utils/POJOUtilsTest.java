package org.apache.beam.sdk.schemas.utils;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.joda.time.DateTime;
import org.junit.Test;

public class POJOUtilsTest {
  static class SimplePojo {
    String str;
    byte aByte;
    short aShort;
    int anInt;
    long aLong;
    boolean aBoolean;
    DateTime dateTime;
    byte[] bytes1;
    ByteBuffer bytes2;
    List<Byte> bytes3;
  }
  static final Schema SIMPLE_SCHEMA = Schema.builder()
      .addStringField("str")
      .addByteField("aByte")
      .addInt16Field("aShort")
      .addInt32Field("anInt")
      .addInt64Field("aLong")
      .addBooleanField("aBoolean")
      .addDateTimeField("dateTime")
      .addByteArrayField("bytes1")
      .addByteArrayField("bytes2")
      .addByteArrayField("bytes3")
      .build();

  @Test
  public void testSimplePOJO() {
    Schema schema = POJOUtils.schemaFromClass(SimplePojo.class);
    assertEquals(SIMPLE_SCHEMA, schema);
  }

  static class NestedPojo {
    int anInt;
    SimplePojo nested;
  }

  @Test
  public void testNestedPOJO() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addRowField("nested", SIMPLE_SCHEMA)
        .build();
    Schema schema = POJOUtils.schemaFromClass(NestedPojo.class);
    assertEquals(expected, schema);
  }

  static class PrimitiveArrayPojo {
    int anInt;
    String[] strings;
  }

  @Test
  public void testPrimitiveArray() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addArrayField("strings", FieldType.STRING)
        .build();
    Schema schema = POJOUtils.schemaFromClass(PrimitiveArrayPojo.class);
    assertEquals(expected, schema);
  }

  static class NestedArrayPojo {
    int anInt;
    SimplePojo[] simples;
  }

  @Test
  public void testNestedArray() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addArrayField("simples", FieldType.row(SIMPLE_SCHEMA))
        .build();
    Schema schema = POJOUtils.schemaFromClass(NestedArrayPojo.class);
    assertEquals(expected, schema);
  }

  static class NestedCollectionPojo {
    int anInt;
    List<SimplePojo> simples;
  }

  @Test
  public void testNestedCollection() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addArrayField("simples", FieldType.row(SIMPLE_SCHEMA))
        .build();
    Schema schema = POJOUtils.schemaFromClass(NestedCollectionPojo.class);
    assertEquals(expected, schema);
  }

  static class PrimitiveMapPojo {
    int anInt;
    Map<String, Integer> map;
  }

  @Test
  public void testPrimitiveMap() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addMapField("map", FieldType.STRING, FieldType.INT32)
        .build();
    Schema schema = POJOUtils.schemaFromClass(PrimitiveMapPojo.class);
    assertEquals(expected, schema);
  }

  static class NestedMapPojo {
    int anInt;
    Map<String, SimplePojo> map;
  }

  @Test
  public void testNestedMap() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addMapField("map", FieldType.STRING, FieldType.row(SIMPLE_SCHEMA))
        .build();
    Schema schema = POJOUtils.schemaFromClass(NestedMapPojo.class);
    assertEquals(expected, schema);
  }
}
