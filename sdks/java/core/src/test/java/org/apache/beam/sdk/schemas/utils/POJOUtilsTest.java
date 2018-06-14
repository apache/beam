package org.apache.beam.sdk.schemas.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.reflect.FieldValueGetter;
import org.apache.beam.sdk.values.reflect.FieldValueSetter;
import org.joda.time.DateTime;
import org.junit.Test;

public class POJOUtilsTest {
  public static class SimplePojo {
    public String str;
    public byte aByte;
    public short aShort;
    public int anInt;
    public long aLong;
    public boolean aBoolean;
    public DateTime dateTime;
    public byte[] bytes1;
    public ByteBuffer bytes2;
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
    Schema schema = POJOUtils.schemaFromPojoClass(SimplePojo.class);
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
    Schema schema = POJOUtils.schemaFromPojoClass(NestedPojo.class);
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
    Schema schema = POJOUtils.schemaFromPojoClass(PrimitiveArrayPojo.class);
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
    Schema schema = POJOUtils.schemaFromPojoClass(NestedArrayPojo.class);
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
    Schema schema = POJOUtils.schemaFromPojoClass(NestedCollectionPojo.class);
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
    Schema schema = POJOUtils.schemaFromPojoClass(PrimitiveMapPojo.class);
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
    Schema schema = POJOUtils.schemaFromPojoClass(NestedMapPojo.class);
    assertEquals(expected, schema);
  }

  @Test
  public void testGeneratedSimpleGetters() {
    SimplePojo simplePojo = new SimplePojo();
    simplePojo.str = "field1";
    simplePojo.aByte = 41;
    simplePojo.aShort = 42;
    simplePojo.anInt = 43;
    simplePojo.aLong = 44;
    simplePojo.aBoolean = true;
    simplePojo.dateTime = DateTime.parse("1979-03-14");
    simplePojo.bytes1 = "bytes1".getBytes(Charset.defaultCharset());
    simplePojo.bytes2 = ByteBuffer.wrap("bytes2".getBytes(Charset.defaultCharset()));
    // simplePojo.bytes3 = Lists.<Byte>newArrayList().addAll("bytes3".getBytes()));

    List<FieldValueGetter> getters = POJOUtils.getGetters(SimplePojo.class);
    assertEquals(10, getters.size());
    assertEquals("str", getters.get(0).name());

    assertEquals("field1", getters.get(0).get(simplePojo));
    assertEquals((byte) 41, getters.get(1).get(simplePojo));
    assertEquals((short) 42, getters.get(2).get(simplePojo));
    assertEquals((int) 43, getters.get(3).get(simplePojo));
    assertEquals((long) 44, getters.get(4).get(simplePojo));
    assertEquals(true, getters.get(5).get(simplePojo));
    assertEquals(DateTime.parse("1979-03-14"), getters.get(6).get(simplePojo));
    assertArrayEquals("Unexpected bytes",
        "bytes1".getBytes(Charset.defaultCharset()),
        (byte[]) getters.get(7).get(simplePojo));
    assertEquals(ByteBuffer.wrap("bytes2".getBytes(Charset.defaultCharset())),
        getters.get(8).get(simplePojo));

  }

  @Test
  public void testGeneratedSimpleSetters() {
    SimplePojo simplePojo = new SimplePojo();
    List<FieldValueSetter> setters = POJOUtils.getSetters(SimplePojo.class);
    assertEquals(10, setters.size());

    setters.get(0).set(simplePojo, "field1");
    setters.get(1).set(simplePojo, (byte) 41);
    setters.get(2).set(simplePojo, (short) 42);
    setters.get(3).set(simplePojo, (int) 43);
    setters.get(4).set(simplePojo, (long) 44);
    setters.get(5).set(simplePojo, true);
    setters.get(6).set(simplePojo, DateTime.parse("1979-03-14"));
    setters.get(7).set(simplePojo, "bytes1".getBytes(Charset.defaultCharset()));
    setters.get(8).set(simplePojo, "bytes2".getBytes(Charset.defaultCharset()));

    assertEquals("field1", simplePojo.str);
    assertEquals((byte) 41, simplePojo.aByte);
    assertEquals((short) 42, simplePojo.aShort);
    assertEquals((int) 43, simplePojo.anInt);
    assertEquals((long) 44, simplePojo.aLong);
    assertEquals(true, simplePojo.aBoolean);
    assertEquals(DateTime.parse("1979-03-14"), simplePojo.dateTime);
    assertArrayEquals("Unexpected bytes",
        "bytes1".getBytes(Charset.defaultCharset()),
        simplePojo.bytes1);
    assertEquals(ByteBuffer.wrap("bytes2".getBytes(Charset.defaultCharset())),
        simplePojo.bytes2);
  }

  public static class PojoWithBoxedFields {
    public Byte aByte;
    public Short aShort;
    public Integer anInt;
    public Long aLong;
    public Boolean aBoolean;
  }

  @Test
  public void testGeneratedSimpleBoxedGetters() {
    PojoWithBoxedFields pojo = new PojoWithBoxedFields();
    pojo.aByte = 41;
    pojo.aShort = 42;
    pojo.anInt = 43;
    pojo.aLong = 44L;
    pojo.aBoolean = true;

    List<FieldValueGetter> getters = POJOUtils.getGetters(PojoWithBoxedFields.class);
    assertEquals((byte) 41, getters.get(0).get(pojo));
    assertEquals((short) 42, getters.get(1).get(pojo));
    assertEquals((int) 43, getters.get(2).get(pojo));
    assertEquals((long) 44, getters.get(3).get(pojo));
    assertEquals(true, getters.get(4).get(pojo));
  }

  @Test
  public void testGeneratedSimpleBoxedSetters() {
    PojoWithBoxedFields pojo = new PojoWithBoxedFields();
    List<FieldValueSetter> setters = POJOUtils.getSetters(PojoWithBoxedFields.class);

    setters.get(0).set(pojo, (byte) 41);
    setters.get(1).set(pojo, (short) 42);
    setters.get(2).set(pojo, (int) 43);
    setters.get(3).set(pojo, (long) 44);
    setters.get(4).set(pojo, true);

    assertEquals((byte) 41, pojo.aByte.byteValue());
    assertEquals((short) 42, pojo.aShort.shortValue());
    assertEquals((int) 43, pojo.anInt.intValue());
    assertEquals((long) 44, pojo.aLong.longValue());
    assertEquals(true, pojo.aBoolean.booleanValue());
  }

  public static class POJOWithByteArray {
    public byte[] bytes1;
    public ByteBuffer bytes2;
  }

  @Test
  public void testGeneratedByteBufferSetters() {
    POJOWithByteArray pojo = new POJOWithByteArray();
    List<FieldValueSetter> setters = POJOUtils.getSetters(POJOWithByteArray.class);
    setters.get(0).set(pojo, "field1".getBytes(Charset.defaultCharset()));
    setters.get(1).set(pojo, "field2".getBytes(Charset.defaultCharset()));

    assertArrayEquals("not equal", "field1".getBytes(Charset.defaultCharset()), pojo.bytes1);
    assertEquals(ByteBuffer.wrap("field2".getBytes(Charset.defaultCharset())), pojo.bytes2);
  }
}
