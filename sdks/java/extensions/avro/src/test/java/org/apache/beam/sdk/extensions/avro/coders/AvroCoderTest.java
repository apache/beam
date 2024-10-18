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
package org.apache.beam.sdk.extensions.avro.coders;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Stringable;
import org.apache.avro.reflect.Union;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory;
import org.apache.beam.sdk.extensions.avro.schemas.TestAvro;
import org.apache.beam.sdk.extensions.avro.schemas.TestAvroConversion;
import org.apache.beam.sdk.extensions.avro.schemas.TestAvroConversionFactory;
import org.apache.beam.sdk.extensions.avro.schemas.TestAvroFactory;
import org.apache.beam.sdk.extensions.avro.schemas.TestAvroNested;
import org.apache.beam.sdk.extensions.avro.schemas.TestEnum;
import org.apache.beam.sdk.extensions.avro.schemas.fixed4;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.InterceptingUrlClassLoader;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.objenesis.strategy.StdInstantiatorStrategy;

/** Tests for {@link AvroCoder}. */
@RunWith(JUnit4.class)
public class AvroCoderTest {

  public static final DateTime DATETIME_A =
      new DateTime().withDate(1994, 10, 31).withZone(DateTimeZone.UTC);
  public static final DateTime DATETIME_B =
      new DateTime().withDate(1997, 4, 25).withZone(DateTimeZone.UTC);
  private static final TestAvroNested AVRO_NESTED_SPECIFIC_RECORD = new TestAvroNested(true, 42);
  private static final TestAvro AVRO_SPECIFIC_RECORD =
      TestAvroFactory.newInstance(
          true,
          43,
          44L,
          44.1f,
          44.2d,
          "mystring",
          ByteBuffer.wrap(new byte[] {1, 2, 3, 4}),
          new fixed4(new byte[] {1, 2, 3, 4}),
          new LocalDate(1979, 3, 14),
          new DateTime().withDate(1979, 3, 14).withTime(1, 2, 3, 4),
          TestEnum.abc,
          AVRO_NESTED_SPECIFIC_RECORD,
          ImmutableList.of(AVRO_NESTED_SPECIFIC_RECORD, AVRO_NESTED_SPECIFIC_RECORD),
          ImmutableMap.of("k1", AVRO_NESTED_SPECIFIC_RECORD, "k2", AVRO_NESTED_SPECIFIC_RECORD));

  private static final String VERSION_AVRO = Schema.class.getPackage().getImplementationVersion();

  @DefaultCoder(AvroCoder.class)
  private static class Pojo {
    public String text;
    public int count;

    @AvroSchema("{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}")
    public DateTime timestamp;

    // Empty constructor required for Avro decoding.
    @SuppressWarnings("unused")
    public Pojo() {}

    public Pojo(String text, int count, DateTime timestamp) {
      this.text = text;
      this.count = count;
      this.timestamp = timestamp;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      Pojo that = (Pojo) other;
      return this.count == that.count
          && Objects.equals(this.text, that.text)
          && Objects.equals(this.timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
      return Objects.hash(text, count, timestamp);
    }

    @Override
    public String toString() {
      return "Pojo{"
          + "text='"
          + text
          + '\''
          + ", count="
          + count
          + ", timestamp="
          + timestamp
          + '}';
    }
  }

  private static class GetTextFn extends DoFn<Pojo, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().text);
    }
  }

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testAvroCoderEncoding() throws Exception {
    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);
    CoderProperties.coderSerializable(coder);
    AvroCoder<Pojo> copy = SerializableUtils.clone(coder);

    Pojo pojo = new Pojo("foo", 3, DATETIME_A);
    Pojo equalPojo = new Pojo("foo", 3, DATETIME_A);
    Pojo otherPojo = new Pojo("bar", -19, DATETIME_B);
    CoderProperties.coderConsistentWithEquals(coder, pojo, equalPojo);
    CoderProperties.coderConsistentWithEquals(copy, pojo, equalPojo);
    CoderProperties.coderConsistentWithEquals(coder, pojo, otherPojo);
    CoderProperties.coderConsistentWithEquals(copy, pojo, otherPojo);
  }

  /**
   * Tests that {@link AvroCoder} works around issues in Avro where cache classes might be from the
   * wrong ClassLoader, causing confusing "Cannot cast X to X" error messages.
   */
  @SuppressWarnings("ReturnValueIgnored")
  @Test
  public void testTwoClassLoaders() throws Exception {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader loader1 =
        new InterceptingUrlClassLoader(contextClassLoader, AvroCoderTestPojo.class.getName());
    ClassLoader loader2 =
        new InterceptingUrlClassLoader(contextClassLoader, AvroCoderTestPojo.class.getName());

    Class<?> pojoClass1 = loader1.loadClass(AvroCoderTestPojo.class.getName());
    Class<?> pojoClass2 = loader2.loadClass(AvroCoderTestPojo.class.getName());

    Object pojo1 = InstanceBuilder.ofType(pojoClass1).withArg(String.class, "hello").build();
    Object pojo2 = InstanceBuilder.ofType(pojoClass2).withArg(String.class, "goodbye").build();

    // Confirm incompatibility
    try {
      pojoClass2.cast(pojo1);
      fail("Expected ClassCastException; without it, this test is vacuous");
    } catch (ClassCastException e) {
      // g2g
    }

    // The first coder is expected to populate the Avro SpecificData cache
    // The second coder is expected to be corrupted if the caching is done wrong.
    AvroCoder<Object> avroCoder1 = (AvroCoder) AvroCoder.of(pojoClass1);
    AvroCoder<Object> avroCoder2 = (AvroCoder) AvroCoder.of(pojoClass2);

    Object cloned1 = CoderUtils.clone(avroCoder1, pojo1);
    Object cloned2 = CoderUtils.clone(avroCoder2, pojo2);

    // Confirming that the uncorrupted coder is fine
    pojoClass1.cast(cloned1);

    // Confirmed to fail prior to the fix
    pojoClass2.cast(cloned2);
  }

  /**
   * Confirm that we can serialize and deserialize an AvroCoder object and still decode after.
   * (https://github.com/apache/beam/issues/18022).
   *
   * @throws Exception
   */
  @Test
  public void testTransientFieldInitialization() throws Exception {
    Pojo value = new Pojo("Hello", 42, DATETIME_A);
    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);

    // Serialization of object
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bos);
    out.writeObject(coder);

    // De-serialization of object
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ObjectInputStream in = new ObjectInputStream(bis);
    AvroCoder<Pojo> copied = (AvroCoder<Pojo>) in.readObject();

    CoderProperties.coderDecodeEncodeEqual(copied, value);
  }

  /**
   * Confirm that we can serialize and deserialize an AvroCoder object using Kryo. (BEAM-626).
   *
   * @throws Exception
   */
  @Test
  public void testKryoSerialization() throws Exception {
    Pojo value = new Pojo("Hello", 42, DATETIME_A);
    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);

    // Kryo instantiation
    Kryo kryo = new Kryo();
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
    kryo.addDefaultSerializer(AvroCoder.SerializableSchemaSupplier.class, JavaSerializer.class);

    // Serialization of object without any memoization
    ByteArrayOutputStream coderWithoutMemoizationBos = new ByteArrayOutputStream();
    try (Output output = new Output(coderWithoutMemoizationBos)) {
      kryo.writeClassAndObject(output, coder);
    }

    // Force thread local memoization to store values.
    CoderProperties.coderDecodeEncodeEqual(coder, value);

    // Serialization of object with memoized fields
    ByteArrayOutputStream coderWithMemoizationBos = new ByteArrayOutputStream();
    try (Output output = new Output(coderWithMemoizationBos)) {
      kryo.writeClassAndObject(output, coder);
    }

    // Copy empty and memoized variants of the Coder
    ByteArrayInputStream bisWithoutMemoization =
        new ByteArrayInputStream(coderWithoutMemoizationBos.toByteArray());
    AvroCoder<Pojo> copiedWithoutMemoization =
        (AvroCoder<Pojo>) kryo.readClassAndObject(new Input(bisWithoutMemoization));
    ByteArrayInputStream bisWithMemoization =
        new ByteArrayInputStream(coderWithMemoizationBos.toByteArray());
    AvroCoder<Pojo> copiedWithMemoization =
        (AvroCoder<Pojo>) kryo.readClassAndObject(new Input(bisWithMemoization));

    CoderProperties.coderDecodeEncodeEqual(copiedWithoutMemoization, value);
    CoderProperties.coderDecodeEncodeEqual(copiedWithMemoization, value);
  }

  @Test
  public void testPojoEncoding() throws Exception {
    Pojo value = new Pojo("Hello", 42, DATETIME_A);
    AvroCoder<Pojo> coder = AvroCoder.reflect(Pojo.class);

    CoderProperties.coderDecodeEncodeEqual(coder, value);
  }

  @Test
  public void testSpecificRecordEncoding() throws Exception {
    // Don't compare the map values because of AVRO-2943
    AVRO_SPECIFIC_RECORD.setMap(ImmutableMap.of());

    AvroCoder<TestAvro> coder = AvroCoder.specific(TestAvro.class);
    AvroCoder<TestAvro> coderWithSchema =
        AvroCoder.specific(TestAvro.class, AVRO_SPECIFIC_RECORD.getSchema());

    assertTrue(SpecificRecord.class.isAssignableFrom(coder.getType()));
    assertTrue(SpecificRecord.class.isAssignableFrom(coderWithSchema.getType()));

    CoderProperties.coderDecodeEncodeEqual(coder, AVRO_SPECIFIC_RECORD);
    CoderProperties.coderDecodeEncodeEqual(coderWithSchema, AVRO_SPECIFIC_RECORD);
  }

  // example to overcome AVRO-2943 limitation with custom datum factory
  // force usage of String instead of Utf8 when avro type is set to CharSequence
  static class CustomSpecificDatumFactory<T> extends AvroDatumFactory.SpecificDatumFactory<T> {

    private static class CustomSpecificDatumReader<T> extends SpecificDatumReader<T> {
      CustomSpecificDatumReader(Class<T> c) {
        super(c);
      }

      // always use String instead of CharSequence
      @Override
      protected Class<?> findStringClass(Schema schema) {
        final Class<?> stringClass = super.findStringClass(schema);
        return stringClass == CharSequence.class ? String.class : stringClass;
      }
    }

    CustomSpecificDatumFactory(Class<T> type) {
      super(type);
    }

    @Override
    public DatumReader<T> apply(Schema writer, Schema reader) {
      CustomSpecificDatumReader<T> datumReader = new CustomSpecificDatumReader<>(this.type);
      datumReader.setExpected(reader);
      datumReader.setSchema(writer);
      return datumReader;
    }
  }

  @Test
  public void testCustomRecordEncoding() throws Exception {
    AvroCoder<TestAvro> coder =
        AvroCoder.of(
            new CustomSpecificDatumFactory<>(TestAvro.class), AVRO_SPECIFIC_RECORD.getSchema());
    assertTrue(SpecificRecord.class.isAssignableFrom(coder.getType()));
    CoderProperties.coderDecodeEncodeEqual(coder, AVRO_SPECIFIC_RECORD);
  }

  @Test
  public void testSpecificRecordConversionEncoding() throws Exception {
    TestAvroConversion record =
        TestAvroConversionFactory.newInstance(new org.joda.time.LocalDate(1979, 3, 14));
    AvroCoder<TestAvroConversion> coder = AvroCoder.specific(TestAvroConversion.class);
    AvroCoder<TestAvroConversion> coderWithSchema =
        AvroCoder.specific(TestAvroConversion.class, record.getSchema());

    assertTrue(SpecificRecord.class.isAssignableFrom(coder.getType()));
    assertTrue(SpecificRecord.class.isAssignableFrom(coderWithSchema.getType()));

    try {
      CoderProperties.coderDecodeEncodeEqual(coder, record);
      CoderProperties.coderDecodeEncodeEqual(coderWithSchema, record);
    } catch (org.apache.avro.AvroRuntimeException e) {
      if (VERSION_AVRO.equals("1.8.2")) {
        // it is expected to fail in avro 1.8.2 but pass for other versions
        // https://issues.apache.org/jira/browse/AVRO-1891
        assertEquals("Unknown datum type org.joda.time.LocalDate: 1979-03-14", e.getMessage());
      } else {
        throw e;
      }
    }
  }

  @Test
  public void testReflectRecordEncoding() throws Exception {
    AvroCoder<TestAvro> coder = AvroCoder.reflect(TestAvro.class);
    AvroCoder<TestAvro> coderWithSchema =
        AvroCoder.reflect(TestAvro.class, AVRO_SPECIFIC_RECORD.getSchema());

    assertTrue(SpecificRecord.class.isAssignableFrom(coder.getType()));
    assertTrue(SpecificRecord.class.isAssignableFrom(coderWithSchema.getType()));

    CoderProperties.coderDecodeEncodeEqual(coder, AVRO_SPECIFIC_RECORD);
    CoderProperties.coderDecodeEncodeEqual(coderWithSchema, AVRO_SPECIFIC_RECORD);
  }

  @Test
  public void testGenericRecordEncoding() throws Exception {
    String schemaString =
        "{\"namespace\": \"example.avro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"User\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"name\", \"type\": \"string\"},\n"
            + "     {\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},\n"
            + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
            + " ]\n"
            + "}";
    Schema schema = new Schema.Parser().parse(schemaString);

    GenericRecord before = new GenericData.Record(schema);
    before.put("name", "Bob");
    before.put("favorite_number", 256);
    // Leave favorite_color null

    AvroCoder<GenericRecord> coder = AvroCoder.generic(schema);

    CoderProperties.coderDecodeEncodeEqual(coder, before);
    assertEquals(schema, coder.getSchema());
  }

  @Test
  public void testEncodingNotBuffered() throws Exception {
    // This test ensures that the coder doesn't read ahead and buffer data.
    // Reading ahead causes a problem if the stream consists of records of different
    // types.
    Pojo before = new Pojo("Hello", 42, DATETIME_A);

    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);
    SerializableCoder<Integer> intCoder = SerializableCoder.of(Integer.class);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    Context context = Context.NESTED;
    coder.encode(before, outStream, context);
    intCoder.encode(10, outStream, context);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());

    Pojo after = coder.decode(inStream, context);
    assertEquals(before, after);

    Integer intAfter = intCoder.decode(inStream, context);
    assertEquals(Integer.valueOf(10), intAfter);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDefaultCoder() throws Exception {
    // Use MyRecord as input and output types without explicitly specifying
    // a coder (this uses the default coders, which may not be AvroCoder).
    PCollection<String> output =
        pipeline
            .apply(Create.of(new Pojo("hello", 1, DATETIME_A), new Pojo("world", 2, DATETIME_B)))
            .apply(ParDo.of(new GetTextFn()));

    PAssert.that(output).containsInAnyOrder("hello", "world");
    pipeline.run();
  }

  @Test
  public void testAvroSpecificCoderIsSerializable() throws Exception {
    AvroCoder<TestAvro> coder = AvroCoder.specific(TestAvro.class);

    // Check that the coder is serializable using the regular JSON approach.
    SerializableUtils.ensureSerializable(coder);
  }

  @Test
  public void testAvroReflectCoderIsSerializable() throws Exception {
    AvroCoder<Pojo> coder = AvroCoder.reflect(Pojo.class);

    // Check that the coder is serializable using the regular JSON approach.
    SerializableUtils.ensureSerializable(coder);
  }

  private void assertDeterministic(AvroCoder<?> coder) {
    try {
      coder.verifyDeterministic();
    } catch (NonDeterministicException e) {
      fail("Expected " + coder + " to be deterministic, but got:\n" + e);
    }
  }

  private void assertNonDeterministic(AvroCoder<?> coder, Matcher<String> reason1) {
    try {
      coder.verifyDeterministic();
      fail("Expected " + coder + " to be non-deterministic.");
    } catch (NonDeterministicException e) {
      assertThat(e.getReasons(), Matchers.iterableWithSize(1));
      assertThat(e.getReasons(), Matchers.contains(reason1));
    }
  }

  @Test
  public void testDeterministicInteger() {
    assertDeterministic(AvroCoder.of(Integer.class));
  }

  @Test
  public void testDeterministicInt() {
    assertDeterministic(AvroCoder.of(int.class));
  }

  private static class SimpleDeterministicClass {
    @SuppressWarnings("unused")
    private Integer intField;

    @SuppressWarnings("unused")
    private char charField;

    @SuppressWarnings("unused")
    private Integer[] intArray;

    @SuppressWarnings("unused")
    private Utf8 utf8field;
  }

  @Test
  public void testDeterministicSimple() {
    assertDeterministic(AvroCoder.of(SimpleDeterministicClass.class));
  }

  private static class UnorderedMapClass {
    @SuppressWarnings("unused")
    private Map<String, String> mapField;
  }

  private Matcher<String> reason(final String prefix, final String messagePart) {
    return new TypeSafeMatcher<String>(String.class) {
      @Override
      public void describeTo(Description description) {
        description.appendText(
            String.format("Reason starting with '%s:' containing '%s'", prefix, messagePart));
      }

      @Override
      protected boolean matchesSafely(String item) {
        return item.startsWith(prefix + ":") && item.contains(messagePart);
      }
    };
  }

  private Matcher<String> reasonClass(Class<?> clazz, String message) {
    return reason(clazz.getName(), message);
  }

  private Matcher<String> reasonField(Class<?> clazz, String field, String message) {
    return reason(clazz.getName() + "#" + field, message);
  }

  @Test
  public void testDeterministicUnorderedMap() {
    assertNonDeterministic(
        AvroCoder.of(UnorderedMapClass.class),
        reasonField(
            UnorderedMapClass.class,
            "mapField",
            "java.util.Map<java.lang.String, java.lang.String> "
                + "may not be deterministically ordered"));
  }

  private static class NonDeterministicArray {
    @SuppressWarnings("unused")
    private UnorderedMapClass[] arrayField;
  }

  @Test
  public void testDeterministicNonDeterministicArray() {
    assertNonDeterministic(
        AvroCoder.of(NonDeterministicArray.class),
        reasonField(
            UnorderedMapClass.class,
            "mapField",
            "java.util.Map<java.lang.String, java.lang.String>"
                + " may not be deterministically ordered"));
  }

  private static class SubclassOfUnorderedMapClass extends UnorderedMapClass {}

  @Test
  public void testDeterministicNonDeterministicChild() {
    // Super class has non deterministic fields.
    assertNonDeterministic(
        AvroCoder.of(SubclassOfUnorderedMapClass.class),
        reasonField(UnorderedMapClass.class, "mapField", "may not be deterministically ordered"));
  }

  private static class SubclassHidingParent extends UnorderedMapClass {
    @SuppressWarnings("unused")
    @AvroName("mapField2") // AvroName is not enough
    private int mapField;
  }

  @Test
  public void testAvroProhibitsShadowing() {
    // This test verifies that Avro won't serialize a class with two fields of
    // the same name. This is important for our error reporting, and also how
    // we lookup a field.
    try {
      ReflectData.get().getSchema(SubclassHidingParent.class);
      fail("Expected AvroTypeException");
    } catch (AvroRuntimeException e) {
      assertThat(e.getMessage(), containsString("mapField"));
      assertThat(e.getMessage(), containsString("two fields named"));
    }
  }

  private static class FieldWithAvroName {
    @AvroName("name")
    @SuppressWarnings("unused")
    private int someField;
  }

  @Test
  public void testDeterministicWithAvroName() {
    assertDeterministic(AvroCoder.of(FieldWithAvroName.class));
  }

  @Test
  public void testDeterminismSortedMap() {
    assertDeterministic(AvroCoder.of(StringSortedMapField.class));
  }

  private static class StringSortedMapField {
    @SuppressWarnings("unused")
    SortedMap<String, String> sortedMapField;
  }

  @Test
  public void testDeterminismTreeMapValue() {
    // The value is non-deterministic, so we should fail.
    assertNonDeterministic(
        AvroCoder.of(TreeMapNonDetValue.class),
        reasonField(
            UnorderedMapClass.class,
            "mapField",
            "java.util.Map<java.lang.String, java.lang.String> "
                + "may not be deterministically ordered"));
  }

  private static class TreeMapNonDetValue {
    @SuppressWarnings("unused")
    TreeMap<String, NonDeterministicArray> nonDeterministicField;
  }

  @Test
  public void testDeterminismUnorderedMap() {
    // LinkedHashMap is not deterministically ordered, so we should fail.
    assertNonDeterministic(
        AvroCoder.of(LinkedHashMapField.class),
        reasonField(
            LinkedHashMapField.class,
            "nonDeterministicMap",
            "java.util.LinkedHashMap<java.lang.String, java.lang.String> "
                + "may not be deterministically ordered"));
  }

  private static class LinkedHashMapField {
    @SuppressWarnings("unused")
    LinkedHashMap<String, String> nonDeterministicMap;
  }

  @Test
  public void testDeterminismCollection() {
    assertNonDeterministic(
        AvroCoder.of(StringCollection.class),
        reasonField(
            StringCollection.class,
            "stringCollection",
            "java.util.Collection<java.lang.String> may not be deterministically ordered"));
  }

  private static class StringCollection {
    @SuppressWarnings("unused")
    Collection<String> stringCollection;
  }

  @Test
  public void testDeterminismList() {
    assertDeterministic(AvroCoder.of(StringList.class));
    assertDeterministic(AvroCoder.of(StringArrayList.class));
  }

  private static class StringList {
    @SuppressWarnings("unused")
    List<String> stringCollection;
  }

  private static class StringArrayList {
    @SuppressWarnings("unused")
    ArrayList<String> stringCollection;
  }

  @Test
  public void testDeterminismSet() {
    assertDeterministic(AvroCoder.of(StringSortedSet.class));
    assertDeterministic(AvroCoder.of(StringTreeSet.class));
    assertNonDeterministic(
        AvroCoder.of(StringHashSet.class),
        reasonField(
            StringHashSet.class,
            "stringCollection",
            "java.util.HashSet<java.lang.String> may not be deterministically ordered"));
  }

  private static class StringSortedSet {
    @SuppressWarnings("unused")
    SortedSet<String> stringCollection;
  }

  private static class StringTreeSet {
    @SuppressWarnings("unused")
    TreeSet<String> stringCollection;
  }

  private static class StringHashSet {
    @SuppressWarnings("unused")
    HashSet<String> stringCollection;
  }

  @Test
  public void testDeterminismCollectionValue() {
    assertNonDeterministic(
        AvroCoder.of(OrderedSetOfNonDetValues.class),
        reasonField(UnorderedMapClass.class, "mapField", "may not be deterministically ordered"));
    assertNonDeterministic(
        AvroCoder.of(ListOfNonDetValues.class),
        reasonField(UnorderedMapClass.class, "mapField", "may not be deterministically ordered"));
  }

  private static class OrderedSetOfNonDetValues {
    @SuppressWarnings("unused")
    SortedSet<UnorderedMapClass> set;
  }

  private static class ListOfNonDetValues {
    @SuppressWarnings("unused")
    List<UnorderedMapClass> set;
  }

  @Test
  public void testDeterminismUnion() {
    assertDeterministic(AvroCoder.of(DeterministicUnionBase.class));
    assertNonDeterministic(
        AvroCoder.of(NonDeterministicUnionBase.class),
        reasonField(UnionCase3.class, "mapField", "may not be deterministically ordered"));
  }

  @Test
  public void testDeterminismStringable() {
    assertDeterministic(AvroCoder.of(String.class));
    assertNonDeterministic(
        AvroCoder.of(StringableClass.class),
        reasonClass(StringableClass.class, "may not have deterministic #toString()"));
  }

  @Stringable
  private static class StringableClass {}

  @Test
  public void testDeterminismCyclicClass() {
    assertNonDeterministic(
        AvroCoder.of(Cyclic.class),
        reasonField(Cyclic.class, "cyclicField", "appears recursively"));
    assertNonDeterministic(
        AvroCoder.of(CyclicField.class),
        reasonField(Cyclic.class, "cyclicField", Cyclic.class.getName() + " appears recursively"));
    assertNonDeterministic(
        AvroCoder.of(IndirectCycle1.class),
        reasonField(
            IndirectCycle2.class,
            "field2",
            IndirectCycle1.class.getName() + " appears recursively"));
  }

  private static class Cyclic {
    @SuppressWarnings("unused")
    int intField;

    @SuppressWarnings("unused")
    Cyclic cyclicField;
  }

  private static class CyclicField {
    @SuppressWarnings("unused")
    Cyclic cyclicField2;
  }

  private static class IndirectCycle1 {
    @SuppressWarnings("unused")
    IndirectCycle2 field1;
  }

  private static class IndirectCycle2 {
    @SuppressWarnings("unused")
    IndirectCycle1 field2;
  }

  @Test
  public void testDeterminismHasGenericRecord() {
    assertDeterministic(AvroCoder.of(HasGenericRecord.class));
  }

  private static class HasGenericRecord {
    @AvroSchema(
        "{\"name\": \"bar\", \"type\": \"record\", \"fields\": ["
            + "{\"name\": \"foo\", \"type\": \"int\"}]}")
    GenericRecord genericRecord;
  }

  @Test
  public void testDeterminismHasCustomSchema() {
    assertNonDeterministic(
        AvroCoder.of(HasCustomSchema.class),
        reasonField(
            HasCustomSchema.class,
            "withCustomSchema",
            "Custom schemas are only supported for subtypes of IndexedRecord."));
  }

  private static class HasCustomSchema {
    @AvroSchema(
        "{\"name\": \"bar\", \"type\": \"record\", \"fields\": ["
            + "{\"name\": \"foo\", \"type\": \"int\"}]}")
    int withCustomSchema;
  }

  @Test
  public void testAvroCoderTreeMapDeterminism() throws Exception, NonDeterministicException {
    TreeMapField size1 = new TreeMapField();
    TreeMapField size2 = new TreeMapField();

    // Different order for entries
    size1.field.put("hello", "world");
    size1.field.put("another", "entry");

    size2.field.put("another", "entry");
    size2.field.put("hello", "world");

    AvroCoder<TreeMapField> coder = AvroCoder.of(TreeMapField.class);
    coder.verifyDeterministic();

    ByteArrayOutputStream outStream1 = new ByteArrayOutputStream();
    ByteArrayOutputStream outStream2 = new ByteArrayOutputStream();

    Context context = Context.NESTED;
    coder.encode(size1, outStream1, context);
    coder.encode(size2, outStream2, context);

    assertArrayEquals(outStream1.toByteArray(), outStream2.toByteArray());
  }

  private static class TreeMapField {
    private TreeMap<String, String> field = new TreeMap<>();
  }

  @Union({UnionCase1.class, UnionCase2.class})
  private abstract static class DeterministicUnionBase {}

  @Union({UnionCase1.class, UnionCase2.class, UnionCase3.class})
  private abstract static class NonDeterministicUnionBase {}

  private static class UnionCase1 extends DeterministicUnionBase {}

  private static class UnionCase2 extends DeterministicUnionBase {
    @SuppressWarnings("unused")
    String field;
  }

  private static class UnionCase3 extends NonDeterministicUnionBase {
    @SuppressWarnings("unused")
    private Map<String, String> mapField;
  }

  @Test
  public void testAvroCoderSimpleSchemaDeterminism() {
    assertDeterministic(AvroCoder.of(SchemaBuilder.record("someRecord").fields().endRecord()));
    assertDeterministic(
        AvroCoder.of(
            SchemaBuilder.record("someRecord")
                .fields()
                .name("int")
                .type()
                .intType()
                .noDefault()
                .endRecord()));
    assertDeterministic(
        AvroCoder.of(
            SchemaBuilder.record("someRecord")
                .fields()
                .name("string")
                .type()
                .stringType()
                .noDefault()
                .endRecord()));

    assertNonDeterministic(
        AvroCoder.of(
            SchemaBuilder.record("someRecord")
                .fields()
                .name("map")
                .type()
                .map()
                .values()
                .stringType()
                .noDefault()
                .endRecord()),
        reason("someRecord.map", "HashMap to represent MAPs"));

    assertDeterministic(
        AvroCoder.of(
            SchemaBuilder.record("someRecord")
                .fields()
                .name("array")
                .type()
                .array()
                .items()
                .stringType()
                .noDefault()
                .endRecord()));

    assertDeterministic(
        AvroCoder.of(
            SchemaBuilder.record("someRecord")
                .fields()
                .name("enum")
                .type()
                .enumeration("anEnum")
                .symbols("s1", "s2")
                .enumDefault("s1")
                .endRecord()));

    assertDeterministic(
        AvroCoder.of(
            SchemaBuilder.unionOf()
                .intType()
                .and()
                .record("someRecord")
                .fields()
                .nullableString("someField", "")
                .endRecord()
                .endUnion()));
  }

  @Test
  public void testAvroCoderStrings() {
    // Custom Strings in Records
    assertDeterministic(
        AvroCoder.of(
            SchemaBuilder.record("someRecord")
                .fields()
                .name("string")
                .prop(SpecificData.CLASS_PROP, "java.lang.String")
                .type()
                .stringType()
                .noDefault()
                .endRecord()));
    assertNonDeterministic(
        AvroCoder.of(
            SchemaBuilder.record("someRecord")
                .fields()
                .name("string")
                .prop(SpecificData.CLASS_PROP, "unknownString")
                .type()
                .stringType()
                .noDefault()
                .endRecord()),
        reason("someRecord.string", "unknownString is not known to be deterministic"));

    // Custom Strings in Unions
    assertNonDeterministic(
        AvroCoder.of(
            SchemaBuilder.unionOf()
                .intType()
                .and()
                .record("someRecord")
                .fields()
                .name("someField")
                .prop(SpecificData.CLASS_PROP, "unknownString")
                .type()
                .stringType()
                .noDefault()
                .endRecord()
                .endUnion()),
        reason("someRecord.someField", "unknownString is not known to be deterministic"));
  }

  @Test
  public void testAvroCoderNestedRecords() {
    // Nested Record
    assertDeterministic(
        AvroCoder.of(
            SchemaBuilder.record("nestedRecord")
                .fields()
                .name("subRecord")
                .type()
                .record("subRecord")
                .fields()
                .name("innerField")
                .type()
                .stringType()
                .noDefault()
                .endRecord()
                .noDefault()
                .endRecord()));
  }

  @Test
  public void testAvroCoderCyclicRecords() {
    // Recursive record
    assertNonDeterministic(
        AvroCoder.of(
            SchemaBuilder.record("cyclicRecord")
                .fields()
                .name("cycle")
                .type("cyclicRecord")
                .noDefault()
                .endRecord()),
        reason("cyclicRecord.cycle", "cyclicRecord appears recursively"));
  }

  private static class NullableField {
    @SuppressWarnings("unused")
    private @Nullable String nullable;
  }

  @Test
  public void testNullableField() {
    assertDeterministic(AvroCoder.of(NullableField.class));
  }

  private static class NullableNonDeterministicField {
    @SuppressWarnings("unused")
    private @Nullable NonDeterministicArray nullableNonDetArray;
  }

  private static class NullableCyclic {
    @SuppressWarnings("unused")
    private @Nullable NullableCyclic nullableNullableCyclicField;
  }

  private static class NullableCyclicField {
    @SuppressWarnings("unused")
    private @Nullable Cyclic nullableCyclicField;
  }

  @Test
  public void testNullableNonDeterministicField() {
    assertNonDeterministic(
        AvroCoder.of(NullableCyclic.class),
        reasonField(
            NullableCyclic.class,
            "nullableNullableCyclicField",
            NullableCyclic.class.getName() + " appears recursively"));
    assertNonDeterministic(
        AvroCoder.of(NullableCyclicField.class),
        reasonField(Cyclic.class, "cyclicField", Cyclic.class.getName() + " appears recursively"));
    assertNonDeterministic(
        AvroCoder.of(NullableNonDeterministicField.class),
        reasonField(UnorderedMapClass.class, "mapField", " may not be deterministically ordered"));
  }

  /**
   * Tests that a parameterized class can have an automatically generated schema if the generic
   * field is annotated with a union tag.
   */
  @Test
  public void testGenericClassWithUnionAnnotation() throws Exception {
    // Cast is safe as long as the same coder is used for encoding and decoding.
    @SuppressWarnings({"unchecked", "rawtypes"})
    AvroCoder<GenericWithAnnotation<String>> coder =
        (AvroCoder) AvroCoder.of(GenericWithAnnotation.class);

    assertThat(
        coder.getSchema().getField("onlySomeTypesAllowed").schema().getType(),
        equalTo(Schema.Type.UNION));

    CoderProperties.coderDecodeEncodeEqual(coder, new GenericWithAnnotation<>("hello"));
  }

  private static class GenericWithAnnotation<T> {
    @AvroSchema("[\"string\", \"int\"]")
    private T onlySomeTypesAllowed;

    public GenericWithAnnotation(T value) {
      onlySomeTypesAllowed = value;
    }

    // For deserialization only
    @SuppressWarnings("unused")
    protected GenericWithAnnotation() {}

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof GenericWithAnnotation
          && onlySomeTypesAllowed.equals(((GenericWithAnnotation<?>) other).onlySomeTypesAllowed);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), onlySomeTypesAllowed);
    }
  }

  @Test
  public void testAvroCoderForGenerics() throws Exception {
    Schema fooSchema = AvroCoder.of(Foo.class).getSchema();
    Schema schema =
        new Schema.Parser()
            .parse(
                "{"
                    + "\"type\":\"record\","
                    + "\"name\":\"SomeGeneric\","
                    + "\"namespace\":\"ns\","
                    + "\"fields\":["
                    + "  {\"name\":\"foo\", \"type\":"
                    + fooSchema.toString()
                    + "}"
                    + "]}");
    @SuppressWarnings("rawtypes")
    AvroCoder<SomeGeneric> coder = AvroCoder.of(SomeGeneric.class, schema);

    assertNonDeterministic(coder, reasonField(SomeGeneric.class, "foo", "erasure"));
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);
    assertThat(coder.getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(Pojo.class)));
  }

  private static class SomeGeneric<T> {
    @SuppressWarnings("unused")
    private T foo;
  }

  private static class Foo {
    @SuppressWarnings("unused")
    String id;
  }
}
