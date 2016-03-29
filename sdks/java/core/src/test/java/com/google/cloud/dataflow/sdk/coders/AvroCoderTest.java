/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.coders.Coder.NonDeterministicException;
import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Stringable;
import org.apache.avro.reflect.Union;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
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

/** Tests for {@link AvroCoder}. */
@RunWith(JUnit4.class)
public class AvroCoderTest {

  @DefaultCoder(AvroCoder.class)
  private static class Pojo {
    public String text;
    public int count;

    // Empty constructor required for Avro decoding.
    @SuppressWarnings("unused")
    public Pojo() {
    }

    public Pojo(String text, int count) {
      this.text = text;
      this.count = count;
    }

    // auto-generated
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Pojo pojo = (Pojo) o;

      if (count != pojo.count) {
        return false;
      }
      if (text != null
          ? !text.equals(pojo.text)
          : pojo.text != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public String toString() {
      return "Pojo{"
          + "text='" + text + '\''
          + ", count=" + count
          + '}';
    }
  }

  private static class GetTextFn extends DoFn<Pojo, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().text);
    }
  }

  @Test
  public void testAvroCoderEncoding() throws Exception {
    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);
    CloudObject encoding = coder.asCloudObject();

    Assert.assertThat(encoding.keySet(),
        Matchers.containsInAnyOrder("@type", "type", "schema", "encoding_id"));
  }

  @Test
  public void testPojoEncoding() throws Exception {
    Pojo value = new Pojo("Hello", 42);
    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);

    CoderProperties.coderDecodeEncodeEqual(coder, value);
  }

  @Test
  public void testPojoEncodingId() throws Exception {
    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);
    CoderProperties.coderHasEncodingId(coder, Pojo.class.getName());
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
    Schema schema = (new Schema.Parser()).parse(schemaString);

    GenericRecord before = new GenericData.Record(schema);
    before.put("name", "Bob");
    before.put("favorite_number", 256);
    // Leave favorite_color null

    AvroCoder<GenericRecord> coder = AvroCoder.of(GenericRecord.class, schema);

    CoderProperties.coderDecodeEncodeEqual(coder, before);
    Assert.assertEquals(schema, coder.getSchema());
  }

  @Test
  public void testEncodingNotBuffered() throws Exception {
    // This test ensures that the coder doesn't read ahead and buffer data.
    // Reading ahead causes a problem if the stream consists of records of different
    // types.
    Pojo before = new Pojo("Hello", 42);

    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);
    SerializableCoder<Integer> intCoder = SerializableCoder.of(Integer.class);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    Context context = Context.NESTED;
    coder.encode(before, outStream, context);
    intCoder.encode(10, outStream, context);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());

    Pojo after = coder.decode(inStream, context);
    Assert.assertEquals(before, after);

    Integer intAfter = intCoder.decode(inStream, context);
    Assert.assertEquals(new Integer(10), intAfter);
  }

  @Test
  public void testDefaultCoder() throws Exception {
    Pipeline p = TestPipeline.create();

    // Use MyRecord as input and output types without explicitly specifying
    // a coder (this uses the default coders, which may not be AvroCoder).
    PCollection<String> output =
        p.apply(Create.of(new Pojo("hello", 1), new Pojo("world", 2)))
            .apply(ParDo.of(new GetTextFn()));

    DataflowAssert.that(output)
        .containsInAnyOrder("hello", "world");
    p.run();
  }

  @Test
  public void testAvroCoderIsSerializable() throws Exception {
    AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);

    // Check that the coder is serializable using the regular JSON approach.
    SerializableUtils.ensureSerializable(coder);
  }

  private final void assertDeterministic(AvroCoder<?> coder) {
    try {
      coder.verifyDeterministic();
    } catch (NonDeterministicException e) {
      fail("Expected " + coder + " to be deterministic, but got:\n" + e);
    }
  }

  private final void assertNonDeterministic(AvroCoder<?> coder,
      Matcher<String> reason1) {
    try {
      coder.verifyDeterministic();
      fail("Expected " + coder + " to be non-deterministic.");
    } catch (NonDeterministicException e) {
      assertThat(e.getReasons(), Matchers.<String>iterableWithSize(1));
      assertThat(e.getReasons(), Matchers.<String>contains(reason1));
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
        description.appendText(String.format("Reason starting with '%s:' containing '%s'",
            prefix, messagePart));
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

  private Matcher<String> reasonField(
      Class<?> clazz, String field, String message) {
    return reason(clazz.getName() + "#" + field, message);
  }

  @Test
  public void testDeterministicUnorderedMap() {
    assertNonDeterministic(AvroCoder.of(UnorderedMapClass.class),
        reasonField(UnorderedMapClass.class, "mapField",
            "java.util.Map<java.lang.String, java.lang.String> "
                + "may not be deterministically ordered"));
  }

  private static class NonDeterministicArray {
    @SuppressWarnings("unused")
    private UnorderedMapClass[] arrayField;
  }
  @Test
  public void testDeterministicNonDeterministicArray() {
    assertNonDeterministic(AvroCoder.of(NonDeterministicArray.class),
        reasonField(UnorderedMapClass.class, "mapField",
            "java.util.Map<java.lang.String, java.lang.String>"
                + " may not be deterministically ordered"));
  }

  private static class SubclassOfUnorderedMapClass extends UnorderedMapClass {}


  @Test
  public void testDeterministicNonDeterministicChild() {
    // Super class has non deterministic fields.
    assertNonDeterministic(AvroCoder.of(SubclassOfUnorderedMapClass.class),
        reasonField(UnorderedMapClass.class, "mapField",
            "may not be deterministically ordered"));
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
    } catch (AvroTypeException e) {
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
    assertNonDeterministic(AvroCoder.of(TreeMapNonDetValue.class),
        reasonField(UnorderedMapClass.class, "mapField",
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
    assertNonDeterministic(AvroCoder.of(LinkedHashMapField.class),
        reasonField(LinkedHashMapField.class, "nonDeterministicMap",
            "java.util.LinkedHashMap<java.lang.String, java.lang.String> "
                + "may not be deterministically ordered"));
  }

  private static class LinkedHashMapField {
    @SuppressWarnings("unused")
    LinkedHashMap<String, String> nonDeterministicMap;
  }

  @Test
  public void testDeterminismCollection() {
    assertNonDeterministic(AvroCoder.of(StringCollection.class),
        reasonField(StringCollection.class, "stringCollection",
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
    assertNonDeterministic(AvroCoder.of(StringHashSet.class),
        reasonField(StringHashSet.class, "stringCollection",
            "java.util.HashSet<java.lang.String> may not be deterministically ordered"));
  }

  private static class StringSortedSet{
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
    assertNonDeterministic(AvroCoder.of(OrderedSetOfNonDetValues.class),
        reasonField(UnorderedMapClass.class, "mapField",
            "may not be deterministically ordered"));
    assertNonDeterministic(AvroCoder.of(ListOfNonDetValues.class),
        reasonField(UnorderedMapClass.class, "mapField",
            "may not be deterministically ordered"));
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
    assertNonDeterministic(AvroCoder.of(NonDeterministicUnionBase.class),
        reasonField(UnionCase3.class, "mapField", "may not be deterministically ordered"));
  }

  @Test
  public void testDeterminismStringable() {
    assertDeterministic(AvroCoder.of(String.class));
    assertNonDeterministic(AvroCoder.of(StringableClass.class),
        reasonClass(StringableClass.class, "may not have deterministic #toString()"));
  }

  @Stringable
  private static class StringableClass {
  }

  @Test
  public void testDeterminismCyclicClass() {
    assertNonDeterministic(AvroCoder.of(Cyclic.class),
        reasonField(Cyclic.class, "cyclicField", "appears recursively"));
    assertNonDeterministic(AvroCoder.of(CyclicField.class),
        reasonField(Cyclic.class, "cyclicField",
    Cyclic.class.getName() + " appears recursively"));
    assertNonDeterministic(AvroCoder.of(IndirectCycle1.class),
        reasonField(IndirectCycle2.class, "field2",
    IndirectCycle1.class.getName() +  " appears recursively"));
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
    @AvroSchema("{\"name\": \"bar\", \"type\": \"record\", \"fields\": ["
        + "{\"name\": \"foo\", \"type\": \"int\"}]}")
    GenericRecord genericRecord;
  }

  @Test
  public void testDeterminismHasCustomSchema() {
    assertNonDeterministic(AvroCoder.of(HasCustomSchema.class),
        reasonField(HasCustomSchema.class, "withCustomSchema",
            "Custom schemas are only supported for subtypes of IndexedRecord."));
  }

  private static class HasCustomSchema {
    @AvroSchema("{\"name\": \"bar\", \"type\": \"record\", \"fields\": ["
        + "{\"name\": \"foo\", \"type\": \"int\"}]}")
    int withCustomSchema;
  }

  @Test
  public void testAvroCoderTreeMapDeterminism()
      throws Exception, NonDeterministicException {
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

    assertTrue(Arrays.equals(
        outStream1.toByteArray(), outStream2.toByteArray()));
  }

  private static class TreeMapField {
    private TreeMap<String, String> field = new TreeMap<>();
  }

  @Union({ UnionCase1.class, UnionCase2.class })
  private abstract static class DeterministicUnionBase {}

  @Union({ UnionCase1.class, UnionCase2.class, UnionCase3.class })
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
    assertDeterministic(AvroCoder.of(SchemaBuilder.record("someRecord").fields()
        .endRecord()));
    assertDeterministic(AvroCoder.of(SchemaBuilder.record("someRecord").fields()
        .name("int").type().intType().noDefault()
        .endRecord()));
    assertDeterministic(AvroCoder.of(SchemaBuilder.record("someRecord").fields()
        .name("string").type().stringType().noDefault()
        .endRecord()));

    assertNonDeterministic(AvroCoder.of(SchemaBuilder.record("someRecord").fields()
        .name("map").type().map().values().stringType().noDefault()
        .endRecord()),
        reason("someRecord.map", "HashMap to represent MAPs"));

    assertDeterministic(AvroCoder.of(SchemaBuilder.record("someRecord").fields()
        .name("array").type().array().items().stringType().noDefault()
        .endRecord()));

    assertDeterministic(AvroCoder.of(SchemaBuilder.record("someRecord").fields()
        .name("enum").type().enumeration("anEnum").symbols("s1", "s2").enumDefault("s1")
        .endRecord()));

    assertDeterministic(AvroCoder.of(SchemaBuilder.unionOf()
        .intType().and()
        .record("someRecord").fields().nullableString("someField", "").endRecord()
        .endUnion()));
  }

  @Test
  public void testAvroCoderStrings() {
    // Custom Strings in Records
    assertDeterministic(AvroCoder.of(SchemaBuilder.record("someRecord").fields()
        .name("string").prop(SpecificData.CLASS_PROP, "java.lang.String")
        .type().stringType().noDefault()
        .endRecord()));
    assertNonDeterministic(AvroCoder.of(SchemaBuilder.record("someRecord").fields()
        .name("string").prop(SpecificData.CLASS_PROP, "unknownString")
        .type().stringType().noDefault()
        .endRecord()),
        reason("someRecord.string", "unknownString is not known to be deterministic"));

    // Custom Strings in Unions
    assertNonDeterministic(AvroCoder.of(SchemaBuilder.unionOf()
        .intType().and()
        .record("someRecord").fields()
        .name("someField").prop(SpecificData.CLASS_PROP, "unknownString")
        .type().stringType().noDefault().endRecord()
        .endUnion()),
        reason("someRecord.someField", "unknownString is not known to be deterministic"));
  }

  @Test
  public void testAvroCoderNestedRecords() {
    // Nested Record
    assertDeterministic(AvroCoder.of(SchemaBuilder.record("nestedRecord").fields()
        .name("subRecord").type().record("subRecord").fields()
            .name("innerField").type().stringType().noDefault()
        .endRecord().noDefault()
        .endRecord()));
  }

  @Test
  public void testAvroCoderCyclicRecords() {
    // Recursive record
    assertNonDeterministic(AvroCoder.of(SchemaBuilder.record("cyclicRecord").fields()
        .name("cycle").type("cyclicRecord").noDefault()
        .endRecord()),
        reason("cyclicRecord.cycle", "cyclicRecord appears recursively"));
  }

  private static class NullableField {
    @SuppressWarnings("unused")
    @Nullable private String nullable;
  }

  @Test
  public void testNullableField() {
    assertDeterministic(AvroCoder.of(NullableField.class));
  }

  private static class NullableNonDeterministicField {
    @SuppressWarnings("unused")
    @Nullable private NonDeterministicArray nullableNonDetArray;
  }

  private static class NullableCyclic {
    @SuppressWarnings("unused")
    @Nullable private NullableCyclic nullableNullableCyclicField;
  }

  private static class NullableCyclicField {
    @SuppressWarnings("unused")
    @Nullable private Cyclic nullableCyclicField;
  }

  @Test
  public void testNullableNonDeterministicField() {
    assertNonDeterministic(AvroCoder.of(NullableCyclic.class),
        reasonField(NullableCyclic.class, "nullableNullableCyclicField",
            NullableCyclic.class.getName() + " appears recursively"));
    assertNonDeterministic(AvroCoder.of(NullableCyclicField.class),
        reasonField(Cyclic.class, "cyclicField",
            Cyclic.class.getName() + " appears recursively"));
    assertNonDeterministic(AvroCoder.of(NullableNonDeterministicField.class),
        reasonField(UnorderedMapClass.class, "mapField",
            " may not be deterministically ordered"));
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

    assertThat(coder.getSchema().getField("onlySomeTypesAllowed").schema().getType(),
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
    protected GenericWithAnnotation() { }

    @Override
    public boolean equals(Object other) {
      return other instanceof GenericWithAnnotation
          && onlySomeTypesAllowed.equals(((GenericWithAnnotation<?>) other).onlySomeTypesAllowed);
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), onlySomeTypesAllowed);
    }
  }
}
