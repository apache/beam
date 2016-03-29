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

package com.google.cloud.dataflow.sdk.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Tests for {@link ProxyInvocationHandler}. */
@RunWith(JUnit4.class)
public class ProxyInvocationHandlerTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  /** A test interface with some primitives and objects. */
  public static interface Simple extends PipelineOptions {
    boolean isOptionEnabled();
    void setOptionEnabled(boolean value);
    int getPrimitive();
    void setPrimitive(int value);
    String getString();
    void setString(String value);
  }

  @Test
  public void testPropertySettingAndGetting() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    Simple proxy = handler.as(Simple.class);
    proxy.setString("OBJECT");
    proxy.setOptionEnabled(true);
    proxy.setPrimitive(4);
    assertEquals("OBJECT", proxy.getString());
    assertTrue(proxy.isOptionEnabled());
    assertEquals(4, proxy.getPrimitive());
  }

  /** A test interface containing all the JLS default values. */
  public static interface JLSDefaults extends PipelineOptions {
    boolean getBoolean();
    void setBoolean(boolean value);
    char getChar();
    void setChar(char value);
    byte getByte();
    void setByte(byte value);
    short getShort();
    void setShort(short value);
    int getInt();
    void setInt(int value);
    long getLong();
    void setLong(long value);
    float getFloat();
    void setFloat(float value);
    double getDouble();
    void setDouble(double value);
    Object getObject();
    void setObject(Object value);
  }

  @Test
  public void testGettingJLSDefaults() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    JLSDefaults proxy = handler.as(JLSDefaults.class);
    assertFalse(proxy.getBoolean());
    assertEquals('\0', proxy.getChar());
    assertEquals((byte) 0, proxy.getByte());
    assertEquals((short) 0, proxy.getShort());
    assertEquals(0, proxy.getInt());
    assertEquals(0L, proxy.getLong());
    assertEquals(0f, proxy.getFloat(), 0f);
    assertEquals(0d, proxy.getDouble(), 0d);
    assertNull(proxy.getObject());
  }

  /** A {@link DefaultValueFactory} that is used for testing. */
  public static class TestOptionFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return "testOptionFactory";
    }
  }

  /** A test enum for testing {@link Default.Enum @Default.Enum}. */
  public enum EnumType {
    MyEnum("MyTestEnum");

    private final String value;
    private EnumType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  /** A test interface containing all the {@link Default} annotations. */
  public static interface DefaultAnnotations extends PipelineOptions {
    @Default.Boolean(true)
    boolean getBoolean();
    void setBoolean(boolean value);
    @Default.Character('a')
    char getChar();
    void setChar(char value);
    @Default.Byte((byte) 4)
    byte getByte();
    void setByte(byte value);
    @Default.Short((short) 5)
    short getShort();
    void setShort(short value);
    @Default.Integer(6)
    int getInt();
    void setInt(int value);
    @Default.Long(7L)
    long getLong();
    void setLong(long value);
    @Default.Float(8f)
    float getFloat();
    void setFloat(float value);
    @Default.Double(9d)
    double getDouble();
    void setDouble(double value);
    @Default.String("testString")
    String getString();
    void setString(String value);
    @Default.Class(DefaultAnnotations.class)
    Class<?> getClassOption();
    void setClassOption(Class<?> value);
    @Default.Enum("MyEnum")
    EnumType getEnum();
    void setEnum(EnumType value);
    @Default.InstanceFactory(TestOptionFactory.class)
    String getComplex();
    void setComplex(String value);
  }

  @Test
  public void testAnnotationDefaults() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    DefaultAnnotations proxy = handler.as(DefaultAnnotations.class);
    assertTrue(proxy.getBoolean());
    assertEquals('a', proxy.getChar());
    assertEquals((byte) 4, proxy.getByte());
    assertEquals((short) 5, proxy.getShort());
    assertEquals(6, proxy.getInt());
    assertEquals(7, proxy.getLong());
    assertEquals(8f, proxy.getFloat(), 0f);
    assertEquals(9d, proxy.getDouble(), 0d);
    assertEquals("testString", proxy.getString());
    assertEquals(DefaultAnnotations.class, proxy.getClassOption());
    assertEquals(EnumType.MyEnum, proxy.getEnum());
    assertEquals("testOptionFactory", proxy.getComplex());
  }

  @Test
  public void testEquals() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    Simple proxy = handler.as(Simple.class);
    JLSDefaults sameAsProxy = proxy.as(JLSDefaults.class);
    ProxyInvocationHandler handler2 = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    Simple proxy2 = handler2.as(Simple.class);
    JLSDefaults sameAsProxy2 = proxy2.as(JLSDefaults.class);
    assertTrue(handler.equals(proxy));
    assertTrue(proxy.equals(proxy));
    assertTrue(proxy.equals(sameAsProxy));
    assertFalse(handler.equals(handler2));
    assertFalse(proxy.equals(proxy2));
    assertFalse(proxy.equals(sameAsProxy2));
  }

  @Test
  public void testHashCode() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    Simple proxy = handler.as(Simple.class);
    JLSDefaults sameAsProxy = proxy.as(JLSDefaults.class);

    ProxyInvocationHandler handler2 = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    Simple proxy2 = handler2.as(Simple.class);
    JLSDefaults sameAsProxy2 = proxy2.as(JLSDefaults.class);

    // Hashcode comparisons below depend on random numbers, so could fail if seed changes.
    assertTrue(handler.hashCode() == proxy.hashCode());
    assertTrue(proxy.hashCode() == sameAsProxy.hashCode());
    assertFalse(handler.hashCode() == handler2.hashCode());
    assertFalse(proxy.hashCode() == proxy2.hashCode());
    assertFalse(proxy.hashCode() == sameAsProxy2.hashCode());
  }

  @Test
  public void testToString() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    Simple proxy = handler.as(Simple.class);
    proxy.setString("stringValue");
    DefaultAnnotations proxy2 = proxy.as(DefaultAnnotations.class);
    proxy2.setLong(57L);
    assertEquals("Current Settings:\n"
        + "  long: 57\n"
        + "  string: stringValue\n",
        proxy.toString());
  }

  @Test
  public void testToStringAfterDeserializationContainsJsonEntries() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    Simple proxy = handler.as(Simple.class);
    proxy.setString("stringValue");
    DefaultAnnotations proxy2 = proxy.as(DefaultAnnotations.class);
    proxy2.setLong(57L);
    assertEquals("Current Settings:\n"
        + "  long: 57\n"
        + "  string: \"stringValue\"\n",
        serializeDeserialize(PipelineOptions.class, proxy2).toString());
  }

  @Test
  public void testToStringAfterDeserializationContainsOverriddenEntries() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    Simple proxy = handler.as(Simple.class);
    proxy.setString("stringValue");
    DefaultAnnotations proxy2 = proxy.as(DefaultAnnotations.class);
    proxy2.setLong(57L);
    Simple deserializedOptions = serializeDeserialize(Simple.class, proxy2);
    deserializedOptions.setString("overriddenValue");
    assertEquals("Current Settings:\n"
        + "  long: 57\n"
        + "  string: overriddenValue\n",
        deserializedOptions.toString());
  }

  /** A test interface containing an unknown method. */
  public static interface UnknownMethod {
    void unknownMethod();
  }

  @Test
  public void testInvokeWithUnknownMethod() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Unknown method [public abstract void com.google.cloud."
        + "dataflow.sdk.options.ProxyInvocationHandlerTest$UnknownMethod.unknownMethod()] "
        + "invoked with args [null].");

    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    handler.invoke(handler, UnknownMethod.class.getMethod("unknownMethod"), null);
  }

  /** A test interface that extends another interface. */
  public static interface SubClass extends Simple {
    String getExtended();
    void setExtended(String value);
  }

  @Test
  public void testSubClassStoresSuperInterfaceValues() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    SubClass extended = handler.as(SubClass.class);

    extended.setString("parentValue");
    assertEquals("parentValue", extended.getString());
  }

  @Test
  public void testUpCastRetainsSuperInterfaceValues() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    SubClass extended = handler.as(SubClass.class);

    extended.setString("parentValue");
    Simple simple = extended.as(Simple.class);
    assertEquals("parentValue", simple.getString());
  }

  @Test
  public void testUpCastRetainsSubClassValues() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    SubClass extended = handler.as(SubClass.class);

    extended.setExtended("subClassValue");
    SubClass extended2 = extended.as(Simple.class).as(SubClass.class);
    assertEquals("subClassValue", extended2.getExtended());
  }

  /** A test interface that is a sibling to {@link SubClass}. */
  public static interface Sibling extends Simple {
    String getSibling();
    void setSibling(String value);
  }

  @Test
  public void testAsSiblingRetainsSuperInterfaceValues() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    SubClass extended = handler.as(SubClass.class);

    extended.setString("parentValue");
    Sibling sibling = extended.as(Sibling.class);
    assertEquals("parentValue", sibling.getString());
  }

  /** A test interface that has the same methods as the parent. */
  public static interface MethodConflict extends Simple {
    @Override
    String getString();
    @Override
    void setString(String value);
  }

  @Test
  public void testMethodConflictProvidesSameValue() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    MethodConflict methodConflict = handler.as(MethodConflict.class);

    methodConflict.setString("conflictValue");
    assertEquals("conflictValue", methodConflict.getString());
    assertEquals("conflictValue", methodConflict.as(Simple.class).getString());
  }

  /** A test interface that has the same methods as its parent and grandparent. */
  public static interface DeepMethodConflict extends MethodConflict {
    @Override
    String getString();
    @Override
    void setString(String value);
    @Override
    int getPrimitive();
    @Override
    void setPrimitive(int value);
  }

  @Test
  public void testDeepMethodConflictProvidesSameValue() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    DeepMethodConflict deepMethodConflict = handler.as(DeepMethodConflict.class);

    // Tests overriding an already overridden method
    deepMethodConflict.setString("conflictValue");
    assertEquals("conflictValue", deepMethodConflict.getString());
    assertEquals("conflictValue", deepMethodConflict.as(MethodConflict.class).getString());
    assertEquals("conflictValue", deepMethodConflict.as(Simple.class).getString());

    // Tests overriding a method from an ancestor class
    deepMethodConflict.setPrimitive(5);
    assertEquals(5, deepMethodConflict.getPrimitive());
    assertEquals(5, deepMethodConflict.as(MethodConflict.class).getPrimitive());
    assertEquals(5, deepMethodConflict.as(Simple.class).getPrimitive());
  }

  /** A test interface that shares the same methods as {@link Sibling}. */
  public static interface SimpleSibling extends PipelineOptions {
    String getString();
    void setString(String value);
  }

  @Test
  public void testDisjointSiblingsShareValues() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    SimpleSibling proxy = handler.as(SimpleSibling.class);
    proxy.setString("siblingValue");
    assertEquals("siblingValue", proxy.getString());
    assertEquals("siblingValue", proxy.as(Simple.class).getString());
  }

  /** A test interface that joins two sibling interfaces that have conflicting methods. */
  public static interface SiblingMethodConflict extends Simple, SimpleSibling {
  }

  @Test
  public void testSiblingMethodConflict() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    SiblingMethodConflict siblingMethodConflict = handler.as(SiblingMethodConflict.class);
    siblingMethodConflict.setString("siblingValue");
    assertEquals("siblingValue", siblingMethodConflict.getString());
    assertEquals("siblingValue", siblingMethodConflict.as(Simple.class).getString());
    assertEquals("siblingValue", siblingMethodConflict.as(SimpleSibling.class).getString());
  }

  /** A test interface that has only the getter and only a setter overriden. */
  public static interface PartialMethodConflict extends Simple {
    @Override
    String getString();
    @Override
    void setPrimitive(int value);
  }

  @Test
  public void testPartialMethodConflictProvidesSameValue() throws Exception {
    ProxyInvocationHandler handler = new ProxyInvocationHandler(Maps.<String, Object>newHashMap());
    PartialMethodConflict partialMethodConflict = handler.as(PartialMethodConflict.class);

    // Tests overriding a getter property that is only partially bound
    partialMethodConflict.setString("conflictValue");
    assertEquals("conflictValue", partialMethodConflict.getString());
    assertEquals("conflictValue", partialMethodConflict.as(Simple.class).getString());

    // Tests overriding a setter property that is only partially bound
    partialMethodConflict.setPrimitive(5);
    assertEquals(5, partialMethodConflict.getPrimitive());
    assertEquals(5, partialMethodConflict.as(Simple.class).getPrimitive());
  }

  @Test
  public void testJsonConversionForDefault() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    assertNotNull(serializeDeserialize(PipelineOptions.class, options));
  }

  /** Test interface for JSON conversion of simple types. */
  private static interface SimpleTypes extends PipelineOptions {
    int getInteger();
    void setInteger(int value);
    String getString();
    void setString(String value);
  }

  @Test
  public void testJsonConversionForSimpleTypes() throws Exception {
    SimpleTypes options = PipelineOptionsFactory.as(SimpleTypes.class);
    options.setString("TestValue");
    options.setInteger(5);
    SimpleTypes options2 = serializeDeserialize(SimpleTypes.class, options);
    assertEquals(5, options2.getInteger());
    assertEquals("TestValue", options2.getString());
  }

  @Test
  public void testJsonConversionOfAJsonConvertedType() throws Exception {
    SimpleTypes options = PipelineOptionsFactory.as(SimpleTypes.class);
    options.setString("TestValue");
    options.setInteger(5);
    // It is important here that our first serialization goes to our most basic
    // type so that we handle the case when we don't know the types of certain
    // properties because the intermediate instance of PipelineOptions never
    // saw their interface.
    SimpleTypes options2 = serializeDeserialize(SimpleTypes.class,
        serializeDeserialize(PipelineOptions.class, options));
    assertEquals(5, options2.getInteger());
    assertEquals("TestValue", options2.getString());
  }

  @Test
  public void testJsonConversionForPartiallySerializedValues() throws Exception {
    SimpleTypes options = PipelineOptionsFactory.as(SimpleTypes.class);
    options.setInteger(5);
    SimpleTypes options2 = serializeDeserialize(SimpleTypes.class, options);
    options2.setString("TestValue");
    SimpleTypes options3 = serializeDeserialize(SimpleTypes.class, options2);
    assertEquals(5, options3.getInteger());
    assertEquals("TestValue", options3.getString());
  }

  @Test
  public void testJsonConversionForOverriddenSerializedValues() throws Exception {
    SimpleTypes options = PipelineOptionsFactory.as(SimpleTypes.class);
    options.setInteger(-5);
    options.setString("NeedsToBeOverridden");
    SimpleTypes options2 = serializeDeserialize(SimpleTypes.class, options);
    options2.setInteger(5);
    options2.setString("TestValue");
    SimpleTypes options3 = serializeDeserialize(SimpleTypes.class, options2);
    assertEquals(5, options3.getInteger());
    assertEquals("TestValue", options3.getString());
  }

  /** Test interface for JSON conversion of container types. */
  private static interface ContainerTypes extends PipelineOptions {
    List<String> getList();
    void setList(List<String> values);
    Map<String, String> getMap();
    void setMap(Map<String, String> values);
    Set<String> getSet();
    void setSet(Set<String> values);
  }

  @Test
  public void testJsonConversionForContainerTypes() throws Exception {
    List<String> list = ImmutableList.of("a", "b", "c");
    Map<String, String> map = ImmutableMap.of("d", "x", "e", "y", "f", "z");
    Set<String> set = ImmutableSet.of("g", "h", "i");
    ContainerTypes options = PipelineOptionsFactory.as(ContainerTypes.class);
    options.setList(list);
    options.setMap(map);
    options.setSet(set);
    ContainerTypes options2 = serializeDeserialize(ContainerTypes.class, options);
    assertEquals(list, options2.getList());
    assertEquals(map, options2.getMap());
    assertEquals(set, options2.getSet());
  }

  /** Test interface for conversion of inner types. */
  private static class InnerType {
    public double doubleField;

    static InnerType of(double value) {
      InnerType rval = new InnerType();
      rval.doubleField = value;
      return rval;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null
          && getClass().equals(obj.getClass())
          && Objects.equals(doubleField, ((InnerType) obj).doubleField);
    }
  }

  /** Test interface for conversion of generics and inner types. */
  private static class ComplexType {
    public String stringField;
    public Integer intField;
    public List<InnerType> genericType;
    public InnerType innerType;

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null
          && getClass().equals(obj.getClass())
          && Objects.equals(stringField, ((ComplexType) obj).stringField)
          && Objects.equals(intField, ((ComplexType) obj).intField)
          && Objects.equals(genericType, ((ComplexType) obj).genericType)
          && Objects.equals(innerType, ((ComplexType) obj).innerType);
    }
  }

  private static interface ComplexTypes extends PipelineOptions {
    ComplexType getComplexType();
    void setComplexType(ComplexType value);
  }

  @Test
  public void testJsonConversionForComplexType() throws Exception {
    ComplexType complexType = new ComplexType();
    complexType.stringField = "stringField";
    complexType.intField = 12;
    complexType.innerType = InnerType.of(12);
    complexType.genericType = ImmutableList.of(InnerType.of(16234), InnerType.of(24));

    ComplexTypes options = PipelineOptionsFactory.as(ComplexTypes.class);
    options.setComplexType(complexType);
    ComplexTypes options2 = serializeDeserialize(ComplexTypes.class, options);
    assertEquals(complexType, options2.getComplexType());
  }

  /** Test interface for testing ignored properties during serialization. */
  private static interface IgnoredProperty extends PipelineOptions {
    @JsonIgnore
    String getValue();
    void setValue(String value);
  }

  @Test
  public void testJsonConversionOfIgnoredProperty() throws Exception {
    IgnoredProperty options = PipelineOptionsFactory.as(IgnoredProperty.class);
    options.setValue("TestValue");

    IgnoredProperty options2 = serializeDeserialize(IgnoredProperty.class, options);
    assertNull(options2.getValue());
  }

  /** Test class that is not serializable by Jackson. */
  public static class NotSerializable {
    private String value;
    public NotSerializable(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  /** Test interface containing a class that is not serializable by Jackson. */
  private static interface NotSerializableProperty extends PipelineOptions {
    NotSerializable getValue();
    void setValue(NotSerializable value);
  }

  @Test
  public void testJsonConversionOfNotSerializableProperty() throws Exception {
    NotSerializableProperty options = PipelineOptionsFactory.as(NotSerializableProperty.class);
    options.setValue(new NotSerializable("TestString"));

    expectedException.expect(JsonMappingException.class);
    expectedException.expectMessage("Failed to serialize and deserialize property 'value'");
    serializeDeserialize(NotSerializableProperty.class, options);
  }

  /**
   * Test interface that has {@link JsonIgnore @JsonIgnore} on a property that Jackson
   * can't serialize.
   */
  private static interface IgnoredNotSerializableProperty extends PipelineOptions {
    @JsonIgnore
    NotSerializable getValue();
    void setValue(NotSerializable value);
  }

  @Test
  public void testJsonConversionOfIgnoredNotSerializableProperty() throws Exception {
    IgnoredNotSerializableProperty options =
        PipelineOptionsFactory.as(IgnoredNotSerializableProperty.class);
    options.setValue(new NotSerializable("TestString"));

    IgnoredNotSerializableProperty options2 =
        serializeDeserialize(IgnoredNotSerializableProperty.class, options);
    assertNull(options2.getValue());
  }

  /** Test class that is only serializable by Jackson with the added metadata. */
  public static class SerializableWithMetadata {
    private String value;
    public SerializableWithMetadata(@JsonProperty("value") String value) {
      this.value = value;
    }

    @JsonProperty("value")
    public String getValue() {
      return value;
    }
  }

  /**
   * Test interface containing a property that is serializable by Jackson only with
   * the additional metadata.
   */
  private static interface SerializableWithMetadataProperty extends PipelineOptions {
    SerializableWithMetadata getValue();
    void setValue(SerializableWithMetadata value);
  }

  @Test
  public void testJsonConversionOfSerializableWithMetadataProperty() throws Exception {
    SerializableWithMetadataProperty options =
        PipelineOptionsFactory.as(SerializableWithMetadataProperty.class);
    options.setValue(new SerializableWithMetadata("TestString"));

    SerializableWithMetadataProperty options2 =
        serializeDeserialize(SerializableWithMetadataProperty.class, options);
    assertEquals("TestString", options2.getValue().getValue());
  }

  private <T extends PipelineOptions> T serializeDeserialize(Class<T> kls, PipelineOptions options)
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String value = mapper.writeValueAsString(options);
    return mapper.readValue(value, PipelineOptions.class).as(kls);
  }
}
