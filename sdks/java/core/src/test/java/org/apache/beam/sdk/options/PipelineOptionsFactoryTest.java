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
package org.apache.beam.sdk.options;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.service.AutoService;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PipelineOptionsFactory}. */
@RunWith(JUnit4.class)
public class PipelineOptionsFactoryTest {
  private static final String DEFAULT_RUNNER_NAME = "DirectRunner";
  private static final Class<? extends PipelineRunner> REGISTERED_RUNNER =
      RegisteredTestRunner.class;

  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(PipelineOptionsFactory.class);

  @Test
  public void testAutomaticRegistrationOfPipelineOptions() {
    assertTrue(PipelineOptionsFactory.getRegisteredOptions().contains(RegisteredTestOptions.class));
  }

  @Test
  public void testAutomaticRegistrationOfRunners() {
    assertEquals(REGISTERED_RUNNER,
        PipelineOptionsFactory.getRegisteredRunners().get(REGISTERED_RUNNER.getSimpleName()));
  }

  @Test
  public void testAppNameIsSet() {
    ApplicationNameOptions options = PipelineOptionsFactory.as(ApplicationNameOptions.class);
    assertEquals(PipelineOptionsFactoryTest.class.getSimpleName(), options.getAppName());
  }

  /** A simple test interface. */
  public interface TestPipelineOptions extends PipelineOptions {
    String getTestPipelineOption();
    void setTestPipelineOption(String value);
  }

  @Test
  public void testAppNameIsSetWhenUsingAs() {
    TestPipelineOptions options = PipelineOptionsFactory.as(TestPipelineOptions.class);
    assertEquals(PipelineOptionsFactoryTest.class.getSimpleName(),
        options.as(ApplicationNameOptions.class).getAppName());
  }

  @Test
  public void testManualRegistration() {
    assertFalse(PipelineOptionsFactory.getRegisteredOptions().contains(TestPipelineOptions.class));
    PipelineOptionsFactory.register(TestPipelineOptions.class);
    assertTrue(PipelineOptionsFactory.getRegisteredOptions().contains(TestPipelineOptions.class));
  }

  @Test
  public void testDefaultRegistration() {
    assertTrue(PipelineOptionsFactory.getRegisteredOptions().contains(PipelineOptions.class));
  }

  /** A test interface missing a getter. */
  public static interface MissingGetter extends PipelineOptions {
    void setObject(Object value);
  }

  @Test
  public void testMissingGetterThrows() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Expected getter for property [object] of type [java.lang.Object] on "
        + "[org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MissingGetter].");

    PipelineOptionsFactory.as(MissingGetter.class);
  }

  /** A test interface missing multiple getters. */
  public static interface MissingMultipleGetters extends MissingGetter {
    void setOtherObject(Object value);
  }

  @Test
  public void testMultipleMissingGettersThrows() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "missing property methods on [org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$MissingMultipleGetters]");
    expectedException.expectMessage("getter for property [object] of type [java.lang.Object]");
    expectedException.expectMessage("getter for property [otherObject] of type [java.lang.Object]");

    PipelineOptionsFactory.as(MissingMultipleGetters.class);
  }

  /** A test interface missing a setter. */
  public static interface MissingSetter extends PipelineOptions {
    Object getObject();
  }

  @Test
  public void testMissingSetterThrows() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Expected setter for property [object] of type [java.lang.Object] on "
        + "[org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MissingSetter].");

    PipelineOptionsFactory.as(MissingSetter.class);
  }

  /** A test interface missing multiple setters. */
  public static interface MissingMultipleSetters extends MissingSetter {
    Object getOtherObject();
  }

  @Test
  public void testMissingMultipleSettersThrows() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "missing property methods on [org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$MissingMultipleSetters]");
    expectedException.expectMessage("setter for property [object] of type [java.lang.Object]");
    expectedException.expectMessage("setter for property [otherObject] of type [java.lang.Object]");

    PipelineOptionsFactory.as(MissingMultipleSetters.class);
  }

  /** A test interface missing a setter and a getter. */
  public static interface MissingGettersAndSetters extends MissingGetter {
    Object getOtherObject();
  }

  @Test
  public void testMissingGettersAndSettersThrows() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "missing property methods on [org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$MissingGettersAndSetters]");
    expectedException.expectMessage("getter for property [object] of type [java.lang.Object]");
    expectedException.expectMessage("setter for property [otherObject] of type [java.lang.Object]");

    PipelineOptionsFactory.as(MissingGettersAndSetters.class);
  }

  /** A test interface with a type mismatch between the getter and setter. */
  public static interface GetterSetterTypeMismatch extends PipelineOptions {
    boolean getValue();
    void setValue(int value);
  }

  @Test
  public void testGetterSetterTypeMismatchThrows() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Type mismatch between getter and setter methods for property [value]. Getter is of type "
        + "[boolean] whereas setter is of type [int].");

    PipelineOptionsFactory.as(GetterSetterTypeMismatch.class);
  }

  /** A test interface with multiple type mismatches between getters and setters. */
  public static interface MultiGetterSetterTypeMismatch extends GetterSetterTypeMismatch {
    long getOther();
    void setOther(String other);
  }

  @Test
  public void testMultiGetterSetterTypeMismatchThrows() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Type mismatches between getters and setters detected:");
    expectedException.expectMessage("Property [value]: Getter is of type "
        + "[boolean] whereas setter is of type [int].");
    expectedException.expectMessage("Property [other]: Getter is of type [long] "
        + "whereas setter is of type [class java.lang.String].");
    PipelineOptionsFactory.as(MultiGetterSetterTypeMismatch.class);
  }

  /** A test interface representing a composite interface. */
  public static interface CombinedObject extends MissingGetter, MissingSetter {
  }

  @Test
  public void testHavingSettersGettersFromSeparateInterfacesIsValid() {
    PipelineOptionsFactory.as(CombinedObject.class);
  }

  /** A test interface that contains a non-bean style method. */
  public static interface ExtraneousMethod extends PipelineOptions {
    public String extraneousMethod(int value, String otherValue);
  }

  @Test
  public void testHavingExtraneousMethodThrows() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Methods [extraneousMethod(int, String)] on "
        + "[org.apache.beam.sdk.options.PipelineOptionsFactoryTest$ExtraneousMethod] "
        + "do not conform to being bean properties.");

    PipelineOptionsFactory.as(ExtraneousMethod.class);
  }

  /** A test interface that has a conflicting return type with its parent. */
  public static interface ReturnTypeConflict extends CombinedObject {
    @Override
    String getObject();
    void setObject(String value);
  }

  @Test
  public void testReturnTypeConflictThrows() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Method [getObject] has multiple definitions [public abstract java.lang.Object "
        + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MissingSetter"
        + ".getObject(), public abstract java.lang.String "
        + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$ReturnTypeConflict"
        + ".getObject()] with different return types for ["
        + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$ReturnTypeConflict].");
    PipelineOptionsFactory.as(ReturnTypeConflict.class);
  }

  /** An interface to provide multiple methods with return type conflicts. */
  public static interface MultiReturnTypeConflictBase extends CombinedObject {
    Object getOther();
    void setOther(Object object);
  }

  /** A test interface that has multiple conflicting return types with its parent. */
  public static interface MultiReturnTypeConflict extends MultiReturnTypeConflictBase {
    @Override
    String getObject();
    void setObject(String value);

    @Override
    Long getOther();
    void setOther(Long other);
  }

  @Test
  public void testMultipleReturnTypeConflictsThrows() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("[org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$MultiReturnTypeConflict]");
    expectedException.expectMessage(
        "Methods with multiple definitions with different return types");
    expectedException.expectMessage("Method [getObject] has multiple definitions");
    expectedException.expectMessage("public abstract java.lang.Object "
        + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$"
        + "MissingSetter.getObject()");
    expectedException.expectMessage(
        "public abstract java.lang.String org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$MultiReturnTypeConflict.getObject()");
    expectedException.expectMessage("Method [getOther] has multiple definitions");
    expectedException.expectMessage("public abstract java.lang.Object "
        + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$"
        + "MultiReturnTypeConflictBase.getOther()");
    expectedException.expectMessage(
        "public abstract java.lang.Long org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$MultiReturnTypeConflict.getOther()");

    PipelineOptionsFactory.as(MultiReturnTypeConflict.class);
  }

  /** Test interface that has {@link JsonIgnore @JsonIgnore} on a setter for a property. */
  public static interface SetterWithJsonIgnore extends PipelineOptions {
    String getValue();
    @JsonIgnore
    void setValue(String value);
  }

  @Test
  public void testSetterAnnotatedWithJsonIgnore() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Expected setter for property [value] to not be marked with @JsonIgnore on ["
        + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$SetterWithJsonIgnore]");
    PipelineOptionsFactory.as(SetterWithJsonIgnore.class);
  }

  /** Test interface that has {@link JsonIgnore @JsonIgnore} on multiple setters. */
  public static interface MultiSetterWithJsonIgnore extends SetterWithJsonIgnore {
    Integer getOther();
    @JsonIgnore
    void setOther(Integer other);
  }

  @Test
  public void testMultipleSettersAnnotatedWithJsonIgnore() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Found setters marked with @JsonIgnore:");
    expectedException.expectMessage(
        "property [other] should not be marked with @JsonIgnore on ["
        + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MultiSetterWithJsonIgnore]");
    expectedException.expectMessage(
        "property [value] should not be marked with @JsonIgnore on ["
        + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$SetterWithJsonIgnore]");
    PipelineOptionsFactory.as(MultiSetterWithJsonIgnore.class);
  }

  /**
   * This class is has a conflicting field with {@link CombinedObject} that doesn't have
   * {@link JsonIgnore @JsonIgnore}.
   */
  public static interface GetterWithJsonIgnore extends PipelineOptions {
    @JsonIgnore
    Object getObject();
    void setObject(Object value);
  }

  @Test
  public void testNotAllGettersAnnotatedWithJsonIgnore() throws Exception {
    // Initial construction is valid.
    GetterWithJsonIgnore options = PipelineOptionsFactory.as(GetterWithJsonIgnore.class);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Expected getter for property [object] to be marked with @JsonIgnore on all ["
        + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$GetterWithJsonIgnore, "
        + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MissingSetter], "
        + "found only on [org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$GetterWithJsonIgnore]");

    // When we attempt to convert, we should error at this moment.
    options.as(CombinedObject.class);
  }

  private static interface MultiGetters extends PipelineOptions {
    Object getObject();
    void setObject(Object value);

    @JsonIgnore
    Integer getOther();
    void setOther(Integer value);

    Void getConsistent();
    void setConsistent(Void consistent);
  }

  private static interface MultipleGettersWithInconsistentJsonIgnore extends PipelineOptions {
    @JsonIgnore
    Object getObject();
    void setObject(Object value);

    Integer getOther();
    void setOther(Integer value);

    Void getConsistent();
    void setConsistent(Void consistent);
  }

  @Test
  public void testMultipleGettersWithInconsistentJsonIgnore() {
    // Initial construction is valid.
    MultiGetters options = PipelineOptionsFactory.as(MultiGetters.class);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Property getters are inconsistently marked with @JsonIgnore:");
    expectedException.expectMessage(
        "property [object] to be marked on all");
    expectedException.expectMessage("found only on [org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$MultiGetters]");
    expectedException.expectMessage(
        "property [other] to be marked on all");
    expectedException.expectMessage("found only on [org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$MultipleGettersWithInconsistentJsonIgnore]");

    expectedException.expectMessage(Matchers.anyOf(
        containsString(java.util.Arrays.toString(new String[]
            {"org.apache.beam.sdk.options."
                + "PipelineOptionsFactoryTest$MultipleGettersWithInconsistentJsonIgnore",
                "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MultiGetters"})),
        containsString(java.util.Arrays.toString(new String[]
            {"org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MultiGetters",
                "org.apache.beam.sdk.options."
                + "PipelineOptionsFactoryTest$MultipleGettersWithInconsistentJsonIgnore"}))));
    expectedException.expectMessage(not(containsString("property [consistent]")));

    // When we attempt to convert, we should error immediately
    options.as(MultipleGettersWithInconsistentJsonIgnore.class);
  }

  @Test
  public void testAppNameIsNotOverriddenWhenPassedInViaCommandLine() {
    ApplicationNameOptions options = PipelineOptionsFactory
        .fromArgs(new String[]{ "--appName=testAppName" })
        .as(ApplicationNameOptions.class);
    assertEquals("testAppName", options.getAppName());
  }

  @Test
  public void testPropertyIsSetOnRegisteredPipelineOptionNotPartOfOriginalInterface() {
    PipelineOptions options = PipelineOptionsFactory
        .fromArgs(new String[]{ "--project=testProject" })
        .create();
    assertEquals("testProject", options.as(GcpOptions.class).getProject());
  }

  /** A test interface containing all the primitives. */
  public static interface Primitives extends PipelineOptions {
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
  }

  @Test
  public void testPrimitives() {
    String[] args = new String[] {
        "--boolean=true",
        "--char=d",
        "--byte=12",
        "--short=300",
        "--int=100000",
        "--long=123890123890",
        "--float=55.5",
        "--double=12.3"};

    Primitives options = PipelineOptionsFactory.fromArgs(args).as(Primitives.class);
    assertTrue(options.getBoolean());
    assertEquals('d', options.getChar());
    assertEquals((byte) 12, options.getByte());
    assertEquals((short) 300, options.getShort());
    assertEquals(100000, options.getInt());
    assertEquals(123890123890L, options.getLong());
    assertEquals(55.5f, options.getFloat(), 0.0f);
    assertEquals(12.3, options.getDouble(), 0.0);
  }

  @Test
  public void testBooleanShorthandArgument() {
    String[] args = new String[] {"--boolean"};

    Primitives options = PipelineOptionsFactory.fromArgs(args).as(Primitives.class);
    assertTrue(options.getBoolean());
  }

  @Test
  public void testEmptyValueNotAllowed() {
    String[] args = new String[] {
        "--byte="};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Empty argument value is only allowed for String, String Array, and Collections"
        + " of Strings");
    PipelineOptionsFactory.fromArgs(args).as(Primitives.class);
  }

  /** Enum used for testing PipelineOptions CLI parsing. */
  public enum TestEnum {
    Value, Value2
  }

  /** A test interface containing all supported objects. */
  public static interface Objects extends PipelineOptions {
    Boolean getBoolean();
    void setBoolean(Boolean value);
    Character getChar();
    void setChar(Character value);
    Byte getByte();
    void setByte(Byte value);
    Short getShort();
    void setShort(Short value);
    Integer getInt();
    void setInt(Integer value);
    Long getLong();
    void setLong(Long value);
    Float getFloat();
    void setFloat(Float value);
    Double getDouble();
    void setDouble(Double value);
    String getString();
    void setString(String value);
    String getEmptyString();
    void setEmptyString(String value);
    Class<?> getClassValue();
    void setClassValue(Class<?> value);
    TestEnum getEnum();
    void setEnum(TestEnum value);
  }

  @Test
  public void testObjects() {
    String[] args = new String[] {
        "--boolean=true",
        "--char=d",
        "--byte=12",
        "--short=300",
        "--int=100000",
        "--long=123890123890",
        "--float=55.5",
        "--double=12.3",
        "--string=stringValue",
        "--emptyString=",
        "--classValue=" + PipelineOptionsFactoryTest.class.getName(),
        "--enum=" + TestEnum.Value};

    Objects options = PipelineOptionsFactory.fromArgs(args).as(Objects.class);
    assertTrue(options.getBoolean());
    assertEquals(Character.valueOf('d'), options.getChar());
    assertEquals(Byte.valueOf((byte) 12), options.getByte());
    assertEquals(Short.valueOf((short) 300), options.getShort());
    assertEquals(Integer.valueOf(100000), options.getInt());
    assertEquals(Long.valueOf(123890123890L), options.getLong());
    assertEquals(Float.valueOf(55.5f), options.getFloat(), 0.0f);
    assertEquals(Double.valueOf(12.3), options.getDouble(), 0.0);
    assertEquals("stringValue", options.getString());
    assertTrue(options.getEmptyString().isEmpty());
    assertEquals(PipelineOptionsFactoryTest.class, options.getClassValue());
    assertEquals(TestEnum.Value, options.getEnum());
  }

  /** A test class for verifying JSON -> Object conversion. */
  public static class ComplexType {
    String value;
    String value2;
    public ComplexType(@JsonProperty("key") String value, @JsonProperty("key2") String value2) {
      this.value = value;
      this.value2 = value2;
    }
  }

  /** A test interface for verifying JSON -> complex type conversion. */
  interface ComplexTypes extends PipelineOptions {
    Map<String, String> getMap();
    void setMap(Map<String, String> value);

    ComplexType getObject();
    void setObject(ComplexType value);
  }

  @Test
  public void testComplexTypes() {
    String[] args = new String[] {
        "--map={\"key\":\"value\",\"key2\":\"value2\"}",
        "--object={\"key\":\"value\",\"key2\":\"value2\"}"};
    ComplexTypes options = PipelineOptionsFactory.fromArgs(args).as(ComplexTypes.class);
    assertEquals(ImmutableMap.of("key", "value", "key2", "value2"), options.getMap());
    assertEquals("value", options.getObject().value);
    assertEquals("value2", options.getObject().value2);
  }

  @Test
  public void testMissingArgument() {
    String[] args = new String[] {};

    Objects options = PipelineOptionsFactory.fromArgs(args).as(Objects.class);
    assertNull(options.getString());
  }

  /** A test interface containing all supported array return types. */
  public static interface Arrays extends PipelineOptions {
    boolean[] getBoolean();
    void setBoolean(boolean[] value);
    char[] getChar();
    void setChar(char[] value);
    short[] getShort();
    void setShort(short[] value);
    int[] getInt();
    void setInt(int[] value);
    long[] getLong();
    void setLong(long[] value);
    float[] getFloat();
    void setFloat(float[] value);
    double[] getDouble();
    void setDouble(double[] value);
    String[] getString();
    void setString(String[] value);
    Class<?>[] getClassValue();
    void setClassValue(Class<?>[] value);
    TestEnum[] getEnum();
    void setEnum(TestEnum[] value);
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testArrays() {
    String[] args = new String[] {
        "--boolean=true",
        "--boolean=true",
        "--boolean=false",
        "--char=d",
        "--char=e",
        "--char=f",
        "--short=300",
        "--short=301",
        "--short=302",
        "--int=100000",
        "--int=100001",
        "--int=100002",
        "--long=123890123890",
        "--long=123890123891",
        "--long=123890123892",
        "--float=55.5",
        "--float=55.6",
        "--float=55.7",
        "--double=12.3",
        "--double=12.4",
        "--double=12.5",
        "--string=stringValue1",
        "--string=stringValue2",
        "--string=stringValue3",
        "--classValue=" + PipelineOptionsFactory.class.getName(),
        "--classValue=" + PipelineOptionsFactoryTest.class.getName(),
        "--enum=" + TestEnum.Value,
        "--enum=" + TestEnum.Value2};

    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    boolean[] bools = options.getBoolean();
    assertTrue(bools[0] && bools[1] && !bools[2]);
    assertArrayEquals(new char[] {'d', 'e', 'f'}, options.getChar());
    assertArrayEquals(new short[] {300, 301, 302}, options.getShort());
    assertArrayEquals(new int[] {100000, 100001, 100002}, options.getInt());
    assertArrayEquals(new long[] {123890123890L, 123890123891L, 123890123892L}, options.getLong());
    assertArrayEquals(new float[] {55.5f, 55.6f, 55.7f}, options.getFloat(), 0.0f);
    assertArrayEquals(new double[] {12.3, 12.4, 12.5}, options.getDouble(), 0.0);
    assertArrayEquals(new String[] {"stringValue1", "stringValue2", "stringValue3"},
        options.getString());
    assertArrayEquals(new Class[] {PipelineOptionsFactory.class,
                                   PipelineOptionsFactoryTest.class},
        options.getClassValue());
    assertArrayEquals(new TestEnum[] {TestEnum.Value, TestEnum.Value2}, options.getEnum());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testEmptyInStringArrays() {
    String[] args = new String[] {
        "--string=",
        "--string=",
        "--string="};

    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    assertArrayEquals(new String[] {"", "", ""},
        options.getString());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testEmptyInStringArraysWithCommaList() {
    String[] args = new String[] {
        "--string=a,,b"};

    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    assertArrayEquals(new String[] {"a", "", "b"},
        options.getString());
  }

  @Test
  public void testEmptyInNonStringArrays() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Empty argument value is only allowed for String, String Array, and Collections"
        + " of Strings");

    String[] args = new String[] {
        "--boolean=true",
        "--boolean=",
        "--boolean=false"};

    PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
  }

  @Test
  public void testEmptyInNonStringArraysWithCommaList() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Empty argument value is only allowed for String, String Array, and Collections"
        + " of Strings");

    String[] args = new String[] {
        "--int=1,,9"};
    PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
  }

  @Test
  public void testOutOfOrderArrays() {
    String[] args = new String[] {
        "--char=d",
        "--boolean=true",
        "--boolean=true",
        "--char=e",
        "--char=f",
        "--boolean=false"};

    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    boolean[] bools = options.getBoolean();
    assertTrue(bools[0] && bools[1] && !bools[2]);
    assertArrayEquals(new char[] {'d', 'e', 'f'}, options.getChar());
  }

  /** A test interface containing all supported List return types. */
  public static interface Lists extends PipelineOptions {
    List<String> getString();
    void setString(List<String> value);
    List<Integer> getInteger();
    void setInteger(List<Integer> value);
    List getList();
    void setList(List value);
  }

  @Test
  public void testListRawDefaultsToString() {
    String[] manyArgs =
        new String[] {"--list=stringValue1", "--list=stringValue2", "--list=stringValue3"};

    Lists options = PipelineOptionsFactory.fromArgs(manyArgs).as(Lists.class);
    assertEquals(ImmutableList.of("stringValue1", "stringValue2", "stringValue3"),
        options.getList());
  }

  @Test
  public void testListString() {
    String[] manyArgs =
        new String[] {"--string=stringValue1", "--string=stringValue2", "--string=stringValue3"};
    String[] oneArg = new String[] {"--string=stringValue1"};

    Lists options = PipelineOptionsFactory.fromArgs(manyArgs).as(Lists.class);
    assertEquals(ImmutableList.of("stringValue1", "stringValue2", "stringValue3"),
        options.getString());

    options = PipelineOptionsFactory.fromArgs(oneArg).as(Lists.class);
    assertEquals(ImmutableList.of("stringValue1"), options.getString());
  }

  @Test
  public void testListInt() {
    String[] manyArgs =
        new String[] {"--integer=1", "--integer=2", "--integer=3"};
    String[] manyArgsShort =
        new String[] {"--integer=1,2,3"};
    String[] oneArg = new String[] {"--integer=1"};
    String[] missingArg = new String[] {"--integer="};

    Lists options = PipelineOptionsFactory.fromArgs(manyArgs).as(Lists.class);
    assertEquals(ImmutableList.of(1, 2, 3), options.getInteger());
    options = PipelineOptionsFactory.fromArgs(manyArgsShort).as(Lists.class);
    assertEquals(ImmutableList.of(1, 2, 3), options.getInteger());
    options = PipelineOptionsFactory.fromArgs(oneArg).as(Lists.class);
    assertEquals(ImmutableList.of(1), options.getInteger());

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
      "Empty argument value is only allowed for String, String Array, and Collections of Strings,"
      + " but received: java.util.List<java.lang.Integer>");
    options = PipelineOptionsFactory.fromArgs(missingArg).as(Lists.class);
  }

  @Test
  public void testListShorthand() {
    String[] args = new String[] {"--string=stringValue1,stringValue2,stringValue3"};

    Lists options = PipelineOptionsFactory.fromArgs(args).as(Lists.class);
    assertEquals(ImmutableList.of("stringValue1", "stringValue2", "stringValue3"),
        options.getString());
  }

  @Test
  public void testMixedShorthandAndLongStyleList() {
    String[] args = new String[] {
        "--char=d",
        "--char=e",
        "--char=f",
        "--char=g,h,i",
        "--char=j",
        "--char=k",
        "--char=l",
        "--char=m,n,o"};

    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    assertArrayEquals(new char[] {'d', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o'},
                      options.getChar());
  }

  @Test
  public void testSetASingularAttributeUsingAListThrowsAnError() {
    String[] args = new String[] {
        "--string=100",
        "--string=200"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expected one element but was");
    PipelineOptionsFactory.fromArgs(args).as(Objects.class);
  }

  @Test
  public void testSetASingularAttributeUsingAListIsIgnoredWithoutStrictParsing() {
    String[] args = new String[] {
        "--diskSizeGb=100",
        "--diskSizeGb=200"};
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
    expectedLogs.verifyWarn("Strict parsing is disabled, ignoring option");
  }

  /** A test interface containing all supported List return types. */
  public static interface Maps extends PipelineOptions {
    Map<Integer, Integer> getMap();
    void setMap(Map<Integer, Integer> value);

    Map<Integer, Map<Integer, Integer>> getNestedMap();
    void setNestedMap(Map<Integer, Map<Integer, Integer>> value);
  }

  @Test
  public void testMapIntInt() {
    String[] manyArgsShort =
        new String[] {"--map={\"1\":1,\"2\":2}"};
    String[] oneArg = new String[] {"--map={\"1\":1}"};
    String[] missingArg = new String[] {"--map="};

    Maps options = PipelineOptionsFactory.fromArgs(manyArgsShort).as(Maps.class);
    assertEquals(ImmutableMap.of(1, 1, 2, 2), options.getMap());
    options = PipelineOptionsFactory.fromArgs(oneArg).as(Maps.class);
    assertEquals(ImmutableMap.of(1, 1), options.getMap());

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
      "Empty argument value is only allowed for String, String Array, and "
      + "Collections of Strings, but received: java.util.Map<java.lang.Integer, "
      + "java.lang.Integer>");
    options = PipelineOptionsFactory.fromArgs(missingArg).as(Maps.class);
  }

  @Test
  public void testNestedMap() {
    String[] manyArgsShort =
        new String[] {"--nestedMap={\"1\":{\"1\":1},\"2\":{\"2\":2}}"};
    String[] oneArg = new String[] {"--nestedMap={\"1\":{\"1\":1}}"};
    String[] missingArg = new String[] {"--nestedMap="};

    Maps options = PipelineOptionsFactory.fromArgs(manyArgsShort).as(Maps.class);
    assertEquals(ImmutableMap.of(1, ImmutableMap.of(1, 1),
                                 2, ImmutableMap.of(2, 2)),
                 options.getNestedMap());
    options = PipelineOptionsFactory.fromArgs(oneArg).as(Maps.class);
    assertEquals(ImmutableMap.of(1, ImmutableMap.of(1, 1)),
                 options.getNestedMap());

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
      "Empty argument value is only allowed for String, String Array, and Collections of "
      + "Strings, but received: java.util.Map<java.lang.Integer, "
      + "java.util.Map<java.lang.Integer, java.lang.Integer>>");
    options = PipelineOptionsFactory.fromArgs(missingArg).as(Maps.class);
  }

  @Test
  public void testSettingRunner() {
    String[] args = new String[] {"--runner=" + RegisteredTestRunner.class.getSimpleName()};

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(RegisteredTestRunner.class, options.getRunner());
  }

  @Test
  public void testSettingRunnerFullName() {
    String[] args =
        new String[] {String.format("--runner=%s", CrashingRunner.class.getName())};
    PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(opts.getRunner(), CrashingRunner.class);
  }


  @Test
  public void testSettingUnknownRunner() {
    String[] args = new String[] {"--runner=UnknownRunner"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Unknown 'runner' specified 'UnknownRunner', supported " + "pipeline runners");
    Set<String> registeredRunners = PipelineOptionsFactory.getRegisteredRunners().keySet();
    assertThat(registeredRunners, hasItem(REGISTERED_RUNNER.getSimpleName()));
    for (String registeredRunner : registeredRunners) {
      expectedException.expectMessage(registeredRunner);
    }

    PipelineOptionsFactory.fromArgs(args).create();
  }

  private static class ExampleTestRunner extends PipelineRunner<PipelineResult> {
    @Override
    public PipelineResult run(Pipeline pipeline) {
      return null;
    }
  }

  @Test
  public void testSettingRunnerCanonicalClassNameNotInSupportedExists() {
    String[] args = new String[] {String.format("--runner=%s", ExampleTestRunner.class.getName())};
    PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(opts.getRunner(), ExampleTestRunner.class);
  }

  @Test
  public void testSettingRunnerCanonicalClassNameNotInSupportedNotPipelineRunner() {
    String[] args = new String[] {"--runner=java.lang.String"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("does not implement PipelineRunner");
    expectedException.expectMessage("java.lang.String");

    PipelineOptionsFactory.fromArgs(args).create();
  }

  @Test
  public void testUsingArgumentWithUnknownPropertyIsNotAllowed() {
    String[] args = new String[] {"--unknownProperty=value"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("missing a property named 'unknownProperty'");
    PipelineOptionsFactory.fromArgs(args).create();
  }

  interface SuggestedOptions extends PipelineOptions {
    String getAbc();
    void setAbc(String value);

    String getAbcdefg();
    void setAbcdefg(String value);
  }

  @Test
  public void testUsingArgumentWithMisspelledPropertyGivesASuggestion() {
    String[] args = new String[] {"--ab=value"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("missing a property named 'ab'. Did you mean 'abc'?");
    PipelineOptionsFactory.fromArgs(args).as(SuggestedOptions.class);
  }

  @Test
  public void testUsingArgumentWithMisspelledPropertyGivesMultipleSuggestions() {
    String[] args = new String[] {"--abcde=value"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "missing a property named 'abcde'. Did you mean one of [abc, abcdefg]?");
    PipelineOptionsFactory.fromArgs(args).as(SuggestedOptions.class);
  }

  @Test
  public void testUsingArgumentWithUnknownPropertyIsIgnoredWithoutStrictParsing() {
    String[] args = new String[] {"--unknownProperty=value"};
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
    expectedLogs.verifyWarn("missing a property named 'unknownProperty'");
  }

  @Test
  public void testUsingArgumentStartingWithIllegalCharacterIsNotAllowed() {
    String[] args = new String[] {" --diskSizeGb=100"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Argument ' --diskSizeGb=100' does not begin with '--'");
    PipelineOptionsFactory.fromArgs(args).create();
  }

  @Test
  public void testUsingArgumentStartingWithIllegalCharacterIsIgnoredWithoutStrictParsing() {
    String[] args = new String[] {" --diskSizeGb=100"};
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
    expectedLogs.verifyWarn("Strict parsing is disabled, ignoring option");
  }

  @Test
  public void testEmptyArgumentIsIgnored() {
    String[] args =
        new String[] {
          "", "--string=100", "", "", "--runner=" + REGISTERED_RUNNER.getSimpleName()
        };
    PipelineOptionsFactory.fromArgs(args).as(Objects.class);
  }

  @Test
  public void testNullArgumentIsIgnored() {
    String[] args =
        new String[] {
          "--string=100", null, null, "--runner=" + REGISTERED_RUNNER.getSimpleName()
        };
    PipelineOptionsFactory.fromArgs(args).as(Objects.class);
  }

  @Test
  public void testUsingArgumentWithInvalidNameIsNotAllowed() {
    String[] args = new String[] {"--=100"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Argument '--=100' starts with '--='");
    PipelineOptionsFactory.fromArgs(args).create();
  }

  @Test
  public void testUsingArgumentWithInvalidNameIsIgnoredWithoutStrictParsing() {
    String[] args = new String[] {"--=100"};
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
    expectedLogs.verifyWarn("Strict parsing is disabled, ignoring option");
  }

  @Test
  public void testWhenNoHelpIsRequested() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    assertFalse(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertEquals("", output);
  }

  @Test
  public void testDefaultHelpAsArgument() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "true");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
    assertThat(output, containsString("Use --help=<OptionsName> for detailed help."));
  }

  @Test
  public void testSpecificHelpAsArgument() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "org.apache.beam.sdk.options.PipelineOptions");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: " + DEFAULT_RUNNER_NAME));
    assertThat(output,
        containsString("The pipeline runner that will be used to execute the pipeline."));
  }

  @Test
  public void testSpecificHelpAsArgumentWithSimpleClassName() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "PipelineOptions");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: " + DEFAULT_RUNNER_NAME));
    assertThat(output,
        containsString("The pipeline runner that will be used to execute the pipeline."));
  }

  @Test
  public void testSpecificHelpAsArgumentWithClassNameSuffix() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "options.PipelineOptions");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: " + DEFAULT_RUNNER_NAME));
    assertThat(output,
        containsString("The pipeline runner that will be used to execute the pipeline."));
  }

  /** Used for a name collision test with the other NameConflict interfaces. */
  private static class NameConflictClassA {
    /** Used for a name collision test with the other NameConflict interfaces. */
    private interface NameConflict extends PipelineOptions {
    }
  }

  /** Used for a name collision test with the other NameConflict interfaces. */
  private static class NameConflictClassB {
    /** Used for a name collision test with the other NameConflict interfaces. */
    private interface NameConflict extends PipelineOptions {
    }
  }

  @Test
  public void testShortnameSpecificHelpHasMultipleMatches() {
    PipelineOptionsFactory.register(NameConflictClassA.NameConflict.class);
    PipelineOptionsFactory.register(NameConflictClassB.NameConflict.class);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "NameConflict");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("Multiple matches found for NameConflict"));
    assertThat(output, containsString("org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$NameConflictClassA$NameConflict"));
    assertThat(output, containsString("org.apache.beam.sdk.options."
        + "PipelineOptionsFactoryTest$NameConflictClassB$NameConflict"));
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
  }

  @Test
  public void testHelpWithOptionThatOutputsValidEnumTypes() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", Objects.class.getName());
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("<Value | Value2>"));
  }

  @Test
  public void testHelpWithBadOptionNameAsArgument() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "org.apache.beam.sdk.Pipeline");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output,
        containsString("Unable to find option org.apache.beam.sdk.Pipeline"));
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
  }

  @Test
  public void testHelpWithHiddenMethodAndInterface() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "org.apache.beam.sdk.option.DataflowPipelineOptions");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    // A hidden interface.
    assertThat(output, not(
        containsString("org.apache.beam.sdk.options.DataflowPipelineDebugOptions")));
    // A hidden option.
    assertThat(output, not(containsString("--gcpCredential")));
  }

  @Test
  public void testProgrammaticPrintHelp() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PipelineOptionsFactory.printHelp(new PrintStream(baos));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
  }

  @Test
  public void testProgrammaticPrintHelpForSpecificType() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PipelineOptionsFactory.printHelp(new PrintStream(baos), PipelineOptions.class);
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: " + DEFAULT_RUNNER_NAME));
    assertThat(output,
        containsString("The pipeline runner that will be used to execute the pipeline."));
  }

  @Test
  public void testFindProperClassLoaderIfContextClassLoaderIsNull() throws InterruptedException {
    final ClassLoader[] classLoader = new ClassLoader[1];
    Thread thread = new Thread(new Runnable() {

      @Override
      public void run() {
        classLoader[0] = PipelineOptionsFactory.findClassLoader();
      }
    });
    thread.setContextClassLoader(null);
    thread.start();
    thread.join();
    assertEquals(PipelineOptionsFactory.class.getClassLoader(), classLoader[0]);
  }

  @Test
  public void testFindProperClassLoaderIfContextClassLoaderIsAvailable()
      throws InterruptedException {
    final ClassLoader[] classLoader = new ClassLoader[1];
    Thread thread = new Thread(new Runnable() {

      @Override
      public void run() {
        classLoader[0] = PipelineOptionsFactory.findClassLoader();
      }
    });
    ClassLoader cl = new ClassLoader() {};
    thread.setContextClassLoader(cl);
    thread.start();
    thread.join();
    assertEquals(cl, classLoader[0]);
  }

  private static class RegisteredTestRunner extends PipelineRunner<PipelineResult> {
    public static PipelineRunner fromOptions(PipelineOptions options) {
      return new RegisteredTestRunner();
    }

    public PipelineResult run(Pipeline p) {
      throw new IllegalArgumentException();
    }
  }


  /**
   * A {@link PipelineRunnerRegistrar} to demonstrate default {@link PipelineRunner} registration.
   */
  @AutoService(PipelineRunnerRegistrar.class)
  public static class RegisteredTestRunnerRegistrar implements PipelineRunnerRegistrar {
    @Override
    public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
      return ImmutableList.<Class<? extends PipelineRunner<?>>>of(RegisteredTestRunner.class);
    }
  }

  private interface RegisteredTestOptions extends PipelineOptions {
    Object getRegisteredExampleFooBar();
    void setRegisteredExampleFooBar(Object registeredExampleFooBar);
  }


  /**
   * A {@link PipelineOptionsRegistrar} to demonstrate default {@link PipelineOptions} registration.
   */
  @AutoService(PipelineOptionsRegistrar.class)
  public static class RegisteredTestOptionsRegistrar implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.<Class<? extends PipelineOptions>>of(RegisteredTestOptions.class);
    }
  }
}
