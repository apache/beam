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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.RestoreSystemProperties;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

/** Tests for {@link PipelineOptionsFactory}. */
@RunWith(JUnit4.class)
public class PipelineOptionsFactoryTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(PipelineOptionsFactory.class);

  @Test
  public void testAutomaticRegistrationOfPipelineOptions() {
    assertTrue(PipelineOptionsFactory.getRegisteredOptions().contains(DirectPipelineOptions.class));
  }

  @Test
  public void testAutomaticRegistrationOfRunners() {
    assertEquals(DirectPipelineRunner.class,
        PipelineOptionsFactory.getRegisteredRunners().get("DirectPipelineRunner"));
  }

  @Test
  public void testCreationFromSystemProperties() throws Exception {
    System.getProperties().putAll(ImmutableMap
        .<String, String>builder()
        .put("worker_id", "test_worker_id")
        .put("job_id", "test_job_id")
        // Set a non-default value for testing
        .put("sdk_pipeline_options", "{\"options\":{\"numWorkers\":999}}")
        .build());

    DataflowWorkerHarnessOptions options = PipelineOptionsFactory.createFromSystemProperties();
    assertEquals("test_worker_id", options.getWorkerId());
    assertEquals("test_job_id", options.getJobId());
    assertEquals(999, options.getNumWorkers());
  }

  @Test
  public void testAppNameIsSet() {
    ApplicationNameOptions options = PipelineOptionsFactory.as(ApplicationNameOptions.class);
    assertEquals(PipelineOptionsFactoryTest.class.getSimpleName(), options.getAppName());
  }

  /** A simple test interface. */
  public static interface TestPipelineOptions extends PipelineOptions {
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
        + "[com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$MissingGetter].");

    PipelineOptionsFactory.as(MissingGetter.class);
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
        + "[com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$MissingSetter].");

    PipelineOptionsFactory.as(MissingSetter.class);
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
        + "[com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$ExtraneousMethod] "
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
        + "com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$MissingSetter"
        + ".getObject(), public abstract java.lang.String "
        + "com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$ReturnTypeConflict"
        + ".getObject()] with different return types for ["
        + "com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$ReturnTypeConflict].");
    PipelineOptionsFactory.as(ReturnTypeConflict.class);
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
        "Expected setter for property [value] to not be marked with @JsonIgnore on [com."
        + "google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$SetterWithJsonIgnore]");
    PipelineOptionsFactory.as(SetterWithJsonIgnore.class);
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
        + "com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$GetterWithJsonIgnore, "
        + "com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$MissingSetter], "
        + "found only on [com.google.cloud.dataflow.sdk.options."
        + "PipelineOptionsFactoryTest$GetterWithJsonIgnore]");

    // When we attempt to convert, we should error at this moment.
    options.as(CombinedObject.class);
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
        "Empty argument value is only allowed for String, String Array, and Collection");
    PipelineOptionsFactory.fromArgs(args).as(Primitives.class);
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
        "--classValue=" + PipelineOptionsFactoryTest.class.getName()};

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
        "--classValue=" + PipelineOptionsFactoryTest.class.getName()};

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
        "Empty argument value is only allowed for String, String Array, and Collection");

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
        "Empty argument value is only allowed for String, String Array, and Collection");

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
  }

  @Test
  public void testList() {
    String[] args =
        new String[] {"--string=stringValue1", "--string=stringValue2", "--string=stringValue3"};

    Lists options = PipelineOptionsFactory.fromArgs(args).as(Lists.class);
    assertEquals(ImmutableList.of("stringValue1", "stringValue2", "stringValue3"),
        options.getString());
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
        "--diskSizeGb=100",
        "--diskSizeGb=200"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expected one element but was");
    PipelineOptionsFactory.fromArgs(args).create();
  }

  @Test
  public void testSetASingularAttributeUsingAListIsIgnoredWithoutStrictParsing() {
    String[] args = new String[] {
        "--diskSizeGb=100",
        "--diskSizeGb=200"};
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
    expectedLogs.verifyWarn("Strict parsing is disabled, ignoring option");
  }

  @Test
  public void testSettingRunner() {
    String[] args = new String[] {"--runner=BlockingDataflowPipelineRunner"};

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(BlockingDataflowPipelineRunner.class, options.getRunner());
  }

  @Test
  public void testSettingUnknownRunner() {
    String[] args = new String[] {"--runner=UnknownRunner"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unknown 'runner' specified 'UnknownRunner', supported "
        + "pipeline runners [BlockingDataflowPipelineRunner, DataflowPipelineRunner, "
        + "DirectPipelineRunner]");
    PipelineOptionsFactory.fromArgs(args).create();
  }

  @Test
  public void testUsingArgumentWithUnknownPropertyIsNotAllowed() {
    String[] args = new String[] {"--unknownProperty=value"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("missing a property named 'unknownProperty'");
    PipelineOptionsFactory.fromArgs(args).create();
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
    String[] args = new String[] {"", "--diskSizeGb=100", "", "", "--runner=DirectPipelineRunner"};
    PipelineOptionsFactory.fromArgs(args).create();
  }

  @Test
  public void testNullArgumentIsIgnored() {
    String[] args = new String[] {"--diskSizeGb=100", null, null, "--runner=DirectPipelineRunner"};
    PipelineOptionsFactory.fromArgs(args).create();
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
    assertThat(output, containsString("com.google.cloud.dataflow.sdk.options.PipelineOptions"));
    assertThat(output, containsString("Use --help=<OptionsName> for detailed help."));
  }

  @Test
  public void testSpecificHelpAsArgument() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "com.google.cloud.dataflow.sdk.options.PipelineOptions");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("com.google.cloud.dataflow.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: DirectPipelineRunner"));
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
    assertThat(output, containsString("com.google.cloud.dataflow.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: DirectPipelineRunner"));
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
    assertThat(output, containsString("com.google.cloud.dataflow.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: DirectPipelineRunner"));
    assertThat(output,
        containsString("The pipeline runner that will be used to execute the pipeline."));
  }

  /** Used for a name collision test with the other DataflowPipelineOptions. */
  private interface DataflowPipelineOptions extends PipelineOptions {
  }

  @Test
  public void testShortnameSpecificHelpHasMultipleMatches() {
    PipelineOptionsFactory.register(DataflowPipelineOptions.class);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "DataflowPipelineOptions");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("Multiple matches found for DataflowPipelineOptions"));
    assertThat(output, containsString("com.google.cloud.dataflow.sdk.options."
        + "PipelineOptionsFactoryTest$DataflowPipelineOptions"));
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("com.google.cloud.dataflow.sdk.options.PipelineOptions"));
  }

  @Test
  public void testHelpWithOptionThatOutputsValidEnumTypes() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("<DEBUG | ERROR | INFO | TRACE | WARN>"));
  }

  @Test
  public void testHelpWithBadOptionNameAsArgument() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "com.google.cloud.dataflow.sdk.Pipeline");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    assertThat(output,
        containsString("Unable to find option com.google.cloud.dataflow.sdk.Pipeline"));
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("com.google.cloud.dataflow.sdk.options.PipelineOptions"));
  }

  @Test
  public void testHelpWithHiddenMethodAndInterface() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "com.google.cloud.dataflow.sdk.option.DataflowPipelineOptions");
    assertTrue(PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
        arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray());
    // A hidden interface.
    assertThat(output, not(
        containsString("com.google.cloud.dataflow.sdk.options.DataflowPipelineDebugOptions")));
    // A hidden option.
    assertThat(output, not(containsString("--gcpCredential")));
  }

  @Test
  public void testProgrammaticPrintHelp() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PipelineOptionsFactory.printHelp(new PrintStream(baos));
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("com.google.cloud.dataflow.sdk.options.PipelineOptions"));
  }

  @Test
  public void testProgrammaticPrintHelpForSpecificType() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PipelineOptionsFactory.printHelp(new PrintStream(baos), PipelineOptions.class);
    String output = new String(baos.toByteArray());
    assertThat(output, containsString("com.google.cloud.dataflow.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: DirectPipelineRunner"));
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
}
