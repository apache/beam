/*
 * Copyright (C) 2014 Google Inc.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.RestoreSystemProperties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
  public void testCreationFromSystemProperties() {
    System.getProperties().putAll(ImmutableMap
        .<String, String>builder()
        .put("root_url", "test_root_url")
        .put("service_path", "test_service_path")
        .put("temp_gcs_directory",
            "gs://tap-testing-30lsaafg6g3zudmjbnsdz6wj/unittesting/staging")
        .put("service_account_name", "test_service_account_name")
        .put("service_account_keyfile", "test_service_account_keyfile")
        .put("worker_id", "test_worker_id")
        .put("project_id", "test_project_id")
        .put("job_id", "test_job_id")
        .build());
    DataflowWorkerHarnessOptions options = PipelineOptionsFactory.createFromSystemProperties();
    assertEquals("test_root_url", options.getApiRootUrl());
    assertEquals("test_service_path", options.getDataflowEndpoint());
    assertEquals("gs://tap-testing-30lsaafg6g3zudmjbnsdz6wj/unittesting/staging",
        options.getTempLocation());
    assertEquals("test_service_account_name", options.getServiceAccountName());
    assertEquals("test_service_account_keyfile", options.getServiceAccountKeyfile());
    assertEquals("test_worker_id", options.getWorkerId());
    assertEquals("test_project_id", options.getProject());
    assertEquals("test_job_id", options.getJobId());
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

  /** A test interface representing a composite interface. */
  public static interface CombinedObject extends MissingGetter, MissingSetter {
  }

  @Test
  public void testHavingSettersGettersFromSeparateInterfacesIsValid() {
    PipelineOptionsFactory.as(CombinedObject.class);
  }

  /** A test interface which contains a non-bean style method. */
  public static interface ExtraneousMethod extends PipelineOptions {
    public String extraneousMethod(int value, String otherValue);
  }

  @Test
  public void testHavingExtraneousMethodThrows() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Methods [java.lang.String extraneousMethod(int, java.lang.String)] on "
        + "[com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$ExtraneousMethod] "
        + "do not conform to being bean properties.");

    PipelineOptionsFactory.as(ExtraneousMethod.class);
  }

  /** A test interface which has a conflicting return type with its parent. */
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
        "Expected getter for property [object] to be marked with @JsonIgnore on all [com."
        + "google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$MissingSetter, "
        + "com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryTest$GetterWithJsonIgnore], "
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
    expectedLogs.expectWarn("Strict parsing is disabled, ignoring option");
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
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
    expectedLogs.expectWarn("missing a property named 'unknownProperty'");
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
  }

  @Test
  public void testUsingArgumentWithoutValueIsNotAllowed() {
    String[] args = new String[] {"--diskSizeGb="};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Argument '--diskSizeGb=' ends with '='");
    PipelineOptionsFactory.fromArgs(args).create();
  }

  @Test
  public void testUsingArgumentWithoutValueIsIgnoredWithoutStrictParsing() {
    String[] args = new String[] {"--diskSizeGb="};
    expectedLogs.expectWarn("Strict parsing is disabled, ignoring option");
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
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
    expectedLogs.expectWarn("Strict parsing is disabled, ignoring option");
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
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
    expectedLogs.expectWarn("Strict parsing is disabled, ignoring option");
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
  }
}
