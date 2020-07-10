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

import static java.util.Locale.ROOT;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps.uniqueIndex;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.auto.service.AutoService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.jobmanagement.v1.JobApi.PipelineOptionDescriptor;
import org.apache.beam.model.jobmanagement.v1.JobApi.PipelineOptionType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.InterceptingUrlClassLoader;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Collections2;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
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
  private static final Class<? extends PipelineRunner<?>> REGISTERED_RUNNER =
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
    assertEquals(
        REGISTERED_RUNNER,
        PipelineOptionsFactory.getRegisteredRunners()
            .get(REGISTERED_RUNNER.getSimpleName().toLowerCase()));
  }

  @Test
  public void testAutomaticRegistrationInculdesWithoutRunnerSuffix() {
    // Sanity check to make sure the substring works appropriately
    assertEquals(
        "RegisteredTest",
        REGISTERED_RUNNER
            .getSimpleName()
            .substring(0, REGISTERED_RUNNER.getSimpleName().length() - "Runner".length()));
    Map<String, Class<? extends PipelineRunner<?>>> registered =
        PipelineOptionsFactory.CACHE.get().getSupportedPipelineRunners();
    assertEquals(
        REGISTERED_RUNNER,
        registered.get(
            REGISTERED_RUNNER
                .getSimpleName()
                .toLowerCase()
                .substring(0, REGISTERED_RUNNER.getSimpleName().length() - "Runner".length())));
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
    assertEquals(
        PipelineOptionsFactoryTest.class.getSimpleName(),
        options.as(ApplicationNameOptions.class).getAppName());
  }

  @Test
  public void testOptionsIdIsSet() throws Exception {
    ObjectMapper mapper =
        new ObjectMapper()
            .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));
    PipelineOptions options = PipelineOptionsFactory.create();
    // We purposely serialize/deserialize to get another instance. This allows to test if the
    // default has been set or not.
    PipelineOptions clone =
        mapper.readValue(mapper.writeValueAsString(options), PipelineOptions.class);
    // It is important that we don't call getOptionsId() before we have created the clone.
    assertEquals(options.getOptionsId(), clone.getOptionsId());
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
  public interface MissingGetter extends PipelineOptions {
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
  public interface MissingMultipleGetters extends MissingGetter {
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
  public interface MissingSetter extends PipelineOptions {
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
  public interface MissingMultipleSetters extends MissingSetter {
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
  public interface MissingGettersAndSetters extends MissingGetter {
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
  public interface GetterSetterTypeMismatch extends PipelineOptions {
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
  public interface MultiGetterSetterTypeMismatch extends GetterSetterTypeMismatch {
    long getOther();

    void setOther(String other);
  }

  @Test
  public void testMultiGetterSetterTypeMismatchThrows() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Type mismatches between getters and setters detected:");
    expectedException.expectMessage(
        "Property [value]: Getter is of type " + "[boolean] whereas setter is of type [int].");
    expectedException.expectMessage(
        "Property [other]: Getter is of type [long] "
            + "whereas setter is of type [class java.lang.String].");
    PipelineOptionsFactory.as(MultiGetterSetterTypeMismatch.class);
  }

  /** A test interface representing a composite interface. */
  public interface CombinedObject extends MissingGetter, MissingSetter {}

  @Test
  public void testHavingSettersGettersFromSeparateInterfacesIsValid() {
    PipelineOptionsFactory.as(CombinedObject.class);
  }

  /** A test interface that contains a non-bean style method. */
  public interface ExtraneousMethod extends PipelineOptions {
    String extraneousMethod(int value, String otherValue);
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
  public interface ReturnTypeConflict extends CombinedObject {
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
  public interface MultiReturnTypeConflictBase extends CombinedObject {
    Object getOther();

    void setOther(Object object);
  }

  /** A test interface that has multiple conflicting return types with its parent. */
  public interface MultiReturnTypeConflict extends MultiReturnTypeConflictBase {
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
    expectedException.expectMessage(
        "[org.apache.beam.sdk.options." + "PipelineOptionsFactoryTest$MultiReturnTypeConflict]");
    expectedException.expectMessage(
        "Methods with multiple definitions with different return types");
    expectedException.expectMessage("Method [getObject] has multiple definitions");
    expectedException.expectMessage(
        "public abstract java.lang.Object "
            + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$"
            + "MissingSetter.getObject()");
    expectedException.expectMessage(
        "public abstract java.lang.String org.apache.beam.sdk.options."
            + "PipelineOptionsFactoryTest$MultiReturnTypeConflict.getObject()");
    expectedException.expectMessage("Method [getOther] has multiple definitions");
    expectedException.expectMessage(
        "public abstract java.lang.Object "
            + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$"
            + "MultiReturnTypeConflictBase.getOther()");
    expectedException.expectMessage(
        "public abstract java.lang.Long org.apache.beam.sdk.options."
            + "PipelineOptionsFactoryTest$MultiReturnTypeConflict.getOther()");

    PipelineOptionsFactory.as(MultiReturnTypeConflict.class);
  }

  /** Test interface that has {@link JsonIgnore @JsonIgnore} on a setter for a property. */
  public interface SetterWithJsonIgnore extends PipelineOptions {
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
  public interface MultiSetterWithJsonIgnore extends SetterWithJsonIgnore {
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
   * This class is has a conflicting field with {@link CombinedObject} that doesn't have {@link
   * JsonIgnore @JsonIgnore}.
   */
  public interface GetterWithJsonIgnore extends PipelineOptions {
    @JsonIgnore
    Object getObject();

    void setObject(Object value);
  }

  /**
   * This class is has a conflicting {@link JsonIgnore @JsonIgnore} value with {@link
   * GetterWithJsonIgnore}.
   */
  public interface GetterWithInconsistentJsonIgnoreValue extends PipelineOptions {
    @JsonIgnore(value = false)
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

  /** Test interface. */
  public interface MultiGetters extends PipelineOptions {
    Object getObject();

    void setObject(Object value);

    @JsonIgnore
    Integer getOther();

    void setOther(Integer value);

    Void getConsistent();

    void setConsistent(Void consistent);
  }

  /** Test interface. */
  public interface MultipleGettersWithInconsistentJsonIgnore extends PipelineOptions {
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
    expectedException.expectMessage("property [object] to be marked on all");
    expectedException.expectMessage(
        "found only on [org.apache.beam.sdk.options." + "PipelineOptionsFactoryTest$MultiGetters]");
    expectedException.expectMessage("property [other] to be marked on all");
    expectedException.expectMessage(
        "found only on [org.apache.beam.sdk.options."
            + "PipelineOptionsFactoryTest$MultipleGettersWithInconsistentJsonIgnore]");

    expectedException.expectMessage(
        anyOf(
            containsString(
                java.util.Arrays.toString(
                    new String[] {
                      "org.apache.beam.sdk.options."
                          + "PipelineOptionsFactoryTest$MultipleGettersWithInconsistentJsonIgnore",
                      "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MultiGetters"
                    })),
            containsString(
                java.util.Arrays.toString(
                    new String[] {
                      "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MultiGetters",
                      "org.apache.beam.sdk.options."
                          + "PipelineOptionsFactoryTest$MultipleGettersWithInconsistentJsonIgnore"
                    }))));
    expectedException.expectMessage(not(containsString("property [consistent]")));

    // When we attempt to convert, we should error immediately
    options.as(MultipleGettersWithInconsistentJsonIgnore.class);
  }

  /** Test interface that has {@link Default @Default} on a setter for a property. */
  public interface SetterWithDefault extends PipelineOptions {
    String getValue();

    @Default.String("abc")
    void setValue(String value);
  }

  @Test
  public void testSetterAnnotatedWithDefault() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Expected setter for property [value] to not be marked with @Default on ["
            + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$SetterWithDefault]");
    PipelineOptionsFactory.as(SetterWithDefault.class);
  }

  /** Test interface that has {@link Default @Default} on multiple setters. */
  public interface MultiSetterWithDefault extends SetterWithDefault {
    Integer getOther();

    @Default.String("abc")
    void setOther(Integer other);
  }

  @Test
  public void testMultipleSettersAnnotatedWithDefault() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Found setters marked with @Default:");
    expectedException.expectMessage(
        "property [other] should not be marked with @Default on ["
            + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MultiSetterWithDefault]");
    expectedException.expectMessage(
        "property [value] should not be marked with @Default on ["
            + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$SetterWithDefault]");
    PipelineOptionsFactory.as(MultiSetterWithDefault.class);
  }

  /**
   * This class is has a conflicting field with {@link CombinedObject} that doesn't have {@link
   * Default @Default}.
   */
  public interface GetterWithDefault extends PipelineOptions {
    @Default.Integer(1)
    Object getObject();

    void setObject(Object value);
  }

  /**
   * This class is consistent with {@link GetterWithDefault} that has the same {@link
   * Default @Default}.
   */
  public interface GetterWithConsistentDefault extends PipelineOptions {
    @Default.Integer(1)
    Object getObject();

    void setObject(Object value);
  }

  /**
   * This class is inconsistent with {@link GetterWithDefault} that has a different {@link
   * Default @Default}.
   */
  public interface GetterWithInconsistentDefaultType extends PipelineOptions {
    @Default.String("abc")
    Object getObject();

    void setObject(Object value);
  }

  /**
   * This class is inconsistent with {@link GetterWithDefault} that has a different {@link
   * Default @Default} value.
   */
  public interface GetterWithInconsistentDefaultValue extends PipelineOptions {
    @Default.Integer(0)
    Object getObject();

    void setObject(Object value);
  }

  @Test
  public void testNotAllGettersAnnotatedWithDefault() throws Exception {
    // Initial construction is valid.
    GetterWithDefault options = PipelineOptionsFactory.as(GetterWithDefault.class);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Expected getter for property [object] to be marked with @Default on all ["
            + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$GetterWithDefault, "
            + "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MissingSetter], "
            + "found only on [org.apache.beam.sdk.options."
            + "PipelineOptionsFactoryTest$GetterWithDefault]");

    // When we attempt to convert, we should error at this moment.
    options.as(CombinedObject.class);
  }

  @Test
  public void testGettersAnnotatedWithConsistentDefault() throws Exception {
    GetterWithConsistentDefault options =
        PipelineOptionsFactory.as(GetterWithDefault.class).as(GetterWithConsistentDefault.class);

    assertEquals(1, options.getObject());
  }

  @Test
  public void testGettersAnnotatedWithInconsistentDefault() throws Exception {
    // Initial construction is valid.
    GetterWithDefault options = PipelineOptionsFactory.as(GetterWithDefault.class);

    expectedException.expect(IllegalArgumentException.class);

    // Make sure the error message says what the problem is, generally
    expectedException.expectMessage("contradictory annotations");

    // Make sure the error message gives actionable details about what
    // annotations were contradictory.
    // Note that the quotes in the unparsed string are present in Java 11 but absent in Java 8
    expectedException.expectMessage(
        anyOf(
            containsString("Default.String(value=\"abc\")"),
            containsString("Default.String(value=abc)")));
    expectedException.expectMessage("Default.Integer(value=1");

    // When we attempt to convert, we should error at this moment.
    options.as(GetterWithInconsistentDefaultType.class);
  }

  @Test
  public void testGettersAnnotatedWithInconsistentDefaultValue() throws Exception {
    // Initial construction is valid.
    GetterWithDefault options = PipelineOptionsFactory.as(GetterWithDefault.class);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Property [object] is marked with contradictory annotations. Found ["
            + "[Default.Integer(value=1) on org.apache.beam.sdk.options.PipelineOptionsFactoryTest"
            + "$GetterWithDefault#getObject()], "
            + "[Default.Integer(value=0) on org.apache.beam.sdk.options.PipelineOptionsFactoryTest"
            + "$GetterWithInconsistentDefaultValue#getObject()]].");

    // When we attempt to convert, we should error at this moment.
    options.as(GetterWithInconsistentDefaultValue.class);
  }

  @Test
  public void testGettersAnnotatedWithInconsistentJsonIgnoreValue() throws Exception {
    // Initial construction is valid.
    GetterWithJsonIgnore options = PipelineOptionsFactory.as(GetterWithJsonIgnore.class);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Property [object] is marked with contradictory annotations. Found ["
            + "[JsonIgnore(value=false) on org.apache.beam.sdk.options.PipelineOptionsFactoryTest"
            + "$GetterWithInconsistentJsonIgnoreValue#getObject()], "
            + "[JsonIgnore(value=true) on org.apache.beam.sdk.options.PipelineOptionsFactoryTest"
            + "$GetterWithJsonIgnore#getObject()]].");

    // When we attempt to convert, we should error at this moment.
    options.as(GetterWithInconsistentJsonIgnoreValue.class);
  }

  /** Test interface. */
  public interface GettersWithMultipleDefault extends PipelineOptions {
    @Default.String("abc")
    @Default.Integer(0)
    Object getObject();

    void setObject(Object value);
  }

  @Test
  public void testGettersWithMultipleDefaults() throws Exception {
    expectedException.expect(IllegalArgumentException.class);

    // Make sure the error message says what the problem is, generally
    expectedException.expectMessage("contradictory annotations");

    // Make sure the error message gives actionable details about what annotations were
    // contradictory.
    // Note that the quotes in the unparsed string are present in Java 11 but absent in Java 8
    expectedException.expectMessage(
        anyOf(
            containsString("Default.String(value=\"abc\")"),
            containsString("Default.String(value=abc)")));
    expectedException.expectMessage("Default.Integer(value=0)");

    // When we attempt to create, we should error at this moment.
    PipelineOptionsFactory.as(GettersWithMultipleDefault.class);
  }

  /** Test interface. */
  public interface MultiGettersWithDefault extends PipelineOptions {
    Object getObject();

    void setObject(Object value);

    @Default.Integer(1)
    Integer getOther();

    void setOther(Integer value);

    Void getConsistent();

    void setConsistent(Void consistent);
  }

  /** Test interface. */
  public interface MultipleGettersWithInconsistentDefault extends PipelineOptions {
    @Default.Boolean(true)
    Object getObject();

    void setObject(Object value);

    Integer getOther();

    void setOther(Integer value);

    Void getConsistent();

    void setConsistent(Void consistent);
  }

  @Test
  public void testMultipleGettersWithInconsistentDefault() {
    // Initial construction is valid.
    MultiGettersWithDefault options = PipelineOptionsFactory.as(MultiGettersWithDefault.class);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Property getters are inconsistently marked with @Default:");
    expectedException.expectMessage("property [object] to be marked on all");
    expectedException.expectMessage(
        "found only on [org.apache.beam.sdk.options."
            + "PipelineOptionsFactoryTest$MultiGettersWithDefault]");
    expectedException.expectMessage("property [other] to be marked on all");
    expectedException.expectMessage(
        "found only on [org.apache.beam.sdk.options."
            + "PipelineOptionsFactoryTest$MultipleGettersWithInconsistentDefault]");

    expectedException.expectMessage(
        anyOf(
            containsString(
                java.util.Arrays.toString(
                    new String[] {
                      "org.apache.beam.sdk.options."
                          + "PipelineOptionsFactoryTest$MultipleGettersWithInconsistentDefault",
                      "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MultiGettersWithDefault"
                    })),
            containsString(
                java.util.Arrays.toString(
                    new String[] {
                      "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$MultiGettersWithDefault",
                      "org.apache.beam.sdk.options."
                          + "PipelineOptionsFactoryTest$MultipleGettersWithInconsistentDefault"
                    }))));
    expectedException.expectMessage(not(containsString("property [consistent]")));

    // When we attempt to convert, we should error immediately
    options.as(MultipleGettersWithInconsistentDefault.class);
  }

  @Test
  public void testAppNameIsNotOverriddenWhenPassedInViaCommandLine() {
    ApplicationNameOptions options =
        PipelineOptionsFactory.fromArgs("--appName=testAppName").as(ApplicationNameOptions.class);
    assertEquals("testAppName", options.getAppName());
  }

  @Test
  public void testPropertyIsSetOnRegisteredPipelineOptionNotPartOfOriginalInterface() {
    PipelineOptions options = PipelineOptionsFactory.fromArgs("--streaming").create();
    assertTrue(options.as(StreamingOptions.class).isStreaming());
  }

  /** A test interface containing all the primitives. */
  public interface Primitives extends PipelineOptions {
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
    String[] args =
        new String[] {
          "--boolean=true",
          "--char=d",
          "--byte=12",
          "--short=300",
          "--int=100000",
          "--long=123890123890",
          "--float=55.5",
          "--double=12.3"
        };

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
    String[] args = new String[] {"--byte="};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(emptyStringErrorMessage());
    PipelineOptionsFactory.fromArgs(args).as(Primitives.class);
  }

  /** Enum used for testing PipelineOptions CLI parsing. */
  public enum TestEnum {
    Value,
    Value2
  }

  /** A test interface containing all supported objects. */
  public interface Objects extends PipelineOptions {
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

    ValueProvider<String> getStringValue();

    void setStringValue(ValueProvider<String> value);

    ValueProvider<Long> getLongValue();

    void setLongValue(ValueProvider<Long> value);

    ValueProvider<TestEnum> getEnumValue();

    void setEnumValue(ValueProvider<TestEnum> value);
  }

  @Test
  public void testObjects() {
    String[] args =
        new String[] {
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
          "--enum=" + TestEnum.Value,
          "--stringValue=beam",
          "--longValue=12389049585840",
          "--enumValue=" + TestEnum.Value
        };

    Objects options = PipelineOptionsFactory.fromArgs(args).as(Objects.class);
    assertTrue(options.getBoolean());
    assertEquals(Character.valueOf('d'), options.getChar());
    assertEquals(Byte.valueOf((byte) 12), options.getByte());
    assertEquals(Short.valueOf((short) 300), options.getShort());
    assertEquals(Integer.valueOf(100000), options.getInt());
    assertEquals(Long.valueOf(123890123890L), options.getLong());
    assertEquals(55.5f, options.getFloat(), 0.0f);
    assertEquals(12.3, options.getDouble(), 0.0);
    assertEquals("stringValue", options.getString());
    assertTrue(options.getEmptyString().isEmpty());
    assertEquals(PipelineOptionsFactoryTest.class, options.getClassValue());
    assertEquals(TestEnum.Value, options.getEnum());
    assertEquals("beam", options.getStringValue().get());
    assertEquals(Long.valueOf(12389049585840L), options.getLongValue().get());
    assertEquals(TestEnum.Value, options.getEnumValue().get());
  }

  @Test
  public void testStringValueProvider() {
    String[] args = new String[] {"--stringValue=beam"};
    String[] emptyArgs = new String[] {"--stringValue="};
    Objects options = PipelineOptionsFactory.fromArgs(args).as(Objects.class);
    assertEquals("beam", options.getStringValue().get());
    options = PipelineOptionsFactory.fromArgs(emptyArgs).as(Objects.class);
    assertEquals("", options.getStringValue().get());
  }

  @Test
  public void testLongValueProvider() {
    String[] args = new String[] {"--longValue=12345678762"};
    String[] emptyArgs = new String[] {"--longValue="};
    Objects options = PipelineOptionsFactory.fromArgs(args).as(Objects.class);
    assertEquals(Long.valueOf(12345678762L), options.getLongValue().get());
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(emptyStringErrorMessage());
    PipelineOptionsFactory.fromArgs(emptyArgs).as(Objects.class);
  }

  @Test
  public void testEnumValueProvider() {
    String[] args = new String[] {"--enumValue=" + TestEnum.Value};
    String[] emptyArgs = new String[] {"--enumValue="};
    Objects options = PipelineOptionsFactory.fromArgs(args).as(Objects.class);
    assertEquals(TestEnum.Value, options.getEnumValue().get());
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(emptyStringErrorMessage());
    PipelineOptionsFactory.fromArgs(emptyArgs).as(Objects.class);
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
  public interface ComplexTypes extends PipelineOptions {
    Map<String, String> getMap();

    void setMap(Map<String, String> value);

    ComplexType getObject();

    void setObject(ComplexType value);

    ValueProvider<ComplexType> getObjectValue();

    void setObjectValue(ValueProvider<ComplexType> value);
  }

  @Test
  public void testComplexTypes() {
    String[] args =
        new String[] {
          "--map={\"key\":\"value\",\"key2\":\"value2\"}",
          "--object={\"key\":\"value\",\"key2\":\"value2\"}",
          "--objectValue={\"key\":\"value\",\"key2\":\"value2\"}"
        };
    ComplexTypes options = PipelineOptionsFactory.fromArgs(args).as(ComplexTypes.class);
    assertEquals(ImmutableMap.of("key", "value", "key2", "value2"), options.getMap());
    assertEquals("value", options.getObject().value);
    assertEquals("value2", options.getObject().value2);
    assertEquals("value", options.getObjectValue().get().value);
    assertEquals("value2", options.getObjectValue().get().value2);
  }

  @Test
  public void testMissingArgument() {
    String[] args = new String[] {};

    Objects options = PipelineOptionsFactory.fromArgs(args).as(Objects.class);
    assertNull(options.getString());
  }

  /** A test interface containing all supported array return types. */
  public interface Arrays extends PipelineOptions {
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

    ValueProvider<String[]> getStringValue();

    void setStringValue(ValueProvider<String[]> value);

    ValueProvider<Long[]> getLongValue();

    void setLongValue(ValueProvider<Long[]> value);

    ValueProvider<TestEnum[]> getEnumValue();

    void setEnumValue(ValueProvider<TestEnum[]> value);
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testArrays() {
    String[] args =
        new String[] {
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
          "--enum=" + TestEnum.Value2,
          "--stringValue=abc",
          "--stringValue=beam",
          "--longValue=123890123890",
          "--longValue=123890123891",
          "--enumValue=" + TestEnum.Value,
          "--enumValue=" + TestEnum.Value2
        };

    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    boolean[] bools = options.getBoolean();
    assertTrue(bools[0] && bools[1] && !bools[2]);
    assertArrayEquals(new char[] {'d', 'e', 'f'}, options.getChar());
    assertArrayEquals(new short[] {300, 301, 302}, options.getShort());
    assertArrayEquals(new int[] {100000, 100001, 100002}, options.getInt());
    assertArrayEquals(new long[] {123890123890L, 123890123891L, 123890123892L}, options.getLong());
    assertArrayEquals(new float[] {55.5f, 55.6f, 55.7f}, options.getFloat(), 0.0f);
    assertArrayEquals(new double[] {12.3, 12.4, 12.5}, options.getDouble(), 0.0);
    assertArrayEquals(
        new String[] {"stringValue1", "stringValue2", "stringValue3"}, options.getString());
    assertArrayEquals(
        new Class[] {PipelineOptionsFactory.class, PipelineOptionsFactoryTest.class},
        options.getClassValue());
    assertArrayEquals(new TestEnum[] {TestEnum.Value, TestEnum.Value2}, options.getEnum());
    assertArrayEquals(new String[] {"abc", "beam"}, options.getStringValue().get());
    assertArrayEquals(new Long[] {123890123890L, 123890123891L}, options.getLongValue().get());
    assertArrayEquals(
        new TestEnum[] {TestEnum.Value, TestEnum.Value2}, options.getEnumValue().get());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testEmptyInStringArrays() {
    String[] args = new String[] {"--string=", "--string=", "--string="};

    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    assertArrayEquals(new String[] {"", "", ""}, options.getString());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testEmptyInStringArraysWithCommaList() {
    String[] args = new String[] {"--string=a,,b"};

    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    assertArrayEquals(new String[] {"a", "", "b"}, options.getString());
  }

  @Test
  public void testEmptyInNonStringArrays() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(emptyStringErrorMessage());

    String[] args = new String[] {"--boolean=true", "--boolean=", "--boolean=false"};

    PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
  }

  @Test
  public void testEmptyInNonStringArraysWithCommaList() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(emptyStringErrorMessage());

    String[] args = new String[] {"--int=1,,9"};
    PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
  }

  @Test
  public void testStringArrayValueProvider() {
    String[] args = new String[] {"--stringValue=abc", "--stringValue=xyz"};
    String[] commaArgs = new String[] {"--stringValue=abc,xyz"};
    String[] emptyArgs = new String[] {"--stringValue=", "--stringValue="};
    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    assertArrayEquals(new String[] {"abc", "xyz"}, options.getStringValue().get());
    options = PipelineOptionsFactory.fromArgs(commaArgs).as(Arrays.class);
    assertArrayEquals(new String[] {"abc", "xyz"}, options.getStringValue().get());
    options = PipelineOptionsFactory.fromArgs(emptyArgs).as(Arrays.class);
    assertArrayEquals(new String[] {"", ""}, options.getStringValue().get());
  }

  @Test
  public void testLongArrayValueProvider() {
    String[] args = new String[] {"--longValue=12345678762", "--longValue=12345678763"};
    String[] commaArgs = new String[] {"--longValue=12345678762,12345678763"};
    String[] emptyArgs = new String[] {"--longValue=", "--longValue="};
    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    assertArrayEquals(new Long[] {12345678762L, 12345678763L}, options.getLongValue().get());
    options = PipelineOptionsFactory.fromArgs(commaArgs).as(Arrays.class);
    assertArrayEquals(new Long[] {12345678762L, 12345678763L}, options.getLongValue().get());
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(emptyStringErrorMessage());
    PipelineOptionsFactory.fromArgs(emptyArgs).as(Arrays.class);
  }

  @Test
  public void testEnumArrayValueProvider() {
    String[] args =
        new String[] {"--enumValue=" + TestEnum.Value, "--enumValue=" + TestEnum.Value2};
    String[] commaArgs = new String[] {"--enumValue=" + TestEnum.Value + "," + TestEnum.Value2};
    String[] emptyArgs = new String[] {"--enumValue="};
    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    assertArrayEquals(
        new TestEnum[] {TestEnum.Value, TestEnum.Value2}, options.getEnumValue().get());
    options = PipelineOptionsFactory.fromArgs(commaArgs).as(Arrays.class);
    assertArrayEquals(
        new TestEnum[] {TestEnum.Value, TestEnum.Value2}, options.getEnumValue().get());
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(emptyStringErrorMessage());
    PipelineOptionsFactory.fromArgs(emptyArgs).as(Arrays.class);
  }

  @Test
  public void testOutOfOrderArrays() {
    String[] args =
        new String[] {
          "--char=d", "--boolean=true", "--boolean=true", "--char=e", "--char=f", "--boolean=false"
        };

    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    boolean[] bools = options.getBoolean();
    assertTrue(bools[0] && bools[1] && !bools[2]);
    assertArrayEquals(new char[] {'d', 'e', 'f'}, options.getChar());
  }

  /** A test interface containing all supported List return types. */
  public interface Lists extends PipelineOptions {
    List<String> getString();

    void setString(List<String> value);

    List<Integer> getInteger();

    void setInteger(List<Integer> value);

    @SuppressWarnings("rawtypes")
    List getList();

    @SuppressWarnings("rawtypes")
    void setList(List value);

    ValueProvider<List<String>> getStringValue();

    void setStringValue(ValueProvider<List<String>> value);

    ValueProvider<List<Long>> getLongValue();

    void setLongValue(ValueProvider<List<Long>> value);

    ValueProvider<List<TestEnum>> getEnumValue();

    void setEnumValue(ValueProvider<List<TestEnum>> value);
  }

  @Test
  public void testListRawDefaultsToString() {
    String[] manyArgs =
        new String[] {"--list=stringValue1", "--list=stringValue2", "--list=stringValue3"};

    String[] manyArgsWithEmptyString =
        new String[] {"--list=stringValue1", "--list=", "--list=stringValue3"};

    Lists options = PipelineOptionsFactory.fromArgs(manyArgs).as(Lists.class);
    assertEquals(
        ImmutableList.of("stringValue1", "stringValue2", "stringValue3"), options.getList());
    options = PipelineOptionsFactory.fromArgs(manyArgsWithEmptyString).as(Lists.class);
    assertEquals(ImmutableList.of("stringValue1", "", "stringValue3"), options.getList());
  }

  @Test
  public void testListString() {
    String[] manyArgs =
        new String[] {"--string=stringValue1", "--string=stringValue2", "--string=stringValue3"};
    String[] oneArg = new String[] {"--string=stringValue1"};
    String[] emptyArg = new String[] {"--string="};

    Lists options = PipelineOptionsFactory.fromArgs(manyArgs).as(Lists.class);
    assertEquals(
        ImmutableList.of("stringValue1", "stringValue2", "stringValue3"), options.getString());

    options = PipelineOptionsFactory.fromArgs(oneArg).as(Lists.class);
    assertEquals(ImmutableList.of("stringValue1"), options.getString());

    options = PipelineOptionsFactory.fromArgs(emptyArg).as(Lists.class);
    assertEquals(ImmutableList.of(""), options.getString());
  }

  @Test
  public void testListInt() {
    String[] manyArgs = new String[] {"--integer=1", "--integer=2", "--integer=3"};
    String[] manyArgsShort = new String[] {"--integer=1,2,3"};
    String[] oneArg = new String[] {"--integer=1"};
    String[] missingArg = new String[] {"--integer="};

    Lists options = PipelineOptionsFactory.fromArgs(manyArgs).as(Lists.class);
    assertEquals(ImmutableList.of(1, 2, 3), options.getInteger());
    options = PipelineOptionsFactory.fromArgs(manyArgsShort).as(Lists.class);
    assertEquals(ImmutableList.of(1, 2, 3), options.getInteger());
    options = PipelineOptionsFactory.fromArgs(oneArg).as(Lists.class);
    assertEquals(ImmutableList.of(1), options.getInteger());

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(emptyStringErrorMessage("java.util.List<java.lang.Integer>"));
    PipelineOptionsFactory.fromArgs(missingArg).as(Lists.class);
  }

  @Test
  public void testListShorthand() {
    String[] args = new String[] {"--string=stringValue1,stringValue2,stringValue3"};

    Lists options = PipelineOptionsFactory.fromArgs(args).as(Lists.class);
    assertEquals(
        ImmutableList.of("stringValue1", "stringValue2", "stringValue3"), options.getString());
  }

  @Test
  public void testMixedShorthandAndLongStyleList() {
    String[] args =
        new String[] {
          "--char=d",
          "--char=e",
          "--char=f",
          "--char=g,h,i",
          "--char=j",
          "--char=k",
          "--char=l",
          "--char=m,n,o"
        };

    Arrays options = PipelineOptionsFactory.fromArgs(args).as(Arrays.class);
    assertArrayEquals(
        new char[] {'d', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o'}, options.getChar());
  }

  @Test
  public void testStringListValueProvider() {
    String[] args = new String[] {"--stringValue=abc", "--stringValue=xyz"};
    String[] commaArgs = new String[] {"--stringValue=abc,xyz"};
    String[] emptyArgs = new String[] {"--stringValue=", "--stringValue="};
    Lists options = PipelineOptionsFactory.fromArgs(args).as(Lists.class);
    assertEquals(ImmutableList.of("abc", "xyz"), options.getStringValue().get());
    options = PipelineOptionsFactory.fromArgs(commaArgs).as(Lists.class);
    assertEquals(ImmutableList.of("abc", "xyz"), options.getStringValue().get());
    options = PipelineOptionsFactory.fromArgs(emptyArgs).as(Lists.class);
    assertEquals(ImmutableList.of("", ""), options.getStringValue().get());
  }

  @Test
  public void testLongListValueProvider() {
    String[] args = new String[] {"--longValue=12345678762", "--longValue=12345678763"};
    String[] commaArgs = new String[] {"--longValue=12345678762,12345678763"};
    String[] emptyArgs = new String[] {"--longValue=", "--longValue="};
    Lists options = PipelineOptionsFactory.fromArgs(args).as(Lists.class);
    assertEquals(ImmutableList.of(12345678762L, 12345678763L), options.getLongValue().get());
    options = PipelineOptionsFactory.fromArgs(commaArgs).as(Lists.class);
    assertEquals(ImmutableList.of(12345678762L, 12345678763L), options.getLongValue().get());
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(emptyStringErrorMessage());
    PipelineOptionsFactory.fromArgs(emptyArgs).as(Lists.class);
  }

  @Test
  public void testEnumListValueProvider() {
    String[] args =
        new String[] {"--enumValue=" + TestEnum.Value, "--enumValue=" + TestEnum.Value2};
    String[] commaArgs = new String[] {"--enumValue=" + TestEnum.Value + "," + TestEnum.Value2};
    String[] emptyArgs = new String[] {"--enumValue="};
    Lists options = PipelineOptionsFactory.fromArgs(args).as(Lists.class);
    assertEquals(ImmutableList.of(TestEnum.Value, TestEnum.Value2), options.getEnumValue().get());
    options = PipelineOptionsFactory.fromArgs(commaArgs).as(Lists.class);
    assertEquals(ImmutableList.of(TestEnum.Value, TestEnum.Value2), options.getEnumValue().get());
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(emptyStringErrorMessage());
    PipelineOptionsFactory.fromArgs(emptyArgs).as(Lists.class);
  }

  @Test
  public void testSetASingularAttributeUsingAListThrowsAnError() {
    String[] args = new String[] {"--string=100", "--string=200"};
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expected one element but was");
    PipelineOptionsFactory.fromArgs(args).as(Objects.class);
  }

  @Test
  public void testSetASingularAttributeUsingAListIsIgnoredWithoutStrictParsing() {
    String[] args = new String[] {"--diskSizeGb=100", "--diskSizeGb=200"};
    PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
    expectedLogs.verifyWarn("Strict parsing is disabled, ignoring option");
  }

  private interface NonPublicPipelineOptions extends PipelineOptions {}

  @Test
  public void testNonPublicInterfaceThrowsException() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Please mark non-public interface "
            + NonPublicPipelineOptions.class.getName()
            + " as public.");

    PipelineOptionsFactory.as(NonPublicPipelineOptions.class);
  }

  /** A test interface containing all supported List return types. */
  public interface Maps extends PipelineOptions {
    Map<Integer, Integer> getMap();

    void setMap(Map<Integer, Integer> value);

    Map<Integer, Map<Integer, Integer>> getNestedMap();

    void setNestedMap(Map<Integer, Map<Integer, Integer>> value);
  }

  @Test
  public void testMapIntInt() {
    String[] manyArgsShort = new String[] {"--map={\"1\":1,\"2\":2}"};
    String[] oneArg = new String[] {"--map={\"1\":1}"};
    String[] missingArg = new String[] {"--map="};

    Maps options = PipelineOptionsFactory.fromArgs(manyArgsShort).as(Maps.class);
    assertEquals(ImmutableMap.of(1, 1, 2, 2), options.getMap());
    options = PipelineOptionsFactory.fromArgs(oneArg).as(Maps.class);
    assertEquals(ImmutableMap.of(1, 1), options.getMap());

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        emptyStringErrorMessage("java.util.Map<java.lang.Integer, java.lang.Integer>"));
    PipelineOptionsFactory.fromArgs(missingArg).as(Maps.class);
  }

  @Test
  public void testNestedMap() {
    String[] manyArgsShort = new String[] {"--nestedMap={\"1\":{\"1\":1},\"2\":{\"2\":2}}"};
    String[] oneArg = new String[] {"--nestedMap={\"1\":{\"1\":1}}"};
    String[] missingArg = new String[] {"--nestedMap="};

    Maps options = PipelineOptionsFactory.fromArgs(manyArgsShort).as(Maps.class);
    assertEquals(
        ImmutableMap.of(
            1, ImmutableMap.of(1, 1),
            2, ImmutableMap.of(2, 2)),
        options.getNestedMap());
    options = PipelineOptionsFactory.fromArgs(oneArg).as(Maps.class);
    assertEquals(ImmutableMap.of(1, ImmutableMap.of(1, 1)), options.getNestedMap());

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        emptyStringErrorMessage(
            "java.util.Map<java.lang.Integer, java.util.Map<java.lang.Integer,"
                + " java.lang.Integer>>"));
    PipelineOptionsFactory.fromArgs(missingArg).as(Maps.class);
  }

  @Test
  public void testSettingRunner() {
    String[] args = new String[] {"--runner=" + RegisteredTestRunner.class.getSimpleName()};

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    assertEquals(RegisteredTestRunner.class, options.getRunner());
  }

  @Test
  public void testSettingRunnerFullName() {
    String[] args = new String[] {String.format("--runner=%s", CrashingRunner.class.getName())};
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
    assertThat(registeredRunners, hasItem(REGISTERED_RUNNER.getSimpleName().toLowerCase()));

    expectedException.expectMessage(
        PipelineOptionsFactory.CACHE.get().getSupportedRunners().toString());

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

  /** Test interface. */
  public interface SuggestedOptions extends PipelineOptions {
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
        new String[] {"", "--string=100", "", "", "--runner=" + REGISTERED_RUNNER.getSimpleName()};
    PipelineOptionsFactory.fromArgs(args).as(Objects.class);
  }

  @Test
  public void testNullArgumentIsIgnored() {
    String[] args =
        new String[] {"--string=100", null, null, "--runner=" + REGISTERED_RUNNER.getSimpleName()};
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
    assertFalse(
        PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
            arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    assertEquals("", output);
  }

  @Test
  public void testDefaultHelpAsArgument() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "true");
    assertTrue(
        PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
            arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
    assertThat(output, containsString("Use --help=<OptionsName> for detailed help."));
  }

  @Test
  public void testSpecificHelpAsArgument() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "org.apache.beam.sdk.options.PipelineOptions");
    assertTrue(
        PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
            arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: " + DEFAULT_RUNNER_NAME));
    assertThat(
        output, containsString("The pipeline runner that will be used to execute the pipeline."));
  }

  @Test
  public void testSpecificHelpAsArgumentWithSimpleClassName() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "PipelineOptions");
    assertTrue(
        PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
            arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: " + DEFAULT_RUNNER_NAME));
    assertThat(
        output, containsString("The pipeline runner that will be used to execute the pipeline."));
  }

  @Test
  public void testSpecificHelpAsArgumentWithClassNameSuffix() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "options.PipelineOptions");
    assertTrue(
        PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
            arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: " + DEFAULT_RUNNER_NAME));
    assertThat(
        output, containsString("The pipeline runner that will be used to execute the pipeline."));
  }

  /** Used for a name collision test with the other NameConflict interfaces. */
  public static class NameConflictClassA {
    /** Used for a name collision test with the other NameConflict interfaces. */
    public interface NameConflict extends PipelineOptions {}
  }

  /** Used for a name collision test with the other NameConflict interfaces. */
  public static class NameConflictClassB {
    /** Used for a name collision test with the other NameConflict interfaces. */
    public interface NameConflict extends PipelineOptions {}
  }

  @Test
  public void testShortnameSpecificHelpHasMultipleMatches() {
    PipelineOptionsFactory.register(NameConflictClassA.NameConflict.class);
    PipelineOptionsFactory.register(NameConflictClassB.NameConflict.class);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "NameConflict");
    assertTrue(
        PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
            arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    assertThat(output, containsString("Multiple matches found for NameConflict"));
    assertThat(
        output,
        containsString(
            "org.apache.beam.sdk.options."
                + "PipelineOptionsFactoryTest$NameConflictClassA$NameConflict"));
    assertThat(
        output,
        containsString(
            "org.apache.beam.sdk.options."
                + "PipelineOptionsFactoryTest$NameConflictClassB$NameConflict"));
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
  }

  @Test
  public void testHelpWithOptionThatOutputsValidEnumTypes() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", Objects.class.getName());
    assertTrue(
        PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
            arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    assertThat(output, containsString("<Value | Value2>"));
  }

  @Test
  public void testHelpWithBadOptionNameAsArgument() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "org.apache.beam.sdk.Pipeline");
    assertTrue(
        PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
            arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    assertThat(output, containsString("Unable to find option org.apache.beam.sdk.Pipeline"));
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
  }

  @Test
  public void testHelpWithHiddenMethodAndInterface() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ListMultimap<String, String> arguments = ArrayListMultimap.create();
    arguments.put("help", "org.apache.beam.sdk.option.DataflowPipelineOptions");
    assertTrue(
        PipelineOptionsFactory.printHelpUsageAndExitIfNeeded(
            arguments, new PrintStream(baos), false /* exit */));
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    // A hidden interface.
    assertThat(
        output, not(containsString("org.apache.beam.sdk.options.DataflowPipelineDebugOptions")));
    // A hidden option.
    assertThat(output, not(containsString("--gcpCredential")));
  }

  @Test
  public void testProgrammaticPrintHelp() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PipelineOptionsFactory.printHelp(new PrintStream(baos));
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    assertThat(output, containsString("The set of registered options are:"));
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
  }

  @Test
  public void testProgrammaticPrintHelpForSpecificType() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PipelineOptionsFactory.printHelp(new PrintStream(baos), PipelineOptions.class);
    String output = new String(baos.toByteArray(), Charsets.UTF_8);
    assertThat(output, containsString("org.apache.beam.sdk.options.PipelineOptions"));
    assertThat(output, containsString("--runner"));
    assertThat(output, containsString("Default: " + DEFAULT_RUNNER_NAME));
    assertThat(
        output, containsString("The pipeline runner that will be used to execute the pipeline."));
  }

  /** Test interface. */
  public interface PipelineOptionsInheritedInvalid
      extends Invalid1, InvalidPipelineOptions2, PipelineOptions {
    String getFoo();

    void setFoo(String value);
  }

  /** Test interface. */
  public interface InvalidPipelineOptions1 {
    String getBar();

    void setBar(String value);
  }

  /** Test interface. */
  public interface Invalid1 extends InvalidPipelineOptions1 {
    @Override
    String getBar();

    @Override
    void setBar(String value);
  }

  /** Test interface. */
  public interface InvalidPipelineOptions2 {
    String getBar();

    void setBar(String value);
  }

  @Test
  public void testAllFromPipelineOptions() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "All inherited interfaces of"
            + " [org.apache.beam.sdk.options.PipelineOptionsFactoryTest$PipelineOptionsInheritedInvalid]"
            + " should inherit from the PipelineOptions interface. The following inherited"
            + " interfaces do not:\n"
            + " - org.apache.beam.sdk.options.PipelineOptionsFactoryTest$InvalidPipelineOptions1\n"
            + " - org.apache.beam.sdk.options.PipelineOptionsFactoryTest$InvalidPipelineOptions2");

    PipelineOptionsFactory.as(PipelineOptionsInheritedInvalid.class);
  }

  private String emptyStringErrorMessage() {
    return emptyStringErrorMessage(null);
  }

  private String emptyStringErrorMessage(String type) {
    String msg =
        "Empty argument value is only allowed for String, String Array, "
            + "Collections of Strings or any of these types in a parameterized ValueProvider";
    if (type != null) {
      return String.format("%s, but received: %s", msg, type);
    } else {
      return msg;
    }
  }

  private static class RegisteredTestRunner extends PipelineRunner<PipelineResult> {
    public static PipelineRunner<PipelineResult> fromOptions(PipelineOptions options) {
      return new RegisteredTestRunner();
    }

    @Override
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
      return ImmutableList.of(RegisteredTestRunner.class);
    }
  }

  /** Test interface. */
  public interface RegisteredTestOptions extends PipelineOptions {
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
      return ImmutableList.of(RegisteredTestOptions.class);
    }
  }

  @Test
  public void testRegistrationOfJacksonModulesForObjectMapper() throws Exception {
    JacksonIncompatibleOptions options =
        PipelineOptionsFactory.fromArgs("--jacksonIncompatible=\"testValue\"")
            .as(JacksonIncompatibleOptions.class);
    assertEquals("testValue", options.getJacksonIncompatible().value);
  }

  /** PipelineOptions used to test auto registration of Jackson modules. */
  public interface JacksonIncompatibleOptions extends PipelineOptions {
    JacksonIncompatible getJacksonIncompatible();

    void setJacksonIncompatible(JacksonIncompatible value);
  }

  /** A Jackson {@link Module} to test auto-registration of modules. */
  @AutoService(Module.class)
  public static class RegisteredTestModule extends SimpleModule {
    public RegisteredTestModule() {
      super("RegisteredTestModule");
      setMixInAnnotation(JacksonIncompatible.class, JacksonIncompatibleMixin.class);
    }
  }

  /** A class which Jackson does not know how to serialize/deserialize. */
  public static class JacksonIncompatible {
    private final String value;

    public JacksonIncompatible(String value) {
      this.value = value;
    }
  }

  /** A Jackson mixin used to add annotations to other classes. */
  @JsonDeserialize(using = JacksonIncompatibleDeserializer.class)
  @JsonSerialize(using = JacksonIncompatibleSerializer.class)
  public static final class JacksonIncompatibleMixin {}

  /** A Jackson deserializer for {@link JacksonIncompatible}. */
  public static class JacksonIncompatibleDeserializer
      extends JsonDeserializer<JacksonIncompatible> {

    @Override
    public JacksonIncompatible deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {
      return new JacksonIncompatible(jsonParser.readValueAs(String.class));
    }
  }

  /** A Jackson serializer for {@link JacksonIncompatible}. */
  public static class JacksonIncompatibleSerializer extends JsonSerializer<JacksonIncompatible> {

    @Override
    public void serialize(
        JacksonIncompatible jacksonIncompatible,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
        throws IOException, JsonProcessingException {
      jsonGenerator.writeString(jacksonIncompatible.value);
    }
  }

  /** Used to test that the thread context class loader is used when creating proxies. */
  public interface ClassLoaderTestOptions extends PipelineOptions {
    @Default.Boolean(true)
    @Description("A test option.")
    boolean isOption();

    void setOption(boolean b);
  }

  @Test
  public void testPipelineOptionsFactoryUsesTccl() throws Exception {
    final Thread thread = Thread.currentThread();
    final ClassLoader testClassLoader = thread.getContextClassLoader();
    final ClassLoader caseLoader =
        new InterceptingUrlClassLoader(
            testClassLoader, name -> name.toLowerCase(ROOT).contains("test"));
    thread.setContextClassLoader(caseLoader);
    PipelineOptionsFactory.resetCache();
    try {
      final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
      final Class optionType =
          caseLoader.loadClass(
              "org.apache.beam.sdk.options.PipelineOptionsFactoryTest$ClassLoaderTestOptions");
      final Object options = pipelineOptions.as(optionType);
      assertSame(caseLoader, options.getClass().getClassLoader());
      assertSame(optionType.getClassLoader(), options.getClass().getClassLoader());
      assertSame(testClassLoader, optionType.getInterfaces()[0].getClassLoader());
      assertTrue(Boolean.class.cast(optionType.getMethod("isOption").invoke(options)));
    } finally {
      thread.setContextClassLoader(testClassLoader);
      PipelineOptionsFactory.resetCache();
    }
  }

  @Test
  public void testDefaultMethodIgnoresDefaultImplementation() {
    OptionsWithDefaultMethod optsWithDefault =
        PipelineOptionsFactory.as(OptionsWithDefaultMethod.class);
    assertThat(optsWithDefault.getValue(), nullValue());

    optsWithDefault.setValue(12.25);
    assertThat(optsWithDefault.getValue(), equalTo(12.25));
  }

  /** Test interface. */
  public interface ExtendedOptionsWithDefault extends OptionsWithDefaultMethod {}

  @Test
  public void testDefaultMethodInExtendedClassIgnoresDefaultImplementation() {
    OptionsWithDefaultMethod extendedOptsWithDefault =
        PipelineOptionsFactory.as(ExtendedOptionsWithDefault.class);
    assertThat(extendedOptsWithDefault.getValue(), nullValue());

    extendedOptsWithDefault.setValue(Double.NEGATIVE_INFINITY);
    assertThat(extendedOptsWithDefault.getValue(), equalTo(Double.NEGATIVE_INFINITY));
  }

  /** Test interface. */
  public interface OptionsWithDefaultMethod extends PipelineOptions {
    default Number getValue() {
      return 1024;
    }

    void setValue(Number value);
  }

  @Test
  public void testStaticMethodsAreAllowed() {
    assertEquals(
        "value",
        OptionsWithStaticMethod.myStaticMethod(
            PipelineOptionsFactory.fromArgs("--myMethod=value").as(OptionsWithStaticMethod.class)));
  }

  /** Test interface. */
  public interface OptionsWithStaticMethod extends PipelineOptions {
    String getMyMethod();

    void setMyMethod(String value);

    static String myStaticMethod(OptionsWithStaticMethod o) {
      return o.getMyMethod();
    }
  }

  /** Test interface. */
  public interface TestDescribeOptions extends PipelineOptions {
    String getString();

    void setString(String value);

    @Description("integer property")
    Integer getInteger();

    void setInteger(Integer value);

    @Description("float number property")
    Float getFloat();

    void setFloat(Float value);

    @Description("simple boolean property")
    @Default.Boolean(true)
    boolean getBooleanSimple();

    void setBooleanSimple(boolean value);

    @Default.Boolean(false)
    Boolean getBooleanWrapper();

    void setBooleanWrapper(Boolean value);

    List<Integer> getList();

    void setList(List<Integer> value);
  }

  @Test
  public void testDescribe() {
    List<PipelineOptionDescriptor> described =
        PipelineOptionsFactory.describe(
            Sets.newHashSet(PipelineOptions.class, TestDescribeOptions.class));

    Map<String, PipelineOptionDescriptor> mapped = uniqueIndex(described, input -> input.getName());
    assertEquals("no duplicates", described.size(), mapped.size());

    Collection<PipelineOptionDescriptor> filtered =
        Collections2.filter(
            described, input -> input.getGroup().equals(TestDescribeOptions.class.getName()));
    assertEquals(6, filtered.size());
    mapped = uniqueIndex(filtered, input -> input.getName());

    PipelineOptionDescriptor listDesc = mapped.get("list");
    assertThat(listDesc, notNullValue());
    assertThat(listDesc.getDescription(), isEmptyString());
    assertEquals(PipelineOptionType.Enum.ARRAY, listDesc.getType());
    assertThat(listDesc.getDefaultValue(), isEmptyString());

    PipelineOptionDescriptor stringDesc = mapped.get("string");
    assertThat(stringDesc, notNullValue());
    assertThat(stringDesc.getDescription(), isEmptyString());
    assertEquals(PipelineOptionType.Enum.STRING, stringDesc.getType());
    assertThat(stringDesc.getDefaultValue(), isEmptyString());

    PipelineOptionDescriptor integerDesc = mapped.get("integer");
    assertThat(integerDesc, notNullValue());
    assertEquals("integer property", integerDesc.getDescription());
    assertEquals(PipelineOptionType.Enum.INTEGER, integerDesc.getType());
    assertThat(integerDesc.getDefaultValue(), isEmptyString());

    PipelineOptionDescriptor floatDesc = mapped.get("float");
    assertThat(integerDesc, notNullValue());
    assertEquals("float number property", floatDesc.getDescription());
    assertEquals(PipelineOptionType.Enum.NUMBER, floatDesc.getType());
    assertThat(floatDesc.getDefaultValue(), isEmptyString());

    PipelineOptionDescriptor booleanSimpleDesc = mapped.get("boolean_simple");
    assertThat(booleanSimpleDesc, notNullValue());
    assertEquals("simple boolean property", booleanSimpleDesc.getDescription());
    assertEquals(PipelineOptionType.Enum.BOOLEAN, booleanSimpleDesc.getType());
    assertThat(booleanSimpleDesc.getDefaultValue(), equalTo("true"));

    PipelineOptionDescriptor booleanWrapperDesc = mapped.get("boolean_wrapper");
    assertThat(booleanWrapperDesc, notNullValue());
    assertThat(booleanWrapperDesc.getDescription(), isEmptyString());
    assertEquals(PipelineOptionType.Enum.BOOLEAN, booleanWrapperDesc.getType());
    assertThat(booleanWrapperDesc.getDefaultValue(), equalTo("false"));
  }
}
