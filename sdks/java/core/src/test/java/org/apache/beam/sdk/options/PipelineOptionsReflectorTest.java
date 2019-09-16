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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Set;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PipelineOptionsReflector}. */
@RunWith(JUnit4.class)
public class PipelineOptionsReflectorTest {
  @Test
  public void testGetOptionSpecs() throws NoSuchMethodException {
    Set<PipelineOptionSpec> properties =
        PipelineOptionsReflector.getOptionSpecs(SimpleOptions.class, true);

    assertThat(
        properties,
        Matchers.hasItems(
            PipelineOptionSpec.of(
                SimpleOptions.class, "foo", SimpleOptions.class.getDeclaredMethod("getFoo"))));
  }

  /** Test interface. */
  public interface SimpleOptions extends PipelineOptions {
    String getFoo();

    void setFoo(String value);
  }

  @Test
  public void testFiltersNonGetterMethods() {
    Set<PipelineOptionSpec> properties =
        PipelineOptionsReflector.getOptionSpecs(OnlyTwoValidGetters.class, true);

    assertThat(properties, not(hasItem(hasName(isOneOf("misspelled", "hasParameter", "prefix")))));
  }

  /** Test interface. */
  public interface OnlyTwoValidGetters extends PipelineOptions {
    String getFoo();

    void setFoo(String value);

    boolean isBar();

    void setBar(boolean value);

    String gtMisspelled();

    void setMisspelled(String value);

    String getHasParameter(String value);

    void setHasParameter(String value);

    String noPrefix();

    void setNoPrefix(String value);
  }

  @Test
  public void testBaseClassOptions() {
    Set<PipelineOptionSpec> props =
        PipelineOptionsReflector.getOptionSpecs(ExtendsSimpleOptions.class, true);

    assertThat(props, hasItem(allOf(hasName("foo"), hasClass(SimpleOptions.class))));
    assertThat(props, hasItem(allOf(hasName("foo"), hasClass(ExtendsSimpleOptions.class))));
    assertThat(props, hasItem(allOf(hasName("bar"), hasClass(ExtendsSimpleOptions.class))));
  }

  /** Test interface. */
  public interface ExtendsSimpleOptions extends SimpleOptions {
    @Override
    String getFoo();

    @Override
    void setFoo(String value);

    String getBar();

    void setBar(String value);
  }

  @Test
  public void testExcludesNonPipelineOptionsMethods() {
    Set<PipelineOptionSpec> properties =
        PipelineOptionsReflector.getOptionSpecs(ExtendsNonPipelineOptions.class, true);

    assertThat(properties, not(hasItem(hasName("foo"))));
  }

  /** Test interface. */
  public interface NoExtendsClause {
    String getFoo();

    void setFoo(String value);
  }

  /** Test interface. */
  public interface ExtendsNonPipelineOptions extends NoExtendsClause, PipelineOptions {}

  @Test
  public void testExcludesHiddenInterfaces() {
    Set<PipelineOptionSpec> properties =
        PipelineOptionsReflector.getOptionSpecs(HiddenOptions.class, true);

    assertThat(properties, not(hasItem(hasName("foo"))));
  }

  @Test
  public void testIncludesHiddenInterfaces() {
    Set<PipelineOptionSpec> properties =
        PipelineOptionsReflector.getOptionSpecs(HiddenOptions.class, false);

    assertThat(properties, hasItem(hasName("foo")));
  }

  /** Test interface. */
  @Hidden
  public interface HiddenOptions extends PipelineOptions {
    String getFoo();

    void setFoo(String value);
  }

  @Test
  public void testShouldSerialize() {
    Set<PipelineOptionSpec> properties =
        PipelineOptionsReflector.getOptionSpecs(JsonIgnoreOptions.class, true);

    assertThat(properties, hasItem(allOf(hasName("notIgnored"), shouldSerialize())));
    assertThat(properties, hasItem(allOf(hasName("ignored"), not(shouldSerialize()))));
  }

  /** Test interface. */
  public interface JsonIgnoreOptions extends PipelineOptions {
    String getNotIgnored();

    void setNotIgnored(String value);

    @JsonIgnore
    String getIgnored();

    void setIgnored(String value);
  }

  @Test
  public void testMultipleInputInterfaces() {
    Set<Class<? extends PipelineOptions>> interfaces =
        ImmutableSet.of(BaseOptions.class, ExtendOptions1.class, ExtendOptions2.class);

    Set<PipelineOptionSpec> props = PipelineOptionsReflector.getOptionSpecs(interfaces);

    assertThat(props, hasItem(allOf(hasName("baseOption"), hasClass(BaseOptions.class))));
    assertThat(props, hasItem(allOf(hasName("extendOption1"), hasClass(ExtendOptions1.class))));
    assertThat(props, hasItem(allOf(hasName("extendOption2"), hasClass(ExtendOptions2.class))));
  }

  /** Test interface. */
  public interface BaseOptions extends PipelineOptions {
    String getBaseOption();

    void setBaseOption(String value);
  }

  /** Test interface. */
  public interface ExtendOptions1 extends BaseOptions {
    String getExtendOption1();

    void setExtendOption1(String value);
  }

  /** Test interface. */
  public interface ExtendOptions2 extends BaseOptions {
    String getExtendOption2();

    void setExtendOption2(String value);
  }

  private static Matcher<PipelineOptionSpec> hasName(String name) {
    return hasName(is(name));
  }

  private static Matcher<PipelineOptionSpec> hasName(Matcher<String> matcher) {
    return new FeatureMatcher<PipelineOptionSpec, String>(matcher, "name", "name") {
      @Override
      protected String featureValueOf(PipelineOptionSpec actual) {
        return actual.getName();
      }
    };
  }

  private static Matcher<PipelineOptionSpec> hasClass(Class<?> clazz) {
    return new FeatureMatcher<PipelineOptionSpec, Class<?>>(
        Matchers.<Class<?>>is(clazz), "defining class", "class") {
      @Override
      protected Class<?> featureValueOf(PipelineOptionSpec actual) {
        return actual.getDefiningInterface();
      }
    };
  }

  private static Matcher<PipelineOptionSpec> hasGetter(String methodName) {
    return new FeatureMatcher<PipelineOptionSpec, String>(is(methodName), "getter method", "name") {
      @Override
      protected String featureValueOf(PipelineOptionSpec actual) {
        return actual.getGetterMethod().getName();
      }
    };
  }

  private static Matcher<PipelineOptionSpec> shouldSerialize() {
    return new FeatureMatcher<PipelineOptionSpec, Boolean>(
        equalTo(true), "should serialize", "shouldSerialize") {

      @Override
      protected Boolean featureValueOf(PipelineOptionSpec actual) {
        return actual.shouldSerialize();
      }
    };
  }
}
