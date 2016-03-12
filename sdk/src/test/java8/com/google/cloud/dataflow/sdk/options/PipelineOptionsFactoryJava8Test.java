/*
 * Copyright (C) 2016 Google Inc.
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Java 8 tests for {@link PipelineOptionsFactory}.
 */
@RunWith(JUnit4.class)
public class PipelineOptionsFactoryJava8Test {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static interface OptionsWithDefaultMethod extends PipelineOptions {
    default Number getValue() {
      return 1024;
    }

    void setValue(Number value);
  }

  @Test
  public void testDefaultMethodIgnoresDefaultImplementation() {
    OptionsWithDefaultMethod optsWithDefault =
        PipelineOptionsFactory.as(OptionsWithDefaultMethod.class);
    assertThat(optsWithDefault.getValue(), nullValue());

    optsWithDefault.setValue(12.25);
    assertThat(optsWithDefault.getValue(), equalTo(Double.valueOf(12.25)));
  }

  private static interface ExtendedOptionsWithDefault extends OptionsWithDefaultMethod {}

  @Test
  public void testDefaultMethodInExtendedClassIgnoresDefaultImplementation() {
    OptionsWithDefaultMethod extendedOptsWithDefault =
        PipelineOptionsFactory.as(ExtendedOptionsWithDefault.class);
    assertThat(extendedOptsWithDefault.getValue(), nullValue());

    extendedOptsWithDefault.setValue(Double.NEGATIVE_INFINITY);
    assertThat(extendedOptsWithDefault.getValue(), equalTo(Double.NEGATIVE_INFINITY));
  }

  private static interface Options extends PipelineOptions {
    Number getValue();

    void setValue(Number value);
  }

  private static interface SubtypeReturingOptions extends Options {
    @Override
    Integer getValue();
    void setValue(Integer value);
  }

  @Test
  public void testReturnTypeConflictThrows() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Method [getValue] has multiple definitions [public abstract java.lang.Integer "
            + "com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryJava8Test$"
            + "SubtypeReturingOptions.getValue(), public abstract java.lang.Number "
            + "com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryJava8Test$Options"
            + ".getValue()] with different return types for ["
            + "com.google.cloud.dataflow.sdk.options.PipelineOptionsFactoryJava8Test$"
            + "SubtypeReturingOptions].");
    PipelineOptionsFactory.as(SubtypeReturingOptions.class);
  }
}
