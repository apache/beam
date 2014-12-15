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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PipelineOptionsValidator}. */
@RunWith(JUnit4.class)
public class PipelineOptionsValidatorTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  /** A test interface with an {@link Validation.Required} annotation. */
  public static interface Required extends PipelineOptions {
    @Validation.Required
    @Description("Fake Description")
    public String getObject();
    public void setObject(String value);
  }

  @Test
  public void testWhenRequiredOptionIsSet() {
    Required required = PipelineOptionsFactory.as(Required.class);
    required.setObject("blah");
    PipelineOptionsValidator.validate(Required.class, required);
  }

  @Test
  public void testWhenRequiredOptionIsSetAndCleared() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Missing required value for "
        + "[public abstract java.lang.String com.google.cloud.dataflow."
        + "sdk.options.PipelineOptionsValidatorTest$Required.getObject(), \"Fake Description\"].");

    Required required = PipelineOptionsFactory.as(Required.class);
    required.setObject("blah");
    required.setObject(null);
    PipelineOptionsValidator.validate(Required.class, required);
  }

  @Test
  public void testWhenRequiredOptionIsNeverSet() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Missing required value for "
        + "[public abstract java.lang.String com.google.cloud.dataflow."
        + "sdk.options.PipelineOptionsValidatorTest$Required.getObject(), \"Fake Description\"].");

    Required required = PipelineOptionsFactory.as(Required.class);
    PipelineOptionsValidator.validate(Required.class, required);
  }

  /** A test interface which overrides the parents method. */
  public static interface SubClassValidation extends Required {
    @Override
    public String getObject();
    @Override
    public void setObject(String value);
  }

  @Test
  public void testValidationOnOverriddenMethods() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Missing required value for "
        + "[public abstract java.lang.String com.google.cloud.dataflow."
        + "sdk.options.PipelineOptionsValidatorTest$Required.getObject(), \"Fake Description\"].");

    SubClassValidation required = PipelineOptionsFactory.as(SubClassValidation.class);
    PipelineOptionsValidator.validate(Required.class, required);
  }
}
