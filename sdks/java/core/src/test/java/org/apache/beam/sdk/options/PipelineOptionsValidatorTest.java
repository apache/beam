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

import org.apache.beam.sdk.testing.CrashingRunner;
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
  public interface Required extends PipelineOptions {
    @Validation.Required
    @Description("Fake Description")
    String getObject();

    void setObject(String value);
  }

  @Test
  public void testWhenRequiredOptionIsSet() {
    Required required = PipelineOptionsFactory.as(Required.class);
    required.setRunner(CrashingRunner.class);
    required.setObject("blah");
    PipelineOptionsValidator.validate(Required.class, required);
  }

  @Test
  public void testWhenRequiredOptionIsSetAndCleared() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Missing required value for "
            + "[public abstract java.lang.String org.apache.beam."
            + "sdk.options.PipelineOptionsValidatorTest$Required.getObject(), \"Fake Description\"].");

    Required required = PipelineOptionsFactory.as(Required.class);
    required.setObject("blah");
    required.setObject(null);
    PipelineOptionsValidator.validate(Required.class, required);
  }

  @Test
  public void testWhenRequiredOptionIsSetAndClearedCli() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Missing required value for " + "[--object, \"Fake Description\"].");

    Required required =
        PipelineOptionsFactory.fromArgs(new String[] {"--object=blah"}).as(Required.class);
    required.setObject(null);
    PipelineOptionsValidator.validateCli(Required.class, required);
  }

  @Test
  public void testWhenRequiredOptionIsNeverSet() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Missing required value for "
            + "[public abstract java.lang.String org.apache.beam."
            + "sdk.options.PipelineOptionsValidatorTest$Required.getObject(), \"Fake Description\"].");

    Required required = PipelineOptionsFactory.as(Required.class);
    PipelineOptionsValidator.validate(Required.class, required);
  }

  @Test
  public void testWhenRequiredOptionIsNeverSetCli() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Missing required value for " + "[--object, \"Fake Description\"].");

    Required required = PipelineOptionsFactory.fromArgs(new String[] {}).as(Required.class);
    PipelineOptionsValidator.validateCli(Required.class, required);
  }

  @Test
  public void testWhenRequiredOptionIsNeverSetOnSuperInterface() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Missing required value for "
            + "[public abstract java.lang.String org.apache.beam."
            + "sdk.options.PipelineOptionsValidatorTest$Required.getObject(), \"Fake Description\"].");

    PipelineOptions options = PipelineOptionsFactory.create();
    PipelineOptionsValidator.validate(Required.class, options);
  }

  @Test
  public void testWhenRequiredOptionIsNeverSetOnSuperInterfaceCli() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Missing required value for " + "[--object, \"Fake Description\"].");

    PipelineOptions options = PipelineOptionsFactory.fromArgs(new String[] {}).create();
    PipelineOptionsValidator.validateCli(Required.class, options);
  }

  /** A test interface that overrides the parent's method. */
  public interface SubClassValidation extends Required {
    @Override
    String getObject();

    @Override
    void setObject(String value);
  }

  @Test
  public void testValidationOnOverriddenMethods() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Missing required value for "
            + "[public abstract java.lang.String org.apache.beam."
            + "sdk.options.PipelineOptionsValidatorTest$Required.getObject(), \"Fake Description\"].");

    SubClassValidation required = PipelineOptionsFactory.as(SubClassValidation.class);
    PipelineOptionsValidator.validate(Required.class, required);
  }

  @Test
  public void testValidationOnOverriddenMethodsCli() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Missing required value for " + "[--object, \"Fake Description\"].");

    SubClassValidation required =
        PipelineOptionsFactory.fromArgs(new String[] {}).as(SubClassValidation.class);
    PipelineOptionsValidator.validateCli(Required.class, required);
  }

  /** A test interface with a required group. */
  public interface GroupRequired extends PipelineOptions {
    @Validation.Required(groups = {"ham"})
    String getFoo();

    void setFoo(String foo);

    @Validation.Required(groups = {"ham"})
    String getBar();

    void setBar(String bar);
  }

  @Test
  public void testWhenOneOfRequiredGroupIsSetIsValid() {
    GroupRequired groupRequired = PipelineOptionsFactory.as(GroupRequired.class);
    groupRequired.setFoo("foo");
    groupRequired.setBar(null);
    groupRequired.setRunner(CrashingRunner.class);

    PipelineOptionsValidator.validate(GroupRequired.class, groupRequired);

    // Symmetric
    groupRequired.setFoo(null);
    groupRequired.setBar("bar");
    PipelineOptionsValidator.validate(GroupRequired.class, groupRequired);
  }

  @Test
  public void testWhenNoneOfRequiredGroupIsSetThrowsException() {
    GroupRequired groupRequired = PipelineOptionsFactory.as(GroupRequired.class);
    groupRequired.setRunner(CrashingRunner.class);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Missing required value for group [ham]");
    expectedException.expectMessage("properties");
    expectedException.expectMessage("getFoo");
    expectedException.expectMessage("getBar");

    PipelineOptionsValidator.validate(GroupRequired.class, groupRequired);
  }

  /** A test interface with a member in multiple required groups. */
  public interface MultiGroupRequired extends PipelineOptions {
    @Validation.Required(groups = {"spam", "ham"})
    String getFoo();

    void setFoo(String foo);

    @Validation.Required(groups = {"spam"})
    String getBar();

    void setBar(String bar);

    @Validation.Required(groups = {"ham"})
    String getBaz();

    void setBaz(String baz);
  }

  @Test
  public void testWhenOneOfMultipleRequiredGroupsIsSetIsValid() {
    MultiGroupRequired multiGroupRequired = PipelineOptionsFactory.as(MultiGroupRequired.class);

    multiGroupRequired.setRunner(CrashingRunner.class);
    multiGroupRequired.setFoo("eggs");

    PipelineOptionsValidator.validate(MultiGroupRequired.class, multiGroupRequired);
  }

  /** Test interface. */
  public interface LeftOptions extends PipelineOptions {
    @Validation.Required(groups = {"left"})
    String getFoo();

    void setFoo(String foo);

    @Validation.Required(groups = {"left"})
    String getLeft();

    void setLeft(String left);

    @Validation.Required(groups = {"both"})
    String getBoth();

    void setBoth(String both);
  }

  /** Test interface. */
  public interface RightOptions extends PipelineOptions {
    @Validation.Required(groups = {"right"})
    String getFoo();

    void setFoo(String foo);

    @Validation.Required(groups = {"right"})
    String getRight();

    void setRight(String right);

    @Validation.Required(groups = {"both"})
    String getBoth();

    void setBoth(String both);
  }

  /** Test interface. */
  public interface JoinedOptions extends LeftOptions, RightOptions {}

  @Test
  public void testWhenOptionIsDefinedInMultipleSuperInterfacesAndIsNotPresentFailsRequirement() {
    RightOptions rightOptions = PipelineOptionsFactory.as(RightOptions.class);
    rightOptions.setBoth("foo");
    rightOptions.setRunner(CrashingRunner.class);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Missing required value for group");
    expectedException.expectMessage("getFoo");

    PipelineOptionsValidator.validate(JoinedOptions.class, rightOptions);
  }

  @Test
  public void testWhenOptionIsDefinedInMultipleSuperInterfacesMeetsGroupRequirement() {
    RightOptions rightOpts = PipelineOptionsFactory.as(RightOptions.class);
    rightOpts.setFoo("true");
    rightOpts.setBoth("bar");

    LeftOptions leftOpts = PipelineOptionsFactory.as(LeftOptions.class);
    leftOpts.setFoo("Untrue");
    leftOpts.setBoth("Raise the");

    rightOpts.setRunner(CrashingRunner.class);
    leftOpts.setRunner(CrashingRunner.class);
    PipelineOptionsValidator.validate(JoinedOptions.class, rightOpts);
    PipelineOptionsValidator.validate(JoinedOptions.class, leftOpts);
  }

  @Test
  public void testWhenOptionIsDefinedOnOtherOptionsClassMeetsGroupRequirement() {
    RightOptions rightOpts = PipelineOptionsFactory.as(RightOptions.class);
    rightOpts.setFoo("true");
    rightOpts.setBoth("bar");

    LeftOptions leftOpts = PipelineOptionsFactory.as(LeftOptions.class);
    leftOpts.setFoo("Untrue");
    leftOpts.setBoth("Raise the");

    rightOpts.setRunner(CrashingRunner.class);
    leftOpts.setRunner(CrashingRunner.class);
    PipelineOptionsValidator.validate(RightOptions.class, leftOpts);
    PipelineOptionsValidator.validate(LeftOptions.class, rightOpts);
  }

  @Test
  public void testWhenOptionIsDefinedOnMultipleInterfacesOnlyListedOnceWhenNotPresent() {
    JoinedOptions options = PipelineOptionsFactory.as(JoinedOptions.class);
    options.setFoo("Hello");
    options.setRunner(CrashingRunner.class);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("required value for group [both]");
    expectedException.expectMessage("properties [getBoth()]");

    PipelineOptionsValidator.validate(JoinedOptions.class, options);
  }

  /** Test interface. */
  public interface SuperOptions extends PipelineOptions {
    @Validation.Required(groups = {"super"})
    String getFoo();

    void setFoo(String foo);

    @Validation.Required(groups = {"sub"})
    String getBar();

    void setBar(String bar);

    @Validation.Required(groups = {"otherSuper"})
    String getSuperclassObj();

    void setSuperclassObj(String sup);
  }

  /** Test interface. */
  public interface SubOptions extends SuperOptions {
    @Override
    @Validation.Required(groups = {"sub"})
    String getFoo();

    @Override
    void setFoo(String foo);

    @Override
    String getSuperclassObj();

    @Override
    void setSuperclassObj(String sup);
  }

  @Test
  public void testSuperInterfaceRequiredOptionsAlsoRequiredInSubInterface() {
    SubOptions subOpts = PipelineOptionsFactory.as(SubOptions.class);
    subOpts.setFoo("Bar");
    subOpts.setRunner(CrashingRunner.class);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("otherSuper");
    expectedException.expectMessage("Missing required value");
    expectedException.expectMessage("getSuperclassObj");

    PipelineOptionsValidator.validate(SubOptions.class, subOpts);
  }

  @Test
  public void
      testSuperInterfaceGroupIsInAdditionToSubInterfaceGroupOnlyWhenValidatingSuperInterface() {
    SubOptions opts = PipelineOptionsFactory.as(SubOptions.class);
    opts.setFoo("Foo");
    opts.setSuperclassObj("Hello world");
    opts.setRunner(CrashingRunner.class);

    // Valid SubOptions, but invalid SuperOptions
    PipelineOptionsValidator.validate(SubOptions.class, opts);

    expectedException.expectMessage("sub");
    expectedException.expectMessage("Missing required value");
    expectedException.expectMessage("getBar");
    PipelineOptionsValidator.validate(SuperOptions.class, opts);
  }

  @Test
  public void testSuperInterfaceRequiredOptionsSatisfiedBySubInterface() {
    SubOptions subOpts = PipelineOptionsFactory.as(SubOptions.class);
    subOpts.setFoo("bar");
    subOpts.setBar("bar");
    subOpts.setSuperclassObj("SuperDuper");
    subOpts.setRunner(CrashingRunner.class);

    PipelineOptionsValidator.validate(SubOptions.class, subOpts);
    PipelineOptionsValidator.validate(SuperOptions.class, subOpts);
  }
}
