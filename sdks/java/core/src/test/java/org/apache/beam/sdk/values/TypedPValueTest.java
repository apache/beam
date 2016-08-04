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
package org.apache.beam.sdk.values;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * Tests for {@link TypedPValue}, primarily focusing on Coder inference.
 */
@RunWith(JUnit4.class)
public class TypedPValueTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static class IdentityDoFn extends OldDoFn<Integer, Integer> {
    private static final long serialVersionUID = 0;
    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element());
    }
  }

  private static PCollectionTuple buildPCollectionTupleWithTags(
      TupleTag<Integer> mainOutputTag, TupleTag<Integer> sideOutputTag) {
    Pipeline p = TestPipeline.create();
    PCollection<Integer> input = p.apply(Create.of(1, 2, 3));
    PCollectionTuple tuple = input.apply(
        ParDo
        .withOutputTags(mainOutputTag, TupleTagList.of(sideOutputTag))
        .of(new IdentityDoFn()));
    return tuple;
  }

  private static <T> TupleTag<T> makeTagStatically() {
    return new TupleTag<T>() {};
  }

  @Test
  public void testUntypedSideOutputTupleTagGivesActionableMessage() {
    TupleTag<Integer> mainOutputTag = new TupleTag<Integer>() {};
    // untypedSideOutputTag did not use anonymous subclass.
    TupleTag<Integer> untypedSideOutputTag = new TupleTag<Integer>();
    PCollectionTuple tuple = buildPCollectionTupleWithTags(mainOutputTag, untypedSideOutputTag);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("No Coder has been manually specified");
    thrown.expectMessage(
        containsString("Building a Coder using a registered CoderFactory failed"));
    thrown.expectMessage(
        containsString("Building a Coder from the @DefaultCoder annotation failed"));
    thrown.expectMessage(
        containsString("Building a Coder from the fallback CoderProvider failed"));

    tuple.get(untypedSideOutputTag).getCoder();
  }

  @Test
  public void testStaticFactorySideOutputTupleTagGivesActionableMessage() {
    TupleTag<Integer> mainOutputTag = new TupleTag<Integer>() {};
    // untypedSideOutputTag constructed from a static factory method.
    TupleTag<Integer> untypedSideOutputTag = makeTagStatically();
    PCollectionTuple tuple = buildPCollectionTupleWithTags(mainOutputTag, untypedSideOutputTag);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("No Coder has been manually specified");
    thrown.expectMessage(
        containsString("Building a Coder using a registered CoderFactory failed"));
    thrown.expectMessage(
        containsString("Building a Coder from the @DefaultCoder annotation failed"));
    thrown.expectMessage(
        containsString("Building a Coder from the fallback CoderProvider failed"));

    tuple.get(untypedSideOutputTag).getCoder();
  }

  @Test
  public void testTypedSideOutputTupleTag() {
    TupleTag<Integer> mainOutputTag = new TupleTag<Integer>() {};
    // typedSideOutputTag was constructed with compile-time type information.
    TupleTag<Integer> typedSideOutputTag = new TupleTag<Integer>() {};
    PCollectionTuple tuple = buildPCollectionTupleWithTags(mainOutputTag, typedSideOutputTag);

    assertThat(tuple.get(typedSideOutputTag).getCoder(), instanceOf(VarIntCoder.class));
  }

  @Test
  public void testUntypedMainOutputTagTypedSideOutputTupleTag() {
    // mainOutputTag is allowed to be untyped because Coder can be inferred other ways.
    TupleTag<Integer> mainOutputTag = new TupleTag<>();
    TupleTag<Integer> typedSideOutputTag = new TupleTag<Integer>() {};
    PCollectionTuple tuple = buildPCollectionTupleWithTags(mainOutputTag, typedSideOutputTag);

    assertThat(tuple.get(typedSideOutputTag).getCoder(), instanceOf(VarIntCoder.class));
  }

  // A simple class for which there should be no obvious Coder.
  static class EmptyClass {
  }

  private static class EmptyClassDoFn extends OldDoFn<Integer, EmptyClass> {
    private static final long serialVersionUID = 0;
    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(new EmptyClass());
    }
  }

  @Test
  public void testParDoWithNoSideOutputsErrorDoesNotMentionTupleTag() {
    Pipeline p = TestPipeline.create();
    PCollection<EmptyClass> input =
        p.apply(Create.of(1, 2, 3)).apply(ParDo.of(new EmptyClassDoFn()));

    thrown.expect(IllegalStateException.class);

    // Output specific to ParDo TupleTag side outputs should not be present.
    thrown.expectMessage(not(containsString("erasure")));
    thrown.expectMessage(not(containsString("see TupleTag Javadoc")));
    // Instead, expect output suggesting other possible fixes.
    thrown.expectMessage(containsString("Building a Coder using a registered CoderFactory failed"));
    thrown.expectMessage(
        containsString("Building a Coder from the @DefaultCoder annotation failed"));
    thrown.expectMessage(containsString("Building a Coder from the fallback CoderProvider failed"));

    input.getCoder();
  }

  @Test
  public void testFinishSpecifyingShouldFailIfNoCoderInferrable() {
    Pipeline p = TestPipeline.create();
    PCollection<EmptyClass> unencodable =
        p.apply(Create.of(1, 2, 3)).apply(ParDo.of(new EmptyClassDoFn()));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to return a default Coder");
    thrown.expectMessage("Inferring a Coder from the CoderRegistry failed");

    unencodable.finishSpecifying();
  }
}
