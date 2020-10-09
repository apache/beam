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
package org.apache.beam.sdk.testing;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.regex.Pattern;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.PAssert.PCollectionContentsAssert.MatcherCheckerFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link PAssert}. */
@RunWith(JUnit4.class)
public class PAssertTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private static class NotSerializableObject {

    @Override
    public boolean equals(@Nullable Object other) {
      return (other instanceof NotSerializableObject);
    }

    @Override
    public int hashCode() {
      return 73;
    }
  }

  private static class NotSerializableObjectCoder extends AtomicCoder<NotSerializableObject> {
    private NotSerializableObjectCoder() {}

    private static final NotSerializableObjectCoder INSTANCE = new NotSerializableObjectCoder();

    @JsonCreator
    public static NotSerializableObjectCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(NotSerializableObject value, OutputStream outStream)
        throws CoderException, IOException {}

    @Override
    public NotSerializableObject decode(InputStream inStream) throws CoderException, IOException {
      return new NotSerializableObject();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(NotSerializableObject value) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(
        NotSerializableObject value, ElementByteSizeObserver observer) throws Exception {
      observer.update(0L);
    }
  }

  private void throwNestedError() {
    throw new RuntimeException("Nested error");
  }

  private void throwWrappedError() {
    try {
      throwNestedError();
    } catch (Exception e) {
      throw new RuntimeException("Wrapped error", e);
    }
  }

  @Test
  public void testFailureWithExceptionEncodedDecoded() throws IOException {
    Throwable error;
    try {
      throwWrappedError();
      throw new IllegalStateException("Should have failed");
    } catch (Throwable e) {
      error = e;
    }
    SuccessOrFailure failure =
        SuccessOrFailure.failure(PAssert.PAssertionSite.capture("here"), error);
    SuccessOrFailure res = CoderUtils.clone(SerializableCoder.of(SuccessOrFailure.class), failure);
    assertEquals(
        "Encode-decode failed SuccessOrFailure",
        Throwables.getStackTraceAsString(failure.assertionError()),
        Throwables.getStackTraceAsString(res.assertionError()));
  }

  @Test
  public void testSuccessEncodedDecoded() throws IOException {
    SuccessOrFailure success = SuccessOrFailure.success();
    SerializableCoder<SuccessOrFailure> coder = SerializableCoder.of(SuccessOrFailure.class);

    byte[] encoded = CoderUtils.encodeToByteArray(coder, success);
    SuccessOrFailure res = CoderUtils.decodeFromByteArray(coder, encoded);

    assertEquals("Encode-decode successful SuccessOrFailure", success.isSuccess(), res.isSuccess());
    assertEquals(
        "Encode-decode successful SuccessOrFailure",
        success.assertionError(),
        res.assertionError());
  }

  /**
   * A {@link PAssert} about the contents of a {@link PCollection} must not require the contents of
   * the {@link PCollection} to be serializable.
   */
  @Test
  @Category({ValidatesRunner.class})
  public void testContainsInAnyOrderNotSerializable() throws Exception {
    PCollection<NotSerializableObject> pcollection =
        pipeline.apply(
            Create.of(new NotSerializableObject(), new NotSerializableObject())
                .withCoder(NotSerializableObjectCoder.of()));

    PAssert.that(pcollection)
        .containsInAnyOrder(new NotSerializableObject(), new NotSerializableObject());

    pipeline.run();
  }

  /**
   * A {@link PAssert} about the contents of a {@link PCollection} is allows to be verified by an
   * arbitrary {@link SerializableFunction}, though.
   */
  @Test
  @Category({ValidatesRunner.class})
  public void testSerializablePredicate() throws Exception {
    PCollection<NotSerializableObject> pcollection =
        pipeline.apply(
            Create.of(new NotSerializableObject(), new NotSerializableObject())
                .withCoder(NotSerializableObjectCoder.of()));

    PAssert.that(pcollection)
        .satisfies(
            contents -> {
              return null; // no problem!
            });

    pipeline.run();
  }

  /**
   * A {@link PAssert} about the contents of a {@link PCollection} is allows to be verified by an
   * arbitrary {@link SerializableFunction}, though.
   */
  @Test
  @Category({ValidatesRunner.class})
  public void testWindowedSerializablePredicate() throws Exception {
    PCollection<NotSerializableObject> pcollection =
        pipeline
            .apply(
                Create.timestamped(
                        TimestampedValue.of(new NotSerializableObject(), new Instant(250L)),
                        TimestampedValue.of(new NotSerializableObject(), new Instant(500L)))
                    .withCoder(NotSerializableObjectCoder.of()))
            .apply(Window.into(FixedWindows.of(Duration.millis(300L))));

    PAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(0L), new Instant(300L)))
        .satisfies(
            contents -> {
              assertThat(Iterables.isEmpty(contents), is(false));
              return null; // no problem!
            });
    PAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(300L), new Instant(600L)))
        .satisfies(
            contents -> {
              assertThat(Iterables.isEmpty(contents), is(false));
              return null; // no problem!
            });

    pipeline.run();
  }

  /**
   * Test that we throw an error at pipeline construction time when the user mistakenly uses {@code
   * PAssert.thatSingleton().equals()} instead of the test method {@code .isEqualTo}.
   */
  @SuppressWarnings({
    "deprecation", // test of deprecated function
    "EqualsIncompatibleType"
  })
  @Test
  public void testPAssertEqualsSingletonUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("isEqualTo");

    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    PAssert.thatSingleton(pcollection).equals(42);
  }

  /**
   * Test that we throw an error at pipeline construction time when the user mistakenly uses {@code
   * PAssert.that().equals()} instead of the test method {@code .containsInAnyOrder}.
   */
  @SuppressWarnings({
    "deprecation", // test of deprecated function
    "EqualsIncompatibleType"
  })
  @Test
  public void testPAssertEqualsIterableUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("containsInAnyOrder");

    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    PAssert.that(pcollection).equals(42);
  }

  /**
   * Test that {@code PAssert.thatSingleton().hashCode()} is unsupported. See {@link
   * #testPAssertEqualsSingletonUnsupported}.
   */
  @SuppressWarnings("deprecation") // test of deprecated function
  @Test
  public void testPAssertHashCodeSingletonUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(".hashCode() is not supported.");

    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    PAssert.thatSingleton(pcollection).hashCode();
  }

  /**
   * Test that {@code PAssert.thatIterable().hashCode()} is unsupported. See {@link
   * #testPAssertEqualsIterableUnsupported}.
   */
  @SuppressWarnings("deprecation") // test of deprecated function
  @Test
  public void testPAssertHashCodeIterableUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(".hashCode() is not supported.");

    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    PAssert.that(pcollection).hashCode();
  }

  /** Basic test for {@code isEqualTo}. */
  @Test
  @Category({ValidatesRunner.class})
  public void testIsEqualTo() throws Exception {
    PCollection<Integer> pcollection = pipeline.apply(Create.of(43));
    PAssert.thatSingleton(pcollection).isEqualTo(43);
    pipeline.run();
  }

  /** Basic test for {@code isEqualTo}. */
  @Test
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class // This test fails if State is unsupported despite no direct usage.
  })
  public void testWindowedIsEqualTo() throws Exception {
    PCollection<Integer> pcollection =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(43, new Instant(250L)),
                    TimestampedValue.of(22, new Instant(-250L))))
            .apply(Window.into(FixedWindows.of(Duration.millis(500L))))
            // Materialize final panes to be able to check for single element ON_TIME panes,
            // elements might be in EARLY panes otherwise.
            .apply(WithKeys.of(0))
            .apply(GroupByKey.create())
            .apply(
                ParDo.of(
                    new DoFn<KV<Integer, Iterable<Integer>>, Integer>() {
                      @ProcessElement
                      public void processElement(ProcessContext ctxt) {
                        for (Integer integer : ctxt.element().getValue()) {
                          ctxt.output(integer);
                        }
                      }
                    }));

    PAssert.thatSingleton(pcollection)
        .inOnlyPane(new IntervalWindow(new Instant(0L), new Instant(500L)))
        .isEqualTo(43);
    PAssert.thatSingleton(pcollection)
        .inOnlyPane(new IntervalWindow(new Instant(-500L), new Instant(0L)))
        .isEqualTo(22);
    pipeline.run();
  }

  /** Basic test for {@code notEqualTo}. */
  @Test
  @Category({ValidatesRunner.class})
  public void testNotEqualTo() throws Exception {
    PCollection<Integer> pcollection = pipeline.apply(Create.of(43));
    PAssert.thatSingleton(pcollection).notEqualTo(42);
    pipeline.run();
  }

  /** Test that we throw an error for false assertion on singleton. */
  @Test
  @Category({ValidatesRunner.class, UsesFailureMessage.class})
  public void testPAssertEqualsSingletonFalse() throws Exception {
    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    PAssert.thatSingleton("The value was not equal to 44", pcollection).isEqualTo(44);

    Throwable thrown = runExpectingAssertionFailure(pipeline);

    String message = thrown.getMessage();

    assertThat(message, containsString("The value was not equal to 44"));
    assertThat(message, containsString("Expected: <44>"));
    assertThat(message, containsString("but: was <42>"));
  }

  /** Test that we throw an error for false assertion on singleton. */
  @Test
  @Category({ValidatesRunner.class, UsesFailureMessage.class})
  public void testPAssertEqualsSingletonFalseDefaultReasonString() throws Exception {
    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    PAssert.thatSingleton(pcollection).isEqualTo(44);

    Throwable thrown = runExpectingAssertionFailure(pipeline);

    String message = thrown.getMessage();

    assertThat(message, containsString("Create.Values/Read(CreateSource)"));
    assertThat(message, containsString("Expected: <44>"));
    assertThat(message, containsString("but: was <42>"));
  }

  /** Tests that {@code containsInAnyOrder} is actually order-independent. */
  @Test
  @Category(ValidatesRunner.class)
  public void testContainsInAnyOrder() throws Exception {
    PCollection<Integer> pcollection = pipeline.apply(Create.of(1, 2, 3, 4));
    PAssert.that(pcollection).containsInAnyOrder(2, 1, 4, 3);
    pipeline.run();
  }

  /** Tests that {@code containsInAnyOrder} is actually order-independent. */
  @Test
  @Category(ValidatesRunner.class)
  public void testGlobalWindowContainsInAnyOrder() throws Exception {
    PCollection<Integer> pcollection = pipeline.apply(Create.of(1, 2, 3, 4));
    PAssert.that(pcollection).inWindow(GlobalWindow.INSTANCE).containsInAnyOrder(2, 1, 4, 3);
    pipeline.run();
  }

  /** Tests that windowed {@code containsInAnyOrder} is actually order-independent. */
  @Test
  @Category(ValidatesRunner.class)
  public void testWindowedContainsInAnyOrder() throws Exception {
    PCollection<Integer> pcollection =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(1, new Instant(100L)),
                    TimestampedValue.of(2, new Instant(200L)),
                    TimestampedValue.of(3, new Instant(300L)),
                    TimestampedValue.of(4, new Instant(400L))))
            .apply(
                Window.into(
                    SlidingWindows.of(Duration.millis(200L))
                        .every(Duration.millis(100L))
                        .withOffset(Duration.millis(50L))));

    PAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(-50L), new Instant(150L)))
        .containsInAnyOrder(1);
    PAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(50L), new Instant(250L)))
        .containsInAnyOrder(2, 1);
    PAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(150L), new Instant(350L)))
        .containsInAnyOrder(2, 3);
    PAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(250L), new Instant(450L)))
        .containsInAnyOrder(4, 3);
    PAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(350L), new Instant(550L)))
        .containsInAnyOrder(4);
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testEmpty() {
    PCollection<Long> vals = pipeline.apply(Create.empty(VarLongCoder.of()));

    PAssert.that(vals).empty();

    pipeline.run();
  }

  /** Tests that {@code containsInAnyOrder} fails when and how it should. */
  @Test
  @Category({ValidatesRunner.class, UsesFailureMessage.class})
  public void testContainsInAnyOrderFalse() throws Exception {
    PCollection<Integer> pcollection = pipeline.apply(Create.of(1, 2, 3, 4));

    PAssert.that(pcollection).containsInAnyOrder(2, 1, 4, 3, 7);

    Throwable exc = runExpectingAssertionFailure(pipeline);
    Pattern expectedPattern =
        Pattern.compile(
            "Expected: iterable with items \\[((<4>|<7>|<3>|<2>|<1>)(, )?){5}\\] in any order");
    // A loose pattern, but should get the job done.
    assertTrue(
        "Expected error message from PAssert with substring matching "
            + expectedPattern
            + " but the message was \""
            + exc.getMessage()
            + "\"",
        expectedPattern.matcher(exc.getMessage()).find());
  }

  @Test
  @Category({ValidatesRunner.class, UsesFailureMessage.class, DataflowRunnerV2Incompatible.class})
  public void testEmptyFalse() throws Exception {
    PCollection<Long> vals = pipeline.apply(GenerateSequence.from(0).to(5));
    PAssert.that("Vals should have been empty", vals).empty();

    Throwable thrown = runExpectingAssertionFailure(pipeline);

    String message = thrown.getMessage();

    assertThat(message, containsString("Vals should have been empty"));
    assertThat(message, containsString("Expected: iterable with items [] in any order"));
  }

  @Test
  @Category({ValidatesRunner.class, UsesFailureMessage.class, DataflowRunnerV2Incompatible.class})
  public void testEmptyFalseDefaultReasonString() throws Exception {
    PCollection<Long> vals = pipeline.apply(GenerateSequence.from(0).to(5));
    PAssert.that(vals).empty();

    Throwable thrown = runExpectingAssertionFailure(pipeline);

    String message = thrown.getMessage();

    assertThat(message, containsString("GenerateSequence/Read(BoundedCountingSource)"));
    assertThat(message, containsString("Expected: iterable with items [] in any order"));
  }

  @Test
  public void testAssertionSiteIsCaptured() {
    // This check should return a failure.
    SuccessOrFailure res =
        PAssert.doChecks(
            PAssert.PAssertionSite.capture("Captured assertion message."),
            10,
            new MatcherCheckerFn(SerializableMatchers.contains(11)));

    String stacktrace = Throwables.getStackTraceAsString(res.assertionError());
    assertFalse(res.isSuccess());
    assertThat(stacktrace, containsString("PAssertionSite.capture"));
  }

  @Test
  @Category({ValidatesRunner.class, UsesFailureMessage.class, DataflowRunnerV2Incompatible.class})
  public void testAssertionSiteIsCapturedWithMessage() throws Exception {
    PCollection<Long> vals = pipeline.apply(GenerateSequence.from(0).to(5));
    assertThatCollectionIsEmptyWithMessage(vals);

    Throwable thrown = runExpectingAssertionFailure(pipeline);

    assertThat(thrown.getMessage(), containsString("Should be empty"));
    assertThat(
        thrown.getMessage(), containsString("Expected: iterable with items [] in any order"));
    String stacktrace = Throwables.getStackTraceAsString(thrown);
    assertThat(stacktrace, containsString("testAssertionSiteIsCapturedWithMessage"));
    assertThat(stacktrace, containsString("assertThatCollectionIsEmptyWithMessage"));
  }

  @Test
  @Category({ValidatesRunner.class, UsesFailureMessage.class, DataflowRunnerV2Incompatible.class})
  public void testAssertionSiteIsCapturedWithoutMessage() throws Exception {
    PCollection<Long> vals = pipeline.apply(GenerateSequence.from(0).to(5));
    assertThatCollectionIsEmptyWithoutMessage(vals);

    Throwable thrown = runExpectingAssertionFailure(pipeline);

    assertThat(
        thrown.getMessage(), containsString("Expected: iterable with items [] in any order"));
    String stacktrace = Throwables.getStackTraceAsString(thrown);
    assertThat(stacktrace, containsString("testAssertionSiteIsCapturedWithoutMessage"));
    assertThat(stacktrace, containsString("assertThatCollectionIsEmptyWithoutMessage"));
  }

  private static void assertThatCollectionIsEmptyWithMessage(PCollection<Long> vals) {
    PAssert.that("Should be empty", vals).empty();
  }

  private static void assertThatCollectionIsEmptyWithoutMessage(PCollection<Long> vals) {
    PAssert.that(vals).empty();
  }

  private static Throwable runExpectingAssertionFailure(Pipeline pipeline) {
    // We cannot use thrown.expect(AssertionError.class) because the AssertionError
    // is first caught by JUnit and causes a test failure.
    try {
      pipeline.run();
    } catch (Throwable exc) {
      return exc;
    }
    fail("assertion should have failed");
    throw new RuntimeException("unreachable");
  }

  @Test
  public void countAssertsSucceeds() {
    PCollection<Integer> create = pipeline.apply("FirstCreate", Create.of(1, 2, 3));

    PAssert.that(create).containsInAnyOrder(1, 2, 3);
    PAssert.thatSingleton(create.apply(Sum.integersGlobally())).isEqualTo(6);
    PAssert.thatMap(pipeline.apply("CreateMap", Create.of(KV.of(1, 2))))
        .isEqualTo(Collections.singletonMap(1, 2));

    assertThat(PAssert.countAsserts(pipeline), equalTo(3));
  }

  @Test
  public void countAssertsMultipleCallsIndependent() {
    PCollection<Integer> create = pipeline.apply("FirstCreate", Create.of(1, 2, 3));

    PAssert.that(create).containsInAnyOrder(1, 2, 3);
    PAssert.thatSingleton(create.apply(Sum.integersGlobally())).isEqualTo(6);
    assertThat(PAssert.countAsserts(pipeline), equalTo(2));

    PAssert.thatMap(pipeline.apply("CreateMap", Create.of(KV.of(1, 2))))
        .isEqualTo(Collections.singletonMap(1, 2));

    assertThat(PAssert.countAsserts(pipeline), equalTo(3));
  }
}
