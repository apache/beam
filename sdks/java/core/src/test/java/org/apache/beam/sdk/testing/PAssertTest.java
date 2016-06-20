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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

import com.google.common.collect.Iterables;

import com.fasterxml.jackson.annotation.JsonCreator;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * Test case for {@link PAssert}.
 */
@RunWith(JUnit4.class)
public class PAssertTest implements Serializable {
  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  private static class NotSerializableObject {

    @Override
    public boolean equals(Object other) {
      return (other instanceof NotSerializableObject);
    }

    @Override
    public int hashCode() {
      return 73;
    }
  }

  private static class NotSerializableObjectCoder extends AtomicCoder<NotSerializableObject> {
    private NotSerializableObjectCoder() { }
    private static final NotSerializableObjectCoder INSTANCE = new NotSerializableObjectCoder();

    @JsonCreator
    public static NotSerializableObjectCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(NotSerializableObject value, OutputStream outStream, Context context)
        throws CoderException, IOException {
    }

    @Override
    public NotSerializableObject decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return new NotSerializableObject();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(NotSerializableObject value, Context context) {
      return true;
    }

    @Override
    public void registerByteSizeObserver(
        NotSerializableObject value, ElementByteSizeObserver observer, Context context)
        throws Exception {
      observer.update(0L);
    }
  }

  /**
   * A {@link PAssert} about the contents of a {@link PCollection}
   * must not require the contents of the {@link PCollection} to be
   * serializable.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testContainsInAnyOrderNotSerializable() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<NotSerializableObject> pcollection = pipeline
        .apply(Create.of(
          new NotSerializableObject(),
          new NotSerializableObject())
            .withCoder(NotSerializableObjectCoder.of()));

    PAssert.that(pcollection).containsInAnyOrder(
      new NotSerializableObject(),
      new NotSerializableObject());

    pipeline.run();
  }

  /**
   * A {@link PAssert} about the contents of a {@link PCollection}
   * is allows to be verified by an arbitrary {@link SerializableFunction},
   * though.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testSerializablePredicate() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<NotSerializableObject> pcollection = pipeline
        .apply(Create.of(
          new NotSerializableObject(),
          new NotSerializableObject())
            .withCoder(NotSerializableObjectCoder.of()));

    PAssert.that(pcollection).satisfies(
        new SerializableFunction<Iterable<NotSerializableObject>, Void>() {
          @Override
          public Void apply(Iterable<NotSerializableObject> contents) {
            return null; // no problem!
          }
        });

    pipeline.run();
  }

  /**
   * A {@link PAssert} about the contents of a {@link PCollection}
   * is allows to be verified by an arbitrary {@link SerializableFunction},
   * though.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testWindowedSerializablePredicate() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<NotSerializableObject> pcollection = pipeline
        .apply(Create.timestamped(
            TimestampedValue.of(new NotSerializableObject(), new Instant(250L)),
            TimestampedValue.of(new NotSerializableObject(), new Instant(500L)))
            .withCoder(NotSerializableObjectCoder.of()))
        .apply(Window.<NotSerializableObject>into(FixedWindows.of(Duration.millis(300L))));

    PAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(0L), new Instant(300L)))
        .satisfies(new SerializableFunction<Iterable<NotSerializableObject>, Void>() {
          @Override
          public Void apply(Iterable<NotSerializableObject> contents) {
            assertThat(Iterables.isEmpty(contents), is(false));
            return null; // no problem!
          }
        });
    PAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(300L), new Instant(600L)))
        .satisfies(new SerializableFunction<Iterable<NotSerializableObject>, Void>() {
          @Override
          public Void apply(Iterable<NotSerializableObject> contents) {
            assertThat(Iterables.isEmpty(contents), is(false));
            return null; // no problem!
          }
        });

    pipeline.run();
  }

  /**
   * Test that we throw an error at pipeline construction time when the user mistakenly uses
   * {@code PAssert.thatSingleton().equals()} instead of the test method {@code .isEqualTo}.
   */
  @SuppressWarnings("deprecation") // test of deprecated function
  @Test
  public void testPAssertEqualsSingletonUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("isEqualTo");

    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    PAssert.thatSingleton(pcollection).equals(42);
  }

  /**
   * Test that we throw an error at pipeline construction time when the user mistakenly uses
   * {@code PAssert.that().equals()} instead of the test method {@code .containsInAnyOrder}.
   */
  @SuppressWarnings("deprecation") // test of deprecated function
  @Test
  public void testPAssertEqualsIterableUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("containsInAnyOrder");

    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    PAssert.that(pcollection).equals(42);
  }

  /**
   * Test that {@code PAssert.thatSingleton().hashCode()} is unsupported.
   * See {@link #testPAssertEqualsSingletonUnsupported}.
   */
  @SuppressWarnings("deprecation") // test of deprecated function
  @Test
  public void testPAssertHashCodeSingletonUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(".hashCode() is not supported.");

    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    PAssert.thatSingleton(pcollection).hashCode();
  }

  /**
   * Test that {@code PAssert.thatIterable().hashCode()} is unsupported.
   * See {@link #testPAssertEqualsIterableUnsupported}.
   */
  @SuppressWarnings("deprecation") // test of deprecated function
  @Test
  public void testPAssertHashCodeIterableUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(".hashCode() is not supported.");

    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    PAssert.that(pcollection).hashCode();
  }

  /**
   * Basic test for {@code isEqualTo}.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testIsEqualTo() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(43));
    PAssert.thatSingleton(pcollection).isEqualTo(43);
    pipeline.run();
  }

  /**
   * Basic test for {@code isEqualTo}.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testWindowedIsEqualTo() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection =
        pipeline.apply(Create.timestamped(TimestampedValue.of(43, new Instant(250L)),
            TimestampedValue.of(22, new Instant(-250L))))
            .apply(Window.<Integer>into(FixedWindows.of(Duration.millis(500L))));
    PAssert.thatSingleton(pcollection)
        .inOnlyPane(new IntervalWindow(new Instant(0L), new Instant(500L)))
        .isEqualTo(43);
    PAssert.thatSingleton(pcollection)
        .inOnlyPane(new IntervalWindow(new Instant(-500L), new Instant(0L)))
        .isEqualTo(22);
    pipeline.run();
  }

  /**
   * Basic test for {@code notEqualTo}.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testNotEqualTo() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(43));
    PAssert.thatSingleton(pcollection).notEqualTo(42);
    pipeline.run();
  }

  /**
   * Tests that {@code containsInAnyOrder} is actually order-independent.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testContainsInAnyOrder() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(1, 2, 3, 4));
    PAssert.that(pcollection).containsInAnyOrder(2, 1, 4, 3);
    pipeline.run();
  }

  /**
   * Tests that {@code containsInAnyOrder} is actually order-independent.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testGlobalWindowContainsInAnyOrder() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(1, 2, 3, 4));
    PAssert.that(pcollection).inWindow(GlobalWindow.INSTANCE).containsInAnyOrder(2, 1, 4, 3);
    pipeline.run();
  }

  /**
   * Tests that windowed {@code containsInAnyOrder} is actually order-independent.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testWindowedContainsInAnyOrder() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection =
        pipeline.apply(Create.timestamped(TimestampedValue.of(1, new Instant(100L)),
            TimestampedValue.of(2, new Instant(200L)),
            TimestampedValue.of(3, new Instant(300L)),
            TimestampedValue.of(4, new Instant(400L))))
            .apply(Window.<Integer>into(SlidingWindows.of(Duration.millis(200L))
                .every(Duration.millis(100L))
                .withOffset(Duration.millis(50L))));

    PAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(-50L), new Instant(150L))).containsInAnyOrder(1);
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

  /**
   * Tests that {@code containsInAnyOrder} fails when and how it should.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testContainsInAnyOrderFalse() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> pcollection = pipeline
        .apply(Create.of(1, 2, 3, 4));

    PAssert.that(pcollection).containsInAnyOrder(2, 1, 4, 3, 7);

    Throwable exc = runExpectingAssertionFailure(pipeline);
    Pattern expectedPattern = Pattern.compile(
        "Expected: iterable over \\[((<4>|<7>|<3>|<2>|<1>)(, )?){5}\\] in any order");
    // A loose pattern, but should get the job done.
    assertTrue(
        "Expected error message from PAssert with substring matching "
            + expectedPattern
            + " but the message was \""
            + exc.getMessage()
            + "\"",
        expectedPattern.matcher(exc.getMessage()).find());
  }

  private static Throwable runExpectingAssertionFailure(Pipeline pipeline) {
    // We cannot use thrown.expect(AssertionError.class) because the AssertionError
    // is first caught by JUnit and causes a test failure.
    try {
      pipeline.run();
    } catch (AssertionError exc) {
      return exc;
    }
    fail("assertion should have failed");
    throw new RuntimeException("unreachable");
  }
}
