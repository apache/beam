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

package com.google.cloud.dataflow.sdk.testing;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
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

import javax.annotation.Nullable;

/**
 * Test case for {@link DataflowAssert}.
 */
@RunWith(JUnit4.class)
public class DataflowAssertTest implements Serializable {
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
   * A {@link DataflowAssert} about the contents of a {@link PCollection}
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

    DataflowAssert.that(pcollection).containsInAnyOrder(
      new NotSerializableObject(),
      new NotSerializableObject());

    pipeline.run();
  }

  /**
   * A {@link DataflowAssert} about the contents of a {@link PCollection}
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

    DataflowAssert.that(pcollection).satisfies(
        new SerializableFunction<Iterable<NotSerializableObject>, Void>() {
          @Override
          public Void apply(Iterable<NotSerializableObject> contents) {
            return null; // no problem!
          }
        });

    pipeline.run();
  }

  /**
   * A {@link DataflowAssert} about the contents of a {@link PCollection}
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

    DataflowAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(0L), new Instant(300L)))
        .satisfies(new SerializableFunction<Iterable<NotSerializableObject>, Void>() {
          @Override
          public Void apply(Iterable<NotSerializableObject> contents) {
            assertThat(Iterables.isEmpty(contents), is(false));
            return null; // no problem!
          }
        });
    DataflowAssert.that(pcollection)
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
   * {@code DataflowAssert.thatSingleton().equals()} instead of the test method {@code .isEqualTo}.
   */
  @SuppressWarnings("deprecation") // test of deprecated function
  @Test
  public void testDataflowAssertEqualsSingletonUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("isEqualTo");

    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    DataflowAssert.thatSingleton(pcollection).equals(42);
  }

  /**
   * Test that we throw an error at pipeline construction time when the user mistakenly uses
   * {@code DataflowAssert.that().equals()} instead of the test method {@code .containsInAnyOrder}.
   */
  @SuppressWarnings("deprecation") // test of deprecated function
  @Test
  public void testDataflowAssertEqualsIterableUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("containsInAnyOrder");

    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    DataflowAssert.that(pcollection).equals(42);
  }

  /**
   * Test that {@code DataflowAssert.thatSingleton().hashCode()} is unsupported.
   * See {@link #testDataflowAssertEqualsSingletonUnsupported}.
   */
  @SuppressWarnings("deprecation") // test of deprecated function
  @Test
  public void testDataflowAssertHashCodeSingletonUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(".hashCode() is not supported.");

    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    DataflowAssert.thatSingleton(pcollection).hashCode();
  }

  /**
   * Test that {@code DataflowAssert.thatIterable().hashCode()} is unsupported.
   * See {@link #testDataflowAssertEqualsIterableUnsupported}.
   */
  @SuppressWarnings("deprecation") // test of deprecated function
  @Test
  public void testDataflowAssertHashCodeIterableUnsupported() throws Exception {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(".hashCode() is not supported.");

    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(42));
    DataflowAssert.that(pcollection).hashCode();
  }

  /**
   * Basic test for {@code isEqualTo}.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testIsEqualTo() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> pcollection = pipeline.apply(Create.of(43));
    DataflowAssert.thatSingleton(pcollection).isEqualTo(43);
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
    DataflowAssert.thatSingleton(pcollection)
        .inWindow(new IntervalWindow(new Instant(0L), new Instant(500L)))
        .isEqualTo(43);
    DataflowAssert.thatSingleton(pcollection)
        .inWindow(new IntervalWindow(new Instant(-500L), new Instant(0L)))
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
    DataflowAssert.thatSingleton(pcollection).notEqualTo(42);
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
    DataflowAssert.that(pcollection).containsInAnyOrder(2, 1, 4, 3);
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
    DataflowAssert.that(pcollection).inWindow(GlobalWindow.INSTANCE).containsInAnyOrder(2, 1, 4, 3);
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

    DataflowAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(-50L), new Instant(150L))).containsInAnyOrder(1);
    DataflowAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(50L), new Instant(250L)))
        .containsInAnyOrder(2, 1);
    DataflowAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(150L), new Instant(350L)))
        .containsInAnyOrder(2, 3);
    DataflowAssert.that(pcollection)
        .inWindow(new IntervalWindow(new Instant(250L), new Instant(450L)))
        .containsInAnyOrder(4, 3);
    DataflowAssert.that(pcollection)
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

    DataflowAssert.that(pcollection).containsInAnyOrder(2, 1, 4, 3, 7);

    // The service runner does not give an exception we can usefully inspect.
    @Nullable
    Throwable exc = runExpectingAssertionFailure(pipeline);
    Pattern expectedPattern = Pattern.compile(
        "Expected: iterable over \\[((<4>|<7>|<3>|<2>|<1>)(, )?){5}\\] in any order");
    if (exc != null) {
      // A loose pattern, but should get the job done.
      assertTrue("Expected error message from DataflowAssert with substring matching "
          + expectedPattern + " but the message was \"" + exc.getMessage() + "\"",
          expectedPattern.matcher(exc.getMessage()).find());
    }
  }

  private static Throwable runExpectingAssertionFailure(Pipeline pipeline) {
    // Even though this test will succeed or fail adequately whether local or on the service,
    // it results in a different exception depending on the runner.
    if (pipeline.getRunner() instanceof DirectPipelineRunner
        || pipeline.getRunner() instanceof InProcessPipelineRunner) {
      // We cannot use thrown.expect(AssertionError.class) because the AssertionError
      // is first caught by JUnit and causes a test failure.
      try {
        pipeline.run();
      } catch (AssertionError exc) {
        return exc;
      }
    } else if (pipeline.getRunner() instanceof TestDataflowPipelineRunner) {
      // Separately, if this is run on the service, then the TestDataflowPipelineRunner throws
      // an IllegalStateException with a basic message.
      try {
        pipeline.run();
      } catch (IllegalStateException exc) {
        assertThat(exc.getMessage(), containsString("The dataflow failed."));
        return null;
      }
    }
    fail("assertion should have failed");
    throw new RuntimeException("unreachable");
  }
}
