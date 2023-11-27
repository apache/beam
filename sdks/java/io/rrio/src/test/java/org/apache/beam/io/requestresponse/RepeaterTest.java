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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.io.requestresponse.Repeater.REPEATABLE_ERROR_TYPES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;

import com.google.api.client.util.ExponentialBackOff;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.junit.Rule;
import org.junit.Test;

public class RepeaterTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static final TupleTag<Integer> OUTPUT_TAG = new TupleTag<Integer>() {};
  private static final TupleTag<String> FAILURE_TAG = new TupleTag<String>() {};
  private static final int LIMIT = 3;

  @Test
  public void givenCallerQuotaErrorsAtLimit_emitsIntoFailurePCollection() {
    PCollectionTuple pct =
        pipeline
            .apply(Create.of(1))
            .apply(
                ParDo.of(
                        new DoFnWithRepeaters(
                            new CallerImpl(LIMIT, UserCodeQuotaException.class),
                            new SetupTeardownImpl(0)))
                    .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(pct.get(OUTPUT_TAG)).empty();
    PAssert.that(pct.get(FAILURE_TAG)).containsInAnyOrder(UserCodeQuotaException.class.getName());

    pipeline.run();
  }

  @Test
  public void givenSetupQuotaErrorsAtLimit_throws() {
    pipeline
        .apply(Create.of(1))
        .apply(
            ParDo.of(
                    new DoFnWithRepeaters(
                        new CallerImpl(0),
                        new SetupTeardownImpl(LIMIT, UserCodeQuotaException.class)))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    UncheckedExecutionException thrown =
        assertThrows(UncheckedExecutionException.class, pipeline::run);
    assertThat(thrown.getCause(), allOf(notNullValue(), instanceOf(UserCodeException.class)));
    assertThat(
        thrown.getCause().getCause(),
        allOf(notNullValue(), instanceOf(UserCodeQuotaException.class)));
  }

  @Test
  public void givenCallerTimeoutErrorsAtLimit_emitsIntoFailurePCollection() {
    PCollectionTuple pct =
        pipeline
            .apply(Create.of(1))
            .apply(
                ParDo.of(
                        new DoFnWithRepeaters(
                            new CallerImpl(LIMIT, UserCodeTimeoutException.class),
                            new SetupTeardownImpl(0)))
                    .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(pct.get(OUTPUT_TAG)).empty();
    PAssert.that(pct.get(FAILURE_TAG)).containsInAnyOrder(UserCodeTimeoutException.class.getName());

    pipeline.run();
  }

  @Test
  public void givenSetupTimeoutErrorsAtLimit_throws() {
    pipeline
        .apply(Create.of(1))
        .apply(
            ParDo.of(
                    new DoFnWithRepeaters(
                        new CallerImpl(0),
                        new SetupTeardownImpl(LIMIT, UserCodeTimeoutException.class)))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    UncheckedExecutionException thrown =
        assertThrows(UncheckedExecutionException.class, pipeline::run);
    assertThat(thrown.getCause(), allOf(notNullValue(), instanceOf(UserCodeException.class)));
    assertThat(
        thrown.getCause().getCause(),
        allOf(notNullValue(), instanceOf(UserCodeTimeoutException.class)));
  }

  @Test
  public void givenCallerRemoteSystemExceptionAtLimit_emitsIntoFailurePCollection() {
    PCollectionTuple pct =
        pipeline
            .apply(Create.of(1))
            .apply(
                ParDo.of(
                        new DoFnWithRepeaters(
                            new CallerImpl(LIMIT, UserCodeRemoteSystemException.class),
                            new SetupTeardownImpl(0)))
                    .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(pct.get(OUTPUT_TAG)).empty();
    PAssert.that(pct.get(FAILURE_TAG))
        .containsInAnyOrder(UserCodeRemoteSystemException.class.getName());

    pipeline.run();
  }

  @Test
  public void givenSetupRemoteSystemErrorsAtLimit_throws() {
    pipeline
        .apply(Create.of(1))
        .apply(
            ParDo.of(
                    new DoFnWithRepeaters(
                        new CallerImpl(0),
                        new SetupTeardownImpl(LIMIT, UserCodeRemoteSystemException.class)))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    UncheckedExecutionException thrown =
        assertThrows(UncheckedExecutionException.class, pipeline::run);
    assertThat(thrown.getCause(), allOf(notNullValue(), instanceOf(UserCodeException.class)));
    assertThat(
        thrown.getCause().getCause(),
        allOf(notNullValue(), instanceOf(UserCodeRemoteSystemException.class)));
  }

  @Test
  public void givenCallerNonRepeatableError_emitsIntoFailurePCollection() {
    PCollectionTuple pct =
        pipeline
            .apply(Create.of(1))
            .apply(
                ParDo.of(
                        new DoFnWithRepeaters(
                            new CallerImpl(1, UserCodeExecutionException.class),
                            new SetupTeardownImpl(0)))
                    .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(pct.get(OUTPUT_TAG)).empty();
    PAssert.that(pct.get(FAILURE_TAG))
        .containsInAnyOrder(UserCodeExecutionException.class.getName());

    pipeline.run();
  }

  @Test
  public void givenSetupNonRepeatableError_throws() {
    pipeline
        .apply(Create.of(1))
        .apply(
            ParDo.of(
                    new DoFnWithRepeaters(
                        new CallerImpl(0),
                        new SetupTeardownImpl(1, UserCodeExecutionException.class)))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    UncheckedExecutionException thrown =
        assertThrows(UncheckedExecutionException.class, pipeline::run);
    assertThat(thrown.getCause(), allOf(notNullValue(), instanceOf(UserCodeException.class)));
    assertThat(
        thrown.getCause().getCause(),
        allOf(notNullValue(), instanceOf(UserCodeExecutionException.class)));
  }

  @Test
  public void givenRepeatableErrorBelowLimit_emitsIntoOutputPCollection() {
    PCollectionTuple pct =
        pipeline
            .apply(Create.of(1))
            .apply(
                ParDo.of(
                        new DoFnWithRepeaters(
                            new CallerImpl(LIMIT - 1, UserCodeQuotaException.class),
                            new SetupTeardownImpl(0)))
                    .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(pct.get(OUTPUT_TAG)).containsInAnyOrder(2);
    PAssert.that(pct.get(FAILURE_TAG)).empty();

    pipeline.run();
  }

  @Test
  public void givenDefaultSleeper_incrementsBackoff() throws IOException {
    int testInitialInterval = 500;
    double testRandomizationFactor = 0.1;
    double testMultiplier = 2.0;
    int testMaxInterval = 5000;
    int testMaxElapsedTime = 900000;
    ExponentialBackOff backOffPolicy =
        new ExponentialBackOff.Builder()
            .setInitialIntervalMillis(testInitialInterval)
            .setRandomizationFactor(testRandomizationFactor)
            .setMultiplier(testMultiplier)
            .setMaxIntervalMillis(testMaxInterval)
            .setMaxElapsedTimeMillis(testMaxElapsedTime)
            .build();
    List<Long> expectedResults =
        ImmutableList.of(500L, 1000L, 2000L, 4000L, 5000L, 5000L, 5000L, 5000L, 5000L, 5000L);
    List<Long> expectedDiff =
        ImmutableList.of(50L, 100L, 200L, 400L, 500L, 500L, 500L, 500L, 500L, 500L);
    Repeater.DefaultSleeper sleeper = Repeater.DefaultSleeper.of(backOffPolicy);
    for (int i = 0; i < expectedResults.size(); i++) {
      long expected = expectedResults.get(i);
      long diff = expectedDiff.get(i);
      long expectedMin = expected - diff;
      long expectedMax = expected + diff;
      assertThat(
          sleeper.getBackOff().nextBackOffMillis(),
          allOf(greaterThanOrEqualTo(expectedMin), lessThan(expectedMax)));
    }
  }

  private static class DoFnWithRepeaters extends DoFn<Integer, Integer> {
    private final CallerImpl caller;
    private final SetupTeardownImpl setupTeardown;

    private DoFnWithRepeaters(CallerImpl caller, SetupTeardownImpl setupTeardown) {
      this.caller = caller;
      this.setupTeardown = setupTeardown;
    }

    @Setup
    public void setup() throws UserCodeExecutionException, InterruptedException {
      Repeater<Void, Void> repeater =
          Repeater.of(
              ignored -> {
                setupTeardown.setup();
                return null;
              },
              new SleeperImpl(0L),
              LIMIT);
      repeater.apply(null);
    }

    @ProcessElement
    public void process(@Element Integer element, MultiOutputReceiver receiver) {
      Repeater<Integer, Integer> repeater = Repeater.of(caller::call, new SleeperImpl(0L), LIMIT);
      try {
        receiver.get(OUTPUT_TAG).output(repeater.apply(element));
      } catch (UserCodeExecutionException e) {
        receiver.get(FAILURE_TAG).output(e.getMessage());
      } catch (InterruptedException ignored) {
      }
    }
  }

  private static class CallerImpl implements Caller<Integer, Integer> {

    private int wantNumErrors;
    private final Class<? extends UserCodeExecutionException> wantThrowWith;
    private final String exceptionName;

    private CallerImpl(int wantNumErrors) {
      this(wantNumErrors, UserCodeExecutionException.class);
    }

    private CallerImpl(
        int wantNumErrors, Class<? extends UserCodeExecutionException> wantThrowWith) {
      this.wantNumErrors = wantNumErrors;
      this.wantThrowWith = wantThrowWith;
      this.exceptionName = getRepeatableErrorTypeName(wantThrowWith);
    }

    @Override
    public Integer call(Integer request) throws UserCodeExecutionException {
      if (wantNumErrors > 0) {
        wantNumErrors--;
        try {
          throw wantThrowWith.getConstructor(String.class).newInstance(exceptionName);
        } catch (InstantiationException
            | NoSuchMethodException
            | InvocationTargetException
            | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
      return request * 2;
    }
  }

  private static class SetupTeardownImpl implements SetupTeardown {
    private int wantNumErrors;
    private final Class<? extends UserCodeExecutionException> wantThrowWith;
    private final String exceptionName;

    private SetupTeardownImpl(int wantNumErrors) {
      this(wantNumErrors, UserCodeExecutionException.class);
    }

    private SetupTeardownImpl(
        int wantNumErrors, Class<? extends UserCodeExecutionException> wantThrowWith) {
      this.wantNumErrors = wantNumErrors;
      this.wantThrowWith = wantThrowWith;
      this.exceptionName = getRepeatableErrorTypeName(wantThrowWith);
    }

    @Override
    public void setup() throws UserCodeExecutionException {
      if (wantNumErrors > 0) {
        wantNumErrors--;
        try {
          throw wantThrowWith.getConstructor(String.class).newInstance(exceptionName);
        } catch (InstantiationException
            | NoSuchMethodException
            | InvocationTargetException
            | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }

  private static class SleeperImpl implements Repeater.Sleeper {
    private final long sleepFor;

    private SleeperImpl(long sleepFor) {
      this.sleepFor = sleepFor;
    }

    @Override
    public void sleep() throws InterruptedException {
      Thread.sleep(sleepFor);
    }
  }

  private static String getRepeatableErrorTypeName(Class<? extends UserCodeExecutionException> e) {
    for (Class<? extends UserCodeExecutionException> ex : REPEATABLE_ERROR_TYPES) {
      if (ex.equals(e)) {
        return ex.getName();
      }
    }
    return e.getName();
  }
}
