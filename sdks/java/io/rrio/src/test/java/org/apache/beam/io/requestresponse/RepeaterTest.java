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

import static org.apache.beam.io.requestresponse.RequestResponseIO.REPEATABLE_ERROR_TYPES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;

import java.lang.reflect.InvocationTargetException;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.junit.Rule;
import org.junit.Test;

public class RepeaterTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static final TupleTag<Integer> OUTPUT_TAG = new TupleTag<Integer>() {};
  private static final TupleTag<String> FAILURE_TAG = new TupleTag<String>() {};

  private static final int LIMIT = 3;
  private static final FluentBackoff FLUENT_BACKOFF = FluentBackoff.DEFAULT.withMaxRetries(LIMIT);

  @Test
  public void givenCallerQuotaErrorsExceedsLimit_emitsIntoFailurePCollection() {
    PCollectionTuple pct =
        pipeline
            .apply(Create.of(1))
            .apply(
                ParDo.of(
                        new DoFnWithRepeaters(
                            new CallerImpl(LIMIT + 1, UserCodeQuotaException.class),
                            new SetupTeardownImpl(0)))
                    .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(pct.get(OUTPUT_TAG)).empty();
    PAssert.that(pct.get(FAILURE_TAG)).containsInAnyOrder(UserCodeQuotaException.class.getName());

    pipeline.run();
  }

  @Test
  public void givenSetupQuotaErrorsExceedsLimit_throws() {
    pipeline
        .apply(Create.of(1))
        .apply(
            ParDo.of(
                    new DoFnWithRepeaters(
                        new CallerImpl(0),
                        new SetupTeardownImpl(LIMIT + 1, UserCodeQuotaException.class)))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    UncheckedExecutionException thrown =
        assertThrows(UncheckedExecutionException.class, pipeline::run);
    assertThat(thrown.getCause(), allOf(notNullValue(), instanceOf(UserCodeException.class)));
    assertThat(
        thrown.getCause().getCause(),
        allOf(notNullValue(), instanceOf(UserCodeQuotaException.class)));
  }

  @Test
  public void givenCallerTimeoutErrorsExceedsLimit_emitsIntoFailurePCollection() {
    PCollectionTuple pct =
        pipeline
            .apply(Create.of(1))
            .apply(
                ParDo.of(
                        new DoFnWithRepeaters(
                            new CallerImpl(LIMIT + 1, UserCodeTimeoutException.class),
                            new SetupTeardownImpl(0)))
                    .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(pct.get(OUTPUT_TAG)).empty();
    PAssert.that(pct.get(FAILURE_TAG)).containsInAnyOrder(UserCodeTimeoutException.class.getName());

    pipeline.run();
  }

  @Test
  public void givenSetupTimeoutErrorsExceedsLimit_throws() {
    pipeline
        .apply(Create.of(1))
        .apply(
            ParDo.of(
                    new DoFnWithRepeaters(
                        new CallerImpl(0),
                        new SetupTeardownImpl(LIMIT + 1, UserCodeTimeoutException.class)))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    UncheckedExecutionException thrown =
        assertThrows(UncheckedExecutionException.class, pipeline::run);
    assertThat(thrown.getCause(), allOf(notNullValue(), instanceOf(UserCodeException.class)));
    assertThat(
        thrown.getCause().getCause(),
        allOf(notNullValue(), instanceOf(UserCodeTimeoutException.class)));
  }

  @Test
  public void givenCallerRemoteSystemExceptionExceedsLimit_emitsIntoFailurePCollection() {
    PCollectionTuple pct =
        pipeline
            .apply(Create.of(1))
            .apply(
                ParDo.of(
                        new DoFnWithRepeaters(
                            new CallerImpl(LIMIT + 1, UserCodeRemoteSystemException.class),
                            new SetupTeardownImpl(0)))
                    .withOutputTags(OUTPUT_TAG, TupleTagList.of(FAILURE_TAG)));

    PAssert.that(pct.get(OUTPUT_TAG)).empty();
    PAssert.that(pct.get(FAILURE_TAG))
        .containsInAnyOrder(UserCodeRemoteSystemException.class.getName());

    pipeline.run();
  }

  @Test
  public void givenSetupRemoteSystemErrorsExceedsLimit_throws() {
    pipeline
        .apply(Create.of(1))
        .apply(
            ParDo.of(
                    new DoFnWithRepeaters(
                        new CallerImpl(0),
                        new SetupTeardownImpl(LIMIT + 1, UserCodeRemoteSystemException.class)))
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

  private static class DoFnWithRepeaters extends DoFn<Integer, Integer> {
    private final CallerImpl caller;
    private final SetupTeardownImpl setupTeardown;

    private DoFnWithRepeaters(CallerImpl caller, SetupTeardownImpl setupTeardown) {
      this.caller = caller;
      this.setupTeardown = setupTeardown;
    }

    @Setup
    public void setup() throws UserCodeExecutionException {
      Repeater<Void, Void> repeater =
          Repeater.<Void, Void>builder()
              .setSleeper(new NoOpSleeper())
              .setBackOff(FLUENT_BACKOFF.backoff())
              .setThrowableFunction(
                  input -> {
                    setupTeardown.setup();
                    return null;
                  })
              .build();
      repeater.apply(null);
    }

    @ProcessElement
    public void process(@Element Integer element, MultiOutputReceiver receiver) {
      Repeater<Integer, Integer> repeater =
          Repeater.<Integer, Integer>builder()
              .setSleeper(new NoOpSleeper())
              .setBackOff(FLUENT_BACKOFF.backoff())
              .setThrowableFunction(caller::call)
              .build();
      try {
        Integer output = repeater.apply(element);
        receiver.get(OUTPUT_TAG).output(output);
      } catch (UserCodeExecutionException e) {
        receiver.get(FAILURE_TAG).output(getRepeatableErrorTypeName(e.getClass()));
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

  private static String getRepeatableErrorTypeName(Class<? extends UserCodeExecutionException> e) {
    for (Class<? extends UserCodeExecutionException> ex : REPEATABLE_ERROR_TYPES) {
      if (ex.equals(e)) {
        return ex.getName();
      }
    }
    return e.getName();
  }

  // We don't want to wait for sleep to delay test invocations.
  private static class NoOpSleeper implements Sleeper {
    @Override
    public void sleep(long millis) throws InterruptedException {}
  }
}
