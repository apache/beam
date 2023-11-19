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
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
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
    PAssert.that(pct.get(FAILURE_TAG))
        .containsInAnyOrder(UserCodeQuotaException.class.getName());

    pipeline.run();
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
    PAssert.that(pct.get(FAILURE_TAG))
            .containsInAnyOrder(UserCodeTimeoutException.class.getName());

    pipeline.run();
  }

  @Test
  public void defaultSleeperFollowsExponentialBackoff() throws InterruptedException {
    Repeater.DefaultSleeper sleeper = Repeater.DefaultSleeper.of();
    Instant t0 = Instant.now();
    sleeper.sleep();
    Instant t1 = Instant.now();
    sleeper.sleep();
    Instant t2 = Instant.now();
    Duration d1 = Duration.millis(t1.getMillis() - t0.getMillis());
    Duration d2 = Duration.millis(t2.getMillis() - t1.getMillis());
    assertTrue(d1.getMillis() > 0L);
    assertTrue(d1.isShorterThan(Duration.standardSeconds(1L)));
    assertTrue(d2.isLongerThan(d1));
    assertTrue(d2.isShorterThan(Duration.standardSeconds(2L)));
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
      wantNumErrors--;
      if (wantNumErrors > 0) {
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
      wantNumErrors--;
      if (wantNumErrors > 0) {
        try {
          throw wantThrowWith.getConstructor(String.class).newInstance(exceptionName);
        } catch (InstantiationException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
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
    for(Class<? extends UserCodeExecutionException> ex : REPEATABLE_ERROR_TYPES) {
      if (ex.equals(e)) {
        return ex.getName();
      }
    }
    return e.getName();
  }
}
