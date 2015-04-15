/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A StateSampler object may be used to obtain an approximate
 * breakdown of the time spent by an execution context in various
 * states, as a fraction of the total time.  The sampling is taken at
 * regular intervals, with adjustment for scheduling delay.
 */
@ThreadSafe
public class StateSampler extends TimerTask implements AutoCloseable {
  public static final long DEFAULT_SAMPLING_PERIOD_MS = 200;

  private final String prefix;
  private final CounterSet.AddCounterMutator counterSetMutator;

  /** Array of counters indexed by their state. */
  private ArrayList<Counter<Long>> countersByState = new ArrayList<>();

  /** Map of state name to state. */
  private HashMap<String, Integer> statesByName = new HashMap<>();

  /** The current state. */
  private volatile int currentState;

  /** Special value of {@code currentState} that means we do not sample. */
  private static final int DO_NOT_SAMPLE = -1;

  /**
   * The timestamp (in nanoseconds) corresponding to the last time the
   * state was sampled (and recorded).
   */
  private long stateTimestampNs = 0;

  /** Using a fixed number of timers for all StateSampler objects. */
  private static final int NUM_TIMER_THREADS = 16;

  /** The timers are used for periodically sampling the states. */
  private static Timer[] timers = new Timer[NUM_TIMER_THREADS];
  static {
    for (int i = 0; i < timers.length; ++i) {
      timers[i] = new Timer("StateSampler_" + i, true /* is daemon */);
    }
  }

  /**
   * Constructs a new {@link StateSampler} that can be used to obtain
   * an approximate breakdown of the time spent by an execution
   * context in various states, as a fraction of the total time.
   *
   * @param prefix the prefix of the counter names for the states
   * @param counterSetMutator the {@link CounterSet.AddCounterMutator}
   * used to create a counter for each distinct state
   * @param samplingPeriodMs the sampling period in milliseconds
   */
  public StateSampler(String prefix,
                      CounterSet.AddCounterMutator counterSetMutator,
                      long samplingPeriodMs) {
    this.prefix = prefix;
    this.counterSetMutator = counterSetMutator;
    currentState = DO_NOT_SAMPLE;
    Random rand = new Random();
    int initialDelay = rand.nextInt((int) samplingPeriodMs);
    timers[rand.nextInt(NUM_TIMER_THREADS)].scheduleAtFixedRate(
        this, initialDelay, samplingPeriodMs);
    stateTimestampNs = System.nanoTime();
  }

  /**
   * Constructs a new {@link StateSampler} that can be used to obtain
   * an approximate breakdown of the time spent by an execution
   * context in various states, as a fraction of the total time.
   *
   * @param prefix the prefix of the counter names for the states
   * @param counterSetMutator the {@link CounterSet.AddCounterMutator}
   * used to create a counter for each distinct state
   */
  public StateSampler(String prefix,
                      CounterSet.AddCounterMutator counterSetMutator) {
    this(prefix, counterSetMutator, DEFAULT_SAMPLING_PERIOD_MS);
  }

  @Override
  public void run() {
    long startTimestampNs = System.nanoTime();
    int state = currentState;
    if (state != DO_NOT_SAMPLE) {
      synchronized (this) {
        countersByState.get(state).addValue(
            TimeUnit.NANOSECONDS.toMillis(startTimestampNs - stateTimestampNs));
      }
    }
    stateTimestampNs = startTimestampNs;
  }

  @Override
  public void close() {
    this.cancel();  // cancel the TimerTask
  }

  /**
   * Returns the state associated with a name; creating a new state if
   * necessary. Using states instead of state names during state
   * transitions is done for efficiency.
   *
   * @name the name for the state
   * @return the state associated with the state name
   */
  public int stateForName(String name) {
    if (name.isEmpty()) {
      return DO_NOT_SAMPLE;
    }

    String counterName = prefix + name + "-msecs";
    synchronized (this) {
      Integer state = statesByName.get(counterName);
      if (state == null) {
        Counter<Long> counter = counterSetMutator.addCounter(
            Counter.longs(counterName, Counter.AggregationKind.SUM));
        state = countersByState.size();
        statesByName.put(name, state);
        countersByState.add(counter);
      }
      return state;
    }
  }

  /**
   * Sets the current thread state.
   *
   * @param state the new state to transition to
   * @return the previous state
   */
  public int setState(int state) {
    int previousState = currentState;
    currentState = state;
    return previousState;
  }

  /**
   * Sets the current thread state.
   *
   * @param name the name of the new state to transition to
   * @return the previous state
   */
  public int setState(String name) {
    return setState(stateForName(name));
  }

  /**
   * Returns an AutoCloseable {@link ScopedState} that will perform a
   * state transition to the given state, and will automatically reset
   * the state to the prior state upon closing.
   *
   * @param state the new state to transition to
   * @return a {@link ScopedState} that automatically resets the state
   * to the prior state
   */
  public ScopedState scopedState(int state) {
    return new ScopedState(this, setState(state));
  }

  /**
   * A nested class that is used to account for states and state
   * transitions based on lexical scopes.
   *
   * <p> Thread-safe.
   */
  public class ScopedState implements AutoCloseable {
    private StateSampler sampler;
    private int previousState;

    private ScopedState(StateSampler sampler, int previousState) {
      this.sampler = sampler;
      this.previousState = previousState;
    }

    @Override
    public void close() {
      sampler.setState(previousState);
    }
  }
}
