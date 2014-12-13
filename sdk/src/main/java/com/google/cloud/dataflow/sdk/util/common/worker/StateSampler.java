/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A StateSampler object may be used to obtain an approximate
 * breakdown of the time spent by an execution context in various
 * states, as a fraction of the total time.  The sampling is taken at
 * regular intervals, with adjustment for scheduling delay.
 *
 * <p> Thread-safe.
 */
public class StateSampler extends TimerTask implements AutoCloseable {
  private final String prefix;
  private CounterSet.AddCounterMutator counterSetMutator;
  // Sampling period of internal Timer (thread).
  public final long samplingPeriodMs;
  public static final int DO_NOT_SAMPLE = -1;
  public static final long DEFAULT_SAMPLING_PERIOD_MS = 200;
  // Array of counters indexed by their state.
  private ArrayList<Counter<Long>> countersByState = new ArrayList<>();
  // Map of state name to state.
  private HashMap<String, Integer> statesByName = new HashMap<>();
  // The current state.
  private int currentState;
  // The timestamp corresponding to the last state change or the last
  // time the current state was sampled (and recorded).
  private long stateTimestamp = 0;

  // When sampling this state, a stack trace is also logged.
  private int stateToSampleThreadStacks = DO_NOT_SAMPLE;
  // The thread that performed the last state transition.
  private Thread sampledThread = null;
  // The frequency with which the stack traces are logged, with respect
  // to the sampling period.
  private static final int SAMPLE_THREAD_STACK_FREQ = 10;
  private int sampleThreadStackFreq = 0;

  // Using a fixed number of timers for all StateSampler objects.
  private static final int NUM_TIMER_THREADS = 16;
  // The timers is used for periodically sampling the states.
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
    this.samplingPeriodMs = samplingPeriodMs;
    currentState = DO_NOT_SAMPLE;
    Random rand = new Random();
    int initialDelay = rand.nextInt((int) samplingPeriodMs);
    timers[rand.nextInt(NUM_TIMER_THREADS)].scheduleAtFixedRate(
        this, initialDelay, samplingPeriodMs);
    stateTimestamp = System.currentTimeMillis();
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

  private void printStackTrace(Thread thread) {
    System.out.println("Sampled stack trace:");
    StackTraceElement[] stack = thread.getStackTrace();
    for (StackTraceElement elem : stack) {
      System.out.println("\t" + elem.toString());
    }
  }

  /**
   * Selects a state for which the thread stacks will also be logged
   * during the sampling.  Useful for debugging.
   *
   * @param state name of the selected state
   */
  public synchronized void setStateToSampleThreadStacks(int state) {
    stateToSampleThreadStacks = state;
  }

  @Override
  public synchronized void run() {
    long now = System.currentTimeMillis();
    if (currentState != DO_NOT_SAMPLE) {
      countersByState.get(currentState).addValue(now - stateTimestamp);
      if (sampledThread != null
          && currentState == stateToSampleThreadStacks
          && ++sampleThreadStackFreq >= SAMPLE_THREAD_STACK_FREQ) {
        printStackTrace(sampledThread);
        sampleThreadStackFreq = 0;
      }
    }
    stateTimestamp = now;
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
  public synchronized int setState(int state) {
    // TODO: investigate whether this can be made cheaper, (e.g.,
    // using atomic operations).
    int previousState = currentState;
    currentState = state;
    if (stateToSampleThreadStacks != DO_NOT_SAMPLE) {
      sampledThread = Thread.currentThread();
    }
    return previousState;
  }

  /**
   * Sets the current thread state.
   *
   * @param name the name of the new state to transition to
   * @return the previous state
   */
  public synchronized int setState(String name) {
    return setState(stateForName(name));
  }

  /**
   *  Returns a tuple consisting of the current state and duration.
   *
   * @return a {@link Map.Entry} entry with current state and duration
   */
  public synchronized Map.Entry<String, Long> getCurrentStateAndDuration() {
    if (currentState == DO_NOT_SAMPLE) {
      return new SimpleEntry<>("", 0L);
    }

    Counter<Long> counter = countersByState.get(currentState);
    return new SimpleEntry<>(counter.getName(),
        counter.getAggregate(false)
        + System.currentTimeMillis() - stateTimestamp);
  }

  /**
   * Get the duration for a given state.
   *
   * @param state the state whose duration is returned
   * @return the duration of a given state
   */
  public synchronized long getStateDuration(int state) {
    Counter<Long> counter = countersByState.get(state);
    return counter.getAggregate(false)
        + (state == currentState
            ? System.currentTimeMillis() - stateTimestamp : 0);
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
  public synchronized ScopedState scopedState(int state) {
    return new ScopedState(this, setState(state));
  }

  /**
   * Returns an AutoCloseable {@link ScopedState} that will perform a
   * state transition to the given state, and will automatically reset
   * the state to the prior state upon closing.
   *
   * @param stateName the name of the new state
   * @return a {@link ScopedState} that automatically resets the state
   * to the prior state
   */
  public synchronized ScopedState scopedState(String stateName) {
    return new ScopedState(this, setState(stateName));
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
