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
package org.apache.beam.sdk.util.common.worker;

import org.apache.beam.sdk.util.common.Counter;
import org.apache.beam.sdk.util.common.CounterSet;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A StateSampler object may be used to obtain an approximate
 * breakdown of the time spent by an execution context in various
 * states, as a fraction of the total time.  The sampling is taken at
 * regular intervals, with adjustment for scheduling delay.
 */
@ThreadSafe
public class StateSampler implements AutoCloseable {

  /** Different kinds of states. */
  public enum StateKind {
    /** IO, user code, etc. */
    USER,
    /** Reading/writing from/to shuffle service, etc. */
    FRAMEWORK
  }

  public static final long DEFAULT_SAMPLING_PERIOD_MS = 200;

  private final String prefix;
  private final CounterSet.AddCounterMutator counterSetMutator;

  /** Array of counters indexed by their state. */
  private ArrayList<Counter<Long>> countersByState = new ArrayList<>();

  /** Map of state name to state. */
  private Map<String, Integer> statesByName = new HashMap<>();

  /** Map of state id to kind. */
  private Map<Integer, StateKind> kindsByState = new HashMap<>();

  /** The current state. */
  private volatile int currentState;

  /** Special value of {@code currentState} that means we do not sample. */
  public static final int DO_NOT_SAMPLE = -1;

  /**
   * A counter that increments with each state transition. May be used
   * to detect a context being stuck in a state for some amount of
   * time.
   */
  private volatile long stateTransitionCount;

  /**
   * The timestamp (in nanoseconds) corresponding to the last time the
   * state was sampled (and recorded).
   */
  private long stateTimestampNs = 0;

  /** Using a fixed number of timers for all StateSampler objects. */
  private static final int NUM_EXECUTOR_THREADS = 16;

  private static final ScheduledExecutorService executorService =
      Executors.newScheduledThreadPool(NUM_EXECUTOR_THREADS,
          new ThreadFactoryBuilder().setDaemon(true).build());

  private Random rand = new Random();

  private List<SamplingCallback> callbacks = new ArrayList<>();

  private ScheduledFuture<?> invocationTriggerFuture = null;

  private ScheduledFuture<?> invocationFuture = null;

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
                      final long samplingPeriodMs) {
    this.prefix = prefix;
    this.counterSetMutator = counterSetMutator;
    currentState = DO_NOT_SAMPLE;
    scheduleSampling(samplingPeriodMs);
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

  /**
   * Called by the constructor to schedule sampling at the given period.
   *
   * <p>Should not be overridden by sub-classes unless they want to change
   * or disable the automatic sampling of state.
   */
  protected void scheduleSampling(final long samplingPeriodMs) {
    // Here "stratified sampling" is used, which makes sure that there's 1 uniformly chosen sampled
    // point in every bucket of samplingPeriodMs, to prevent pathological behavior in case some
    // states happen to occur at a similar period.
    // The current implementation uses a fixed-rate timer with a period samplingPeriodMs as a
    // trampoline to a one-shot random timer which fires with a random delay within
    // samplingPeriodMs.
    stateTimestampNs = System.nanoTime();
    invocationTriggerFuture =
        executorService.scheduleAtFixedRate(
            new Runnable() {
              @Override
              public void run() {
                long delay = rand.nextInt((int) samplingPeriodMs);
                synchronized (StateSampler.this) {
                  if (invocationFuture != null) {
                    invocationFuture.cancel(false);
                  }
                  invocationFuture =
                      executorService.schedule(
                          new Runnable() {
                            @Override
                            public void run() {
                              StateSampler.this.run();
                            }
                          },
                          delay,
                          TimeUnit.MILLISECONDS);
                }
              }
            },
            0,
            samplingPeriodMs,
            TimeUnit.MILLISECONDS);
  }

  public synchronized void run() {
    long startTimestampNs = System.nanoTime();
    int state = currentState;
    if (state != DO_NOT_SAMPLE) {
      StateKind kind = null;
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(startTimestampNs - stateTimestampNs);
      kind = kindsByState.get(state);
      countersByState.get(state).addValue(elapsedMs);
      // Invoke all callbacks.
      for (SamplingCallback c : callbacks) {
        c.run(state, kind, elapsedMs);
      }
    }
    stateTimestampNs = startTimestampNs;
  }

  @Override
  public synchronized void close() {
    currentState = DO_NOT_SAMPLE;
    if (invocationTriggerFuture != null) {
      invocationTriggerFuture.cancel(false);
    }
    if (invocationFuture != null) {
      invocationFuture.cancel(false);
    }
  }

  /**
   * Returns the state associated with a name; creating a new state if
   * necessary. Using states instead of state names during state
   * transitions is done for efficiency.
   *
   * @name the name for the state
   * @kind kind of the state, see {#code StateKind}
   * @return the state associated with the state name
   */
  public int stateForName(String name, StateKind kind) {
    if (name.isEmpty()) {
      return DO_NOT_SAMPLE;
    }

    synchronized (this) {
      Integer state = statesByName.get(name);
      if (state == null) {
        String counterName = prefix + name + "-msecs";
        Counter<Long> counter = counterSetMutator.addCounter(
            Counter.longs(counterName, Counter.AggregationKind.SUM));
        state = countersByState.size();
        statesByName.put(name, state);
        countersByState.add(counter);
        kindsByState.put(state, kind);
      }
      StateKind originalKind = kindsByState.get(state);
      if (originalKind != kind) {
        throw new IllegalArgumentException(
            "for state named " + name
            + ", requested kind " + kind + " different from the original kind " + originalKind);
      }
      return state;
    }
  }

  /**
   * An internal class for representing StateSampler information
   * typically used for debugging.
   */
  public static class StateSamplerInfo {
    public final String state;
    public final Long transitionCount;
    public final Long stateDurationMillis;

    public StateSamplerInfo(String state, Long transitionCount,
                            Long stateDurationMillis) {
      this.state = state;
      this.transitionCount = transitionCount;
      this.stateDurationMillis = stateDurationMillis;
    }
  }

  /**
   * Returns information about the current state of this state sampler
   * into a {@link StateSamplerInfo} object, or null if sampling is
   * not turned on.
   *
   * @return information about this state sampler or null if sampling is off
   */
  public synchronized StateSamplerInfo getInfo() {
    return currentState == DO_NOT_SAMPLE ? null
        : new StateSamplerInfo(countersByState.get(currentState).getFlatName(),
            stateTransitionCount, null);
  }

  /**
   * Returns the current state of this state sampler.
   */
  public int getCurrentState() {
    return currentState;
  }

  /**
   * Sets the current thread state.
   *
   * @param state the new state to transition to
   * @return the previous state
   */
  public int setState(int state) {
    // Updates to stateTransitionCount are always done by the same
    // thread, making the non-atomic volatile update below safe. The
    // count is updated first to avoid incorrectly attributing
    // stuckness occuring in an old state to the new state.
    long previousStateTransitionCount = this.stateTransitionCount;
    this.stateTransitionCount = previousStateTransitionCount + 1;
    int previousState = currentState;
    currentState = state;
    return previousState;
  }

  /**
   * Sets the current thread state.
   *
   * @param name the name of the new state to transition to
   * @param kind kind of the new state
   * @return the previous state
   */
  public int setState(String name, StateKind kind) {
    return setState(stateForName(name, kind));
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
   * Add a callback to the sampler.
   * The callbacks will be executed sequentially upon {@link StateSampler#run}.
   */
  public synchronized void addSamplingCallback(SamplingCallback callback) {
    callbacks.add(callback);
  }

  /** Get the counter prefix associated with this sampler. */
  public String getPrefix() {
    return prefix;
  }

  /**
   * A nested class that is used to account for states and state
   * transitions based on lexical scopes.
   *
   * <p>Thread-safe.
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

  /**
   * Callbacks which supposed to be called sequentially upon {@link StateSampler#run}.
   * They should be registered via {@link #addSamplingCallback}.
   */
  public static interface SamplingCallback {
    /**
     * The entrance method of the callback, it is called in {@link StateSampler#run},
     * once per sample. This method should be thread safe.
     *
     * @param state The state of the StateSampler at the time of sample.
     * @param kind The kind associated with the state, see {@link StateKind}.
     * @param elapsedMs Milliseconds since last sample.
     */
    public void run(int state, StateKind kind, long elapsedMs);
  }
}
