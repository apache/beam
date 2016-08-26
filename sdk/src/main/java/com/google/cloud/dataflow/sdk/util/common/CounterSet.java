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

package com.google.cloud.dataflow.sdk.util.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A CounterSet maintains a set of {@link Counter}s.
 *
 * <p>Thread-safe.
 */
public class CounterSet extends AbstractSet<Counter<?>> {

  /** Registered counters. */
  private final ConcurrentHashMap<Counter.Name, Counter<?>> counters = new ConcurrentHashMap<>();

  private final AddCounterMutator addCounterMutator = new AddCounterMutator();

  /**
   * Constructs a CounterSet containing the given Counters.
   */
  public CounterSet(Counter<?>... counters) {
    for (Counter<?> counter : counters) {
      addNewCounter(counter);
    }
  }

  /**
   * Returns an object that supports adding additional counters into
   * this CounterSet.
   */
  public AddCounterMutator getAddCounterMutator() {
    return addCounterMutator;
  }

  /**
   * Returns an object that supports adding additional counters into
   * this {@link CounterSet} with the supplied {@link NameContext}.
   */
  public AddCounterMutator getAddCounterMutator(NameContext context) {
    AddCounterMutator addCounterWithContextMutator = new AddCounterMutator(context);
    return addCounterWithContextMutator;
  }

  /**
   * Adds a new counter, throwing an exception if a counter of the
   * same name already exists.
   */
  public void addNewCounter(Counter<?> counter) {
    if (!addCounter(counter)) {
      throw new IllegalArgumentException(
          "Counter " + counter + " duplicates an existing counter in " + this);
    }
  }

  /**
   * Adds the given Counter to this CounterSet.
   *
   * <p>If a counter with the same name already exists, it will be
   * reused, as long as it is compatible.
   *
   * @return the Counter that was reused, or added
   * @throws IllegalArgumentException if a counter with the same
   * name but an incompatible kind had already been added
   */
  public <T> Counter<T> addOrReuseCounter(Counter<T> counter) {
    Counter<?> oldCounter = counters.putIfAbsent(counter.getUniqueName(), counter);
    if (oldCounter != null) {
      checkArgument(counter.isCompatibleWith(oldCounter),
        "Counter %s duplicates incompatible counter %s in %s", counter, oldCounter, this);
      // Return the counter to reuse.
      @SuppressWarnings("unchecked")
      Counter<T> compatibleCounter = (Counter<T>) oldCounter;
      return compatibleCounter;
    }
    return counter;
  }

  /**
   * Adds a counter. Returns {@code true} if the counter was added to the set
   * and false if the given counter was {@code null} or it already existed in
   * the set.
   *
   * @param counter to register
   */
  public boolean addCounter(Counter<?> counter) {
    return add(counter);
  }

  /**
   * Returns the {@link Counter} with the given {@link Counter.Name} in this {@link CounterSet};
   * returns null if no such {@link Counter} exists.
   */
  public synchronized Counter<?> getExistingCounter(Counter.Name name) {
    return counters.get(name);
  }

  /**
   * Returns the Counter with the given name in this CounterSet;
   * returns null if no such Counter exists.
   */
  public Counter<?> getExistingCounter(String name) {
    Counter.Name unstructuredName = Counter.Name.withoutStructure(name);
    return counters.get(unstructuredName);
  }

  @Override
  public Iterator<Counter<?>> iterator() {
    return counters.values().iterator();
  }

  @Override
  public int size() {
    return counters.size();
  }

  @Override
  public boolean add(Counter<?> e) {
    if (null == e) {
      return false;
    }
    return counters.putIfAbsent(e.getUniqueName(), e) == null;
  }

  public void merge(CounterSet that) {
    for (Counter<?> theirCounter : that) {
      Counter<?> myCounter = counters.get(theirCounter.getUniqueName());
      if (myCounter != null) {
        mergeCounters(myCounter, theirCounter);
      } else {
        addCounter(theirCounter);
      }
    }
  }

  private <T> void mergeCounters(Counter<T> mine, Counter<?> theirCounter) {
    checkArgument(
        mine.isCompatibleWith(theirCounter),
        "Can't merge CounterSets containing incompatible counters with the same name: "
            + "%s (existing) and %s (merged)",
        mine,
        theirCounter);
    @SuppressWarnings("unchecked")
    Counter<T> theirs = (Counter<T>) theirCounter;
    mine.merge(theirs);
  }

  /**
   * A nested class that supports adding additional counters into the
   * enclosing CounterSet. This is useful as a mutator, hiding other
   * public methods of the CounterSet.
   */
  public class AddCounterMutator {

    /**
     * The {@link NameContext} to add counters into, optional.
     */
    private final NameContext context;

    /**
     * Constructor which takes a {@link NameContext}.
     */
    public AddCounterMutator(NameContext context) {
      this.context = checkNotNull(context);
    }

    /**
     * Default Constructor has no context associated.
     */
    public AddCounterMutator() {
      this.context = null;
    }

    /**
     * Adds the given Counter into the enclosing CounterSet.
     *
     * <p>If a counter with the same name already exists, it will be
     * reused, as long as it has the same type.
     *
     * @return the Counter that was reused, or added
     * @throws IllegalArgumentException if a counter with the same
     * name but an incompatible kind had already been added
     */
    public <T> Counter<T> addCounter(Counter<T> counter) {
      return addOrReuseCounter(counter);
    }

    /**
     * Returns the {@link NameContext} associated with this mutator, if any.
     */
    public NameContext getNameContext() {
      return this.context;
    }
  }
}
