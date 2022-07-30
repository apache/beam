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
package org.apache.beam.runners.core;

import java.io.Serializable;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.Timers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * Specification for processing to happen after elements have been grouped by key.
 *
 * @param <K> The type of key being processed.
 * @param <InputT> The type of input values associated with the key.
 * @param <OutputT> The output type that will be produced for each key.
 * @param <W> The type of windows this operates on.
 */
public abstract class ReduceFn<K, InputT, OutputT, W extends BoundedWindow>
    implements Serializable {

  /** Information accessible to all the processing methods in this {@code ReduceFn}. */
  public abstract class Context {
    /** Return the key that is being processed. */
    public abstract K key();

    /** The window that is being processed. */
    public abstract W window();

    /** Access the current {@link WindowingStrategy}. */
    public abstract WindowingStrategy<?, W> windowingStrategy();

    /** Return the interface for accessing state. */
    public abstract StateAccessor<K> state();

    /** Return the interface for accessing timers. */
    public abstract Timers timers();
  }

  /** Information accessible within {@link #processValue}. */
  public abstract class ProcessValueContext extends Context {
    /** Return the actual value being processed. */
    public abstract InputT value();

    /** Return the timestamp associated with the value. */
    public abstract Instant timestamp();
  }

  /** Information accessible within {@link #onMerge}. */
  public abstract class OnMergeContext extends Context {
    /** Return the interface for accessing state. */
    @Override
    public abstract MergingStateAccessor<K, W> state();
  }

  /** Information accessible within {@link #onTrigger}. */
  public abstract class OnTriggerContext extends Context {
    /** Returns the {@link PaneInfo} for the trigger firing being processed. */
    public abstract PaneInfo paneInfo();

    /** Output the given value in the current window. */
    public abstract void output(OutputT value);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  /** Called for each value of type {@code InputT} associated with the current key. */
  public abstract void processValue(ProcessValueContext c) throws Exception;

  /** Called when windows are merged. */
  public abstract void onMerge(OnMergeContext context) throws Exception;

  /**
   * Called when triggers fire.
   *
   * <p>Implementations of {@link ReduceFn} should call {@link OnTriggerContext#output} to emit any
   * results that should be included in the pane produced by this trigger firing.
   */
  public abstract void onTrigger(OnTriggerContext context) throws Exception;

  /**
   * Called before {@link #onMerge} is invoked to provide an opportunity to prefetch any needed
   * state.
   *
   * @param c Context to use prefetch from.
   */
  public void prefetchOnMerge(MergingStateAccessor<K, W> c) throws Exception {}

  /**
   * Called before {@link #onTrigger} is invoked to provide an opportunity to prefetch any needed
   * state.
   *
   * @param context Context to use prefetch from.
   */
  public void prefetchOnTrigger(StateAccessor<K> context) {}

  /**
   * Called to clear any persisted state that the {@link ReduceFn} may be holding. This will be
   * called when the windowing is closing and will receive no future interactions.
   */
  public abstract void clearState(Context context) throws Exception;

  /** Returns true if there is no buffered state. */
  public abstract ReadableState<Boolean> isEmpty(StateAccessor<K> context);
}
