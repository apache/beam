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
package org.apache.beam.sdk.extensions.ordered;

import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Classes extending this interface will be called by {@link OrderedEventProcessor} to examine every
 * incoming event.
 *
 * @param <EventT>
 * @param <StateT>
 */
public interface EventExaminer<EventT, StateT extends MutableState<EventT, ?>>
    extends Serializable {

  /**
   * Is this event the first expected event for the given key and window if the per key sequence is
   * used? In case of global sequence it determines the first global sequence event.
   *
   * @param sequenceNumber the sequence number of the event as defined by the key of the input
   *     PCollection to {@link OrderedEventProcessor}
   * @param event being processed
   * @return true if this is the initial sequence.
   */
  boolean isInitialEvent(long sequenceNumber, EventT event);

  /**
   * If the event was the first event for a given key, create the state to hold the required data
   * needed for processing. This data will be persisted in a Beam state.
   *
   * @param event the first event in the sequence.
   * @return the state to persist.
   */
  @NonNull
  StateT createStateOnInitialEvent(EventT event);

  /**
   * Is this event the last expected event for a given key and window?
   *
   * <p>Note, this method is not used yet with global sequences.
   *
   * @param sequenceNumber of the event
   * @param event being processed
   * @return true if the last event. There are cases where it's impossible to know whether it's the
   *     last event. False should be returned in those cases.
   */
  boolean isLastEvent(long sequenceNumber, EventT event);
}
