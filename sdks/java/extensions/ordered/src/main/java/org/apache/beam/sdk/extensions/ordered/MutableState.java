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

/** Mutable state mutates when events apply to it. It is stored in a Beam state. */
public interface MutableState<EventT, ResultT> extends Serializable {

  /**
   * The interface assumes that events will mutate the state without the possibility of throwing an
   * error.
   *
   * @param event to be processed
   * @throws Exception if a checked exception is thrown, the event will be output into {@link
   *     OrderedEventProcessorResult#unprocessedEvents()} with
   */
  void mutate(EventT event) throws Exception;

  /**
   * This method is called after each state mutation.
   *
   * @return Result of the processing. Can be null if nothing needs to be output after this
   *     mutation.
   */
  ResultT produceResult();
}
