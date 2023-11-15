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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ListState;

/** {@link PushedBackElementsHandler} that stores elements in a Flink operator state list. */
class NonKeyedPushedBackElementsHandler<T> implements PushedBackElementsHandler<T> {

  static <T> NonKeyedPushedBackElementsHandler<T> create(ListState<T> elementState) {
    return new NonKeyedPushedBackElementsHandler<>(elementState);
  }

  private final ListState<T> elementState;

  private NonKeyedPushedBackElementsHandler(ListState<T> elementState) {
    this.elementState = checkNotNull(elementState);
  }

  @Override
  public Stream<T> getElements() throws Exception {
    return StreamSupport.stream(elementState.get().spliterator(), false);
  }

  @Override
  public void clear() {
    elementState.clear();
  }

  @Override
  public void pushBack(T element) throws Exception {
    elementState.add(element);
  }

  @Override
  public void pushBackAll(Iterable<T> elements) throws Exception {
    for (T e : elements) {
      // TODO: use addAll() once Flink has addAll(Iterable<T>)
      elementState.add(e);
    }
  }
}
