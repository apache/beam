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
package org.apache.beam.runners.flink.translation.wrappers.streaming.stableinput;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ListState;

/** A non-keyed implementation of a {@link BufferingElementsHandler}. */
public class NonKeyedBufferingElementsHandler<T> implements BufferingElementsHandler {

  static <T> NonKeyedBufferingElementsHandler<T> create(ListState<BufferedElement> elementState) {
    return new NonKeyedBufferingElementsHandler<>(elementState);
  }

  private final ListState<BufferedElement> elementState;

  private NonKeyedBufferingElementsHandler(ListState<BufferedElement> elementState) {
    this.elementState = checkNotNull(elementState);
  }

  @Override
  public Stream<BufferedElement> getElements() {
    try {
      return StreamSupport.stream(elementState.get().spliterator(), false);
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve buffered element from state backend.", e);
    }
  }

  @Override
  public void buffer(BufferedElement element) {
    try {
      elementState.add(element);
    } catch (Exception e) {
      throw new RuntimeException("Failed to buffer element in state backend.", e);
    }
  }

  @Override
  public void clear() {
    elementState.clear();
  }
}
