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
package org.apache.beam.sdk.state;

import javax.annotation.Nonnull;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * A {@link ReadableState} cell containing a bag of values. Items can be added to the bag and the
 * contents read out.
 *
 * <p>Implementations of this form of state are expected to implement {@link #add} efficiently, not
 * via a sequence of read-modify-write.
 *
 * @param <T> The type of elements in the bag.
 */
@Experimental(Kind.STATE)
public interface BagState<T> extends GroupingState<T, Iterable<T>> {

  @Override
  @Nonnull
  Iterable<T> read();

  @Override
  BagState<T> readLater();
}
