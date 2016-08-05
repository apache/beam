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
package org.apache.beam.sdk.util.state;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.GroupByKey;

import java.io.IOException;
import java.io.Serializable;

/**
 * An address for persistent state. This includes a unique identifier for the location, the
 * information necessary to encode the value, and details about the intended access pattern.
 *
 * <p>State can be thought of as a sparse table, with each {@code StateTag} defining a column
 * that has cells of type {@code StateT}.
 *
 * <p>Currently, this can only be used in a step immediately following a {@link GroupByKey}.
 *
 * @param <K> The type of key that must be used with the state tag. Contravariant: methods should
 *            accept values of type {@code KeyedStateTag<? super K, StateT>}.
 * @param <StateT> The type of state being tagged.
 */
@Experimental(Kind.STATE)
public interface StateTag<K, StateT extends State> extends Serializable {

  /** Append the UTF-8 encoding of this tag to the given {@link Appendable}. */
  void appendTo(Appendable sb) throws IOException;

  /**
   * Returns the user-provided name of this state cell.
   */
  String getId();

  /**
   * Returns the spec for the enclosed state cell.
   */
  StateSpec<K, StateT> getSpec();
}
