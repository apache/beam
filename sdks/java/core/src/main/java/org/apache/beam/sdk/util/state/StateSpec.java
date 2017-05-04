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

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;

/**
 * A specification of a persistent state cell. This includes information necessary to encode the
 * value and details about the intended access pattern.
 *
 * @param <StateT> The type of state being described.
 */
@Experimental(Kind.STATE)
public interface StateSpec<StateT extends State> extends Serializable {

  /**
   * Use the {@code binder} to create an instance of {@code StateT} appropriate for this address.
   */
  StateT bind(String id, StateBinder binder);

  /**
   * Given {code coders} are inferred from type arguments defined for this class. Coders which are
   * already set should take precedence over offered coders.
   *
   * @param coders Array of coders indexed by the type arguments order. Entries might be null if the
   *     coder could not be inferred.
   */
  void offerCoders(Coder[] coders);

  /**
   * Validates that this {@link StateSpec} has been specified correctly and finalizes it.
   * Automatically invoked when the pipeline is built.
   */
  void finishSpecifying();
}
