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
package org.apache.beam.sdk.coders;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * An abstract base class that implements all methods of {@link Coder} except {@link Coder#encode}
 * and {@link Coder#decode}.
 *
 * @param <T> the type of values being encoded and decoded
 */
public abstract class CustomCoder<T> extends Coder<T> implements Serializable {

  /**
   * {@inheritDoc}.
   *
   * <p>Returns an empty list. A {@link CustomCoder} has no default argument {@link Coder coders}.
   */
  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException a {@link CustomCoder} is presumed nondeterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(
        this,
        "CustomCoder implementations must override verifyDeterministic,"
            + " or they are presumed nondeterministic.");
  }

  // This coder inherits isRegisterByteSizeObserverCheap,
  // getEncodedElementByteSize and registerByteSizeObserver
  // from Coder. Override if we can do better.
}
