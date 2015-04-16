/*
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
 */

package com.google.cloud.dataflow.sdk.coders;

/**
 * A {@code DeterministicStandardCoder} is a {@link StandardCoder} that is
 * deterministic, in the sense that for objects considered equal
 * according to {@code Object.equals()}, the encoded bytes are
 * also equal.
 *
 * @param <T> the type of the values being transcoded
 */
public abstract class DeterministicStandardCoder<T> extends StandardCoder<T> {
  private static final long serialVersionUID = 0;

  protected DeterministicStandardCoder() {}

  /**
   * As a {@code DeterministicStandardCoder} is presumed deterministic, this
   * method does nothing.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException { }
}
