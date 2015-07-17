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
package com.google.cloud.dataflow.sdk.util.state;

/**
 * {@code State} that is automatically mergeable and supports buffering values.
 *
 * @param <InputT> The type of values put into the buffer.
 * @param <OutputT> The type of values extracted from the buffer.
 */
public interface MergeableState<InputT, OutputT> extends State {

  /**
   * Add a value to the buffer.
   */
  void add(InputT value);

  /**
   * Return the {@link StateContents} object to use for accessing the contents of the buffer.
   */
  StateContents<OutputT> get();

  /**
   * Return true if this state is empty.
   */
  StateContents<Boolean> isEmpty();
}
