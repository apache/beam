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
package org.apache.beam.runners.dataflow.worker;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.DoFnInfo;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Contains methods for obtaining and clearing {@link DoFnInfo} instances. t */
public interface DoFnInstanceManager {
  /**
   * Get the {@link DoFnInfo} contained by this {@link DoFnInstanceManager} without obtaining
   * ownership of that {@link DoFnInfo}. {@link DoFn} processing methods should not be called on the
   * {@link DoFn} contained within the {@link DoFnInfo} returned by this call.
   */
  DoFnInfo<?, ?> peek() throws Exception;

  /**
   * Get the {@link DoFnInfo} contained by this {@link DoFnInstanceManager}, and obtain ownership of
   * the returned {@link DoFnInfo}.
   */
  DoFnInfo<?, ?> get() throws Exception;

  /**
   * Relinquish ownership of the provided {@link DoFnInfo} after successfully completing a bundle.
   */
  void complete(DoFnInfo<?, ?> fnInfo) throws Exception;

  /** Relinquish ownership of the provided {@link DoFnInfo} after aborting a bundle. */
  void abort(@Nullable DoFnInfo<?, ?> fnInfo) throws Exception;
}
