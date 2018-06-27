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
package org.apache.beam.sdk.transforms.windowing;

/**
 * Abstract base class for {@link WindowFn WindowFns} that do not merge windows.
 *
 * @param <T> type of elements being windowed
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this {@code
 *     WindowFn}
 */
public abstract class NonMergingWindowFn<T, W extends BoundedWindow> extends WindowFn<T, W> {
  @Override
  public final void mergeWindows(MergeContext c) {}

  @Override
  public final boolean isNonMerging() {
    return true;
  }
}
