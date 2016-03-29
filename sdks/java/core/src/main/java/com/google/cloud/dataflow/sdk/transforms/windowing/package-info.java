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

/**
 * Defines the {@link com.google.cloud.dataflow.sdk.transforms.windowing.Window} transform
 * for dividing the elements in a PCollection into windows, and the
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.Trigger} for controlling when those
 * elements are output.
 *
 * <p>{@code Window} logically divides up or groups the elements of a
 * {@link com.google.cloud.dataflow.sdk.values.PCollection} into finite windows according to a
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}.
 * The output of {@code Window} contains the same elements as input, but they
 * have been logically assigned to windows. The next
 * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey}s, including one
 * within composite transforms, will group by the combination of keys and
 * windows.
 *
 * <p>Windowing a {@code PCollection} allows chunks of it to be processed
 * individually, before the entire {@code PCollection} is available.  This is
 * especially important for {@code PCollection}s with unbounded size, since the full
 * {@code PCollection} is never available at once.
 *
 * <p>For {@code PCollection}s with a bounded size, by default, all data is implicitly in a
 * single window, and this replicates conventional batch mode. However, windowing can still be a
 * convenient way to express time-sliced algorithms over bounded {@code PCollection}s.
 *
 * <p>As elements are assigned to a window, they are are placed into a pane. When the trigger fires
 * all of the elements in the current pane are output.
 *
 * <p>The {@link com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger} will output a
 * window when the system watermark passes the end of the window.  See
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark} for details on the
 * watermark.
 */
package com.google.cloud.dataflow.sdk.transforms.windowing;
