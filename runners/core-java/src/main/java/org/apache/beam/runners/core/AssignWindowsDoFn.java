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
package org.apache.beam.runners.core;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterables;
import java.util.Collection;
import org.apache.beam.runners.core.OldDoFn.RequiresWindowAccess;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

/**
 * {@link OldDoFn} that tags elements of a {@link PCollection} with windows, according to the
 * provided {@link WindowFn}.
 *
 * @param <T> Type of elements being windowed
 * @param <W> Window type
 */
@SystemDoFnInternal
public class AssignWindowsDoFn<T, W extends BoundedWindow> extends OldDoFn<T, T>
    implements RequiresWindowAccess {
  private WindowFn<? super T, W> fn;

  public AssignWindowsDoFn(WindowFn<? super T, W> fn) {
    this.fn =
        checkNotNull(
            fn,
            "%s provided to %s cannot be null",
            WindowFn.class.getSimpleName(),
            AssignWindowsDoFn.class.getSimpleName());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(final ProcessContext c) throws Exception {
    Collection<W> windows =
        ((WindowFn<T, W>) fn).assignWindows(
            ((WindowFn<T, W>) fn).new AssignContext() {
                @Override
                public T element() {
                  return c.element();
                }

                @Override
                public Instant timestamp() {
                  return c.timestamp();
                }

                @Override
                public BoundedWindow window() {
                  return Iterables.getOnlyElement(c.windowingInternals().windows());
                }
              });

    c.windowingInternals()
        .outputWindowedValue(c.element(), c.timestamp(), windows, PaneInfo.NO_FIRING);
  }
}
