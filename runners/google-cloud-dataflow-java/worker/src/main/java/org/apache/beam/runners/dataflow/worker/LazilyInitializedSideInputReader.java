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

import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link SideInputReader} which initializes on first {@link #get(PCollectionView, BoundedWindow)}
 * call.
 */
public class LazilyInitializedSideInputReader implements SideInputReader {
  private final Set<TupleTag<?>> tupleTags;
  private final Supplier<SideInputReader> lazyInitSideInputReader;

  public LazilyInitializedSideInputReader(
      Iterable<? extends SideInputInfo> sideInputInfos, Supplier<SideInputReader> sideInputReader) {
    tupleTags = new HashSet<>();
    for (SideInputInfo sideInputInfo : sideInputInfos) {
      tupleTags.add(new TupleTag<>(sideInputInfo.getTag()));
    }
    lazyInitSideInputReader = Suppliers.memoize(sideInputReader);
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    return lazyInitSideInputReader.get().get(view, window);
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return tupleTags.contains(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return tupleTags.isEmpty();
  }
}
