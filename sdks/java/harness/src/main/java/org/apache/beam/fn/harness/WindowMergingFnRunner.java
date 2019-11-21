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
package org.apache.beam.fn.harness;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn.MergeContext;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

/**
 * Merges windows using a {@link org.apache.beam.sdk.transforms.windowing.WindowFn}.
 *
 * <p>Window merging function:
 *
 * <ul>
 *   <li>Input: {@code KV<nonce, iterable<OriginalWindow>>}
 *   <li>Output: {@code KV<nonce, KV<iterable<UnmergedOriginalWindow>, iterable<KV<MergedWindow,
 *       iterable<ConsumedOriginalWindow>>>>}
 * </ul>
 *
 * <p>For each set of original windows, a list of all unmerged windows is output alongside a map of
 * merged window to set of consumed windows. All original windows must be contained in either the
 * unmerged original window set or one of the consumed original window sets. Each original window
 * can only be part of one output set. The nonce is used by a runner to associate each input with
 * its output. The nonce is represented as an opaque set of bytes.
 */
public abstract class WindowMergingFnRunner<T, W extends BoundedWindow> {
  static final String URN = BeamUrns.getUrn(StandardPTransforms.Primitives.MERGE_WINDOWS);

  /**
   * A registrar which provides a factory to handle merging windows based upon the {@link WindowFn}.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(
          URN,
          MapFnRunners.forValueMapFnFactory(WindowMergingFnRunner::createMapFunctionForPTransform));
    }
  }

  static <T, W extends BoundedWindow>
      ThrowingFunction<KV<T, Iterable<W>>, KV<T, KV<Iterable<W>, Iterable<KV<W, Iterable<W>>>>>>
          createMapFunctionForPTransform(String ptransformId, PTransform ptransform)
              throws IOException {
    RunnerApi.FunctionSpec payload =
        RunnerApi.FunctionSpec.parseFrom(ptransform.getSpec().getPayload());

    WindowFn<?, W> windowFn =
        (WindowFn<?, W>) WindowingStrategyTranslation.windowFnFromProto(payload);
    return WindowMergingFnRunner.<T, W>create(windowFn)::mergeWindows;
  }

  static <T, W extends BoundedWindow> WindowMergingFnRunner<T, W> create(WindowFn<?, W> windowFn) {
    if (windowFn.isNonMerging()) {
      return new NonMergingWindowFnRunner();
    } else {
      return new MergingViaWindowFnRunner(windowFn);
    }
  }

  /**
   * Returns the set of unmerged windows and a mapping from merged windows to sets of original
   * windows.
   */
  abstract KV<T, KV<Iterable<W>, Iterable<KV<W, Iterable<W>>>>> mergeWindows(
      KV<T, Iterable<W>> windowsToMerge) throws Exception;

  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * An optimized version of window merging where the {@link WindowFn} does not do any window
   * merging.
   *
   * <p>Note that this is likely to never be invoked and the identity mapping will be handled
   * directly by runners. We have this here because runners may not perform this optimization.
   */
  private static class NonMergingWindowFnRunner<T, W extends BoundedWindow>
      extends WindowMergingFnRunner<T, W> {
    @Override
    KV<T, KV<Iterable<W>, Iterable<KV<W, Iterable<W>>>>> mergeWindows(
        KV<T, Iterable<W>> windowsToMerge) {
      return KV.of(
          windowsToMerge.getKey(), KV.of(windowsToMerge.getValue(), Collections.emptyList()));
    }
  }

  /** An implementation which uses a {@link WindowFn} to merge windows. */
  private static class MergingViaWindowFnRunner<T, W extends BoundedWindow>
      extends WindowMergingFnRunner<T, W> {
    private final WindowFn<T, W> windowFn;
    private final WindowFn<?, W>.MergeContext mergeContext;
    private Collection<W> currentWindows;
    private List<KV<W, Collection<W>>> mergedWindows;

    private MergingViaWindowFnRunner(WindowFn<T, W> windowFn) {
      this.windowFn = windowFn;
      this.mergedWindows = new ArrayList<>();
      this.currentWindows = new ArrayList<>();
      this.mergeContext =
          windowFn.new MergeContext() {

            @Override
            public Collection<W> windows() {
              return currentWindows;
            }

            @Override
            public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
              mergedWindows.add(KV.of(mergeResult, toBeMerged));
            }
          };
    }

    @Override
    KV<T, KV<Iterable<W>, Iterable<KV<W, Iterable<W>>>>> mergeWindows(
        KV<T, Iterable<W>> windowsToMerge) throws Exception {
      currentWindows = Sets.newHashSet(windowsToMerge.getValue());
      windowFn.mergeWindows((MergeContext) mergeContext);
      for (KV<W, Collection<W>> mergedWindow : mergedWindows) {
        currentWindows.removeAll(mergedWindow.getValue());
      }
      return KV.of(windowsToMerge.getKey(), KV.of(currentWindows, (Iterable) mergedWindows));
    }
  }
}
