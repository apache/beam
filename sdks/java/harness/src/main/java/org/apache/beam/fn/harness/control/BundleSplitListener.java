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
package org.apache.beam.fn.harness.control;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;

/**
 * Listens to splits happening to a single bundle application. See <a
 * href="https://s.apache.org/beam-breaking-fusion">Breaking the Fusion Barrier</a> for a discussion
 * of the design.
 */
public interface BundleSplitListener {
  /**
   * Signals that the current application should be split into the given primary and residual roots.
   *
   * <p>Primary roots are the new decomposition of the bundle's work into transform applications
   * that have happened or will happen as part of this bundle (modulo future splits). Residual roots
   * are a decomposition of work that has been given away by the bundle, so the runner must delegate
   * it for someone else to execute.
   */
  void split(List<BundleApplication> primaryRoots, List<DelayedBundleApplication> residualRoots);

  /** A {@link BundleSplitListener} which gathers all splits produced and stores them in memory. */
  @AutoValue
  @NotThreadSafe
  abstract class InMemory implements BundleSplitListener {
    public static InMemory create() {
      return new AutoValue_BundleSplitListener_InMemory(new ArrayList<>(), new ArrayList<>());
    }

    @Override
    public void split(
        List<BundleApplication> primaryRoots, List<DelayedBundleApplication> residualRoots) {
      getPrimaryRoots().addAll(primaryRoots);
      getResidualRoots().addAll(residualRoots);
    }

    public void clear() {
      getPrimaryRoots().clear();
      getResidualRoots().clear();
    }

    public abstract List<BundleApplication> getPrimaryRoots();

    public abstract List<DelayedBundleApplication> getResidualRoots();
  }
}
