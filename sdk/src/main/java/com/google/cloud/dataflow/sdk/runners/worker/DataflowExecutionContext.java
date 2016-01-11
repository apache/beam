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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.BaseExecutionContext;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.NullSideInputReader;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;

/**
 * Extensions to {@link BaseExecutionContext} specific to the Dataflow worker.
 */
public abstract class DataflowExecutionContext<T extends ExecutionContext.StepContext>
    extends BaseExecutionContext<T> {

  /**
   * Returns a {@link SideInputReader} based on {@link SideInputInfo} descriptors
   * and {@link PCollectionView PCollectionViews}.
   *
   * <p>If side input source metadata is provided by the service in
   * {@link SideInputInfo sideInputInfos}, we request
   * a {@link SideInputReader} from the {@code executionContext} using that info.
   * If no side input source metadata is provided but the DoFn expects side inputs, as a
   * fallback, we request a {@link SideInputReader} based only on the expected views.
   *
   * <p>These cases are not disjoint: Whenever a {@link DoFn} takes side inputs,
   * {@code doFnInfo.getSideInputViews()} should be non-empty.
   *
   * <p>A note on the behavior of the Dataflow service: Today, the first case corresponds to
   * batch mode, while the fallback corresponds to streaming mode.
   */
  public SideInputReader getSideInputReader(
      @Nullable Iterable<? extends SideInputInfo> sideInputInfos,
      @Nullable Iterable<? extends PCollectionView<?>> views) throws Exception {
    if (sideInputInfos != null && sideInputInfos.iterator().hasNext()) {
      return getSideInputReader(sideInputInfos);
    } else if (views != null && Iterables.size(views) > 0) {
      return getSideInputReaderForViews(views);
    } else {
      return NullSideInputReader.empty();
    }
  }

  /**
   * Returns a {@link SideInputReader} for all the side inputs described in the given
   * {@link SideInputInfo} descriptors.
   */
  protected abstract SideInputReader getSideInputReader(
      Iterable<? extends SideInputInfo> sideInputInfos) throws Exception;

  /**
   * Returns a {@link SideInputReader} for all the provided views, where the execution context
   * itself knows how to read data for the view.
   */
  protected abstract SideInputReader getSideInputReaderForViews(
      Iterable<? extends PCollectionView<?>> views) throws Exception;
}
