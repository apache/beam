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
import com.google.cloud.dataflow.sdk.util.BaseExecutionContext;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

/**
 * Extensions to {@link BaseExecutionContext} specific to the Dataflow worker.
 */
public abstract class DataflowExecutionContext extends BaseExecutionContext {
  /**
   * Returns a {@link SideInputReader} for all the side inputs described in the given
   * {@link SideInputInfo} descriptors.
   */
  public abstract SideInputReader getSideInputReader(
      Iterable<? extends SideInputInfo> sideInputInfos) throws Exception;

  /**
   * Returns a {@link SideInputReader} for all the provided views, where the execution context
   * itself knows how to read data for the view.
   */
  public abstract SideInputReader getSideInputReaderForViews(
      Iterable<? extends PCollectionView<?>> views) throws Exception;
}
