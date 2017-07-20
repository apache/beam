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

package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/** A {@link DataflowRunner} marker class for creating a {@link PCollectionView}. */
public class CreateDataflowView<ElemT, ViewT>
    extends PTransform<PCollection<ElemT>, PCollectionView<ViewT>> {
  public static <ElemT, ViewT> CreateDataflowView<ElemT, ViewT> of(PCollectionView<ViewT> view) {
    return new CreateDataflowView<>(view);
  }

  private final PCollectionView<ViewT> view;

  private CreateDataflowView(PCollectionView<ViewT> view) {
    this.view = view;
  }

  @Override
  public PCollectionView<ViewT> expand(PCollection<ElemT> input) {
    return view;
  }

  public PCollectionView<ViewT> getView() {
    return view;
  }
}
