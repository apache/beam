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

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility DoFn for writing streaming PCollectionViews.
 *
 * @param <T> element type
 */
public class StreamingPCollectionViewWriterFn<T>
    extends DoFn<Iterable<T>, T> implements DoFn.RequiresWindowAccess {
  private static final long serialVersionUID = 0;

  private final PCollectionView<?> view;
  private final Coder<T> dataCoder;

  public static <T> StreamingPCollectionViewWriterFn<T> create(
      PCollectionView<?> view, Coder<T> dataCoder) {
    return new StreamingPCollectionViewWriterFn<T>(view, dataCoder);
  }

  private StreamingPCollectionViewWriterFn(PCollectionView<?> view, Coder<T> dataCoder) {
    this.view = view;
    this.dataCoder = dataCoder;
  }

  @Override
  public void processElement(ProcessContext c) throws Exception {
    List<WindowedValue<T>> output = new ArrayList<>();
    for (T elem : c.element()) {
      output.add(WindowedValue.of(elem, c.timestamp(), c.window(), c.pane()));
    }

    c.windowingInternals().writePCollectionViewData(
        view.getTagInternal(), output, dataCoder);
  }
}
