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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * {@link Operation} that executes a {@link DoFn}.
 */
public class NormalParDoOperation<InputT, OutputT> extends ParDoOperation<InputT, OutputT> {

  private final DoFn<InputT, OutputT> doFn;

  public NormalParDoOperation(
      DoFn<InputT, OutputT> doFn,
      PipelineOptions options,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      List<Graphs.Tag> sideInputTags,
      WindowingStrategy<?, ?> windowingStrategy) {
    super(options, mainOutputTag, sideOutputTags, sideInputTags, windowingStrategy);
    this.doFn = checkNotNull(doFn, "doFn");
  }

  @Override
  DoFn<InputT, OutputT> getDoFn() {
    return doFn;
  }
}
