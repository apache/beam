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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * {@link Operation} that executes a {@link GroupAlsoByWindowsViaOutputBufferDoFn}.
 */
public class GroupAlsoByWindowsParDoOperation extends ParDoOperation {

  private final Coder<?> inputCoder;

  public GroupAlsoByWindowsParDoOperation(
      PipelineOptions options,
      WindowingStrategy<?, ?> windowingStrategy,
      Coder<?> inputCoder) {
    super(options, new TupleTag<>(), ImmutableList.<TupleTag<?>>of(), windowingStrategy);
    this.inputCoder = checkNotNull(inputCoder, "inputCoder");
  }

  @Override
  DoFn<Object, Object> getDoFn() {
    return new GroupAlsoByWindowsViaOutputBufferDoFn(
        windowingStrategy,
        SystemReduceFn.buffering(inputCoder),
        mainOutputTag,
        createOutputManager());
  }
}
