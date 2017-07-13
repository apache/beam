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
package org.apache.beam.runners.jstorm.translation.runtime;

import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * JStorm {@link Executor} for {@link View}.
 */
public class ViewExecutor implements Executor {

  private final String description;
  private final TupleTag outputTag;
  private ExecutorsBolt executorsBolt;

  public ViewExecutor(String description, TupleTag outputTag) {
    this.description = description;
    this.outputTag = outputTag;
  }

  @Override
  public void init(ExecutorContext context) {
    this.executorsBolt = context.getExecutorsBolt();
  }

  @Override
  public <T> void process(TupleTag<T> tag, WindowedValue<T> elem) {
    executorsBolt.processExecutorElem(outputTag, elem);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String toString() {
    return description;
  }
}
