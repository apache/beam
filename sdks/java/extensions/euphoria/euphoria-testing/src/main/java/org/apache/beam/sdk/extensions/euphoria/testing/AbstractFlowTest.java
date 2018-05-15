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
package org.apache.beam.sdk.extensions.euphoria.testing;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSink;
import org.apache.beam.sdk.extensions.euphoria.core.executor.Executor;

import java.util.List;

/**
 * Abstract test class for user's {@link Flow} testing.
 *
 * @param <OutputT> type of output dataset
 */
public abstract class AbstractFlowTest<OutputT> {

  /**
   * This method describes how the final dataset should look like.
   *
   * @return expected output dataset (ordering does not matter)
   */
  protected abstract List<OutputT> getOutput();

  /**
   * Creates input using provided {@link Flow} and creates output dataset.
   *
   * @param flow to lift inputs from
   * @return output dataset
   */
  protected abstract Dataset<OutputT> buildFlow(Flow flow);

  public void execute(Executor executor) {
    final Flow flow = Flow.create("test");
    final ListDataSink<OutputT> sink = ListDataSink.get();
    buildFlow(flow).persist(sink);
    executor.submit(flow).join();
    DatasetAssert.unorderedEquals(getOutput(), sink.getOutputs());
  }
}
