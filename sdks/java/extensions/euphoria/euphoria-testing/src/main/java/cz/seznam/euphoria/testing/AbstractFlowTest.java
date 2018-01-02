/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.testing;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.executor.local.LocalExecutor;

import java.util.List;

/**
 * Abstract test class for user's {@link Flow} testing.
 *
 * @param <OUT> type of output dataset
 */
public abstract class AbstractFlowTest<OUT> {

  /**
   * This method describes how the final dataset should look like.
   *
   * @return expected output dataset (ordering does not matter)
   */
  protected abstract List<OUT> getOutput();

  /**
   * Creates input using provided {@link Flow} and creates output dataset.
   *
   * @param flow to lift inputs from
   * @return output dataset
   */
  protected abstract Dataset<OUT> buildFlow(Flow flow);

  public void execute() {
    final Flow flow = Flow.create("test");
    final ListDataSink<OUT> sink = ListDataSink.get();
    buildFlow(flow).persist(sink);
    final Executor executor = new LocalExecutor();
    executor.submit(flow).join();
    DatasetAssert.unorderedEquals(getOutput(), sink.getOutputs());
  }
}
