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
package cz.seznam.euphoria.examples.wordcount;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.testing.AbstractFlowTest;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class SimpleWordCountTest {

  private static void execute(FlowTest test) {
    test.execute(new LocalExecutor());
  }

  @Test
  public void test_allLowercase() {
    execute(
        new FlowTest() {

          @Override
          List<String> getInput() {
            return Arrays.asList(
                "first second third fourth",
                "first second third fourth",
                "first second third fourth",
                "first second third fourth");
          }

          @Override
          protected List<String> getOutput() {
            return Arrays.asList("first:4", "second:4", "third:4", "fourth:4");
          }
        });
  }

  @Test
  public void test_firstLetterUppercase() {
    execute(
        new FlowTest() {

          @Override
          List<String> getInput() {
            return Arrays.asList(
                "First Second Third Fourth",
                "First Second Third Fourth",
                "First Second Third Fourth",
                "First Second Third Fourth");
          }

          @Override
          protected List<String> getOutput() {
            return Arrays.asList("first:4", "second:4", "third:4", "fourth:4");
          }
        });
  }

  private abstract static class FlowTest extends AbstractFlowTest<String> {

    abstract List<String> getInput();

    @Override
    protected Dataset<String> buildFlow(Flow flow) {
      return SimpleWordCount.buildFlow(flow.createInput(ListDataSource.bounded(getInput())));
    }
  }
}
