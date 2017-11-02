/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;


/**
 * Test for operator {@code Union}
 */
@Processing(Processing.Type.ALL)
public class UnionTest extends AbstractOperatorTest {

  @Test
  public void testUnion() {
    execute(new TestCase<Integer>() {

      @Override
      public Dataset<Integer> getOutput(Flow flow, boolean bounded) {
        Dataset<Integer> first = flow.createInput(ListDataSource.of(bounded,
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6)));

        Dataset<Integer> second = flow.createInput(ListDataSource.of(bounded,
            Arrays.asList(7, 8, 9),
            Arrays.asList(10, 11, 12)));

        return Union.of(first, second).output();
      }

      @Override
      public List<Integer> getUnorderedOutput() {
        return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
      }
    });
  }

}
