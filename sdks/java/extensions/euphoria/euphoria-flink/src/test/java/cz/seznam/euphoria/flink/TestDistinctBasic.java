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
package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.asserts.DatasetAssert;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

public class TestDistinctBasic {

  @Test
  public void test() throws Exception {
    Flow f = Flow.create("Test");

    ListDataSink<Pair<String, String>> output = ListDataSink.get();

    Dataset<Pair<String, String>> input =
        f.createInput(ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("foo", "bar"),
                Pair.of("quux", "ibis"),
                Pair.of("foo", "bar"))),
            // ~ force event time
            e -> 1L);
    Distinct.of(input)
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output()
        .persist(output);

    new TestFlinkExecutor().submit(f).get();

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Pair.of("foo", "bar"),
        Pair.of("quux", "ibis"));
  }
}