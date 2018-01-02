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
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import org.junit.Test;

import java.util.Arrays;


public class TestConsecutiveBasicOpsTranslation {

  @Test
  public void test() throws Exception {
    Flow f = Flow.create("Test");

    ListDataSink<Triple<String, String, String>> output = ListDataSink.get();

    Dataset<Pair<String, String>> input =
        f.createInput(ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("foo", "bar"),
                Pair.of("quux", "ibis"))));
    Dataset<Pair<String, String>> intermediate =
        MapElements.of(input)
            .using(e -> Pair.of(e.getFirst(), e.getSecond()))
            .output();
    MapElements.of(intermediate)
        .using(e -> Triple.of("uf", e.getFirst(), e.getSecond()))
        .output()
        .persist(output);

    new TestFlinkExecutor().submit(f).get();

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Triple.of("uf", "foo", "bar"),
        Triple.of("uf", "quux", "ibis"));
  }
}