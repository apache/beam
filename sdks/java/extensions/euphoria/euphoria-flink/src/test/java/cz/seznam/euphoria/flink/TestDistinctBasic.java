/**
 * Copyright 2016 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TestDistinctBasic {

  @Test
  public void test() throws Exception {
    Flow f = Flow.create("Test");

    ListDataSink<Pair<String, String>> output = ListDataSink.get(1);

    Dataset<Pair<String, String>> input =
        f.createInput(ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("foo", "bar"),
                Pair.of("quux", "ibis"),
                Pair.of("foo", "bar"))));
    Distinct.of(input)
        .windowBy(Time.of(Duration.ofSeconds(1)), e -> 1L) // ~ force event time
        .output()
        .persist(output);

    new TestFlinkExecutor().submit(f).get();

    assertEquals(
        Arrays.asList(
            Pair.of("foo", "bar"),
            Pair.of("quux", "ibis"))
            .stream().sorted(Comparator.comparing(Pair::getFirst))
            .collect(Collectors.toList()),
        output.getOutput(0)
            .stream()
            .sorted(Comparator.comparing(Pair::getFirst))
            .collect(Collectors.toList()));
  }
}