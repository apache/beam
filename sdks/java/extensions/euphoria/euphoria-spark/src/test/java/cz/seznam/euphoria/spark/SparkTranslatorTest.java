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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.io.VoidSink;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.hint.ComputationHint;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.testing.DatasetAssert;
import cz.seznam.euphoria.spark.accumulators.SparkAccumulatorFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class SparkTranslatorTest {

  /**
   * Dataset {@code mapped} and {@code reduced} are used twice in flow so they should be cached. In
   * flow translation it corresponds with node 3 (mapped) and node 6 (reduced).
   */
  @Test
  public void testRDDCaching() {

    final Flow flow = Flow.create(getClass().getSimpleName());

    final ListDataSource<Integer> dataSource =
        ListDataSource.bounded(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

    final Dataset<Integer> input = flow.createInput(dataSource);

    final Dataset<Integer> mapped = MapElements.of(input)
        .using(e -> e)
        .output(ComputationHint.EXPENSIVE);

    final Dataset<Pair<Integer, Long>> reduced =
        ReduceByKey.of(mapped)
            .keyBy(e -> e)
            .reduceBy(values -> 1L)
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output(ComputationHint.EXPENSIVE);

    MapElements.of(mapped)
        .using(e -> e)
        .output()
        .persist(new VoidSink<>());

    MapElements.of(reduced)
        .using(Pair::getFirst)
        .output()
        .persist(new VoidSink<>());

    Join.of(mapped, reduced)
        .by(e -> e, Pair::getFirst)
        .using((Integer l, Pair<Integer, Long> r, Collector<Long> c) ->
            c.collect(r.getSecond()))
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output()
        .persist(new VoidSink<>());

    final JavaSparkContext sparkContext =
        new JavaSparkContext(
            new SparkConf()
                .setAppName("test")
                .setMaster("local[4]")
                .set("spark.serializer", KryoSerializer.class.getName())
                .set("spark.kryo.registrationRequired", "false"));

    final SparkAccumulatorFactory mockedFactory = mock(SparkAccumulatorFactory.class);

    final SparkFlowTranslator translator =
        new SparkFlowTranslator(sparkContext, flow.getSettings(), mockedFactory);
    translator.translateInto(flow, StorageLevel.MEMORY_ONLY());

    final List<Integer> expectedCachedNodeNumbers = new ArrayList<>(Arrays.asList(3, 6));

    assertEquals(2, sparkContext.getPersistentRDDs().size());
    DatasetAssert.unorderedEquals(
        expectedCachedNodeNumbers, new ArrayList<>(sparkContext.getPersistentRDDs().keySet()));

    sparkContext.close();
  }
}
