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
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.testing.DatasetAssert;
import cz.seznam.euphoria.spark.accumulators.SparkAccumulatorFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class SparkTranslatorTest {

  /**
   * Dataset {@code mapped} and {@code reduced} are used twice in flow so they should be cached.
   * In flow translation it corresponds with node 3 (mapped) and node 6 (reduced).
   */
  @Test
  public void testRDDcaching() {
    Flow flow = Flow.create(getClass().getSimpleName());
    final ListDataSource<Integer> dataSource = ListDataSource.bounded(Arrays.asList(
        1, 2, 3,
        4, 5, 6, 7));
    Dataset<Integer> input = flow.createInput(dataSource);

    Dataset<Integer> mapped = MapElements.of(input).using(e -> e).output();
    Dataset<Pair<Integer, Long>> reduced = ReduceByKey
        .of(mapped)
        .keyBy(e -> e)
        .reduceBy(values -> 1L)
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output();

    Dataset<Integer> mapped2 = MapElements.of(mapped).using(e -> e).output();
    mapped2.persist(new VoidSink<>());

    Dataset<Integer> mapped3 = MapElements.of(reduced).using(Pair::getFirst).output();
    mapped3.persist(new VoidSink<>());

    Dataset<Pair<Integer, Long>> output = Join.of(mapped, reduced)
        .by(e -> e, Pair::getFirst)
        .using((Integer l, Pair<Integer, Long> r, Collector<Long> c) -> c.collect(r.getSecond()))
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output();

    output.persist(new VoidSink<>());

    JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
        .setAppName("test")
        .setMaster("local[4]")
        .set("spark.serializer", KryoSerializer.class.getName())
        .set("spark.kryo.registrationRequired", "false"));

    SparkAccumulatorFactory mockedFactory = mock(SparkAccumulatorFactory.class);


    SparkFlowTranslator translator = new SparkFlowTranslator(sparkContext, flow.getSettings(), mockedFactory);
    translator.translateInto(flow);

    List<Integer> expectedCachedNodeNumbers = new ArrayList<>(Arrays.asList(3, 6));

    assertEquals(2, sparkContext.getPersistentRDDs().size());
    DatasetAssert.unorderedEquals(expectedCachedNodeNumbers,
        new ArrayList<>(sparkContext.getPersistentRDDs().keySet()));

    sparkContext.close();
  }
}
