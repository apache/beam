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
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.hadoop.output.HadoopSink;
import cz.seznam.euphoria.hadoop.output.SequenceFileSink;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static org.junit.Assert.*;

public class HadoopSinkTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void test() throws InterruptedException {

    final Flow flow = Flow.create();

    final DataSource<Pair<Text, LongWritable>> source = ListDataSource.bounded(
        Collections.singletonList(Pair.of(new Text("first"), new LongWritable(1L))),
        Collections.singletonList(Pair.of(new Text("second"), new LongWritable(2L))),
        Collections.singletonList(Pair.of(new Text("third"), new LongWritable(3L))),
        Collections.singletonList(Pair.of(new Text("fourth"), new LongWritable(3L))));

    final HadoopSink<Text, LongWritable> sink =
        new SequenceFileSink<>(Text.class, LongWritable.class, tmp.getRoot().getAbsolutePath());

    MapElements
        .of(flow.createInput(source))
        .using(p -> p)
        .output()
        .persist(sink);

    final Executor executor = new LocalExecutor().setDefaultParallelism(4);
    executor.submit(flow).join();

    final long numFiles = Arrays
        .stream(Objects.requireNonNull(tmp.getRoot().list()))
        .filter(file -> file.startsWith("part-r-"))
        .count();

    assertEquals(4, numFiles);
  }

}
