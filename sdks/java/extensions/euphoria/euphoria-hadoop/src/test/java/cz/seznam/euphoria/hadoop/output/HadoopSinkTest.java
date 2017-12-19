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
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.dataset.asserts.DatasetAssert;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.ExceptionUtils;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class HadoopSinkTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void test() throws InterruptedException, IOException {

    final String outputDir = tmp.getRoot() + "/output";

    final Flow flow = Flow.create();

    final DataSource<Pair<Text, LongWritable>> source = ListDataSource.bounded(
        Collections.singletonList(Pair.of(new Text("first"), new LongWritable(1L))),
        Collections.singletonList(Pair.of(new Text("second"), new LongWritable(2L))),
        Collections.singletonList(Pair.of(new Text("third"), new LongWritable(3L))),
        Collections.singletonList(Pair.of(new Text("fourth"), new LongWritable(3L))));

    final HadoopSink<Text, LongWritable> sink =
        new SequenceFileSink<>(Text.class, LongWritable.class, outputDir);

    MapElements
        .of(flow.createInput(source))
        .using(p -> p)
        .output()
        .persist(sink);

    final Executor executor = new LocalExecutor().setDefaultParallelism(4);
    executor.submit(flow).join();

    final List<Pair<Text, LongWritable>> output = Arrays
        .stream(Objects.requireNonNull(new File(outputDir).list()))
        .filter(file -> file.startsWith("part-r-"))
        .flatMap(part -> ExceptionUtils.unchecked(() -> {
          try (final SequenceFileRecordReader<Text, LongWritable> reader =
                   new SequenceFileRecordReader<>()) {
            final Path path = new Path(outputDir + "/" + part);
            final TaskAttemptContext taskContext =
                HadoopUtils.createTaskContext(new Configuration(), HadoopUtils.getJobID(), 0);
            reader.initialize(
                new FileSplit(path, 0L, Long.MAX_VALUE, new String[]{"localhost"}),
                taskContext);
            final List<Pair<Text, LongWritable>> result = new ArrayList<>();
            while (reader.nextKeyValue()) {
              result.add(Pair.of(reader.getCurrentKey(), reader.getCurrentValue()));
            }
            return result.stream();
          }
        }))
        .collect(Collectors.toList());

    DatasetAssert.unorderedEquals(Arrays.asList(
        Pair.of(new Text("first"), new LongWritable(1L)),
        Pair.of(new Text("second"), new LongWritable(2L)),
        Pair.of(new Text("third"), new LongWritable(3L)),
        Pair.of(new Text("fourth"), new LongWritable(3L))
    ), output);
  }

}
