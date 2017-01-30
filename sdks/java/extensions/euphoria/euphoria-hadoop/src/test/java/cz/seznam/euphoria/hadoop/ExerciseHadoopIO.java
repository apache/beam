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
package cz.seznam.euphoria.hadoop;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.input.SimpleHadoopTextFileSource;
import cz.seznam.euphoria.inmem.InMemExecutor;

import java.net.URI;
import java.util.regex.Pattern;

/** Implements a very simplistic WordCount over text files using hadoop data sinks. */
public class ExerciseHadoopIO {

  private static final Pattern SPLIT_RE = Pattern.compile("\\s+");

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: " + ExerciseHadoopIO.class + " <input-uri>");
      System.exit(1);
    }

    final URI inputUri = URI.create(args[0]);

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.webhdfs",
        SimpleHadoopTextFileSource.Factory.class);
    settings.setClass("euphoria.io.datasource.factory.hdfs",
        SimpleHadoopTextFileSource.Factory.class);
    settings.setClass("euphoria.io.datasource.factory.file",
        SimpleHadoopTextFileSource.Factory.class);

    Flow flow = Flow.create("WordCount", settings);

    // set-up our input source (a stream)
    Dataset<String> lines = flow.createInput(inputUri);

    Dataset<Pair<String, Long>> tuples = FlatMap.of(lines)
        .using((String line, Context<Pair<String, Long>> out) ->
            SPLIT_RE.splitAsStream(line)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .forEachOrdered(s -> out.collect(Pair.of(s, 1L))))
        .output();

    // reduce it to counts, use windowing
    Dataset<Pair<String, Long>> wordCount = ReduceByKey
            .of(tuples)
            .keyBy(Pair::getFirst)
            .valueBy(Pair::getSecond)
            .combineBy(Sums.ofLongs())
            .output();

    // produce the output
    wordCount.persist(new StdoutSink<>());

    Executor executor = new InMemExecutor();
    executor.submit(flow).get();
  }
}
