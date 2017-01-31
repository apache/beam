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
package cz.seznam.euphoria.examples.wordcount;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.examples.Executors;
import cz.seznam.euphoria.hadoop.input.SimpleHadoopTextFileSource;
import cz.seznam.euphoria.hadoop.output.SimpleHadoopTextFileSink;

import java.net.URI;
import java.util.regex.Pattern;

/**
 * Demostrates a very simple word-count supported batched input.
 *
 * Example usage on flink:
 * <pre>{@code
 *   $ flink run -m yarn-cluster \
 *      -yn 1 -ys 2 -ytm 800 \
 *      -c cz.seznam.euphoria.examples.wordcount.SimpleWordCount \
 *      euphoria-examples/assembly/euphoria-examples.jar \
 *      "flink" \
 *      "hdfs:///tmp/swc-input" \
 *      "hdfs:///tmp/swc-output" \
 *      "2"
 *}</pre>
 *
 * Example usage on spark:
 * <pre>{@code
 *   $ spark-submit --verbose --deploy-mode cluster \
 *       --master yarn \
 *       --executor-memory 1g \
 *       --num-executors 1 \
 *       --class cz.seznam.euphoria.examples.wordcount.SimpleWordCount \
 *       euphoria-examples/assembly/euphoria-examples.jar \
 *       "spark" \
 *       "hdfs:///tmp/swc-input" \
 *       "hdfs:///tmp/swc-output" \
 *       "1"
 * }</pre>
 */
public class SimpleWordCount {

  private static final Pattern SPLIT_RE = Pattern.compile("\\s+");

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: " + SimpleWordCount.class
          + " <executor-name> <input-uri> <output-uri> [num-output-partitions]");
      System.exit(1);
    }

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.file", SimpleHadoopTextFileSource.Factory.class);
    settings.setClass("euphoria.io.datasource.factory.hdfs", SimpleHadoopTextFileSource.Factory.class);
    settings.setClass("euphoria.io.datasink.factory.file", SimpleHadoopTextFileSink.Factory.class);
    settings.setClass("euphoria.io.datasink.factory.hdfs", SimpleHadoopTextFileSink.Factory.class);
    settings.setClass("euphoria.io.datasink.factory.stdout", StdoutSink.Factory.class);

    final String executorName = args[0];
    final String input = args[1];
    final String output = args[2];
    final int partitions = args.length > 3 ? Integer.parseInt(args[3]) : -1;

    Flow flow = buildFlow(settings, URI.create(input), URI.create(output), partitions);

    Executor executor = Executors.createExecutor(executorName);
    executor.submit(flow).get();
  }

  private static Flow buildFlow(Settings settings, URI input, URI output, int partitions)
      throws Exception
  {
    Flow flow = Flow.create(SimpleWordCount.class.getSimpleName(), settings);

    Dataset<String> lines = flow.createInput(input);

    // split lines to words
    Dataset<String> words = FlatMap.named("TOKENIZER")
            .of(lines)
            .using((String line, Context<String> c) ->
                SPLIT_RE.splitAsStream(line).forEachOrdered(c::collect))
            .output();

    // count per word
    Dataset<Pair<String, Long>> counted = ReduceByKey.named("REDUCE")
        .of(words)
        .keyBy(e -> e)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .applyIf(partitions > 0, op -> op.setNumPartitions(partitions))
        .output();

    // format output
    MapElements.named("FORMAT")
        .of(counted)
        .using(p -> p.getKey() + "\t" + p.getSecond())
        .output()
        .persist(output);

    return flow;
  }
}
