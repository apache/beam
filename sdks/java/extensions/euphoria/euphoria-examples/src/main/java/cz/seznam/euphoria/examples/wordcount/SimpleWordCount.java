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
package cz.seznam.euphoria.examples.wordcount;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.examples.Executors;
import cz.seznam.euphoria.hadoop.input.SimpleHadoopTextFileSource;
import cz.seznam.euphoria.hadoop.output.SimpleHadoopTextFileSink;
import java.util.regex.Pattern;

/**
 * Demonstrates a very simple word-count supporting batched input
 * without windowing.
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
          + " <executor-name> <input-path> <output-path>");
      System.exit(1);
    }
    final String executorName = args[0];
    final String input = args[1];
    final String output = args[2];

    // Define a source of data to read text lines from.  We utilize an
    // already predefined DataSource implementations hiding some of
    // implementation details.  Note that at this point in time the data
    // is not read.  The source files will be opened and read in a distributed
    // manner only when the "WordCount" flow is submitted for execution.
    DataSource<String> inSource = new SimpleHadoopTextFileSource(input);

    // Define a sink where to write the program's final results to.  As with
    // the above defined source, no resources are opened for writing yet.
    // Only when the program is submitted for execution, the sink will be
    // instructed to open writers to the final, physical destination.  Here,
    // we utilize an already predefined DataSink which simply writes a string
    // on its own line.
    DataSink<String> outSink = new SimpleHadoopTextFileSink<>(output);

    // Construct a flow which we'll later submit for execution. For the sake
    // of readability we've moved the definition into its own method.
    Flow flow = buildFlow(inSource, outSink);

    // Allocate an executor by the specified name.
    Executor executor = Executors.createExecutor(executorName);

    // Only now we submit the flow and will have the executor execute
    // the business logic defined by the flow. Only, we data sources
    // and sinks will be opened.
    //
    // As you can see the submission of flow happens in the background,
    // and we could submit other flows to execute concurrently with the
    // one just submitted.  To await the termination of a flow, we just
    // ask for the result of the `Future` object returned by `submit()`.
    executor.submit(flow).get();
  }

  /**
   * This method defines the executor independent business logic of the program.
   *
   * @param input the source to read lines of text from
   * @param output the sink to write the output of the business logic to
   *
   * @return a flow, a unit to be executed on a specific executor
   */
  private static Flow buildFlow(DataSource<String> input, DataSink<String> output) {
    // The first step in building a euphoria flow is creating a ...
    // well, a `Flow` object. It is a container encapsulating a chain
    // of transformations. Within a program we can have many flows. Though,
    // these all will be independent. Dependencies between operation
    // can be expressed only within a single flow.
    //
    // It is usually good practice to give each flow within a program a
    // unique name to make it easier to distinguish corresponding statistics
    // or otherwise displayed information from other flow which may be
    // potentially part of the program.
    Flow flow = Flow.create(SimpleWordCount.class.getSimpleName());

    // Given a data source we lift this source up into an abstract data
    // set. A data set is the input and output of operators. While a source
    // describes a particular source a data set is abstracting from this
    // particular notion. It can be literally thought of as a "set of data"
    // (without the notion of uniqueness of elements.)
    //
    // Note: we ask the flow to do this lifting. The new data set will
    // automatically be associated with the flow. All operators processing
    // this data set will also become automatically associated with the
    // flow. Using the data set (or an operator) associated with a flow
    // in a different flow, is considered an error and will lead to
    // exceptions before the flow is even executed.
    Dataset<String> lines = flow.createInput(input);

    // In the next step we want to chop up the data set of strings into a
    // data set of words. Using the `FlatMap` operator we can process each
    // element/string from the original data set to transform it into words.
    //
    // Note: Generally we are never modifying the original input data set but
    // merely produce a new one. Further, the processing order of the input
    // elements is generally unknown and typically happens in parallel.
    //
    // The `FlatMap` operator in particular is a handy choice at this point.
    // It processes one input element at a time and allows user code to emit
    // zero, one, or more output elements. We use it here to chop up a long
    // string into individual words and emit each individually instead.
    Dataset<String> words = FlatMap.named("TOKENIZER")
            .of(lines)
            .using((String line, Collector<String> c) ->
                SPLIT_RE.splitAsStream(line).forEachOrdered(c::collect))
            .output();

    // Given the "words" data set, we want to reduce it to a collection
    // of word-counts, i.e. a collection which counts the number occurrences
    // of every distinct word.
    //
    // From each input element we extract a key, which is the word itself
    // here, and a value, which is the constant `1` in this example. Then, we
    // reduce by the key - the operator ensures that all values for the same
    // key end up being processed together.  It applies our `combineBy` user
    // defined function to these values. The result of this user defined
    // function is then emitted to the output along with its corresponding
    // key.
    Dataset<Pair<String, Long>> counted = ReduceByKey.named("REDUCE")
        .of(words)
        .keyBy(e -> e)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .output();

    // Lastly we merely format the output of the preceding operator and
    // call `.persist()` with a data sink specifying the "persistent"
    // destination of the data. A data source itself describes where to
    // write the data to and how to physically lay it out.
    //
    // Note: a flow without any call to `.persist()` is meaningless as
    // such a flow would never produces anything. Executors are free to
    // reject such flows.
    MapElements.named("FORMAT")
        .of(counted)
        .using(p -> p.getFirst() + "\t" + p.getSecond())
        .output()
        .persist(output);

    return flow;
  }
}
