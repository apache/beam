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
import cz.seznam.euphoria.hadoop.input.HadoopTextFileSource;
import cz.seznam.euphoria.inmem.InMemExecutor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.net.URI;

public class ExerciseHadoopIO {

  public static void main(String[] args) throws Exception {
    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.webhdfs",
        HadoopTextFileSource.Factory.class);

    Flow flow = Flow.create("Test", settings);

    // set-up our input source (a stream)
    Dataset<Pair<LongWritable, Text>> lines = flow.createInput(
            URI.create("webhdfs://gin.dev/user/fulltext/shakespeare.txt"));

    Dataset<Pair<String, Long>> tuples = FlatMap.of(lines)
            .using((Pair<LongWritable, Text> line, Context<Pair<String, Long>> out) -> {
              for (String w : line.getSecond().toString().split(" ")) {
                out.collect(Pair.of(w, 1L));
              }
            }).output();

    // reduce it to counts, use windowing
    Dataset<Pair<String, Long>> wordCount = ReduceByKey
            .of(tuples)
            .keyBy(Pair::getFirst)
            .valueBy(Pair::getSecond)
            .combineBy(Sums.ofLongs())
            .output();

    // produce the output
    wordCount.persist(new StdoutSink<>(false, "\n"));

    Executor executor = new InMemExecutor();
    executor.submit(flow).get();
  }
}
