package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.InMemExecutor;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.net.URI;

public class ExerciseHadoopInputSource {

  public static void main(String[] args) throws Exception {
    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.hdfs",
            HadoopTextFileSourceFactory.class);
    settings.setClass("euphoria.io.datasink.factory.stdout",
            StdoutSink.Factory.class);

    Flow flow = Flow.create("Test", settings);

    // set-up our input source (a stream)
    PCollection<Pair<LongWritable, Text>> lines = flow.createBatchInput(
            URI.create("hdfs://gin.dev/user/fulltext/shakespeare.txt"));

    PCollection<Pair<String, Long>> tuples = FlatMap.of(lines)
            .by((Pair<LongWritable, Text> line, Collector<Pair<String, Long>> out) -> {
              for (String w : line.getSecond().toString().split(" ")) {
                out.collect(Pair.of(w, 1L));
              }
            }).output();

    // reduce it to counts, use windowing
    PCollection<Pair<String, Long>> wordCount = ReduceByKey
            .of(tuples)
            .keyBy(Pair::getFirst)
            .valueBy(Pair::getSecond)
            .combineBy((Iterable<Long> values) -> {
              long s = 0;
              for (Long v : values) {
                s += v;
              }
              return s;
            })
            .output();

    // produce the output
    wordCount.persist(URI.create("stdout:///"));

    Executor executor = new InMemExecutor();
    executor.waitForCompletion(flow);
  }
}
