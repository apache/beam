package cz.seznam.euphoria.hadoop;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.InMemExecutor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.input.HadoopTextFileSourceFactory;
import cz.seznam.euphoria.hadoop.output.HadoopTextFileSinkFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.net.URI;

public class ExerciseHadoopIO {

  public static void main(String[] args) throws Exception {
    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.webhdfs",
            HadoopTextFileSourceFactory.class);
    settings.setClass("euphoria.io.datasink.factory.stdout",
            StdoutSink.Factory.class);
    settings.setClass("euphoria.io.datasink.factory.hdfs",
            HadoopTextFileSinkFactory.class);

    Flow flow = Flow.create("Test", settings);

    // set-up our input source (a stream)
    Dataset<Pair<LongWritable, Text>> lines = flow.createInput(
            URI.create("webhdfs://gin.dev/user/fulltext/shakespeare.txt"));

    Dataset<Pair<String, Long>> tuples = FlatMap.of(lines)
            .using((Pair<LongWritable, Text> line, Collector<Pair<String, Long>> out) -> {
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
    wordCount.persist(URI.create("stdout:///"));

    // write output to HDFS
    //wordCount.persist(URI.create("hdfs://gin.dev/tmp/euphoria-test"));


    Executor executor = new InMemExecutor();
    executor.waitForCompletion(flow);
  }
}
