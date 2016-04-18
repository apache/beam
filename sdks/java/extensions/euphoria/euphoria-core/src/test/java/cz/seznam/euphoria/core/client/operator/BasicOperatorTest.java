

package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.MockBatchDataSourceFactory;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.io.TCPLineStreamSource;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.InMemExecutor;
import cz.seznam.euphoria.core.executor.InMemFileSystem;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;

// TODO: Will be moved to euphoria-examples
/**
 * Test basic operator functionality and ability to compile.
 */
public class BasicOperatorTest {

  Executor executor = new InMemExecutor();

  @Test
  public void testWordCountStreamAndBatch() throws Exception {

    InMemFileSystem.get()
        .reset()
        .setFile("/tmp/foo.txt", Duration.ofSeconds(1), Arrays.asList(
          " Anna Pavlovna's drawing room was gradually filling. The highest Petersburg\n",
          "society was assembled there: people differing widely in age and\n",
          "character but alike in the social circle to which they belonged. Prince\n",
          "Vasili's daughter, the beautiful Helene, came to take her father to the\n",
          "ambassador's entertainment; she wore a ball dress and her badge as maid\n",
          "of honor. The youthful little Princess Bolkonskaya, known as la femme la\n",
          " plus seduisante de Petersbourg,* was also there. She had been married\n",
          "during the previous winter, and being pregnant did not go to any large\n",
          "gatherings, but only to small receptions. Prince Vasili's son,\n",
          "Hippolyte, had come with Mortemart, whom he introduced. The Abbe Morio\n",
          "and many others had also come.\n"
        ));

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.inmem",
        InMemFileSystem.Factory.class);
    settings.setClass("euphoria.io.datasink.factory.stdout",
        StdoutSink.Factory.class);

    Flow flow = Flow.create("Test", settings);
    Dataset<String> lines = flow.createInput(URI.create("inmem:///tmp/foo.txt"));

    // expand it to words
    Dataset<Pair<String, Long>> words = FlatMap.of(lines)
        .by((String s, Collector<Pair<String, Long>> c) -> {
          for (String part : s.split(" ")) {
            c.collect(Pair.of(part, 1L));
          }})
        .output();

    // reduce it to counts, use windowing, so the output is batch or stream
    // depending on the type of input
    Dataset<Pair<String, Long>> streamOutput = ReduceByKey
        .of(words)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .combineBy((Iterable<Long> values) -> {
          long s = 0;
          for (Long v : values) {
            s += v;
          }
          return s;
        })
        .windowBy(Windowing.Time.seconds(5).aggregating())
        .output();
    
    streamOutput.persist(URI.create("stdout:///"));

    executor.waitForCompletion(flow);
  }

  @Test
  @Ignore
  public void testWordCountBatch() throws Exception {

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.hdfs",
        MockBatchDataSourceFactory.class);


    Flow flow = Flow.create("Test", settings);
    
    PCollection<String> lines = flow.createBatchInput(new URI("hdfs:///data"));

    // expand it to words
    PCollection<Pair<String, Long>> words = FlatMap.of(lines)
        .by((String s, Collector<Pair<String, Long>> c) -> {
          for (String part : s.split(" ")) {
            c.collect(Pair.of(part, 1L));
          }})
        .output();

    // reduce it to counts, use windowing, so the output is batch or stream
    // depending on the type of input
    PCollection<Pair<String, Long>> output = ReduceByKey
        .of(words)
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

    executor.waitForCompletion(flow);

  }


  @Test
  @Ignore
  public void testDistinctGroupByStream() throws Exception {

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.tcp",
        TCPLineStreamSource.Factory.class);
    settings.setClass("euphoria.io.datasink.factory.stdout",
        StdoutSink.Factory.class);

    Flow flow = Flow.create("Test", settings);

    Dataset<String> lines = flow.createInput(new URI("tcp://localhost:8080"));

    // get rid of empty lines
    Dataset<String> nonempty = Filter.of(lines)
        .by(line -> !line.isEmpty() && line.contains("\t"))
        .output();

    // split lines to pairs
    Dataset<Pair<String, String>> pairs = Map.of(nonempty)
        .by(s -> {
          String[] parts = s.split("\t", 2);
          return Pair.of(parts[0], parts[1]);
        })
        .output();

    // group all lines by the first element
    GroupedDataset<String, String> grouped = GroupByKey.of(pairs)
        .valueBy(Pair::getSecond)
        .keyBy(Pair::getFirst)
        .output();

    // calculate all distinct values within each group
    Dataset<Pair<String, Pair<String, Void>>> reduced = ReduceByKey.of(grouped)
        .valueBy(e -> (Void) null)
        .combineBy(values -> null)
        .windowBy(Windowing.Time.seconds(1).aggregating())
        .output();

    // take distinct tuples
    Dataset<Pair<String, String>> distinct = Map.of(reduced)
        .by(p -> Pair.of(p.getFirst(), p.getSecond().getFirst()))
        .output();

    // calculate the final distinct values per key
    Dataset<Pair<String, Long>> output = CountByKey.of(distinct)
        .by(Pair::getFirst)
        .windowBy(Windowing.Time.seconds(1))
        .output();

    output.persist(URI.create("stdout:///"));

    executor.waitForCompletion(flow);

  }

  @Test
  @Ignore
  public void testJoinOnStreams() throws Exception {
    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.tcp",
        TCPLineStreamSource.Factory.class);
    settings.setClass("euphoria.io.datasink.factory.stdout",
        StdoutSink.Factory.class);

    Flow flow = Flow.create("Test", settings);

    Dataset<String> first = flow.createInput(new URI("tcp://localhost:8080"));
    Dataset<String> second = flow.createInput(new URI("tcp://localhost:8081"));

    UnaryFunctor<String, Pair<String, Integer>> tokv = (s, c) -> {
      String[] parts = s.split("\t", 2);
      if (parts.length == 2) {
        c.collect(Pair.of(parts[0], Integer.valueOf(parts[1])));
      }
    };

    Dataset<Pair<String, Integer>> firstkv = FlatMap.of(first)
        .by(tokv)
        .output();
    Dataset<Pair<String, Integer>> secondkv = FlatMap.of(second)
        .by(tokv)
        .output();

    Dataset<Pair<String, Object>> output = Join.of(firstkv, secondkv)
        .by(Pair::getFirst, Pair::getFirst)
        .using((l, r, c) -> {
          c.collect((l == null ? 0 : l.getSecond()) + (r == null ? 0 : r.getSecond()));
        })
        .windowBy(Windowing.Time.seconds(10))
        .outer()
        .output();

    Map.of(output).by(p -> p.getFirst() + ", " + p.getSecond())
        .output().persist(URI.create("stdout:///"));

    executor.waitForCompletion(flow);

  }

}
