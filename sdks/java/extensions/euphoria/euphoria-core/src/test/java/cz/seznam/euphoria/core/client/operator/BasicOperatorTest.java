

package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.io.MockBatchDataSourceFactory;
import cz.seznam.euphoria.core.client.io.TCPLineStreamSource;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.InMemExecutor;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.util.Settings;
import java.io.IOException;
import java.net.URI;

import org.junit.Ignore;
import org.junit.Test;

// TODO: Will be moved to euphoria-examples
/**
 * Test basic operator functionality and ability to compile.
 */
public class BasicOperatorTest {

  Executor executor = new InMemExecutor();

  @Test
  @Ignore
  public void testWordCountStreamAndBatch() throws Exception {

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.tcp",
        TCPLineStreamSource.Factory.class);

    Flow flow = Flow.create("Test", settings);
    // let's pretend we have this, this dataset might be stream or batch
    // (don't know and don't care)
    Dataset<String> lines = flow.createInput(new URI("tcp://localhost:8080"));
    
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
    
    streamOutput.persist(getSink());

    executor.waitForCompletion(flow);
        
  }

  @Test
  @Ignore
  public void testWordCountBatch() throws Exception {

    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.hdfs",
        MockBatchDataSourceFactory.class);


    Flow flow = Flow.create("Test", settings);
    // let's pretend we have this, this dataset might be stream or batch
    // (don't know and don't care)
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
    PCollection<Pair<String, Long>> streamOutput = ReduceByKey
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

    Flow flow = Flow.create("Test", settings);
    // let's pretend we have this, this dataset might be stream or batch
    // (don't know and don't care)
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

    
    output.persist(getSink());


    executor.waitForCompletion(flow);

  }


  private <T> DataSink<T> getSink() {
    return new DataSink<T>() {
      @Override
      public Writer<T> openWriter(int partitionId) {
        return new Writer<T>() {
          @Override
          public void write(T elem) {
            System.out.println(elem.toString());
          }

          @Override
          public void commit() throws IOException {
          }
        };
      }

      @Override
      public void commit() throws IOException {
      }

      @Override
      public void rollback() {
      }
    };
  }


}
