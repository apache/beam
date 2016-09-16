
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeSliding;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.streaming.io.DataSinkWrapper;
import cz.seznam.euphoria.flink.streaming.io.DataSourceWrapper;
import cz.seznam.euphoria.flink.streaming.windowing.EmissionWindow;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindow;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.shaded.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test suite for {@code StreamWindower}.
 */
public class StreamWindowerTest {

  StreamWindower windower = new StreamWindower();
  LocalStreamEnvironment env;

  @Before
  public void setUp() {
    env = StreamExecutionEnvironment.createLocalEnvironment(1);
  }

  @Test
  public void testTimeSlidingWindowing() throws Exception {

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStream<StreamingWindowedElement<?, ?, Pair<Integer, Integer>>> input
        = env.addSource(new DataSourceWrapper(ListDataSource.unbounded(
            Arrays.asList(Pair.of(1, 1), Pair.of(1, 2), Pair.of(2, 3), Pair.of(3, 4)),
            Arrays.asList(Pair.of(1, 8), Pair.of(1, 7), Pair.of(2, 6), Pair.of(3, 5)))
    ));

    WindowedStream<StreamingWindowedElement<Void, TimeInterval, Pair<Integer, Integer>>, Integer, EmissionWindow<FlinkWindow>> windowed;
    windowed = windower.genericWindow(
        input, (Pair<Integer, Integer> i) -> i.getSecond() % 2,
        (Pair<Integer, Integer> i) -> 2 * i.getSecond(),
        TimeSliding.of(Duration.ofSeconds(2), Duration.ofSeconds(1))
            .using((Pair<Integer, Integer> p) -> 1000L * p.getFirst()));

    DataStream<StreamingWindowedElement<Void, TimeInterval, Pair<Integer, Integer>>> reduced;
    reduced = windowed.reduce(
        new ReduceFunction<StreamingWindowedElement<Void, TimeInterval, Pair<Integer, Integer>>>() {
          @Override
          public StreamingWindowedElement<Void, TimeInterval, Pair<Integer, Integer>>
              reduce(StreamingWindowedElement<Void, TimeInterval, Pair<Integer, Integer>> a,
                     StreamingWindowedElement<Void, TimeInterval, Pair<Integer, Integer>> b) {
                return new StreamingWindowedElement(a.getWindowID(), Pair.of(
                    a.get().getFirst(),
                    a.get().getSecond() + b.get().getSecond()));
              }
        });

    reduced = reduced.map(
        new MapFunction<
            StreamingWindowedElement<Void, TimeInterval, Pair<Integer, Integer>>,
            StreamingWindowedElement<Void, TimeInterval, Pair<Integer, Integer>>>() {
      @Override
      public StreamingWindowedElement<Void, TimeInterval, Pair<Integer, Integer>> map(
          StreamingWindowedElement<Void, TimeInterval, Pair<Integer, Integer>> t) throws Exception {
        return new StreamingWindowedElement<>(t.getWindowID(),
            WindowedPair.of(t.getWindowID().getLabel(),
                t.get().getFirst(), t.get().getSecond()));
      }});

    reduced.addSink(new DataSinkWrapper<>(new StdoutSink(false, "\n")));

    ListDataSink<WindowedPair<TimeInterval, Integer, Integer>> sink = ListDataSink.get(1);

    reduced
        .addSink(new DataSinkWrapper(sink));


    JobExecutionResult result = env.execute();
    assertTrue(result.isJobExecutionResult());
    assertEquals(sink.getOutputs().size(), 1);

    SortedMap<TimeInterval, Set<Pair<Integer, Integer>>> results =
        new TreeMap<>((a, b) -> Long.compare(a.getStartMillis(), b.getStartMillis()));
    toResultMap(sink, results);

    assertEquals("We should have 4 emitted windows", 4, results.size());

    // first window: [1, 2, 8, 7] -> [
    //                (1, 2), (0, 4), (0, 16), (1, 14)
    //               ]
    // second window: [1, 2, 8, 7, 3, 6] -> [
    //                (1, 2), (0, 4), (0, 16), (1, 14), (1, 6), (0, 12)
    //               ]
    // third window: [3, 6, 4, 5] -> [
    //                (1, 6), (0, 12), (0, 8), (1, 10)
    //               ]
    // fourth window: [4, 5} -> [
    //                (0, 8), (1, 10)
    //               ]

    Iterator<Map.Entry<TimeInterval , Set<Pair<Integer, Integer>>>> iterator
        = results.entrySet().iterator();
    Set<Pair<Integer, Integer>> first = iterator.next().getValue();
    Set<Pair<Integer, Integer>> second = iterator.next().getValue();
    Set<Pair<Integer, Integer>> third = iterator.next().getValue();
    Set<Pair<Integer, Integer>> fourth = iterator.next().getValue();

    assertEquals(Sets.newHashSet(Pair.of(1, 16), Pair.of(0, 20)), first);
    assertEquals(Sets.newHashSet(Pair.of(1, 22), Pair.of(0, 32)), second);
    assertEquals(Sets.newHashSet(Pair.of(1, 16), Pair.of(0, 20)), third);
    assertEquals(Sets.newHashSet(Pair.of(1, 10), Pair.of(0, 8)), fourth);

  }

  private <LABEL> void toResultMap(
      ListDataSink<WindowedPair<LABEL, Integer, Integer>> sink,
      SortedMap<LABEL, Set<Pair<Integer, Integer>>> results) {

    sink.getOutput(0).stream().forEach(p -> {
      Set<Pair<Integer, Integer>> list = results.get(p.getWindowLabel());
      if (list == null) {
        list = new HashSet<>();
        results.put(p.getWindowLabel(), list);
      }
      list.add(p);
    });
  }

  private <LABEL> SingleOutputStreamOperator<WindowedPair<LABEL, Integer, Integer>> sumReduce(
      WindowedStream<WindowedPair<LABEL, Integer, Integer>, Integer, FlinkWindow> windowed)
  {
    SingleOutputStreamOperator<WindowedPair<LABEL, Integer, Integer>> reduce;
    reduce = windowed.reduce(
        new ReduceFunction<WindowedPair<LABEL, Integer, Integer>>() {
          @Override
          public WindowedPair<LABEL, Integer, Integer> reduce(
              WindowedPair<LABEL, Integer, Integer> a,
              WindowedPair<LABEL, Integer, Integer> b) throws Exception {
            return WindowedPair.of(a.getWindowLabel(), a.getKey(), a.getValue() + b.getValue());
          }
        });
    return reduce;
  }


}
