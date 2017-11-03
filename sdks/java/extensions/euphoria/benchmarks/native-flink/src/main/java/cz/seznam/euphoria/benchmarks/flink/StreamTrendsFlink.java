/**
 * Copyright 2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.benchmarks.flink;

import cz.seznam.euphoria.benchmarks.datamodel.Benchmarks;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.CodeAnalysisMode;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Iterator;

public class StreamTrendsFlink {
  
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: " + StreamTrendsFlink.class + " <config-file>");
      System.exit(1);
    }
    execute(args[0]);
  }

  private static void execute(String configFile) throws Exception {
    Parameters trendsParam = Parameters.fromFile(configFile, Parameters.RequireSection.STREAM);
    Parameters.Stream streamParams = trendsParam.getStream();

    // set up streaming execution environment
    StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    // configure event-time characteristics
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // generate a Watermark every ... second
    env.getConfig().setAutoWatermarkInterval(streamParams.getWatermarkInterval().toMilliseconds());
    env.getConfig().enableObjectReuse();
    env.getConfig().setCodeAnalysisMode(CodeAnalysisMode.OPTIMIZE);

    if (streamParams.getRocksDbCheckpoint() != null) {
      env.setStateBackend(new RocksDBStateBackend(streamParams.getRocksDbCheckpoint()));
    }

    // create Kafka consumer data source
    DataStream<Tuple2<Long, String>> input =
        streamParams.getSource() == null
            ? Util.addTestStreamSource(env)
            : Util.addKafkaSource(env,
                                  streamParams.getSource().getKafkaBrokers(),
                                  streamParams.getSource().getKafkaTopic());

    DataStream<Tuple2<String, Integer>> prepared = input
            // set event time
            .assignTimestampsAndWatermarks(new Util.TimeAssigner<>(streamParams.getAllowedLateness()))
            // map to word -> count
            .map(t -> Tuple2.of(t.f1, 1))
            // flink type hint
            .returns(new TypeHint<Tuple2<String, Integer>>(){});

    // long trends sliding
    DataStream<Tuple2<String, Integer>> longTrends = prepared
            .keyBy(0)
            .timeWindow(trendsParam.getLongStats(), trendsParam.getShortStats())
            .sum(1);

    // short trends sliding
    DataStream<Tuple2<String, Integer>> shortTrends = prepared
            .keyBy(0)
            .timeWindow(trendsParam.getShortStats())
            .sum(1);

    // join on term in sliding window and compute score
    DataStream<Tuple2<String, Double>> joined = longTrends.join(shortTrends)
            .where(new FirstFieldSelector())
            .equalTo(new FirstFieldSelector())
            .window(TumblingEventTimeWindows.of(trendsParam.getShortStats()))
            .apply(new Joiner(trendsParam.getLongStats().toMilliseconds(),
                            trendsParam.getShortStats().toMilliseconds(),
                            trendsParam.getRankSmoothness(),
                            trendsParam.getRankThreshold()),
                    TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}));

    // compute max from joined results in the sliding window
    DataStream<Tuple3<Long, String, Double>> top = joined
            .timeWindowAll(trendsParam.getShortStats())
            .apply(new TopAllWindow())
            .returns(new TypeHint<Tuple3<Long, String, Double>>(){});

    // print results
    top.addSink(value -> {
      Date now = new Date();
      Date stamp = new Date(value.f0);
      System.out.println(now + ": " + stamp + ", " + value.f1 + ", " + value.f2);
    }).setParallelism(1);

    env.execute();
  }

  private static class FirstFieldSelector implements KeySelector<Tuple2<String, Integer>, String> {
    @Override
    public String getKey(Tuple2<String, Integer> value) throws Exception {
      return value.f0;
    }
  }

  @RequiredArgsConstructor
  private static class TopAllWindow implements AllWindowFunction<Tuple2<String, Double>, Tuple3<Long, String, Double>, TimeWindow> {
    @Override
    public void apply(TimeWindow window, Iterable<Tuple2<String, Double>> values,
                      Collector<Tuple3<Long, String, Double>> out) throws Exception {
      Iterator<Tuple2<String, Double>> iter = values.iterator();
      Tuple2<String, Double> next = iter.next();
      Tuple2<String, Double> max = Tuple2.of(next.f0, next.f1);
      while (iter.hasNext()) {
        next = iter.next();
        if (next.f1 > max.f1) {
          max.f0 = next.f0;
          max.f1 = next.f1;
        }
      }
      out.collect(Tuple3.of(window.getEnd(), max.f0, max.f1));
    }
  }

  @RequiredArgsConstructor
  private static class Joiner 
  implements FlatJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Double>> {
    
    private final long longInterval;
    private final long shortInterval;
    private final int smooth;
    private final double threshold;
    
    @Override
    public void join(Tuple2<String, Integer> first, Tuple2<String, Integer> second,
        Collector<Tuple2<String, Double>> out) throws Exception {
      Double score = Benchmarks.trendsRank(longInterval, first.f1, shortInterval, second.f1, smooth);
      if (score > threshold) {
        out.collect(Tuple2.of(first.f0, score));
      }
    }
  }
}
