/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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

import cz.seznam.euphoria.benchmarks.model.Benchmarks;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.CodeAnalysisMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class BatchTrendsFlink {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: " + BatchTrendsFlink.class + " <config-file>");
      System.exit(1);
    }
    execute(args[0]);
  }

  private static void execute(String configFile) throws Exception {
    Parameters trendsParams = Parameters.fromFile(configFile, Parameters.RequireSection.BATCH);
    Parameters.Batch batchParams = trendsParams.getBatch();

    // set up streaming execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.getConfig().setCodeAnalysisMode(CodeAnalysisMode.OPTIMIZE);

    // create input
    DataSet<Tuple2<Long, String>> input =
        batchParams.getSourceHdfsUri() == null
            ? Util.addTestBatchSource(env)
            : Util.getHdfsSource(env, batchParams.getSourceHdfsUri());

    // long trends sliding
    DataSet<Tuple3<Long, String, Integer>> longTrends = input
        .flatMap(new WindowFlatMap(SlidingEventTimeWindows.of(trendsParams.getLongStats(), trendsParams.getShortStats())))
        .groupBy(0, 1)
        .sum(2);
    
    // short trends sliding
    DataSet<Tuple3<Long, String, Integer>> shortTrends = input
        .flatMap(new WindowFlatMap(TumblingEventTimeWindows.of(trendsParams.getShortStats())))
        .groupBy(0, 1)
        .sum(2);

    // join on term in sliding window, compute score, group by window and find maximum
    DataSet<Tuple3<Long, String, Double>> joined =
        longTrends.join(shortTrends)
            .where(0, 1).equalTo(0, 1)
            .flatMap(new Joiner(trendsParams.getLongStats().toMilliseconds(),
                                trendsParams.getShortStats().toMilliseconds(),
                                trendsParams.getRankSmoothness(),
                                trendsParams.getRankThreshold()))
            .groupBy(0)
            .maxBy(2)
            .setParallelism(1);

    // print results
    DataSet<String> outputs = joined.map(value -> {
        Date now = new Date();
        Date stamp = new Date(value.f0);
        return (now + ": " + stamp + ", " + value.f1 + ", " + value.f2);
      })
      .returns(String.class);

    outputs.writeAsText(Benchmarks.createOutputPath(
        batchParams.getSinkHdfsBaseUri().toString(),
        BatchTrendsFlink.class.getSimpleName()));

    env.execute();
  }

  @RequiredArgsConstructor
  private static class WindowFlatMap
          implements FlatMapFunction<Tuple2<Long, String>, Tuple3<Long, String, Integer>> {
    
    private final WindowAssigner<Object, TimeWindow> windowing;

    @Override
    public void flatMap(Tuple2<Long, String> value,
                        Collector<Tuple3<Long, String, Integer>> out)
        throws Exception {
      // ~ context == null; it just happens so that the windowing instances
      // we're using do not utilize that paremeter at all
      for (TimeWindow w : windowing.assignWindows(value, value.f0, null)) {
        out.collect(Tuple3.of(w.getEnd(), value.f1, 1));
      }
    }
  }

  @RequiredArgsConstructor
  private static class Joiner
          implements FlatMapFunction<
          Tuple2<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>>,
          Tuple3<Long, String, Double>> {
    private final long longInterval;
    private final long shortInterval;
    private final int smooth;
    private final double threshold;

    @Override
    public void flatMap(
            Tuple2<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>> value,
            Collector<Tuple3<Long, String, Double>> out) throws Exception {
      Tuple3<Long, String, Integer> first = value.f0;
      Tuple3<Long, String, Integer> second = value.f1;
      Double score = Benchmarks.trendsRank(longInterval, first.f2, shortInterval, second.f2, smooth);
      if (score > threshold) {
        out.collect(Tuple3.of(first.f0, first.f1, score));
      }
    }
  }
}
