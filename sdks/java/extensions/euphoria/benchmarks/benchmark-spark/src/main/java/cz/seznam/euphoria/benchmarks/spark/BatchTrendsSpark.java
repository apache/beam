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
package cz.seznam.euphoria.benchmarks.spark;


import cz.seznam.euphoria.shadow.com.google.common.collect.Iterables;
import cz.seznam.euphoria.benchmarks.model.Benchmarks;
import cz.seznam.euphoria.benchmarks.model.windowing.Time;
import cz.seznam.euphoria.benchmarks.model.windowing.TimeSliding;
import cz.seznam.euphoria.benchmarks.model.windowing.Windowing;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import scala.Tuple2;

import java.io.IOException;
import java.time.Duration;
import java.util.Date;
import java.util.List;

public class BatchTrendsSpark {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: Stream <properties-file-path>");
      System.exit(1);
    }
    Parameters params = Parameters.fromFile(args[0]);
    BatchTrendsSpark trendsSparkRDD = new BatchTrendsSpark();
    trendsSparkRDD.run(params);
  }

  public void run(Parameters params) throws IOException {
    Duration longInterval = params.getLongStats();
    Duration shortInterval = params.getShortStats();
    int smooth = params.getRankSmoothness();
    double threshold = params.getRankThreshold();
    
    SparkConf conf = new SparkConf();
    conf.setAppName(getClass().getSimpleName());
    conf.set("spark.serializer", KryoSerializer.class.getName());

    conf.registerKryoClasses(new Class[] {
            Pair.class
    });

    boolean local = params.getBatch().getSourceHdfsUri() == null;
    if (local) {
      conf.setMaster("local[3]").setAppName("LocalSparkTrends");
    }
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Pair<Long, String>> input =
        local
            ? Util.getTestInput(sc)
            : Util.getHdfsSource(sc, params.getBatch().getSourceHdfsUri());

    // define windowing
    Windowing longWindowing = TimeSliding.of(longInterval, shortInterval);
    Windowing shortWindowing = Time.of(shortInterval);

    // ~ assign elements to windows - only window end timestamp is important
    // to join big and small windows
    JavaPairRDD<Pair<Long, String>, Long> longInput = input.flatMapToPair(p -> {
      // assign windows
      List<Long> windows = longWindowing.generate(p.getFirst());
      return windows.stream()
             .map(i -> new Tuple2<>(
                     Pair.of(i, p.getSecond()), 1L))
             .iterator();
    });

    JavaPairRDD<Pair<Long, String>, Long> shortInput = input.mapToPair(p -> {
      List<Long> windows = shortWindowing.generate(p.getFirst());
      Long i = Iterables.getOnlyElement(windows);
      return new Tuple2<>(Pair.of(i, p.getSecond()), 1L);
    });

    // SUM & JOIN
    JavaPairRDD<Pair<Long, String>, Long> longStats = longInput.reduceByKey(Long::sum);
    JavaPairRDD<Pair<Long, String>, Long> shortStats = shortInput.reduceByKey(Long::sum);
    JavaPairRDD<Pair<Long, String>, Tuple2<Long, Long>> joined = shortStats.join(longStats);

    // RANK
    JavaPairRDD<Pair<Long, String>, Double> ranked = joined.mapToPair(t -> {
      int shortWindowCnt = t._2()._1().intValue();
      int longWindowCnt = t._2()._2().intValue();
      double rank = Benchmarks.trendsRank(longInterval.toMillis(), longWindowCnt, shortInterval.toMillis(), shortWindowCnt, smooth);
      return new Tuple2<>(t._1(), rank);
    }).filter(t -> t._2() > threshold);

    
    JavaPairRDD<Long, Tuple2<String, Double>> byWindow = ranked.mapToPair(t -> {
      return new Tuple2<>(t._1().getFirst(), new Tuple2<>(t._1().getSecond(), t._2()));
    });
    
    JavaPairRDD<Long, Tuple2<String, Double>> maxByWindow = 
        byWindow.reduceByKey((t1, t2) -> t1._2() > t2._2() ? t1 : t2, 1);
    
    // format output
    JavaRDD<String> formatted = maxByWindow.map(t -> {
      String key = t._2()._1();
      Double rank = t._2()._2();
      Date now = new Date();
      Date stamp = new Date(t._1());
      return (now + ": " + stamp + ", " + key + ", " + rank);
    });

    formatted.saveAsTextFile(Benchmarks.createOutputPath(
        params.getBatch().getSinkHdfsBaseUri().toString(), 
        BatchTrendsSpark.class.getSimpleName()));
    
    sc.close();
  }
}
