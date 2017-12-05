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
package cz.seznam.euphoria.benchmarks.euphoria.common.trends;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeSliding;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.TopPerKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.core.util.Settings;

import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Calculate traffic in time windows.
 */
public class EuphoriaTrends {

  private final ExecutorFactory executorFactory;
  private final Config config;

  public EuphoriaTrends(File config, ExecutorFactory executorFactory) {
    Config cfg = ConfigFactory.defaultOverrides()
        .withFallback(ConfigFactory.parseFileAnySyntax(
            config, ConfigParseOptions.defaults().setAllowMissing(false)))
        .withFallback(ConfigFactory.defaultReference())
        .resolve();
    this.executorFactory = Objects.requireNonNull(executorFactory);
    this.config = Objects.requireNonNull(cfg);
  }

  public void execute() throws Exception {
    Parameters params = configToParams(this.config);

    Settings settings = executorFactory.newFlowSettings(this.config);

    // ~ handle some set-up specific to repeated runs of this benchmark
    {
      String randomString = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());

      // ~ if we're reading from kafka, make sure we start reading from the beginning
      if (params.getSourceUri() != null
          && "kafka".equals(params.getSourceUri().getScheme().toLowerCase(Locale.ENGLISH))) {
        String cfgPrefix = "_cfg" + randomString;
        String kafkaUri = params.getSourceUri() + "?cfg=" + cfgPrefix;
        params.setSourceUri(URI.create(kafkaUri));
        settings.setString(cfgPrefix + ".group.id", randomString);
        settings.setString(cfgPrefix + ".auto.offset.reset", "earliest");
      }
      // ~ if we're writing the output to hdfs, make sure to create a uniq folder
      if (params.getSinkUri() != null
          && Arrays.asList("hdfs", "file").contains(params.getSinkUri().getScheme().toLowerCase(Locale.ENGLISH))) {
        String s = params.getSinkUri().toString();
        if (s.endsWith("/")) {
          s += randomString;
        } else {
          s += "/" + randomString;
        }
        params.setSinkUri(URI.create(s));
      }
    }

    Flow flow = Flow.create(getClass().getSimpleName(), settings);

    Dataset<Pair<Long, String>> input = Util.getInput(
        params.getSourceUri(),
        params.getEuphoriaSettings(),
        flow);

    Dataset<String> queries = FlatMap.of(input)
        .using((UnaryFunctor<Pair<Long, String>, String>)
            (elem, context) -> context.collect(elem.getSecond()))
        .eventTimeBy(Pair::getFirst)
        .output();

    Dataset<Pair<String, Integer>> longStats =
        ReduceByKey.named("REDUCE-LARGE")
            .of(queries)
            .keyBy(e -> e)
            .valueBy(e -> 1)
            .combineBy(Sums.ofInts())
            .windowBy(TimeSliding.of(params.getLongStats(), params.getShortStats()))
            .output();

    Dataset<Pair<String, Integer>> shortStats =
        ReduceByKey.named("REDUCE-SHORT")
            .of(queries)
            .keyBy(e -> e)
            .valueBy(e -> 1)
            .combineBy(Sums.ofInts())
            .windowBy(Time.of(params.getShortStats()))
            .output();

    long longIntervalMillis = params.getLongStats().toMillis();
    long shortItervalMillis = params.getShortStats().toMillis();
    int rankSmoothness = params.getRankSmoothness();
    double rankThreshold = params.getRankThreshold();
    Dataset<Pair<String, Double>> joined =
        Join.of(longStats, shortStats)
            .by(Pair::getFirst, Pair::getFirst)
            .using((Pair<String, Integer> left,
                    Pair<String, Integer> right,
                    Collector<Double> context) -> {
              double score = rank(
                  longIntervalMillis, left.getSecond(),
                  shortItervalMillis, right.getSecond(),
                  rankSmoothness);
              if (score > rankThreshold) {
                context.collect(score);
              }
            })
            .windowBy(Time.of(params.getShortStats()))
            .output();

    Dataset<Triple<Byte, Pair<String, Double>, Double>> output =
        TopPerKey.of(joined)
            .keyBy(e -> (byte) 0)
            .valueBy(e -> e)
            .scoreBy(Pair::getSecond)
            .output();

    FlatMap.of(output)
        .using((Triple<Byte, Pair<String, Double>, Double> e, Collector<String> c) -> {
          Date now = new Date();
          Date stamp = new Date(TimeSliding.getLabel(c).getEndMillis());
          c.collect(now + ": " + stamp + ", " + e.getSecond().getFirst() + ", " + e.getSecond().getSecond());
        })
        .output()
        .persist(Util.getStringSink(params));

    executorFactory.newExecutor(this.config, dataClasses()).submit(flow).get();
  }

  private Set<Class<?>> dataClasses() {
    HashSet<Class<?>> types = new HashSet<>();
    return types;
  }

  private Parameters configToParams(Config cfg) {
    cfg = cfg.getConfig("trends");
    return Parameters.builder()
        .longStats(Duration.ofMillis(cfg.getDuration("long-stats-duration", TimeUnit.MILLISECONDS)))
        .shortStats(Duration.ofMillis(cfg.getDuration("short-stats-duration", TimeUnit.MILLISECONDS)))
        .rankSmoothness(cfg.getInt("rank-smoothness"))
        .rankThreshold(cfg.getDouble("rank-threshold"))
        .sourceUri(cfg.hasPath("source-uri") ? URI.create(cfg.getString("source-uri")) : null)
        .sinkUri(cfg.hasPath("sink-uri") ? URI.create(cfg.getString("sink-uri")) : null)
        .parallelism(cfg.hasPath("parallelism") ? cfg.getInt("parallelism") : 0)
        .build();
  }

  private static double rank(long longInterval, int longCount,
                             long shortInterval, int shortCount,
                             int smooth) {
    return ((double) shortCount / (longCount + smooth)) * ((double) longInterval / shortInterval);
  }

}
