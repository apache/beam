/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.testutils.jmh;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.openjdk.jmh.annotations.Mode.SingleShotTime;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.testutils.publishing.InfluxDBPublisher;
import org.apache.beam.sdk.testutils.publishing.InfluxDBPublisher.DataPoint;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.BenchmarkResultMetaData;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;

/**
 * Custom main wrapper around the {@link Runner JMH runner} that supports publishing JMH benchmark
 * results to InfluxDB.
 *
 * <h3>Schema</h3>
 *
 * <p>The wrapper writes an aggregated InfluxDB datapoint for each benchmark to <b>measurement</b>
 * {@code {INFLUXDB_BASE_MEASUREMENT}_{mode}}. Typically this is {@code java_jmh_thrpt}.
 *
 * <p>The <b>timestamp</b> of the datapoint corresponds to the start time of the respective
 * benchmark.
 *
 * <p>Individual timeseries are discriminated using the following <b>tags</b> including tags
 * corresponding to additional benchmark parameters in case of parameterized benchmarks:
 *
 * <ul>
 *   <li>{@code benchmark} (string): Fully qualified name of the benchmark
 *   <li>{@code scoreUnit} (string): JMH score unit
 *   <li>optionally, additional parameters in case of a parameterized benchmark (string)
 * </ul>
 *
 * <p>The following fields are captured for each benchmark:
 *
 * <ul>
 *   <li>{@code score} (float): JMH score
 *   <li>{@code scoreMean} (float): Mean score of all iterations
 *   <li>{@code scoreMedian} (float): Median score of all iterations
 *   <li>{@code scoreError} (float): Mean error of the score
 *   <li>{@code sampleCount} (integer): Number of score samples
 *   <li>{@code durationMs} (integer): Total benchmark duration (including warmups)
 * </ul>
 *
 * <h3>Configuration</h3>
 *
 * <p>If {@link InfluxDBSettings} can be inferred from the environment, benchmark results will be
 * published to InfluxDB. Otherwise this will just delegate to the default {@link
 * org.openjdk.jmh.Main JMH Main} class.
 *
 * <p>Use the following environment variables to configure {@link InfluxDBSettings}:
 *
 * <ul>
 *   <li>{@link #INFLUXDB_HOST}
 *   <li>{@link #INFLUXDB_DATABASE}
 *   <li>{@link #INFLUXDB_BASE_MEASUREMENT}
 * </ul>
 */
public class Main {
  private static final String INFLUXDB_HOST = "INFLUXDB_HOST";
  private static final String INFLUXDB_DATABASE = "INFLUXDB_DATABASE";
  private static final String INFLUXDB_BASE_MEASUREMENT = "INFLUXDB_BASE_MEASUREMENT";

  public static void main(String[] args)
      throws CommandLineOptionException, IOException, RunnerException {
    final CommandLineOptions opts = new CommandLineOptions(args);
    final InfluxDBSettings influxDB = influxDBSettings();

    if (influxDB == null
        || isSingleShotTimeOnly(opts.getBenchModes())
        || opts.shouldHelp()
        || opts.shouldList()
        || opts.shouldListWithParams()
        || opts.shouldListProfilers()
        || opts.shouldListResultFormats()) {
      // delegate to JMH runner
      org.openjdk.jmh.Main.main(args);
      return;
    }

    final Runner runner = new Runner(opts);
    final Collection<RunResult> results = runner.run();

    final Collection<DataPoint> dataPoints =
        results.stream()
            .filter(r -> r.getParams().getMode() != SingleShotTime)
            .map(r -> dataPoint(influxDB.measurement, r))
            .collect(toList());

    InfluxDBPublisher.publish(influxDB, dataPoints);
  }

  private static boolean isSingleShotTimeOnly(Collection<Mode> modes) {
    return !modes.isEmpty() && modes.stream().allMatch(SingleShotTime::equals);
  }

  private static DataPoint dataPoint(String baseMeasurement, RunResult run) {
    final BenchmarkParams params = run.getParams();
    final Result<?> result = run.getPrimaryResult();

    final long startTimeMs =
        metaDataStream(run).mapToLong(BenchmarkResultMetaData::getStartTime).min().getAsLong();
    final long stopTimeMs =
        metaDataStream(run).mapToLong(BenchmarkResultMetaData::getStopTime).max().getAsLong();

    final String measurement =
        String.format("%s_%s", baseMeasurement, params.getMode().shortLabel());

    final Map<String, String> tags = new HashMap<>();
    tags.put("benchmark", params.getBenchmark());
    tags.put("scoreUnit", result.getScoreUnit());
    // add params of parameterized benchmarks as tags
    tags.putAll(params.getParamsKeys().stream().collect(toMap(identity(), params::getParam)));

    final Map<String, Number> fields = new HashMap<>();
    fields.put("score", result.getScore());
    fields.put("scoreMean", result.getStatistics().getMean());
    fields.put("scoreMedian", result.getStatistics().getPercentile(0.5));
    if (!Double.isNaN(result.getScoreError())) {
      fields.put("scoreError", result.getScoreError());
    }
    fields.put("sampleCount", result.getSampleCount());
    fields.put("durationMs", stopTimeMs - startTimeMs);

    return InfluxDBPublisher.dataPoint(
        measurement, tags, fields, startTimeMs, TimeUnit.MILLISECONDS);
  }

  private static Stream<BenchmarkResultMetaData> metaDataStream(RunResult runResult) {
    return runResult.getBenchmarkResults().stream()
        .map(BenchmarkResult::getMetadata)
        .filter(Objects::nonNull);
  }

  /** Construct InfluxDB settings from environment variables to not mess with JMH args. */
  private static @Nullable InfluxDBSettings influxDBSettings() {
    String host = System.getenv(INFLUXDB_HOST);
    String database = System.getenv(INFLUXDB_DATABASE);
    String measurement = System.getenv(INFLUXDB_BASE_MEASUREMENT);
    if (measurement == null || database == null) {
      return null;
    }

    InfluxDBSettings.Builder builder = InfluxDBSettings.builder();
    if (host != null) {
      builder.withHost(host); // default to localhost otherwise
    }
    return builder.withDatabase(database).withMeasurement(measurement).get();
  }
}
