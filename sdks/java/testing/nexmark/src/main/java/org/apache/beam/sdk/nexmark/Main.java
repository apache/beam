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
package org.apache.beam.sdk.nexmark;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.beam.sdk.nexmark.NexmarkUtils.processingMode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testutils.publishing.BigQueryResultsPublisher;
import org.apache.beam.sdk.testutils.publishing.InfluxDBPublisher;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An implementation of the 'NEXMark queries' for Beam. These are multiple queries over a three
 * table schema representing an online auction system:
 *
 * <ul>
 *   <li>{@link Person} represents a person submitting an item for auction and/or making a bid on an
 *       auction.
 *   <li>{@link Auction} represents an item under auction.
 *   <li>{@link Bid} represents a bid for an item under auction.
 * </ul>
 *
 * The queries exercise many aspects of the Beam model.
 *
 * <p>We synthesize the creation of people, auctions and bids in real-time. The data is not
 * particularly sensible.
 *
 * <p>See <a
 * href="https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/">
 * Nexmark website</a>
 */
public class Main {

  private static class Result {
    private final NexmarkConfiguration configuration;
    private final NexmarkPerf perf;

    private Result(NexmarkConfiguration configuration, NexmarkPerf perf) {
      this.configuration = configuration;
      this.perf = perf;
    }
  }

  private static class Run implements Callable<Result> {
    private final NexmarkLauncher<NexmarkOptions> nexmarkLauncher;
    private final NexmarkConfiguration configuration;

    private Run(NexmarkOptions options, NexmarkConfiguration configuration) {
      this.nexmarkLauncher = new NexmarkLauncher<>(options, configuration);
      this.configuration = configuration;
    }

    @Override
    public Result call() throws IOException {
      NexmarkPerf perf = nexmarkLauncher.run();
      return new Result(configuration, perf);
    }
  }

  /** Entry point. */
  void runAll(String[] args) throws IOException {
    NexmarkOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(NexmarkOptions.class);

    Instant start = Instant.now();
    Map<NexmarkConfiguration, NexmarkPerf> baseline = loadBaseline(options.getBaselineFilename());
    Map<NexmarkConfiguration, NexmarkPerf> actual = new LinkedHashMap<>();
    Set<NexmarkConfiguration> configurations = options.getSuite().getConfigurations(options);
    int nThreads = Math.min(options.getNexmarkParallel(), configurations.size());
    ExecutorService executor = Executors.newFixedThreadPool(nThreads);
    CompletionService<Result> completion = new ExecutorCompletionService(executor);

    boolean successful = true;
    try {
      // Schedule all the configurations.
      for (NexmarkConfiguration configuration : configurations) {
        NexmarkOptions optionsCopy = PipelineOptionsFactory.fromArgs(args).as(NexmarkOptions.class);
        completion.submit(new Run(optionsCopy, configuration));
      }

      // Collect all the results.
      for (int scheduled = configurations.size(); scheduled > 0; scheduled--) {
        Result result;
        try {
          result = completion.take().get();
        } catch (InterruptedException e) {
          break;
        } catch (ExecutionException e) {
          Throwable t = e.getCause();
          if (t instanceof IOException) {
            throw new IOException(t);
          } else {
            throw new RuntimeException(t);
          }
        }

        NexmarkConfiguration configuration = result.configuration;
        NexmarkPerf perf = result.perf;
        if (perf == null) {
          continue;
        } else if (perf.errors == null || perf.errors.size() > 0) {
          successful = false;
        }
        appendPerf(options.getPerfFilename(), configuration, perf);
        actual.put(configuration, perf);
        // Summarize what we've run so far.
        saveSummary(null, configurations, actual, baseline, start, options);
      }

      final ImmutableMap<String, String> schema =
          ImmutableMap.<String, String>builder()
              .put("timestamp", "timestamp")
              .put("runtimeSec", "float")
              .put("eventsPerSec", "float")
              .put("numResults", "integer")
              .build();

      if (options.getExportSummaryToBigQuery()) {
        savePerfsToBigQuery(
            BigQueryResultsPublisher.create(options.getBigQueryDataset(), schema),
            options,
            actual,
            start);
      }

      if (options.getExportSummaryToInfluxDB()) {
        final long timestamp = start.getMillis() / 1000; // seconds
        savePerfsToInfluxDB(options, schema, actual, timestamp);
      }

    } finally {
      if (options.getMonitorJobs()) {
        // Report overall performance.
        saveSummary(options.getSummaryFilename(), configurations, actual, baseline, start, options);
        saveJavascript(options.getJavascriptFilename(), configurations, actual, baseline, start);
      }

      executor.shutdown();
    }
    if (!successful) {
      throw new RuntimeException("Execution was not successful");
    }
  }

  @VisibleForTesting
  static void savePerfsToBigQuery(
      BigQueryResultsPublisher publisher,
      NexmarkOptions options,
      Map<NexmarkConfiguration, NexmarkPerf> perfs,
      Instant start) {

    for (Map.Entry<NexmarkConfiguration, NexmarkPerf> entry : perfs.entrySet()) {
      String queryName =
          NexmarkUtils.fullQueryName(
              options.getQueryLanguage(), entry.getKey().query.getNumberOrName());
      String tableName = NexmarkUtils.tableName(options, queryName, 0L, null);

      publisher.publish(entry.getValue(), tableName, start.getMillis());
    }
  }

  private static void savePerfsToInfluxDB(
      final NexmarkOptions options,
      final Map<String, String> schema,
      final Map<NexmarkConfiguration, NexmarkPerf> results,
      final long timestamp) {
    final InfluxDBSettings settings = getInfluxSettings(options);
    final String runner = options.getRunner().getSimpleName();
    final List<Map<String, Object>> schemaResults =
        results.entrySet().stream()
            .map(
                entry ->
                    getResultsFromSchema(
                        entry.getValue(),
                        schema,
                        timestamp,
                        runner,
                        produceMeasurement(options, entry)))
            .collect(toList());
    InfluxDBPublisher.publishNexmarkResults(schemaResults, settings);
  }

  private static InfluxDBSettings getInfluxSettings(final NexmarkOptions options) {
    return InfluxDBSettings.builder()
        .withHost(options.getInfluxHost())
        .withDatabase(options.getInfluxDatabase())
        .withMeasurement(options.getBaseInfluxMeasurement())
        .withRetentionPolicy(options.getInfluxRetentionPolicy())
        .get();
  }

  private static String produceMeasurement(
      final NexmarkOptions options, Map.Entry<NexmarkConfiguration, NexmarkPerf> entry) {
    final String queryName =
        NexmarkUtils.fullQueryName(
            options.getQueryLanguage(), entry.getKey().query.getNumberOrName());
    return String.format(
        "%s_%s_%s",
        options.getBaseInfluxMeasurement(), queryName, processingMode(options.isStreaming()));
  }

  private static Map<String, Object> getResultsFromSchema(
      final NexmarkPerf results,
      final Map<String, String> schema,
      final long timestamp,
      final String runner,
      final String measurement) {
    final Map<String, Object> schemaResults =
        results.toMap().entrySet().stream()
            .filter(element -> schema.containsKey(element.getKey()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    final int runtimeMs =
        (int) ((double) schemaResults.get("runtimeSec") * 1000); // change sec to ms
    schemaResults.put("timestamp", timestamp);
    schemaResults.put("runner", runner);
    schemaResults.put("measurement", measurement);

    // By default, InfluxDB treats all number values as floats. We need to add 'i' suffix to
    // interpret the value as an integer.
    schemaResults.put("runtimeMs", runtimeMs + "i");
    schemaResults.put("numResults", schemaResults.get("numResults") + "i");

    return schemaResults;
  }

  /** Append the pair of {@code configuration} and {@code perf} to perf file. */
  private void appendPerf(
      @Nullable String perfFilename, NexmarkConfiguration configuration, NexmarkPerf perf) {
    if (perfFilename == null) {
      return;
    }
    List<String> lines = new ArrayList<>();
    lines.add("");
    lines.add(String.format("# %s", Instant.now()));
    lines.add(String.format("# %s", configuration.toShortString()));
    lines.add(configuration.toString());
    lines.add(perf.toString());
    try {
      Files.write(
          Paths.get(perfFilename),
          lines,
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new RuntimeException("Unable to write perf file: ", e);
    }
    NexmarkUtils.console("appended results to perf file %s.", perfFilename);
  }

  /** Load the baseline perf. */
  private static @Nullable Map<NexmarkConfiguration, NexmarkPerf> loadBaseline(
      @Nullable String baselineFilename) {
    if (baselineFilename == null) {
      return null;
    }
    Map<NexmarkConfiguration, NexmarkPerf> baseline = new LinkedHashMap<>();
    List<String> lines;
    try {
      lines = Files.readAllLines(Paths.get(baselineFilename), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read baseline perf file: ", e);
    }
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).startsWith("#") || lines.get(i).trim().isEmpty()) {
        continue;
      }
      NexmarkConfiguration configuration = NexmarkConfiguration.fromString(lines.get(i++));
      NexmarkPerf perf = NexmarkPerf.fromString(lines.get(i));
      baseline.put(configuration, perf);
    }
    NexmarkUtils.console(
        "loaded %d entries from baseline file %s.", baseline.size(), baselineFilename);
    return baseline;
  }

  private static final String LINE =
      "==========================================================================================";

  /** Print summary of {@code actual} vs (if non-null) {@code baseline}. */
  private static void saveSummary(
      @Nullable String summaryFilename,
      Iterable<NexmarkConfiguration> configurations,
      Map<NexmarkConfiguration, NexmarkPerf> actual,
      @Nullable Map<NexmarkConfiguration, NexmarkPerf> baseline,
      Instant start,
      NexmarkOptions options) {

    List<String> lines = new ArrayList<>();

    lines.add("");
    lines.add(LINE);

    lines.add(
        String.format("Run started %s and ran for %s", start, new Duration(start, Instant.now())));
    lines.add("");

    lines.add("Default configuration:");
    lines.add(NexmarkConfiguration.DEFAULT.toString());
    lines.add("");

    lines.add("Configurations:");
    lines.add("  Conf  Description");
    int conf = 0;
    for (NexmarkConfiguration configuration : configurations) {
      lines.add(String.format("  %04d  %s", conf++, configuration.toShortString()));
      NexmarkPerf actualPerf = actual.get(configuration);
      if (actualPerf != null && actualPerf.jobId != null) {
        lines.add(String.format("  %4s  [Ran as job %s]", "", actualPerf.jobId));
      }
    }

    lines.add("");
    lines.add("Performance:");
    lines.add(
        String.format(
            "  %4s  %12s  %12s  %12s  %12s  %12s  %12s",
            "Conf",
            "Runtime(sec)",
            "(Baseline)",
            "Events(/sec)",
            "(Baseline)",
            "Results",
            "(Baseline)"));
    conf = 0;
    for (NexmarkConfiguration configuration : configurations) {
      String line = String.format("  %04d  ", conf++);
      NexmarkPerf actualPerf = actual.get(configuration);
      if (actualPerf == null) {
        line += "*** not run ***";
      } else {
        NexmarkPerf baselinePerf = baseline == null ? null : baseline.get(configuration);
        double runtimeSec = actualPerf.runtimeSec;
        line += String.format("%12.1f  ", runtimeSec);
        if (baselinePerf == null) {
          line += String.format("%12s  ", "");
        } else {
          double baselineRuntimeSec = baselinePerf.runtimeSec;
          double diff = ((runtimeSec - baselineRuntimeSec) / baselineRuntimeSec) * 100.0;
          line += String.format("%+11.2f%%  ", diff);
        }

        double eventsPerSec = actualPerf.eventsPerSec;
        line += String.format("%12.1f  ", eventsPerSec);
        if (baselinePerf == null) {
          line += String.format("%12s  ", "");
        } else {
          double baselineEventsPerSec = baselinePerf.eventsPerSec;
          double diff = ((eventsPerSec - baselineEventsPerSec) / baselineEventsPerSec) * 100.0;
          line += String.format("%+11.2f%%  ", diff);
        }

        long numResults = actualPerf.numResults;
        line += String.format("%12d  ", numResults);
        if (baselinePerf == null) {
          line += String.format("%12s", "");
        } else {
          long baselineNumResults = baselinePerf.numResults;
          long diff = numResults - baselineNumResults;
          line += String.format("%+12d", diff);
        }
      }
      lines.add(line);

      if (actualPerf != null) {
        List<String> errors = actualPerf.errors;
        if (errors == null) {
          errors = new ArrayList<>();
          errors.add("NexmarkGoogleRunner returned null errors list");
        }
        for (String error : errors) {
          lines.add(String.format("  %4s  *** %s ***", "", error));
        }
      }
    }

    lines.add(LINE);
    lines.add("");

    for (String line : lines) {
      System.out.println(line);
    }

    if (summaryFilename != null) {
      try {
        Files.write(
            Paths.get(summaryFilename),
            lines,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND);
      } catch (IOException e) {
        throw new RuntimeException("Unable to save summary file: ", e);
      }
      NexmarkUtils.console("appended summary to summary file %s.", summaryFilename);
    }
  }

  /**
   * Write all perf data and any baselines to a javascript file which can be used by graphing page
   * etc.
   */
  private static void saveJavascript(
      @Nullable String javascriptFilename,
      Iterable<NexmarkConfiguration> configurations,
      Map<NexmarkConfiguration, NexmarkPerf> actual,
      @Nullable Map<NexmarkConfiguration, NexmarkPerf> baseline,
      Instant start) {
    if (javascriptFilename == null) {
      return;
    }

    List<String> lines = new ArrayList<>();
    lines.add(
        String.format(
            "// Run started %s and ran for %s", start, new Duration(start, Instant.now())));
    lines.add("var all = [");

    for (NexmarkConfiguration configuration : configurations) {
      lines.add("  {");
      lines.add(String.format("    config: %s", configuration));
      NexmarkPerf actualPerf = actual.get(configuration);
      if (actualPerf != null) {
        lines.add(String.format("    ,perf: %s", actualPerf));
      }
      NexmarkPerf baselinePerf = baseline == null ? null : baseline.get(configuration);
      if (baselinePerf != null) {
        lines.add(String.format("    ,baseline: %s", baselinePerf));
      }
      lines.add("  },");
    }

    lines.add("];");

    try {
      Files.write(
          Paths.get(javascriptFilename),
          lines,
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException("Unable to save javascript file: ", e);
    }
    NexmarkUtils.console("saved javascript to file %s.", javascriptFilename);
  }

  public static void main(String[] args) throws IOException {
    new Main().runAll(args);
  }
}
