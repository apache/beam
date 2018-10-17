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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
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
 * <p>See <a href="http://datalab.cs.pdx.edu/niagaraST/NEXMark/">
 * http://datalab.cs.pdx.edu/niagaraST/NEXMark/</a>
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

    private Run(String[] args, NexmarkConfiguration configuration) {
      NexmarkOptions options = PipelineOptionsFactory.fromArgs(args).as(NexmarkOptions.class);
      this.nexmarkLauncher = new NexmarkLauncher<>(options);
      this.configuration = configuration;
    }

    @Override
    public Result call() throws IOException {
      NexmarkPerf perf = nexmarkLauncher.run(configuration);
      return new Result(configuration, perf);
    }
  }

  /** Entry point. */
  void runAll(String[] args) throws IOException {
    Instant start = Instant.now();
    NexmarkOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(NexmarkOptions.class);
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
        completion.submit(new Run(args, configuration));
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

      if (options.getExportSummaryToBigQuery()) {
        savePerfsToBigQuery(options, actual, null, start);
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
      NexmarkOptions options,
      Map<NexmarkConfiguration, NexmarkPerf> perfs,
      @Nullable BigQueryServices testBigQueryServices,
      Instant start) {
    Pipeline pipeline = Pipeline.create(options);
    PCollection<KV<NexmarkConfiguration, NexmarkPerf>> perfsPCollection =
        pipeline.apply(
            Create.of(perfs)
                .withCoder(
                    KvCoder.of(
                        SerializableCoder.of(NexmarkConfiguration.class),
                        new CustomCoder<NexmarkPerf>() {

                          @Override
                          public void encode(NexmarkPerf value, OutputStream outStream)
                              throws CoderException, IOException {
                            StringUtf8Coder.of().encode(value.toString(), outStream);
                          }

                          @Override
                          public NexmarkPerf decode(InputStream inStream)
                              throws CoderException, IOException {
                            String perf = StringUtf8Coder.of().decode(inStream);
                            return NexmarkPerf.fromString(perf);
                          }
                        })));

    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"),
                    new TableFieldSchema().setName("runtimeSec").setType("FLOAT"),
                    new TableFieldSchema().setName("eventsPerSec").setType("FLOAT"),
                    new TableFieldSchema().setName("numResults").setType("INTEGER")));

    String queryName = "{query}";
    if (options.getQueryLanguage() != null) {
      queryName = queryName + "_" + options.getQueryLanguage();
    }
    final String tableSpec = NexmarkUtils.tableSpec(options, queryName, 0L, null);
    SerializableFunction<
            ValueInSingleWindow<KV<NexmarkConfiguration, NexmarkPerf>>, TableDestination>
        tableFunction =
            input ->
                new TableDestination(
                    tableSpec.replace("{query}", String.valueOf(input.getValue().getKey().query)),
                    "perfkit queries");
    SerializableFunction<KV<NexmarkConfiguration, NexmarkPerf>, TableRow> rowFunction =
        input -> {
          NexmarkPerf nexmarkPerf = input.getValue();
          TableRow row =
              new TableRow()
                  .set("timestamp", start.getMillis() / 1000)
                  .set("runtimeSec", nexmarkPerf.runtimeSec)
                  .set("eventsPerSec", nexmarkPerf.eventsPerSec)
                  .set("numResults", nexmarkPerf.numResults);
          return row;
        };
    BigQueryIO.Write io =
        BigQueryIO.<KV<NexmarkConfiguration, NexmarkPerf>>write()
            .to(tableFunction)
            .withSchema(tableSchema)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withFormatFunction(rowFunction);
    if (testBigQueryServices != null) {
      io = io.withTestServices(testBigQueryServices);
    }
    perfsPCollection.apply("savePerfsToBigQuery", io);
    pipeline.run();
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
  @Nullable
  private static Map<NexmarkConfiguration, NexmarkPerf> loadBaseline(
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
