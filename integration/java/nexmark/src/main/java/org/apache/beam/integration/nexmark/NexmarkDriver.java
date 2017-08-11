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
package org.apache.beam.integration.nexmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An implementation of the 'NEXMark queries' for Google Dataflow.
 * These are multiple queries over a three table schema representing an online auction system:
 * <ul>
 * <li>{@link Person} represents a person submitting an item for auction and/or making a bid
 * on an auction.
 * <li>{@link Auction} represents an item under auction.
 * <li>{@link Bid} represents a bid for an item under auction.
 * </ul>
 * The queries exercise many aspects of streaming dataflow.
 *
 * <p>We synthesize the creation of people, auctions and bids in real-time. The data is not
 * particularly sensible.
 *
 * <p>See <a href="http://datalab.cs.pdx.edu/niagaraST/NEXMark/">
 * http://datalab.cs.pdx.edu/niagaraST/NEXMark/</a>
 */
public class NexmarkDriver<OptionT extends Options> {

  /**
   * Entry point.
   */
  public void runAll(OptionT options, NexmarkRunner runner) {
    Instant start = Instant.now();
    Map<NexmarkConfiguration, NexmarkPerf> baseline = loadBaseline(options.getBaselineFilename());
    Map<NexmarkConfiguration, NexmarkPerf> actual = new LinkedHashMap<>();
    Iterable<NexmarkConfiguration> configurations = options.getSuite().getConfigurations(options);

    boolean successful = true;
    try {
      // Run all the configurations.
      for (NexmarkConfiguration configuration : configurations) {
        NexmarkPerf perf = runner.run(configuration);
        if (perf != null) {
          if (perf.errors == null || perf.errors.size() > 0) {
            successful = false;
          }
          appendPerf(options.getPerfFilename(), configuration, perf);
          actual.put(configuration, perf);
          // Summarize what we've run so far.
          saveSummary(null, configurations, actual, baseline, start);
        }
      }
    } finally {
      if (options.getMonitorJobs()) {
        // Report overall performance.
        saveSummary(options.getSummaryFilename(), configurations, actual, baseline, start);
        saveJavascript(options.getJavascriptFilename(), configurations, actual, baseline, start);
      }
    }

    if (!successful) {
      System.exit(1);
    }
  }

  /**
   * Append the pair of {@code configuration} and {@code perf} to perf file.
   */
  private void appendPerf(
      @Nullable String perfFilename, NexmarkConfiguration configuration,
      NexmarkPerf perf) {
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
      Files.write(Paths.get(perfFilename), lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new RuntimeException("Unable to write perf file: ", e);
    }
    NexmarkUtils.console("appended results to perf file %s.", perfFilename);
  }

  /**
   * Load the baseline perf.
   */
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
    NexmarkUtils.console("loaded %d entries from baseline file %s.", baseline.size(),
        baselineFilename);
    return baseline;
  }

  private static final String LINE =
      "==========================================================================================";

  /**
   * Print summary  of {@code actual} vs (if non-null) {@code baseline}.
   *
   * @throws IOException
   */
  private static void saveSummary(
      @Nullable String summaryFilename,
      Iterable<NexmarkConfiguration> configurations, Map<NexmarkConfiguration, NexmarkPerf> actual,
      @Nullable Map<NexmarkConfiguration, NexmarkPerf> baseline, Instant start) {
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
    lines.add(String.format("  %4s  %12s  %12s  %12s  %12s  %12s  %12s", "Conf", "Runtime(sec)",
        "(Baseline)", "Events(/sec)", "(Baseline)", "Results", "(Baseline)"));
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
          errors = new ArrayList<String>();
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
        Files.write(Paths.get(summaryFilename), lines, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      } catch (IOException e) {
        throw new RuntimeException("Unable to save summary file: ", e);
      }
      NexmarkUtils.console("appended summary to summary file %s.", summaryFilename);
    }
  }

  /**
   * Write all perf data and any baselines to a javascript file which can be used by
   * graphing page etc.
   */
  private static void saveJavascript(
      @Nullable String javascriptFilename,
      Iterable<NexmarkConfiguration> configurations, Map<NexmarkConfiguration, NexmarkPerf> actual,
      @Nullable Map<NexmarkConfiguration, NexmarkPerf> baseline, Instant start) {
    if (javascriptFilename == null) {
      return;
    }

    List<String> lines = new ArrayList<>();
    lines.add(String.format(
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
      Files.write(Paths.get(javascriptFilename), lines, StandardCharsets.UTF_8,
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException("Unable to save javascript file: ", e);
    }
    NexmarkUtils.console("saved javascript to file %s.", javascriptFilename);
  }
}
