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
package org.apache.beam.sdk.options;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.index.qual.NonNegative;

/** Options that are used to control configuration of the SDK harness. */
@Description("Options that are used to control configuration of the SDK harness.")
public interface SdkHarnessOptions extends PipelineOptions, MemoryMonitorOptions {
  /** The set of log levels that can be used in the SDK harness. */
  enum LogLevel {
    /** Special level used to turn off logging. */
    OFF,

    /** LogLevel for logging error messages. */
    ERROR,

    /** LogLevel for logging warning messages. */
    WARN,

    /** LogLevel for logging informational messages. */
    INFO,

    /** LogLevel for logging diagnostic messages. */
    DEBUG,

    /** LogLevel for logging tracing messages. */
    TRACE;

    /** Map from LogLevel enums to java logging level. */
    public static final ImmutableMap<LogLevel, Level> LEVEL_CONFIGURATION =
        ImmutableMap.<SdkHarnessOptions.LogLevel, Level>builder()
            .put(OFF, Level.OFF)
            .put(ERROR, Level.SEVERE)
            .put(WARN, Level.WARNING)
            .put(INFO, Level.INFO)
            .put(DEBUG, Level.FINE)
            .put(TRACE, Level.FINEST)
            .build();
  }

  /** This option controls the default log level of all loggers without a log level override. */
  @Description("Controls the default log level of all loggers without a log level override.")
  @Default.Enum("INFO")
  LogLevel getDefaultSdkHarnessLogLevel();

  void setDefaultSdkHarnessLogLevel(LogLevel logLevel);

  /**
   * This option controls the log levels for specifically named loggers.
   *
   * <p>Later options with equivalent names override earlier options.
   *
   * <p>See {@link SdkHarnessLogLevelOverrides} for more information on how to configure logging on
   * a per {@link Class}, {@link Package}, or name basis. If used from the command line, the
   * expected format is {"Name":"LogLevel",...}, further details on {@link
   * SdkHarnessLogLevelOverrides#from}.
   */
  @Description(
      "This option controls the log levels for specifically named loggers. "
          + "The expected format is {\"Name\":\"LogLevel\",...}. The SDK harness supports a logging "
          + "hierarchy based off of names that are '.' separated. For example, by specifying the value "
          + "{\"a.b.c.Foo\":\"DEBUG\"}, the logger for the class 'a.b.c.Foo' will be configured to "
          + "output logs at the DEBUG level. Similarly, by specifying the value {\"a.b.c\":\"WARN\"}, "
          + "all loggers underneath the 'a.b.c' package will be configured to output logs at the WARN "
          + "level. System.out and System.err levels are configured via loggers of the corresponding "
          + "name. Also, note that when multiple overrides are specified, the exact name followed by "
          + "the closest parent takes precedence.")
  SdkHarnessLogLevelOverrides getSdkHarnessLogLevelOverrides();

  void setSdkHarnessLogLevelOverrides(SdkHarnessLogLevelOverrides value);

  /** Whether to include SLF4J MDC in log entries. */
  @Description(
      "This option controls whether SLF4J MDC keys and values will be appended to log entries. "
          + "This used by Beam to add structured data to log entries, such as quota events and "
          + "return statuses.")
  @Default.Boolean(true)
  boolean getLogMdc();

  void setLogMdc(boolean value);

  /**
   * Size (in MB) of each grouping table used to pre-combine elements. Larger values may reduce the
   * amount of data shuffled. If unset, defaults to 100 MB.
   *
   * <p>CAUTION: If set too large, workers may run into OOM conditions more easily, each worker may
   * have many grouping tables in-memory concurrently.
   *
   * <p>CAUTION: This option does not apply to portable runners such as Dataflow Prime. See {@link
   * #setMaxCacheMemoryUsageMb}, {@link #setMaxCacheMemoryUsagePercent}, or {@link
   * #setMaxCacheMemoryUsageMbClass} to configure memory thresholds that apply to the grouping table
   * and other cached objects.
   */
  @Description(
      "The size (in MB) of the grouping tables used to pre-combine elements before shuffling. If "
          + "unset, defaults to 100 MB. Larger values may reduce the amount of data shuffled. "
          + "CAUTION: If set too large, workers may run into OOM conditions more easily, each "
          + "worker may have many grouping tables in-memory concurrently. CAUTION: This option "
          + "does not apply to portable runners such as Dataflow Prime. See "
          + "--maxCacheMemoryUsageMb, --maxCacheMemoryUsagePercent, or "
          + "--maxCacheMemoryUsageMbClass to configure memory thresholds that apply to the "
          + "grouping table and other cached objects.")
  @Default.Integer(100)
  int getGroupingTableMaxSizeMb();

  void setGroupingTableMaxSizeMb(int value);

  /**
   * Size (in MB) for the process wide cache within the SDK harness. The cache is responsible for
   * storing all values which are cached within a bundle and across bundles such as side inputs and
   * user state.
   *
   * <p>CAUTION: If set too large, SDK harness instances may run into OOM conditions more easily.
   *
   * <p>See {@link DefaultMaxCacheMemoryUsageMbFactory} for details on how {@link
   * #getMaxCacheMemoryUsageMb() maxCacheMemoryUsageMb} is computed if this parameter is
   * unspecified.
   */
  @Description(
      "The size (in MB) for the process wide cache within the SDK harness. The cache is responsible for "
          + "storing all values which are cached within a bundle and across bundles such as side inputs "
          + "and user state. CAUTION: If set too large, SDK harness instances may run into OOM conditions more easily.")
  @Default.InstanceFactory(DefaultMaxCacheMemoryUsageMbFactory.class)
  @NonNegative
  int getMaxCacheMemoryUsageMb();

  void setMaxCacheMemoryUsageMb(@NonNegative int value);

  /**
   * Size (in % [0 - 100]) for the process wide cache within the SDK harness. The cache is
   * responsible for storing all values which are cached within a bundle and across bundles such as
   * side inputs and user state.
   *
   * <p>This parameter will only be used if an explicit value was not specified for {@link
   * #getMaxCacheMemoryUsageMb() maxCacheMemoryUsageMb}.
   */
  @Description(
      "The size (in % [0 - 100]) for the process wide cache within the SDK harness. The cache is responsible for "
          + "storing all values which are cached within a bundle and across bundles such as side inputs "
          + "and user state. CAUTION: If set too large, SDK harness instances may run into OOM conditions more easily.")
  @Default.Float(20)
  @NonNegative
  float getMaxCacheMemoryUsagePercent();

  void setMaxCacheMemoryUsagePercent(@NonNegative float value);

  /**
   * An instance of this class will be used to specify the maximum amount of memory to allocate to a
   * cache within an SDK harness instance.
   *
   * <p>This parameter will only be used if an explicit value was not specified for {@link
   * #getMaxCacheMemoryUsageMb() maxCacheMemoryUsageMb}.
   *
   * <p>See {@link DefaultMaxCacheMemoryUsageMb} for details on how {@link
   * #getMaxCacheMemoryUsageMb() maxCacheMemoryUsageMb} is computed if this parameter is
   * unspecified.
   */
  @Description(
      "An instance of this class will be used to specify the maximum amount of memory to allocate to a "
          + " process wide cache within an SDK harness instance. This parameter will only be used if an explicit value was not specified for --maxCacheMemoryUsageMb.")
  @Default.Class(DefaultMaxCacheMemoryUsageMb.class)
  Class<? extends MaxCacheMemoryUsageMb> getMaxCacheMemoryUsageMbClass();

  void setMaxCacheMemoryUsageMbClass(Class<? extends MaxCacheMemoryUsageMb> kls);

  /**
   * A {@link DefaultValueFactory} which constructs an instance of the class specified by {@link
   * #getMaxCacheMemoryUsageMbClass maxCacheMemoryUsageMbClass} to compute the maximum amount of
   * memory to allocate to the process wide cache within an SDK harness instance.
   */
  class DefaultMaxCacheMemoryUsageMbFactory implements DefaultValueFactory<@NonNegative Integer> {

    @Override
    public @NonNegative Integer create(PipelineOptions options) {
      SdkHarnessOptions sdkHarnessOptions = options.as(SdkHarnessOptions.class);
      return (Integer)
          checkNotNull(
              InstanceBuilder.ofType(MaxCacheMemoryUsageMb.class)
                  .fromClass(sdkHarnessOptions.getMaxCacheMemoryUsageMbClass())
                  .build()
                  .getMaxCacheMemoryUsage(options));
    }
  }

  /** Specifies the maximum amount of memory to use within the current SDK harness instance. */
  interface MaxCacheMemoryUsageMb {
    @NonNegative
    int getMaxCacheMemoryUsage(PipelineOptions options);
  }

  /**
   * The default implementation which detects how much memory to use for a process wide cache.
   *
   * <p>If the {@link Runtime} provides a maximum amount of memory (typically specified with {@code
   * -Xmx} JVM argument), then {@link #getMaxCacheMemoryUsagePercent maxCacheMemoryUsagePercent}
   * will be used to compute the upper bound as a percentage of the maximum amount of memory.
   * Otherwise {@code 100} is returned.
   */
  class DefaultMaxCacheMemoryUsageMb implements MaxCacheMemoryUsageMb {
    @Override
    public int getMaxCacheMemoryUsage(PipelineOptions options) {
      return getMaxCacheMemoryUsage(options, Runtime.getRuntime().maxMemory());
    }

    @VisibleForTesting
    int getMaxCacheMemoryUsage(PipelineOptions options, long maxMemory) {
      if (maxMemory == Long.MAX_VALUE) {
        return 100;
      }
      float maxPercent = options.as(SdkHarnessOptions.class).getMaxCacheMemoryUsagePercent();
      if (maxPercent < 0 || maxPercent > 100) {
        throw new IllegalArgumentException(
            "--maxCacheMemoryUsagePercent must be between 0 and 100.");
      }
      return (int) (maxMemory / 1048576. * maxPercent / 100.);
    }
  }

  /**
   * Defines a log level override for a specific class, package, or name.
   *
   * <p>The SDK harness supports a logging hierarchy based off of names that are "." separated. It
   * is a common pattern to have the logger for a given class share the same name as the class
   * itself. Given the classes {@code a.b.c.Foo}, {@code a.b.c.Xyz}, and {@code a.b.Bar}, with
   * loggers named {@code "a.b.c.Foo"}, {@code "a.b.c.Xyz"}, and {@code "a.b.Bar"} respectively, we
   * can override the log levels:
   *
   * <ul>
   *   <li>for {@code Foo} by specifying the name {@code "a.b.c.Foo"} or the {@link Class}
   *       representing {@code a.b.c.Foo}.
   *   <li>for {@code Foo}, {@code Xyz}, and {@code Bar} by specifying the name {@code "a.b"} or the
   *       {@link Package} representing {@code a.b}.
   *   <li>for {@code Foo} and {@code Bar} by specifying both of their names or classes.
   * </ul>
   *
   * <p>{@code System.out} and {@code System.err} messages are configured via loggers of the
   * corresponding name. Note that by specifying multiple overrides, the exact name followed by the
   * closest parent takes precedence.
   */
  class SdkHarnessLogLevelOverrides extends HashMap<String, LogLevel> {
    /**
     * Overrides the default log level for the passed in class.
     *
     * <p>This is equivalent to calling {@link #addOverrideForName(String, LogLevel)} and passing in
     * the {@link Class#getName() class name}.
     */
    public SdkHarnessLogLevelOverrides addOverrideForClass(Class<?> klass, LogLevel logLevel) {
      checkNotNull(klass, "Expected class to be not null.");
      addOverrideForName(klass.getName(), logLevel);
      return this;
    }

    /**
     * Overrides the default log level for the passed in package.
     *
     * <p>This is equivalent to calling {@link #addOverrideForName(String, LogLevel)} and passing in
     * the {@link Package#getName() package name}.
     */
    public SdkHarnessLogLevelOverrides addOverrideForPackage(Package pkg, LogLevel logLevel) {
      checkNotNull(pkg, "Expected package to be not null.");
      addOverrideForName(pkg.getName(), logLevel);
      return this;
    }

    /**
     * Overrides the default log logLevel for the passed in name.
     *
     * <p>Note that because of the hierarchical nature of logger names, this will override the log
     * logLevel of all loggers that have the passed in name or a parent logger that has the passed
     * in name.
     */
    public SdkHarnessLogLevelOverrides addOverrideForName(String name, LogLevel logLevel) {
      checkNotNull(name, "Expected name to be not null.");
      checkNotNull(
          logLevel, "Expected logLevel to be one of %s.", Arrays.toString(LogLevel.values()));
      put(name, logLevel);
      return this;
    }

    /**
     * Expects a map keyed by logger {@code Name}s with values representing {@code LogLevel}s. The
     * {@code Name} generally represents the fully qualified Java {@link Class#getName() class
     * name}, or fully qualified Java {@link Package#getName() package name}, or custom logger name.
     * The {@code LogLevel} represents the log level and must be one of {@link LogLevel}.
     */
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public static SdkHarnessLogLevelOverrides from(Map<String, String> values) {
      checkNotNull(values, "Expected values to be not null.");
      SdkHarnessLogLevelOverrides overrides = new SdkHarnessLogLevelOverrides();
      for (Map.Entry<String, String> entry : values.entrySet()) {
        String module = entry.getKey();
        String level = entry.getValue();
        if (level.equals("WARNING")) {
          // alias: "WARNING" -> "WARN"
          level = "WARN";
        }
        try {
          overrides.addOverrideForName(module, LogLevel.valueOf(level));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              String.format(
                  "Unsupported log level '%s' requested for %s. Must be one of %s.",
                  level, module, Arrays.toString(LogLevel.values())));
        }
      }
      return overrides;
    }
  }

  /**
   * Open modules needed for reflection that access JDK internals with Java 9+
   *
   * <p>With JDK 16+, <a href="#{https://openjdk.java.net/jeps/403}">JDK internals are strongly
   * encapsulated</a> and can result in an InaccessibleObjectException being thrown if a tool or
   * library uses reflection that access JDK internals. If you see these errors in your worker logs,
   * you can pass in modules to open using the format module/package=target-module(,target-module)*
   * to allow access to the library. E.g. java.base/java.lang=jamm
   *
   * <p>You may see warnings that jamm, a library used to more accurately size objects, is unable to
   * make a private field accessible. To resolve the warning, open the specified module/package to
   * jamm.
   */
  @Description("Open modules needed for reflection with Java 17+.")
  List<String> getJdkAddOpenModules();

  void setJdkAddOpenModules(List<String> options);

  /**
   * Configure log manager's default log level and log level overrides from the sdk harness options,
   * and return the list of configured loggers.
   */
  static List<Logger> getConfiguredLoggerFromOptions(SdkHarnessOptions loggingOptions) {
    ArrayList<Logger> configuredLoggers = new ArrayList<>();
    LogManager logManager = LogManager.getLogManager();
    Logger rootLogger = logManager.getLogger("");

    // Use the passed in logging options to configure the various logger levels.
    if (loggingOptions.getDefaultSdkHarnessLogLevel() != null) {
      rootLogger.setLevel(
          SdkHarnessOptions.LogLevel.LEVEL_CONFIGURATION.get(
              loggingOptions.getDefaultSdkHarnessLogLevel()));
    }

    if (loggingOptions.getSdkHarnessLogLevelOverrides() != null) {
      for (Map.Entry<String, SdkHarnessOptions.LogLevel> loggerOverride :
          loggingOptions.getSdkHarnessLogLevelOverrides().entrySet()) {
        Logger logger = logManager.getLogger(loggerOverride.getKey());
        if (logger == null) {
          // create a logger if not exist
          logger = Logger.getLogger(loggerOverride.getKey());
        }
        logger.setLevel(
            SdkHarnessOptions.LogLevel.LEVEL_CONFIGURATION.get(loggerOverride.getValue()));
        configuredLoggers.add(logger);
      }
    }
    return configuredLoggers;
  }

  @Hidden
  @Description(
      "Timeout used for cache of bundle processors. Defaults to a minute for batch and an hour for streaming.")
  @Default.InstanceFactory(BundleProcessorCacheTimeoutFactory.class)
  Duration getBundleProcessorCacheTimeout();

  void setBundleProcessorCacheTimeout(Duration duration);

  class BundleProcessorCacheTimeoutFactory implements DefaultValueFactory<Duration> {
    @Override
    public Duration create(PipelineOptions options) {
      return options.as(StreamingOptions.class).isStreaming()
          ? Duration.ofHours(1)
          : Duration.ofMinutes(1);
    }
  }
}
