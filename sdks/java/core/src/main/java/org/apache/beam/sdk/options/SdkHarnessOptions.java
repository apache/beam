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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/** Options that are used to control configuration of the SDK harness. */
@Experimental(Kind.PORTABILITY)
@Description("Options that are used to control configuration of the SDK harness.")
public interface SdkHarnessOptions extends PipelineOptions {
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
    TRACE
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

  /**
   * Size (in MB) of each grouping table used to pre-combine elements. If unset, defaults to 100 MB.
   *
   * <p>CAUTION: If set too large, workers may run into OOM conditions more easily, each worker may
   * have many grouping tables in-memory concurrently.
   */
  @Description(
      "The size (in MB) of the grouping tables used to pre-combine elements before "
          + "shuffling.  Larger values may reduce the amount of data shuffled.")
  @Default.Integer(100)
  int getGroupingTableMaxSizeMb();

  void setGroupingTableMaxSizeMb(int value);

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
    @JsonCreator
    public static SdkHarnessLogLevelOverrides from(Map<String, String> values) {
      checkNotNull(values, "Expected values to be not null.");
      SdkHarnessLogLevelOverrides overrides = new SdkHarnessLogLevelOverrides();
      for (Map.Entry<String, String> entry : values.entrySet()) {
        try {
          overrides.addOverrideForName(entry.getKey(), LogLevel.valueOf(entry.getValue()));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              String.format(
                  "Unsupported log level '%s' requested for %s. Must be one of %s.",
                  entry.getValue(), entry.getKey(), Arrays.toString(LogLevel.values())));
        }
      }
      return overrides;
    }
  }
}
