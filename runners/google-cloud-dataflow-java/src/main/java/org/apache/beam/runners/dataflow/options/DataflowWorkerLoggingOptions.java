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
package org.apache.beam.runners.dataflow.options;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options that are used to control logging configuration on the Dataflow worker.
 *
 * @deprecated This interface will no longer be the source of truth for worker logging configuration
 *     once jobs are executed using a dedicated SDK harness instead of user code being co-located
 *     alongside Dataflow worker code. Please set the option below and also the corresponding option
 *     within {@link org.apache.beam.sdk.options.SdkHarnessOptions} to ensure forward compatibility.
 */
@Description("Options that are used to control logging configuration on the Dataflow worker.")
@Deprecated
public interface DataflowWorkerLoggingOptions extends PipelineOptions {
  /** The set of log levels that can be used on the Dataflow worker. */
  enum Level {
    /** Special level used to turn off logging. */
    OFF,

    /** Level for logging error messages. */
    ERROR,

    /** Level for logging warning messages. */
    WARN,

    /** Level for logging informational messages. */
    INFO,

    /** Level for logging diagnostic messages. */
    DEBUG,

    /** Level for logging tracing messages. */
    TRACE
  }

  /** This option controls the default log level of all loggers without a log level override. */
  @Description("Controls the default log level of all loggers without a log level override.")
  @Default.Enum("INFO")
  Level getDefaultWorkerLogLevel();

  void setDefaultWorkerLogLevel(Level level);

  /**
   * Controls the log level given to messages printed to {@code System.out}.
   *
   * <p>Note that the message may be filtered depending on the {@link #getDefaultWorkerLogLevel
   * defaultWorkerLogLevel} or if a {@code System.out} override is specified via {@link
   * #getWorkerLogLevelOverrides workerLogLevelOverrides}.
   */
  @Description(
      "Controls the log level given to messages printed to System.out. Note that the "
          + "message may be filtered depending on the defaultWorkerLogLevel or if a 'System.out' "
          + "override is specified via workerLogLevelOverrides.")
  @Default.Enum("INFO")
  Level getWorkerSystemOutMessageLevel();

  void setWorkerSystemOutMessageLevel(Level level);

  /**
   * Controls the log level given to messages printed to {@code System.err}.
   *
   * <p>Note that the message may be filtered depending on the {@link #getDefaultWorkerLogLevel
   * defaultWorkerLogLevel} or if a {@code System.err} override is specified via {@link
   * #getWorkerLogLevelOverrides workerLogLevelOverrides}.
   */
  @Description(
      "Controls the log level given to messages printed to System.err. Note that the "
          + "message may be filtered depending on the defaultWorkerLogLevel or if a 'System.err' "
          + "override is specified via workerLogLevelOverrides.")
  @Default.Enum("ERROR")
  Level getWorkerSystemErrMessageLevel();

  void setWorkerSystemErrMessageLevel(Level level);

  /**
   * This option controls the log levels for specifically named loggers.
   *
   * <p>Later options with equivalent names override earlier options.
   *
   * <p>See {@link WorkerLogLevelOverrides} for more information on how to configure logging on a
   * per {@link Class}, {@link Package}, or name basis. If used from the command line, the expected
   * format is {"Name":"Level",...}, further details on {@link WorkerLogLevelOverrides#from}.
   */
  @Description(
      "This option controls the log levels for specifically named loggers. "
          + "The expected format is {\"Name\":\"Level\",...}. The Dataflow worker supports a logging "
          + "hierarchy based off of names that are '.' separated. For example, by specifying the value "
          + "{\"a.b.c.Foo\":\"DEBUG\"}, the logger for the class 'a.b.c.Foo' will be configured to "
          + "output logs at the DEBUG level. Similarly, by specifying the value {\"a.b.c\":\"WARN\"}, "
          + "all loggers underneath the 'a.b.c' package will be configured to output logs at the WARN "
          + "level. System.out and System.err levels are configured via loggers of the corresponding "
          + "name. Also, note that when multiple overrides are specified, the exact name followed by "
          + "the closest parent takes precedence.")
  WorkerLogLevelOverrides getWorkerLogLevelOverrides();

  void setWorkerLogLevelOverrides(WorkerLogLevelOverrides value);

  /**
   * Defines a log level override for a specific class, package, or name.
   *
   * <p>The Dataflow worker harness supports a logging hierarchy based off of names that are "."
   * separated. It is a common pattern to have the logger for a given class share the same name as
   * the class itself. Given the classes {@code a.b.c.Foo}, {@code a.b.c.Xyz}, and {@code a.b.Bar},
   * with loggers named {@code "a.b.c.Foo"}, {@code "a.b.c.Xyz"}, and {@code "a.b.Bar"}
   * respectively, we can override the log levels:
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
  class WorkerLogLevelOverrides extends HashMap<String, Level> {
    /**
     * Overrides the default log level for the passed in class.
     *
     * <p>This is equivalent to calling {@link #addOverrideForName(String,
     * DataflowWorkerLoggingOptions.Level)} and passing in the {@link Class#getName() class name}.
     */
    public WorkerLogLevelOverrides addOverrideForClass(Class<?> klass, Level level) {
      checkNotNull(klass, "Expected class to be not null.");
      addOverrideForName(klass.getName(), level);
      return this;
    }

    /**
     * Overrides the default log level for the passed in package.
     *
     * <p>This is equivalent to calling {@link #addOverrideForName(String,
     * DataflowWorkerLoggingOptions.Level)} and passing in the {@link Package#getName() package
     * name}.
     */
    public WorkerLogLevelOverrides addOverrideForPackage(Package pkg, Level level) {
      checkNotNull(pkg, "Expected package to be not null.");
      addOverrideForName(pkg.getName(), level);
      return this;
    }

    /**
     * Overrides the default log level for the passed in name.
     *
     * <p>Note that because of the hierarchical nature of logger names, this will override the log
     * level of all loggers that have the passed in name or a parent logger that has the passed in
     * name.
     */
    public WorkerLogLevelOverrides addOverrideForName(String name, Level level) {
      checkNotNull(name, "Expected name to be not null.");
      checkNotNull(level, "Expected level to be one of %s.", Arrays.toString(Level.values()));
      put(name, level);
      return this;
    }

    /**
     * Expects a map keyed by logger {@code Name}s with values representing {@code Level}s. The
     * {@code Name} generally represents the fully qualified Java {@link Class#getName() class
     * name}, or fully qualified Java {@link Package#getName() package name}, or custom logger name.
     * The {@code Level} represents the log level and must be one of {@link Level}.
     */
    @JsonCreator
    public static WorkerLogLevelOverrides from(Map<String, String> values) {
      checkNotNull(values, "Expected values to be not null.");
      WorkerLogLevelOverrides overrides = new WorkerLogLevelOverrides();
      for (Map.Entry<String, String> entry : values.entrySet()) {
        try {
          overrides.addOverrideForName(entry.getKey(), Level.valueOf(entry.getValue()));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(
              String.format(
                  "Unsupported log level '%s' requested for %s. Must be one of %s.",
                  entry.getValue(), entry.getKey(), Arrays.toString(Level.values())));
        }
      }
      return overrides;
    }
  }
}
