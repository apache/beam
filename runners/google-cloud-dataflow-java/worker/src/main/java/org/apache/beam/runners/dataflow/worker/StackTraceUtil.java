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
package org.apache.beam.runners.dataflow.worker;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingHandler;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods to print the stack traces of all the threads. */
@Internal
public final class StackTraceUtil {
  private static final ImmutableSet<String> FRAMEWORK_CLASSES =
      ImmutableSet.of(SimpleDoFnRunner.class.getName(), DoFnInstanceManagers.class.getName());
  private static final Logger LOG = LoggerFactory.getLogger(StackTraceUtil.class);

  public static String getStackTraceForLullMessage(StackTraceElement[] stackTrace) {
    StringBuilder message = new StringBuilder();
    for (StackTraceElement e : stackTrace) {
      if (FRAMEWORK_CLASSES.contains(e.getClassName())) {
        break;
      }
      message.append("  at ").append(e).append("\n");
    }
    return message.toString();
  }

  public static void logAllStackTraces() {
    DataflowWorkerLoggingHandler dataflowLoggingHandler =
        DataflowWorkerLoggingInitializer.getLoggingHandler();
    Map<Thread, StackTraceElement[]> threadSet = Thread.getAllStackTraces();
    for (Map.Entry<Thread, StackTraceElement[]> entry : threadSet.entrySet()) {
      Thread thread = entry.getKey();
      StackTraceElement[] stackTrace = entry.getValue();
      StringBuilder message = new StringBuilder();
      message.append(thread.toString()).append(":\n");
      message.append(getStackTraceForLullMessage(stackTrace));
      LogRecord logRecord = new LogRecord(Level.INFO, message.toString());
      logRecord.setLoggerName(StackTraceUtil.LOG.getName());
      dataflowLoggingHandler.publish(logRecord);
    }
  }

  private StackTraceUtil() {}
}
