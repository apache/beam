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
package org.apache.beam.runners.fnexecution.logging;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link LogWriter} which uses an {@link Logger SLF4J Logger} as the underlying log backend.
 *
 * <p>Ignores the {@code timestamp}, {@code instruction reference}, {@code primitive transform
 * reference}, and {@code thread} fields.
 */
public class Slf4jLogWriter implements LogWriter {
  public static LogWriter getDefault() {
    return new Slf4jLogWriter();
  }

  private Slf4jLogWriter() {}

  @Override
  public void log(LogEntry entry) {
    Logger log;
    String location = entry.getLogLocation();
    if (location != null) {
      log = LoggerFactory.getLogger(location);
    } else {
      // TODO: Provide a useful default
      log = LoggerFactory.getLogger("RemoteLog");
    }
    String message = entry.getMessage();
    String trace = entry.getTrace();
    switch (entry.getSeverity()) {
      case ERROR:
      case CRITICAL:
        if (trace == null) {
          log.error(message);
        } else {
          log.error("{} {}", message, trace);
        }
        break;
      case WARN:
        if (trace == null) {
          log.warn(message);
        } else {
          log.warn("{} {}", message, trace);
        }
        break;
      case INFO:
      case NOTICE:
        log.info(message);
        break;
      case DEBUG:
        log.debug(message);
        break;
      case UNSPECIFIED:
      case TRACE:
        log.trace(message);
        break;
      default:
        log.warn("Unknown message severity {}", entry.getSeverity());
        log.info(message);
        break;
    }
  }
}
