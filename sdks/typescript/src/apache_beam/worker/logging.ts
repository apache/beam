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

import { AsyncLocalStorage } from "node:async_hooks";

export const loggingLocalStorage = new AsyncLocalStorage();

import * as grpc from "@grpc/grpc-js";
import { startCapture } from "capture-console";
import { Queue } from "queue-typescript";

import { LogEntry, LogEntry_Severity_Enum } from "../proto/beam_fn_api";
import { BeamFnLoggingClient } from "../proto/beam_fn_api.grpc-client";

export interface LoggingStageInfo {
  instructionId?: string;
  transformId?: string;
}

export class LoggingStageInfoHolder {
  loggingStageInfo: LoggingStageInfo;
}

export function createLoggingChannel(workerId: string, endpoint: string) {
  const logQueue = new Queue<LogEntry>();
  function toEntry(line: string): LogEntry {
    const now_ms = Date.now();
    const seconds = Math.trunc(now_ms / 1000);
    const stageInfo = loggingLocalStorage.getStore() as LoggingStageInfo;
    let logRecord = {
      severity: LogEntry_Severity_Enum.INFO,
      message: line,
      timestamp: {
        seconds: BigInt(seconds),
        nanos: Math.trunc((now_ms - seconds * 1000) * 1e6),
      },
      instructionId: stageInfo?.instructionId,
      transformId: stageInfo?.transformId,
    };
    try {
      const structuredLog = JSON.parse(line);
      logRecord.severity = guessLogLevel(structuredLog);
      logRecord = { ...structuredLog, ...logRecord };
    } catch {}
    return LogEntry.create(logRecord);
  }

  let currentConsoleLogLevel = undefined;
  const consoleLogLevels = {
    // Don't include log as other loggers may use it; let it default to INFO.
    error: LogEntry_Severity_Enum.ERROR,
    warn: LogEntry_Severity_Enum.WARN,
    info: LogEntry_Severity_Enum.INFO,
    debug: LogEntry_Severity_Enum.DEBUG,
  };

  // The createLogMethod is explicitly called to capture the
  // iteration-specific closure.
  function createLogMethod(method, level) {
    const original = console[method];
    return function () {
      const old = currentConsoleLogLevel;
      try {
        currentConsoleLogLevel = level;
        original(...arguments);
      } finally {
        currentConsoleLogLevel = old;
      }
    };
  }

  for (const [method, level] of Object.entries(consoleLogLevels)) {
    console[method] = createLogMethod(method, level);
  }

  function guessLogLevel(structuredLog: any) {
    if (currentConsoleLogLevel !== undefined) {
      return currentConsoleLogLevel;
    } else {
      try {
        if (structuredLog.level !== undefined) {
          if (0 <= structuredLog.level && structuredLog.level <= 7) {
            // Assume https://www.rfc-editor.org/rfc/rfc5424
            return [
              LogEntry_Severity_Enum.CRITICAL,
              LogEntry_Severity_Enum.CRITICAL,
              LogEntry_Severity_Enum.CRITICAL,
              LogEntry_Severity_Enum.ERROR,
              LogEntry_Severity_Enum.WARN,
              LogEntry_Severity_Enum.INFO,
              LogEntry_Severity_Enum.INFO,
              LogEntry_Severity_Enum.DEBUG,
            ][structuredLog.level];
          } else {
            // Something like pinojs.
            const truncatedLogLevel = Math.floor(structuredLog.level / 10);
            if (1 <= truncatedLogLevel && truncatedLogLevel <= 7) {
              return [
                LogEntry_Severity_Enum.TRACE,
                LogEntry_Severity_Enum.TRACE,
                LogEntry_Severity_Enum.DEBUG,
                LogEntry_Severity_Enum.INFO,
                LogEntry_Severity_Enum.WARN,
                LogEntry_Severity_Enum.ERROR,
                LogEntry_Severity_Enum.CRITICAL,
                LogEntry_Severity_Enum.CRITICAL,
              ][truncatedLogLevel];
            }
          }
        }
      } catch (e) {}
      return LogEntry_Severity_Enum.INFO;
    }
  }

  startCapture(process.stdout, (out) => logQueue.enqueue(toEntry(out)));
  startCapture(process.stderr, (out) => logQueue.enqueue(toEntry(out)));
  const metadata = new grpc.Metadata();
  metadata.add("worker_id", workerId);
  const client = new BeamFnLoggingClient(
    endpoint,
    grpc.ChannelCredentials.createInsecure(),
    {},
    {},
  );
  const channel = client.logging(metadata);

  return async function () {
    while (true) {
      if (logQueue.length) {
        const messages: LogEntry[] = [];
        while (logQueue.length && messages.length < 100) {
          messages.push(logQueue.dequeue());
        }
        await channel.write({ logEntries: messages });
      } else {
        await new Promise((r) => setTimeout(r, 100));
      }
    }
  };
}
