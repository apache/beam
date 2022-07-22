#!/usr/bin/env node

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

import * as yargs from "yargs";
import * as grpc from "@grpc/grpc-js";
import { Queue } from "queue-typescript";
import { startCapture } from "capture-console";

import * as beam from "../index";
import { Worker, WorkerEndpoints } from "./worker";
import { LogEntry, LogEntry_Severity_Enum } from "../proto/beam_fn_api";
import { BeamFnLoggingClient } from "../proto/beam_fn_api.grpc-client";

function createLoggingChannel(workerId: string, endpoint: string) {
  const logQueue = new Queue<LogEntry>();
  function toEntry(line: string): LogEntry {
    const now_ms = Date.now();
    const seconds = Math.trunc(now_ms / 1000);
    return LogEntry.create({
      severity: LogEntry_Severity_Enum.INFO,
      message: line,
      timestamp: {
        seconds: BigInt(seconds),
        nanos: Math.trunc((now_ms - seconds * 1000) * 1e6),
      },
    });
  }
  startCapture(process.stdout, (out) => logQueue.enqueue(toEntry(out)));
  startCapture(process.stderr, (out) => logQueue.enqueue(toEntry(out)));
  const metadata = new grpc.Metadata();
  metadata.add("worker_id", workerId);
  const client = new BeamFnLoggingClient(
    endpoint,
    grpc.ChannelCredentials.createInsecure(),
    {},
    {}
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

async function main() {
  const argv = yargs.argv;
  console.log(argv);

  let pushLogs;
  if (argv.logging_endpoint) {
    pushLogs = createLoggingChannel(argv.id, argv.logging_endpoint);
  }

  let options = JSON.parse(argv.options);
  if (options["options"]) {
    // Dataflow adds another level of nesting.
    options = options["options"];
  }
  (
    options["beam:option:registered_node_modules:v1"] ||
    options["registered_node_modules"] ||
    []
  ).forEach(require);

  console.log("Starting worker", argv.id);
  const worker = new Worker(
    argv.id,
    {
      controlUrl: argv.control_endpoint,
      //loggingUrl: argv.logging_endpoint,
    },
    options
  );
  if (pushLogs) {
    await pushLogs();
  }
  console.log("Worker started.");
  await worker.wait();
  console.log("Worker stoped.");
}

main()
  .catch((e) => console.error(e))
  .finally(() => process.exit());
