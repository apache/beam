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

import * as beam from "../index";
import { createLoggingChannel } from "./logging";
import { Worker, WorkerEndpoints } from "./worker";

async function main() {
  const argv = yargs.argv;

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
  ).forEach((m) => {
    try {
      require(m);
    } catch (error) {
      console.error(
        `**ERROR**
      Unable to require module '${m}' used in requireForSerialization:
      please ensure that it is available in the package exports.`,
      );
      // Explicitly exit the process to avoid the error getting swallowed
      // by a long traceback.
      process.exit(1);
    }
  });

  console.info("Starting worker", argv.id);
  const worker = new Worker(
    argv.id,
    {
      controlUrl: argv.control_endpoint,
    },
    options,
  );
  console.info("Worker started.");
  if (pushLogs) {
    pushLogs().catch();
  }
  await worker.wait();
  console.info("Worker stoped.");
}

main()
  .catch((e) => console.error(e))
  .finally(() => process.exit());
