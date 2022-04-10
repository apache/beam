import * as yargs from "yargs";
import * as grpc from "@grpc/grpc-js";
import { Queue } from "queue-typescript";
import { startCapture } from "capture-console";

import * as beam from "../index";
import { Worker, WorkerEndpoints } from "./worker";
import { LogEntry, LogEntry_Severity_Enum } from "../proto/beam_fn_api";
import { BeamFnLoggingClient } from "../proto/beam_fn_api.grpc-client";

// Needed for registration.
import * as row_coder from "../coders/row_coder";
import * as combiners from "../transforms/combiners";

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

  console.log("Starting worker", argv.id);
  const worker = new Worker(
    argv.id,
    {
      controlUrl: argv.control_endpoint,
      //loggingUrl: argv.logging_endpoint,
    },
    JSON.parse(argv.options)
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
