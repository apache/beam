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

const fs = require("fs");
const os = require("os");
const path = require("path");

const { BigQuery } = require("@google-cloud/bigquery");
const { PubSub } = require("@google-cloud/pubsub");

import * as uuid from "uuid";
import * as assert from "assert";

import * as beam from "../src/apache_beam";
import * as testing from "../src/apache_beam/testing/assert";
import * as internal from "../src/apache_beam/transforms/internal";
import { createRunner } from "../src/apache_beam/runners/runner";
import { BytesCoder } from "../src/apache_beam/coders/required_coders";
import { RowCoder } from "../src/apache_beam/coders/row_coder";
import * as avroio from "../src/apache_beam/io/avroio";
import * as bigqueryio from "../src/apache_beam/io/bigqueryio";
import * as parquetio from "../src/apache_beam/io/parquetio";
import * as textio from "../src/apache_beam/io/textio";
import * as pubsub from "../src/apache_beam/io/pubsub";
import * as service from "../src/apache_beam/utils/service";

const lines = [
  "In the beginning God created the heaven and the earth.",
  "And the earth was without form, and void; and darkness was upon the face of the deep.",
  "And the Spirit of God moved upon the face of the waters.",
  "And God said, Let there be light: and there was light.",
];

const elements = [
  { label: "11a", rank: 0 },
  { label: "37a", rank: 1 },
  { label: "389a", rank: 2 },
];

let subprocessCache;
before(() => {
  subprocessCache = service.SubprocessService.createCache();
});

after(() => subprocessCache.stopAll());

function xlang_it(name, fn) {
  return (process.env.BEAM_SERVICE_OVERRIDES ? it : it.skip)(
    name + " @xlang",
    fn
  );
}

// These depends on fixes that will be released in 2.40.
// They can be run manually by setting an environment variable
// export BEAM_SERVICE_OVERRIDES = '{python:*": "/path/to/dev/venv/bin/python"}'
// TODO: Automatically set up/depend on such a venv in dev environments and/or
// testing infra.
describe("IO Tests", function () {
  xlang_it("textio file test", async function () {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "apache-beam-test"));

    await createRunner().run(async (root) => {
      await root //
        .apply(beam.create(lines))
        .applyAsync(textio.writeToText(path.join(tempDir, "out.txt")));
    });

    await createRunner().run(async (root) => {
      (
        await root.applyAsync(
          textio.readFromText(path.join(tempDir, "out.txt*"))
        )
      ).apply(testing.assertDeepEqual(lines));
    });
  }).timeout(15000);

  xlang_it("textio csv file test", async function () {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "apache-beam-test"));

    await createRunner().run(async (root) => {
      await root //
        .apply(beam.create(elements))
        .apply(internal.withCoderInternal(RowCoder.fromJSON(elements[0])))
        .applyAsync(textio.writeToCsv(path.join(tempDir, "out.csv")));
    });
    console.log(tempDir);

    await createRunner().run(async (root) => {
      (
        await root.applyAsync(
          textio.readFromCsv(path.join(tempDir, "out.csv*"))
        )
      ).apply(testing.assertDeepEqual(elements));
    });
  }).timeout(15000);

  xlang_it("textio json file test", async function () {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "apache-beam-test"));

    await createRunner().run(async (root) => {
      await root //
        .apply(beam.create(elements))
        .apply(internal.withCoderInternal(RowCoder.fromJSON(elements[0])))
        .applyAsync(textio.writeToJson(path.join(tempDir, "out.json")));
    });

    await createRunner().run(async (root) => {
      (
        await root.applyAsync(
          textio.readFromJson(path.join(tempDir, "out.json*"))
        )
      ).apply(testing.assertDeepEqual(elements));
    });
  }).timeout(15000);

  xlang_it("parquetio file test", async function () {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "apache-beam-test"));

    await createRunner().run(async (root) => {
      await root //
        .apply(beam.create(elements))
        .apply(internal.withCoderInternal(RowCoder.fromJSON(elements[0])))
        .applyAsync(
          parquetio.writeToParquet(path.join(tempDir, "out.parquet"))
        );
    });

    await createRunner().run(async (root) => {
      (
        await root.applyAsync(
          parquetio.readFromParquet(path.join(tempDir, "out.parquet*"))
        )
      ).apply(testing.assertDeepEqual(elements));
    });

    await createRunner().run(async (root) => {
      (
        await root.applyAsync(
          parquetio.readFromParquet(path.join(tempDir, "out.parquet*"), {
            columns: ["label", "rank"],
          })
        )
      ).apply(testing.assertDeepEqual(elements));
    });
  }).timeout(15000);

  it.skip("avroio file test", async function () {
    // Requires access to a distributed filesystem.
    const options = {
      //      runner: "dataflow",
      project: "apache-beam-testing",
      tempLocation: "gs://temp-storage-for-end-to-end-tests/temp-it",
      region: "us-central1",
    };

    // TODO: Allow local testing.
    // const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "apache-beam-test"));
    function path_join(...args) {
      return path.join(...args).replace("gs:/", "gs://");
    }
    const tempDir = path_join(options.tempLocation, uuid.v4());
    console.log(options.tempLocation);
    console.log(tempDir);
    // TODO: Allow schema to be inferred.
    const schema = RowCoder.inferSchemaOfJSON(elements[0]);

    await createRunner(options).run(async (root) => {
      await root //
        .apply(beam.create(elements))
        .applyAsync(
          avroio.writeToAvro(path_join(tempDir, "out.avro"), { schema })
        );
    });

    await createRunner(options).run(async (root) => {
      (
        await root.applyAsync(
          avroio.readFromAvro(path_join(tempDir, "out.avro*"), { schema })
        )
      ).apply(testing.assertDeepEqual(elements));
    });
  }).timeout(60000);

  it.skip("bigqueryio test", async function () {
    // This only passes when it is run on its own.
    // TODO: Figure out what is going on here.
    // The error is a java.lang.NullPointerException at
    // org.apache.beam.sdk.io.gcp.bigquery.BigQueryTableSourceDef.getBeamSchema(BigQueryTableSourceDef.java:111)
    this.skip();

    // TODO: There should be a better way to pass this to the underlying
    // services.
    if (!process.env.CLOUDSDK_CONFIG) {
      const cloudSdkConfig = fs.mkdtempSync(
        path.join(os.tmpdir(), "apache-beam-test")
      );
      fs.writeFileSync(
        path.join(cloudSdkConfig, "properties"),
        "project=apache-beam-testing"
      );
      process.env.CLOUDSDK_CONFIG = cloudSdkConfig;
    }

    if (!process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      const defaultCredentials = path.join(
        os.homedir(),
        ".config",
        "gcloud",
        "application_default_credentials.json"
      );
      if (fs.existsSync(defaultCredentials)) {
        process.env.GOOGLE_APPLICATION_CREDENTIALS = defaultCredentials;
      }
    }

    const options = {
      project: "apache-beam-testing",
      tempLocation: "gs://temp-storage-for-end-to-end-tests/temp-it",
      region: "us-central1",
    };

    const bigqueryClient = new BigQuery({ projectId: options.project });
    const datasetName = "beam_temp_dataset_" + uuid.v4().replaceAll("-", "");
    const [dataset] = await bigqueryClient.createDataset(datasetName);

    try {
      const table =
        datasetName + ".beam_temp_table" + uuid.v4().replaceAll("-", "");
      await createRunner(options).run(async (root) => {
        await root //
          .apply(beam.create(elements))
          .apply(internal.withCoderInternal(RowCoder.fromJSON(elements[0])))
          .applyAsync(
            bigqueryio.writeToBigQuery(table, { createDisposition: "IfNeeded" })
          );
      });

      await createRunner(options).run(async (root) => {
        (await root.applyAsync(bigqueryio.readFromBigQuery({ table }))) //
          .apply(testing.assertDeepEqual(elements));
      });

      await createRunner(options).run(async (root) => {
        (
          await root.applyAsync(
            bigqueryio.readFromBigQuery({
              query: `SELECT label, rank FROM ${table}`,
            })
          )
        ) //
          .apply(testing.assertDeepEqual(elements));
      });
    } finally {
      await dataset.delete({ force: true });
    }
  }).timeout(300000);

  it.skip("pubsub test", async function () {
    const options = {
      runner: "dataflow",
      project: "apache-beam-testing",
      tempLocation: "gs://temp-storage-for-end-to-end-tests/temp-it",
      region: "us-central1",
    };

    const pubsubClient = new PubSub({ projectId: options.project });
    const [readTopic] = await pubsubClient.createTopic("testing-" + uuid.v4());
    const [readSubscription] = await readTopic.createSubscription(
      "sub-" + uuid.v4()
    );
    const [writeTopic] = await pubsubClient.createTopic("testing-" + uuid.v4());
    const [writeSubscription] = await writeTopic.createSubscription(
      "sub-" + uuid.v4()
    );

    let pipelineHandle;
    try {
      pipelineHandle = await createRunner(options).runAsync(async (root) => {
        await (
          await root.applyAsync(
            pubsub.readFromPubSub({
              subscription: readSubscription.name,
            })
          )
        )
          .map((encoded) => new TextDecoder().decode(encoded))
          .map((msg) => msg.toUpperCase())
          .map((msg) => new TextEncoder().encode(msg))
          .apply(internal.withCoderInternal(new BytesCoder()))
          .applyAsync(pubsub.writeToPubSub(writeTopic.name));
      });
      console.log("Pipeline started", pipelineHandle.jobId);

      console.log("Publishing");
      for (const line of lines) {
        readTopic.publish(Buffer.from(line));
      }

      console.log("Reading");
      const received: string[] = [];
      await new Promise<void>((resolve, reject) =>
        writeSubscription.on("message", (message) => {
          received.push(message.data.toString());
          if (received.length == lines.length) {
            resolve();
          }
          message.ack();
        })
      );
      assert.deepEqual(
        received.sort(),
        lines.map((s) => s.toUpperCase()).sort()
      );
    } finally {
      console.log("Cleaning up");
      await writeSubscription.delete();
      await readTopic.delete();
      await writeTopic.delete();
      if (pipelineHandle) {
        await pipelineHandle.cancel();
      }
    }
  }).timeout(600000);
});
