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

import Long from "long";

import * as beam from "../../src/apache_beam";
import { PCollection } from "../../src/apache_beam";
import {
  VarIntCoder,
  StrUtf8Coder,
} from "../../src/apache_beam/coders/standard_coders";
import { KVCoder } from "../../src/apache_beam/coders/required_coders";
import { withCoderInternal } from "../../src/apache_beam/transforms/internal";
import * as combiners from "../../src/apache_beam/transforms/combiners";
import * as pardo from "../../src/apache_beam/transforms/pardo";
import * as windowings from "../../src/apache_beam/transforms/windowings";
import * as row_coder from "../../src/apache_beam/coders/row_coder";
import { requireForSerialization } from "../../src/apache_beam/serialization";
import {
  pythonTransform,
  pythonCallable,
} from "../../src/apache_beam/transforms/python";
import * as service from "../../src/apache_beam/utils/service";
import { loopbackRunner } from "../../src/apache_beam/runners/runner";
import { assertDeepEqual } from "../../src/apache_beam/testing/assert";

let subprocessCache;
before(async function () {
  this.timeout(30000);
  subprocessCache = service.SubprocessService.createCache();
  if (process.env.BEAM_SERVICE_OVERRIDES) {
    // Start it up here so we don't timeout any individual test.
    await loopbackRunner().run(function pipeline(root) {
      root.apply(beam.impulse());
    });
  }
});

after(() => subprocessCache.stopAll());

function needs_ulr_it(name, fn) {
  return (process.env.BEAM_SERVICE_OVERRIDES ? it : it.skip)(
    name + " @ulr",
    fn
  );
}

describe("Programming Guide Tested Samples", function () {
  describe("Pipelines", function () {
    it("pipelines_constructing_creating", async function () {
      // [START pipelines_constructing_creating]
      await beam.createRunner().run(function pipeline(root) {
        // Use root to build a pipeline.
      });
      // [END pipelines_constructing_creating]
    });

    it("pipeline_options", async function () {
      const yargs = { argv: {} };
      // [START pipeline_options]
      const pipeline_options = {
        runner: "default",
        project: "my_project",
      };

      const runner = beam.createRunner(pipeline_options);

      const runnerFromCommandLineOptions = beam.createRunner(yargs.argv);
      // [END pipeline_options]
    });

    it("pipeline_options_define_custom", async function () {
      const yargs = { argv: {} };
      // [START pipeline_options_define_custom]
      const options = yargs.argv; // Or an alternative command-line parsing library.

      // Use options.input and options.output during pipeline construction.
      // [END pipeline_options_define_custom]
    });

    it("pipelines_constructing_reading", async function () {
      const textio = require("../../src/apache_beam/io/textio");
      // [START pipelines_constructing_reading]
      async function pipeline(root: beam.Root) {
        // Note that textio.ReadFromText is an AsyncPTransform.
        const pcoll: PCollection<string> = await root.applyAsync(
          textio.ReadFromText("path/to/text_pattern")
        );
      }
      // [END pipelines_constructing_reading]
    });
  });

  describe("PCollections", function () {
    it("create_pcollection", async function () {
      // [START create_pcollection]
      function pipeline(root: beam.Root) {
        const pcoll = root.apply(
          beam.create([
            "To be, or not to be: that is the question: ",
            "Whether 'tis nobler in the mind to suffer ",
            "The slings and arrows of outrageous fortune, ",
            "Or to take arms against a sea of troubles, ",
          ])
        );
      }
      // [END create_pcollection]
      await beam.createRunner().run(pipeline);
    });
  });

  describe("Transforms", function () {
    it("model_pardo", async function () {
      // [START model_pardo_pardo]
      function computeWordLengthFn(): beam.DoFn<string, number> {
        return {
          process: function* (element) {
            yield element.length;
          },
        };
      }
      // [END model_pardo_pardo]
      await beam.createRunner().run((root: beam.Root) => {
        const words = root.apply(beam.create(["a", "bb", "cccc"]));
        // [START model_pardo_apply]
        const result = words.apply(beam.parDo(computeWordLengthFn()));
        // [END model_pardo_apply]
        result.apply(assertDeepEqual([1, 2, 4]));
      });
    });

    it("model_pardo_using_flatmap", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        const words = root.apply(beam.create(["a", "bb", "cccc"]));
        // [START model_pardo_using_flatmap]
        const result = words.flatMap((word) => [word.length]);
        // [END model_pardo_using_flatmap]
        result.apply(assertDeepEqual([1, 2, 4]));
      });
    });

    it("model_pardo_using_map", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        const words = root.apply(beam.create(["a", "bb", "cccc"]));
        // [START model_pardo_using_map]
        const result = words.map((word) => word.length);
        // [END model_pardo_using_map]
        result.apply(assertDeepEqual([1, 2, 4]));
      });
    });

    it("groupby", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        const cats = [
          { word: "cat", score: 1 },
          { word: "cat", score: 5 },
        ];
        const dogs = [{ word: "dog", score: 5 }];
        function sortValues({ key, value }) {
          return { key, value: value.sort() };
        }

        const scores = root.apply(beam.create(cats.concat(dogs)));
        // [START groupby]
        // This will produce a PCollection with elements like
        //   {key: "cat", value: [{ word: "cat", score: 1 },
        //                        { word: "cat", score: 5 }, ...]}
        //   {key: "dog", value: [{ word: "dog", score: 5 }, ...]}
        const grouped_by_word = scores.apply(beam.groupBy("word"));

        // This will produce a PCollection with elements like
        //   {key: 3, value: [{ word: "cat", score: 1 },
        //                    { word: "dog", score: 5 },
        //                    { word: "cat", score: 5 }, ...]}
        const by_word_length = scores.apply(beam.groupBy((x) => x.word.length));
        // [END groupby]

        // XXX Should it be {key, values} rather than {key, value}?
        grouped_by_word //
          .map(sortValues)
          .apply(
            assertDeepEqual([
              { key: "cat", value: cats },
              { key: "dog", value: dogs },
            ])
          );

        by_word_length
          .map(beam.withName("sortLenValues", sortValues))
          .apply(
            assertDeepEqual([{ key: 3, value: cats.concat(dogs).sort() }])
          );
      });
    });

    it("cogroupbykey", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        // [START cogroupbykey_inputs]
        const emails_list = [
          { name: "amy", email: "amy@example.com" },
          { name: "carl", email: "carl@example.com" },
          { name: "julia", email: "julia@example.com" },
          { name: "carl", email: "carl@email.com" },
        ];
        const phones_list = [
          { name: "amy", phone: "111-222-3333" },
          { name: "james", phone: "222-333-4444" },
          { name: "amy", phone: "333-444-5555" },
          { name: "carl", phone: "444-555-6666" },
        ];

        const emails = root.apply(
          beam.withName("createEmails", beam.create(emails_list))
        );
        const phones = root.apply(
          beam.withName("createPhones", beam.create(phones_list))
        );
        // [END cogroupbykey_inputs]

        // [START cogroupbykey_raw_outputs]
        const results = [
          {
            name: "amy",
            values: {
              emails: [{ name: "amy", email: "amy@example.com" }],
              phones: [
                { name: "amy", phone: "111-222-3333" },
                { name: "amy", phone: "333-444-5555" },
              ],
            },
          },
          {
            name: "carl",
            values: {
              emails: [
                { name: "carl", email: "carl@example.com" },
                { name: "carl", email: "carl@email.com" },
              ],
              phones: [{ name: "carl", phone: "444-555-6666" }],
            },
          },
          {
            name: "james",
            values: {
              emails: [],
              phones: [{ name: "james", phone: "222-333-4444" }],
            },
          },
          {
            name: "julia",
            values: {
              emails: [{ name: "julia", email: "julia@example.com" }],
              phones: [],
            },
          },
        ];
        // [END cogroupbykey_raw_outputs]

        // [START cogroupbykey]
        const formatted_results_pcoll = beam
          .P({ emails, phones })
          .apply(beam.coGroupBy("name"))
          .map(function formatResults({ key, values }) {
            const emails = values.emails.map((x) => x.email).sort();
            const phones = values.phones.map((x) => x.phone).sort();
            return `${key}; [${emails}]; [${phones}]`;
          });
        // [END cogroupbykey]

        // [START cogroupbykey_formatted_outputs]
        const formatted_results = [
          "amy; [amy@example.com]; [111-222-3333,333-444-5555]",
          "carl; [carl@email.com,carl@example.com]; [444-555-6666]",
          "james; []; [222-333-4444]",
          "julia; [julia@example.com]; []",
        ];
        // [END cogroupbykey_formatted_outputs]

        formatted_results_pcoll.apply(assertDeepEqual(formatted_results));
      });
    });

    it("combine_simple_sum", async function () {
      // prettier-ignore
      await beam.createRunner().run((root: beam.Root) => {
        // [START combine_simple_sum]
        const pcoll = root.apply(beam.create([1, 10, 100, 1000]));
        const result = pcoll.apply(
          beam
            .groupGlobally()
            .combining((c) => c, (x, y) => x + y, "sum")
            .combining((c) => c, (x, y) => x * y, "product")
        );
        const expected = { sum: 1111, product: 1000000 }
        // [END combine_simple_sum]
        result.apply(assertDeepEqual([expected]));
      });
    });

    it("combine_custom_average", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        // [START combine_custom_average]
        const meanCombineFn: beam.CombineFn<number, [number, number], number> =
          {
            createAccumulator: () => [0, 0],
            addInput: ([sum, count]: [number, number], i: number) => [
              sum + i,
              count + 1,
            ],
            mergeAccumulators: (accumulators: [number, number][]) =>
              accumulators.reduce(([sum0, count0], [sum1, count1]) => [
                sum0 + sum1,
                count0 + count1,
              ]),
            extractOutput: ([sum, count]: [number, number]) => sum / count,
          };
        // [END combine_custom_average]
        // [START combine_global_average]
        const pcoll = root.apply(beam.create([4, 5, 6]));
        const result = pcoll.apply(
          beam.groupGlobally().combining((c) => c, meanCombineFn, "mean")
        );
        // [END combine_global_average]
        result.apply(assertDeepEqual([{ mean: 5 }]));
      });
    });

    it("combine_globally", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        // [START combine_globally]
        const pcoll = root.apply(
          beam.create([
            { player: "alice", accuracy: 1.0 },
            { player: "bob", accuracy: 0.99 },
            { player: "eve", accuracy: 0.5 },
            { player: "eve", accuracy: 0.25 },
          ])
        );
        const result = pcoll.apply(
          beam
            .groupGlobally()
            .combining("accuracy", combiners.mean, "mean")
            .combining("accuracy", combiners.max, "max")
        );
        const expected = [{ max: 1.0, mean: 0.685 }];
        // [END combine_globally]
        result.apply(assertDeepEqual(expected));
      });
    });

    it("combine_per_key", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        // [START combine_per_key]
        const pcoll = root.apply(
          beam.create([
            { player: "alice", accuracy: 1.0 },
            { player: "bob", accuracy: 0.99 },
            { player: "eve", accuracy: 0.5 },
            { player: "eve", accuracy: 0.25 },
          ])
        );
        const result = pcoll.apply(
          beam
            .groupBy("player")
            .combining("accuracy", combiners.mean, "mean")
            .combining("accuracy", combiners.max, "max")
        );
        const expected = [
          { player: "alice", mean: 1.0, max: 1.0 },
          { player: "bob", mean: 0.99, max: 0.99 },
          { player: "eve", mean: 0.375, max: 0.5 },
        ];
        // [END combine_per_key]
        result.apply(assertDeepEqual(expected));
      });
    });

    it("model_multiple_pcollections_flatten", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        // [START model_multiple_pcollections_flatten]
        const fib = root.apply(
          beam.withName("createFib", beam.create([1, 1, 2, 3, 5, 8]))
        );
        const pow = root.apply(
          beam.withName("createPow", beam.create([1, 2, 4, 8, 16, 32]))
        );
        const result = beam.P([fib, pow]).apply(beam.flatten());
        // [END model_multiple_pcollections_flatten]
        result.apply(assertDeepEqual([1, 1, 1, 2, 2, 3, 4, 5, 8, 8, 16, 32]));
      });
    });

    it("model_multiple_pcollections_partition", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        type Student = number;
        function getPercentile(s: Student) {
          return s;
        }
        const students = root.apply(beam.create([1, 5, 50, 98, 99]));
        // [START model_multiple_pcollections_partition]
        const deciles: PCollection<Student>[] = students.apply(
          beam.partition(
            (student, numPartitions) =>
              Math.floor((getPercentile(student) / 100) * numPartitions),
            10
          )
        );
        const topDecile: PCollection<Student> = deciles[9];
        // [END model_multiple_pcollections_partition]
        topDecile.apply(assertDeepEqual([98, 99]));
      });
    });

    it("model_pardo_side_input", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        const words = root.apply(beam.create(["a", "bb", "cccc"]));
        // [START model_pardo_side_input]
        // meanLengthPColl will contain a single number whose value is the
        // average length of the words
        const meanLengthPColl: PCollection<number> = words
          .apply(
            beam
              .groupGlobally<string>()
              .combining((word) => word.length, combiners.mean, "mean")
          )
          .map(({ mean }) => mean);

        // Now we use this as a side input to yield only words that are
        // smaller than average.
        const smallWords = words.flatMap(
          // This is the function, taking context as a second argument.
          function* keepSmall(word, context) {
            if (word.length < context.meanLength.lookup()) {
              yield word;
            }
          },
          // This is the context that will be passed as a second argument.
          { meanLength: pardo.singletonSideInput(meanLengthPColl) }
        );
        // [END model_pardo_side_input]
        smallWords.apply(assertDeepEqual(["a", "bb"]));
      });
    });

    it("model_multiple_output", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        const words = root.apply(beam.create(["a", "bb", "cccccc", "xx"]));
        function isMarkedWord(word) {
          return word.includes("x");
        }

        // [START model_multiple_output_dofn]
        const to_split = words.flatMap(function* (word) {
          if (word.length < 5) {
            yield { below: word };
          } else {
            yield { above: word };
          }
          if (isMarkedWord(word)) {
            yield { marked: word };
          }
        });
        // [END model_multiple_output_dofn]

        // [START model_multiple_output]
        const { below, above, marked } = to_split.apply(
          beam.split(["below", "above", "marked"])
        );
        // [END model_multiple_output]

        below.apply(assertDeepEqual(["a", "bb", "xx"]));
        above.apply(assertDeepEqual(["cccccc"]));
        marked.apply(assertDeepEqual(["xx"]));
      });
    });

    it("other_params", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        const pcoll = root.apply(beam.create(["a", "b", "c"]));
        {
          // [START timestamp_param]
          function processFn(element, context) {
            return context.timestamp.lookup();
          }

          pcoll.map(processFn, { timestamp: pardo.timestampParam() });
          // [END timestamp_param]
        }

        {
          function processWithWindow(element, context) {}
          function processWithPaneInfo(element, context) {}

          // [START window_param]
          pcoll.map(processWithWindow, { timestamp: pardo.windowParam() });
          // [END window_param]

          // [START pane_info_param]
          pcoll.map(processWithPaneInfo, { timestamp: pardo.paneInfoParam() });
          // [END pane_info_param]
        }
      });
    });

    it("countwords_composite", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        const lines = root.apply(beam.create(["a b", "b c c c"]));
        // [START countwords_composite]
        function countWords(lines: PCollection<string>) {
          return lines //
            .map((s: string) => s.toLowerCase())
            .flatMap(function* splitWords(line: string) {
              yield* line.split(/[^a-z]+/);
            })
            .apply(beam.countPerElement());
        }

        const counted = lines.apply(countWords);
        // [END countwords_composite]
        counted.apply(
          assertDeepEqual([
            { element: "a", count: 1 },
            { element: "b", count: 2 },
            { element: "c", count: 3 },
          ])
        );
      });
    });
  });

  describe("Coders", function () {
    it("with_row_coder", async function () {
      await beam.createRunner().run((root: beam.Root) => {
        const elements = [
          { intFieldName: 1, stringFieldName: "foo" },
          { intFieldName: 2, stringFieldName: "bar" },
        ];
        const pcoll = root.apply(beam.create(elements));
        // [START with_row_coder]
        const result = pcoll.apply(
          beam.withRowCoder({ intFieldName: 0, stringFieldName: "" })
        );
        // [END with_row_coder]
        result.apply(assertDeepEqual(elements));
      });
    });
  });

  describe("Windowing", function () {
    it("fixed_windows", async function () {
      await beam.createRunner().run(async (root: beam.Root) => {
        const pcoll = root
          .apply(beam.create([1, 2, 3, 4, 5, 60, 61, 62]))
          .apply(beam.assignTimestamps((t) => Long.fromValue(t * 1000)));

        // [START setting_fixed_windows]
        pcoll
          .apply(beam.windowInto(windowings.fixedWindows(60)))
          // [END setting_fixed_windows]
          .apply(beam.groupBy((e: number) => ""))
          .apply(
            assertDeepEqual([
              { key: "", value: [1, 2, 3, 4, 5] },
              { key: "", value: [60, 61, 62] },
            ])
          );
      });
    });

    it("sliding_windows", async function () {
      await beam.createRunner().run(async (root: beam.Root) => {
        const pcoll = root
          .apply(beam.create([1, 2, 3, 4, 5, 12]))
          .apply(beam.assignTimestamps((t) => Long.fromValue(t * 1000)));

        // [START setting_sliding_windows]
        pcoll
          .apply(beam.windowInto(windowings.slidingWindows(30, 5)))
          // [END setting_sliding_windows]
          .apply(beam.groupBy((e: number) => ""))
          .apply(
            assertDeepEqual([
              { key: "", value: [1, 2, 3, 4, 5, 12] },
              { key: "", value: [1, 2, 3, 4, 5, 12] },
              { key: "", value: [1, 2, 3, 4, 5, 12] },
              { key: "", value: [1, 2, 3, 4, 5, 12] },
              { key: "", value: [1, 2, 3, 4, 5] },
              { key: "", value: [1, 2, 3, 4] },
              { key: "", value: [5, 12] },
              { key: "", value: [12] },
            ])
          );
      });
    });

    needs_ulr_it("session_windows", async function () {
      await beam.createRunner().run(async (root: beam.Root) => {
        const pcoll = root
          .apply(beam.create([1, 2, 600, 1800, 1900]))
          .apply(beam.assignTimestamps((t) => Long.fromValue(t * 1000)));

        // [START setting_session_windows]
        pcoll
          .apply(beam.windowInto(windowings.sessions(10 * 60)))
          // [END setting_session_windows]
          .apply(beam.groupBy((e: number) => ""))
          .apply(
            assertDeepEqual([
              { key: "", value: [1, 2, 600] },
              { key: "", value: [1800, 1900] },
            ])
          );
      });
    }).timeout(10000);

    it("global_windows", async function () {
      await beam.createRunner().run(async (root: beam.Root) => {
        const pcoll = root
          .apply(beam.create([1, 2, 3, 4, 5, 60, 61, 62]))
          .apply(beam.assignTimestamps((t) => Long.fromValue(t * 1000)))
          // So setting global windows is non-trivial.
          .apply(beam.windowInto(windowings.fixedWindows(60)));
        // [START setting_global_window]
        pcoll
          .apply(beam.windowInto(windowings.globalWindows()))
          // [END setting_global_window]
          .apply(beam.groupBy((e: number) => ""))
          .apply(
            assertDeepEqual([{ key: "", value: [1, 2, 3, 4, 5, 60, 61, 62] }])
          );
      });
    });
  });

  describe("Schemas and Coders", function () {
    it("schema_def", async function () {
      await beam.createRunner().run(async (root: beam.Root) => {
        // [START schema_def]
        const pcoll = root
          .apply(
            beam.create([
              { intField: 1, stringField: "a" },
              { intField: 2, stringField: "b" },
            ])
          )
          // Let beam know the type of the elements by providing an exemplar.
          .apply(beam.withRowCoder({ intField: 0, stringField: "" }));
        // [END schema_def]
      });
    });

    it("logical_types", async function () {
      await beam.createRunner().run(async (root: beam.Root) => {
        // [START schema_logical_register]
        class Foo {
          constructor(public value: string) {}
        }
        requireForSerialization("apache-beam", { Foo });
        row_coder.registerLogicalType({
          urn: "beam:logical_type:typescript_foo:v1",
          reprType: row_coder.RowCoder.inferTypeFromJSON("string", false),
          toRepr: (foo) => foo.value,
          fromRepr: (value) => new Foo(value),
        });
        // [END schema_logical_register]

        // [START schema_logical_use]
        const pcoll = root
          .apply(beam.create([new Foo("a"), new Foo("b")]))
          // Use beamLogicalType in the exemplar to indicate its use.
          .apply(
            beam.withRowCoder({
              beamLogicalType: "beam:logical_type:typescript_foo:v1",
            } as any)
          );
        // [END schema_logical_use]
      });
    });
  });

  // TODO: Enable cross-language after next release or set up dev virtual env.
  describe("MultiLangauge", function () {
    needs_ulr_it("python_map", async function () {
      await beam.createRunner().run(async (root: beam.Root) => {
        const pcoll = root.apply(
          beam.create([
            { a: 1, b: 10 },
            { a: 2, b: 20 },
          ])
        );
        // [START python_map]
        const result: PCollection<number> = await pcoll
          .apply(
            beam.withName("UpdateCoder1", beam.withRowCoder({ a: 0, b: 0 }))
          )
          .applyAsync(
            pythonTransform(
              // Fully qualified name
              "apache_beam.transforms.Map",
              // Positional arguments
              [pythonCallable("lambda x: x.a + x.b")],
              // Keyword arguments
              {},
              // Output type if it cannot be inferred
              { requestedOutputCoders: { output: new VarIntCoder() } }
            )
          );
        // [END python_map]
        result.apply(assertDeepEqual([11, 22]));
      });
    }).timeout(10000);

    needs_ulr_it("stateful_dofn", async function () {
      await beam.createRunner().run(async (root: beam.Root) => {
        // [START stateful_dofn]
        const pcoll = root.apply(
          beam.create([
            { key: "a", value: 1 },
            { key: "b", value: 10 },
            { key: "a", value: 100 },
          ])
        );
        const result: PCollection<number> = await pcoll
          .apply(
            withCoderInternal(
              new KVCoder(new StrUtf8Coder(), new VarIntCoder())
            )
          )
          .applyAsync(
            pythonTransform(
              // Construct a new Transform from source.
              "__constructor__",
              [
                pythonCallable(`
                # Define a DoFn to be used below.
                class ReadModifyWriteStateDoFn(beam.DoFn):
                  STATE_SPEC = beam.transforms.userstate.ReadModifyWriteStateSpec(
                      'num_elements', beam.coders.VarIntCoder())

                  def process(self, element, state=beam.DoFn.StateParam(STATE_SPEC)):
                    current_value = state.read() or 0
                    state.write(current_value + 1)
                    yield current_value + 1

                class MyPythonTransform(beam.PTransform):
                  def expand(self, pcoll):
                    return pcoll | beam.ParDo(ReadModifyWriteStateDoFn())
              `),
              ],
              // Keyword arguments to pass to the transform, if any.
              {},
              // Output type if it cannot be inferred
              { requestedOutputCoders: { output: new VarIntCoder() } }
            )
          );
        // [END stateful_dofn]
        result.apply(assertDeepEqual([1, 1, 2]));
      });
    }).timeout(10000);

    needs_ulr_it("cross_lang_transform", async function () {
      await beam.createRunner().run(async (root: beam.Root) => {
        const pcoll = root.apply(beam.create(["a", "bb"]));
        // [START cross_lang_transform]
        const result: PCollection<string> = await pcoll
          .apply(withCoderInternal(new StrUtf8Coder()))
          .applyAsync(
            pythonTransform(
              // Define an arbitrary transform from a callable.
              "__callable__",
              [
                pythonCallable(`
              def apply(pcoll, prefix, postfix):
                return pcoll | beam.Map(lambda s: prefix + s + postfix)
              `),
              ],
              // Keyword arguments to pass above, if any.
              { prefix: "x", postfix: "y" },
              // Output type if it cannot be inferred
              { requestedOutputCoders: { output: new StrUtf8Coder() } }
            )
          );
        // [END cross_lang_transform]
        result.apply(assertDeepEqual(["xay", "xbby"]));
      });
    }).timeout(10000);
  });
});
