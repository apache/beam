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

import * as beam from "../../src/apache_beam";
import { PCollection } from "../../src/apache_beam";
import * as combiners from "../../src/apache_beam/transforms/combiners";
import * as pardo from "../../src/apache_beam/transforms/pardo";
import { assertDeepEqual } from "../../src/apache_beam/testing/assert";

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
    //
  });
});
