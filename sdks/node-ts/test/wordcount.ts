import * as beam from '../src/apache_beam';
import * as runnerApi from '../src/apache_beam/proto/beam_runner_api';
import { DirectRunner } from '../src/apache_beam/runners/direct_runner'
import * as testing from '../src/apache_beam/testing/assert';

import { NodeRunner } from '../src/apache_beam/runners/node_runner/runner'
import { RemoteJobServiceClient } from "../src/apache_beam/runners/node_runner/client";


class CountElements extends beam.PTransform<beam.PCollection<any>, beam.PCollection<beam.KV<any, number>>> {
    expand(input: beam.PCollection<any>) {
        return input
            .apply(new beam.GroupBy((e) => e))  // TODO: GroupBy
            .map((kvs) => ({
                element: kvs.key,
                count: kvs.value.length  // TODO: Combine
            }));
    }
}

function wordCount(lines: beam.PCollection<string>): beam.PCollection<beam.KV<string, number>> {
        return lines
            .map((s) => s.toLowerCase())
            .flatMap(function*(line) {
                yield* line.split(/[^a-z]+/);
            })
            .apply(new CountElements("Count"));
}

describe("wordcount", function() {
    it("wordcount", async function() {
//         await new NodeRunner(new RemoteJobServiceClient('localhost:3333')).run(
        await new DirectRunner().run(
            (root) => {
                const lines = root.apply(new beam.Create([
                    "In the beginning God created the heaven and the earth.",
                    "And the earth was without form, and void; and darkness was upon the face of the deep.",
                    "And the Spirit of God moved upon the face of the waters.",
                    "And God said, Let there be light: and there was light.",
                ]));

                lines.apply(wordCount).map(console.log)
            })
    });

    it("wordcount assert", async function() {
        await new DirectRunner().run(
            (root) => {
                const lines = root.apply(new beam.Create([
                    "And God said, Let there be light: and there was light",
                ]));

                lines.apply(wordCount).apply(new testing.AssertDeepEqual([
                    { element: 'and', count: 2 },
                    { element: 'god', count: 1 },
                    { element: 'said', count: 1 },
                    { element: 'let', count: 1 },
                    { element: 'there', count: 2 },
                    { element: 'be', count: 1 },
                    { element: 'light', count: 2 },
                    { element: 'was', count: 1 },
                ]))
            })
    });

});
