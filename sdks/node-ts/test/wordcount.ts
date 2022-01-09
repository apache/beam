import * as beam from '../src/apache_beam';
import { DirectRunner } from '../src/apache_beam/runners/direct_runner'
import * as testing from '../src/apache_beam/testing/assert';
import { KV } from "../src/apache_beam/values";

import { NodeRunner } from '../src/apache_beam/runners/node_runner/runner'
import { RemoteJobServiceClient } from "../src/apache_beam/runners/node_runner/client";
import { countPerKey } from '../src/apache_beam/transforms/combine';
import { keyBy } from '../src/apache_beam';


class CountElements extends beam.PTransform<beam.PCollection<any>, beam.PCollection<KV<any, number>>> {
    expand(input: beam.PCollection<any>) {
        return input
            .apply(keyBy((e) => e))
            .apply(countPerKey());
    }
}

function wordCount(lines: beam.PCollection<string>): beam.PCollection<beam.KV<string, number>> {
    return lines
        .map((s: string) => s.toLowerCase())
        .flatMap(function*(line: string) {
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

                lines.apply(wordCount)
                    .apply(new testing.AssertDeepEqual([
                        { key: 'and', value: 2 },
                        { key: 'god', value: 1 },
                        { key: 'said', value: 1 },
                        { key: 'let', value: 1 },
                        { key: 'there', value: 2 },
                        { key: 'be', value: 1 },
                        { key: 'light', value: 2 },
                        { key: 'was', value: 1 },
                    ]))
            })
    });

});
