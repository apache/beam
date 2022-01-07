import * as beam from '../src/apache_beam';
import * as runnerApi from '../src/apache_beam/proto/beam_runner_api';
import { DirectRunner } from '../src/apache_beam/runners/direct_runner'
import * as testing from '../src/apache_beam/testing/assert';


class CountElements extends beam.PTransform<beam.PCollection, beam.PCollection> {
    expand(input: beam.PCollection) {
        return input
            .apply(new beam.GroupBy((e) => e))  // TODO: GroupBy
            .map((kvs) => ({
                element: kvs.key,
                count: kvs.value.length  // TODO: Combine
            }));
    }
}

class WordCount extends beam.PTransform<beam.PCollection, beam.PCollection> {
    expand(lines: beam.PCollection) {
        return lines
                    .map((s) => s.toLowerCase())
                    .flatMap(function*(line) {
                        yield* line.split(/[^a-z]+/);
                    })
                    .apply(new CountElements("Count"))
      }
}

describe("wordcount", function() {
    it("wordcount", async function() {
        await new DirectRunner().run(
            (root) => {
                const lines = root.apply(new beam.Create([
                    "In the beginning God created the heaven and the earth.",
                    "And the earth was without form, and void; and darkness was upon the face of the deep.",
                    "And the Spirit of God moved upon the face of the waters.",
                    "And God said, Let there be light: and there was light.",
                ]));

                lines.apply(new WordCount()).map(console.log)
            })
    });

    it("wordcount assert", async function() {
        await new DirectRunner().run(
            (root) => {
                const lines = root.apply(new beam.Create([
                    "And God said, Let there be light: and there was light",
                ]));

                lines.apply(new WordCount()).apply(new testing.AssertDeepEqual([
                    {element: 'and', count: 2},
                    {element: 'god', count: 1},
                    {element: 'said', count: 1},
                    {element: 'let', count: 1},
                    {element: 'there', count: 2},
                    {element: 'be', count: 1},
                    {element: 'light', count: 2},
                    {element: 'was', count: 1},
                ]))
            })
    });

});
