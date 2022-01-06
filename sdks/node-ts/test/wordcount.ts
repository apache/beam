import * as beam from '../src/apache_beam';
import * as runnerApi from '../src/apache_beam/proto/beam_runner_api';


class CountElements extends beam.PTransform<beam.PCollection, beam.PCollection> {
    expand(input: beam.PCollection) {
        return input
            .map((e) => ({ key: e, value: 1 }))
            .apply(new beam.GroupByKey("GBK"))  // TODO: GroupBy
            .map((kvs) => ({
                key: kvs.key,
                count: kvs.values.reduce((a, b) => a + b)  // TODO: Combine
            }));
    }
}

describe("wordcount", function() {
    it("wordcount", async function() {
        await new beam.ProtoPrintingRunner().run(
            (root) => {
                const lines = root.apply(new beam.Create([
                    "In the beginning God created the heaven and the earth.",
                    "And the earth was without form, and void; and darkness was upon the face of the deep.",
                    "And the Spirit of God moved upon the face of the waters.",
                    "And God said, Let there be light: and there was light.",
                ]));

                lines
                    .map((s) => s.toLowerCase())
                    .flatMap(function*(line) {
                        yield* line.split(/[^a-z]/);
                    })
                    .apply(new CountElements("Count"))
                    .map(console.log);
            })
    });
});
