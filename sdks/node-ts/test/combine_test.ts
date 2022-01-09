import * as beam from '../src/apache_beam';
import { DirectRunner } from '../src/apache_beam/runners/direct_runner'
import * as testing from '../src/apache_beam/testing/assert';
import { KV } from "../src/apache_beam/values";

import { NodeRunner } from '../src/apache_beam/runners/node_runner/runner'
import { RemoteJobServiceClient } from "../src/apache_beam/runners/node_runner/client";
import { combineGlobally, countGlobally, countPerKey } from '../src/apache_beam/transforms/combine';
import { CombineFn, keyBy } from '../src/apache_beam';


describe("Apache Beam combiners", function() {
    it("runs wordcount with a countPerKey transform and asserts the result", async function() {
        //         await new NodeRunner(new RemoteJobServiceClient('localhost:3333')).run(
        await new DirectRunner().run(
            (root) => {
                const lines = root.apply(new beam.Create([
                    "In the beginning God created the heaven and the earth.",
                    "And the earth was without form, and void; and darkness was upon the face of the deep.",
                    "And the Spirit of God moved upon the face of the waters.",
                    "And God said, Let there be light: and there was light.",
                ]));

                lines
                .map((s: string) => s.toLowerCase())
                .flatMap(function*(line: string) {
                    yield* line.split(/[^a-z]+/);
                })
                .apply(keyBy(elm => elm))
                .apply(countPerKey())
                .apply(new testing.AssertDeepEqual([
                    { key: 'in', value: 1 },
                    { key: 'the', value: 9 },
                    { key: 'beginning', value: 1 },
                    { key: 'god', value: 3 },
                    { key: 'created', value: 1 },
                    { key: 'heaven', value: 1 },
                    { key: 'and', value: 7 },
                    { key: 'earth', value: 2 },
                    { key: '', value: 4 },
                    { key: 'was', value: 3 },
                    { key: 'without', value: 1 },
                    { key: 'form', value: 1 },
                    { key: 'void', value: 1 },
                    { key: 'darkness', value: 1 },
                    { key: 'upon', value: 2 },
                    { key: 'face', value: 2 },
                    { key: 'of', value: 3 },
                    { key: 'deep', value: 1 },
                    { key: 'spirit', value: 1 },
                    { key: 'moved', value: 1 },
                    { key: 'waters', value: 1 },
                    { key: 'said', value: 1 },
                    { key: 'let', value: 1 },
                    { key: 'there', value: 2 },
                    { key: 'be', value: 1 },
                    { key: 'light', value: 2 }
                ]))
            })
    });

    it("runs wordcount with a countGlobally transform and asserts the result", async function() {
        await new DirectRunner().run(
            (root) => {
                const lines = root.apply(new beam.Create([
                    "And God said, Let there be light: and there was light",
                ]));

                lines
                .map((s: string) => s.toLowerCase())
                .flatMap(function*(line: string) {
                    yield* line.split(/[^a-z]+/);
                })
                .apply(countGlobally())
                .apply(new testing.AssertDeepEqual([11]))
            })
    });
    
    it("runs an example where a custom combine function is passed", async function() {
        type MeandAndStdDev = {mean: number, stdDev: number};
        type MeanAndStdDevAcc = {count:number, sum:number, sumOfSquares:number}
        class MedianAndUnstableStdDev implements CombineFn<number, any, MeandAndStdDev> {
            // NOTE: This Standard Deviation algorithm is **unstable**, so it is not recommended
            //    for an actual production-level pipeline.
            createAccumulator() {return {'count': 0, 'sum': 0, 'sumOfSquares': 0}}
            addInput(acc: MeanAndStdDevAcc, inp: number) {
                return {
                    count: acc.count + 1,
                    sum: acc.sum + inp,
                    sumOfSquares: acc.sumOfSquares + inp*inp
                }
            }
            mergeAccumulators(accumulators: MeanAndStdDevAcc[]) {
                return accumulators.reduce((previous, current) => ({count: previous.count + current.count, sum: previous.sum + current.sum, sumOfSquares: previous.sumOfSquares + current.sumOfSquares}))
            }
            extractOutput(acc: MeanAndStdDevAcc) {
                const mean = acc.sum / acc.count
                return {mean: mean, stdDev: (acc.sumOfSquares / acc.count) - (mean*mean)}
            }
        }
        await new DirectRunner().run(
            (root) => {
                const lines = root.apply(new beam.Create([
                    "In the beginning God created the heaven and the earth.",
                    "And the earth was without form, and void; and darkness was upon the face of the deep.",
                    "And the Spirit of God moved upon the face of the waters.",
                    "And God said, Let there be light: and there was light.",
                ]));

                lines
                .map((s: string) => s.toLowerCase())
                .flatMap(function*(line: string) {
                    yield* line.split(/[^a-z]+/);
                })
                .map(word => word.length)
                .apply(combineGlobally(new MedianAndUnstableStdDev()))
                .apply(new testing.AssertDeepEqual([{mean: 3.611111111111111, stdDev: 3.2746913580246897}]))
            })
    })
});
