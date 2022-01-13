import * as beam from "../src/apache_beam";
import { DirectRunner } from "../src/apache_beam/runners/direct_runner";
import * as testing from "../src/apache_beam/testing/assert";
import { KV } from "../src/apache_beam/values";
import { GroupBy } from "../src/apache_beam/transforms/group_and_combine";
import { SumFn } from "../src/apache_beam/transforms/combiners";

import { PortableRunner } from "../src/apache_beam/runners/portable_runner/runner";

function wordCount(
  lines: beam.PCollection<string>
): beam.PCollection<beam.KV<string, number>> {
  return lines
    .map((s: string) => s.toLowerCase())
    .flatMap(function* (line: string) {
      yield* line.split(/[^a-z]+/);
    })
    .apply(new CountElements("Count"));
}

class CountElements extends beam.PTransform<
  beam.PCollection<any>,
  beam.PCollection<KV<any, number>>
> {
  expand(input: beam.PCollection<any>) {
    return input.apply(
      new GroupBy((e) => e, "element").combining((e) => 1, new SumFn(), "count")
    );
  }
}

describe("wordcount", function () {
  it("wordcount", async function () {
    //         await new PortableRunner('localhost:3333').run(
    await new DirectRunner().run((root) => {
      const lines = root.apply(
        new beam.Create([
          "In the beginning God created the heaven and the earth.",
          "And the earth was without form, and void; and darkness was upon the face of the deep.",
          "And the Spirit of God moved upon the face of the waters.",
          "And God said, Let there be light: and there was light.",
        ])
      );

      lines.apply(wordCount).map(console.log);
    });
  });

  it("wordcount assert", async function () {
    await new DirectRunner().run((root) => {
      const lines = root.apply(
        new beam.Create([
          "And God said, Let there be light: and there was light",
        ])
      );

      lines.apply(wordCount).apply(
        new testing.AssertDeepEqual([
          { element: "and", count: 2 },
          { element: "god", count: 1 },
          { element: "said", count: 1 },
          { element: "let", count: 1 },
          { element: "there", count: 2 },
          { element: "be", count: 1 },
          { element: "light", count: 2 },
          { element: "was", count: 1 },
        ])
      );
    });
  });
});
