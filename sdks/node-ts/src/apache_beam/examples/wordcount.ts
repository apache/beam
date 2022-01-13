// TODO: Should this be in a top-level examples dir, rather than under apache_beam.

import * as beam from "../../apache_beam";
import { DirectRunner } from "../runners/direct_runner";

import { CountFn } from "../transforms/combiners";
import { GroupBy } from "../transforms/group_and_combine";

class CountElements extends beam.PTransform<
  beam.PCollection<any>,
  beam.PCollection<any>
> {
  expand(input: beam.PCollection<any>) {
    return input
      .map((e) => ({ element: e }))
      .apply(
        new GroupBy("element").combining("element", new CountFn(), "count")
      );
  }
}

function wordCount(lines: beam.PCollection<string>): beam.PCollection<any> {
  return lines
    .map((s: string) => s.toLowerCase())
    .flatMap(function* (line: string) {
      yield* line.split(/[^a-z]+/);
    })
    .apply(new CountElements("Count"));
}

async function main() {
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
}

main()
  .catch((e) => console.error(e))
  .finally(() => process.exit());
