// TODO: Should this be in a top-level examples dir, rather than under apache_beam.

import * as beam from "../../apache_beam";
import * as textio from "../io/textio";
import { DirectRunner } from "../runners/direct_runner";

import { CountFn } from "../transforms/combiners";
import { GroupBy } from "../transforms/group_and_combine";

import { PortableRunner } from "../runners/portable_runner/runner";

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
  // python apache_beam/runners/portability/local_job_service_main.py --port 3333
  await new PortableRunner("localhost:3333").run(async (root) => {
    const lines = await root.asyncApply(
      new textio.ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
    );

    lines.apply(wordCount).map(console.log);
  });
}

main()
  .catch((e) => console.error(e))
  .finally(() => process.exit());
