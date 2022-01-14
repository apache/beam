import * as beam from "../../apache_beam";
import * as external from "../transforms/external";

export class ReadFromText extends beam.AsyncPTransform<
  beam.Root,
  beam.PCollection<string>
> {
  constructor(private filePattern: string) {
    super();
  }

  async asyncExpand(root: beam.Root) {
    return await root.asyncApply(
      new external.RawExternalTransform<
        beam.PValue<any>,
        beam.PCollection<any>
      >(
        "beam:transforms:python:fully_qualified_named",
        {
          constructor: "apache_beam.io.ReadFromText",
          kwargs: { file_pattern: this.filePattern },
        },
        // python apache_beam/runners/portability/expansion_service_main.py --fully_qualified_name_glob='*' --port 4444 --environment_type='beam:env:embedded_python:v1'
        "localhost:4444"
      )
    );
  }
}
