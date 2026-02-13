# Jinja Inheritance Example

This folder contains an example of how to use Jinja2 inheritance in Beam YAML pipelines.

## Files

*   **base/base_pipeline.yaml**: A complete WordCount pipeline (Read -> Split -> Explode -> Combine -> MapToFields -> Write). It defines a block `extra_steps` between `Explode` and `MapToFields` to allow child pipelines to inject additional transforms.
*   **wordCountInheritance.yaml**: Extends `base/base_pipeline.yaml` and injects a `Combine` transform into the `extra_steps` block to combine words.

## Running the Example

To run the child pipeline (which includes the inherited base pipeline logic + the new filter):

General setup:
```sh
export PIPELINE_FILE=apache_beam/yaml/examples/transforms/jinja/inheritance/wordCountInheritance.yaml
export KINGLEAR="gs://dataflow-samples/shakespeare/kinglear.txt"
export TEMP_LOCATION="gs://MY-BUCKET/wordCounts/"
export PROJECT="MY-PROJECT"
export REGION="MY-REGION"

cd <PATH_TO_BEAM_REPO>/beam/sdks/python
```

Multiline Run Example:
```sh
python -m apache_beam.yaml.main \
  --project=${PROJECT} \
  --region=${REGION} \
  --yaml_pipeline_file="${PIPELINE_FILE}" \
  --jinja_variables='{
    "readFromTextTransform": {"path": "'"${KINGLEAR}"'"},
    "mapToFieldsSplitConfig": {
      "language": "python",
      "fields": {
        "value": "1"
      }
    },
    "explodeTransform": {"fields": "word"},
    "combineTransform": {
      "group_by": "word",
      "combine": {"value": "sum"}
    },
    "mapToFieldsCountConfig": {
      "language": "python",
      "fields": {"output": "word + \" - \" + str(value)"}
    },
    "writeToTextTransform": {"path": "'"${TEMP_LOCATION}"'"}
  }'
```

Single Line Run Example:
```sh
python -m apache_beam.yaml.main --project=${PROJECT} --region=${REGION} \
--yaml_pipeline_file="${PIPELINE_FILE}" --jinja_variables='{"readFromTextTransform":
{"path": "'"${KINGLEAR}"'"}, "mapToFieldsSplitConfig": {"language": "python", "fields":{"value":"1"}}, "explodeTransform":{"fields":"word"}, "combineTransform":{"group_by":"word", "combine":{"value":"sum"}}, "mapToFieldsCountConfig":{"language": "python", "fields":{"output":"word + \" - \" + str(value)"}}, "writeToTextTransform":{"path":"'"${TEMP_LOCATION}"'"}}'
```

