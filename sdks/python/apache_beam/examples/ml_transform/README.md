<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# MLTransform Examples

This directory contains Apache Beam examples for MLTransform pipelines.

## MLTransform - Generate Vocab (Batch only)

`mltransform_generate_vocab.py` builds a vocabulary artifact from batch input
rows using `MLTransform` + `ComputeAndApplyVocabulary`.

### What it does

1. Reads input rows from JSONL (`--input_file`) or BigQuery (`--input_table`).
2. Extracts specified columns (`--columns`).
3. Normalizes and combines text values (`trim`, optional lowercasing).
4. Runs `ComputeAndApplyVocabulary` with top-k and min-frequency constraints
   using space-delimited token splitting.
5. Writes the vocabulary as one token per line.

### Required arguments

- `--output_vocab`
- `--columns`
- and one of:
  - `--input_file`
  - `--input_table`

### Optional arguments

- `--vocab_size` (default: `50000`)
- `--min_frequency` (default: `1`)
- `--lowercase` (default: `true`)
- `--input_expand_factor` (default: `1`, useful for perf/load testing)

### Local batch example

```sh
python -m apache_beam.examples.ml_transform.mltransform_generate_vocab \
  --input_file=/tmp/input.jsonl \
  --output_vocab=/tmp/vocab.txt \
  --columns=text,category \
  --vocab_size=5 \
  --min_frequency=1 \
  --lowercase=true \
  --input_expand_factor=1 \
  --runner=DirectRunner
```

### Input format

JSONL input with object rows, for example:

```json
{"id":"1","text":"Beam beam ML pipeline"}
{"id":"2","text":"Beam pipeline dataflow"}
{"id":"3","text":"ML transform beam"}
{"id":"4","text":"vocab vocab vocab test"}
{"id":"5","text":"rare_token_once"}
{"id":"6","text":""}
{"id":"7","text":null}
```

The integration tests in `mltransform_generate_vocab_test.py` generate this
sample data programmatically.

### Output format

One token per line:

1. tokens follow the vocabulary order produced by `ComputeAndApplyVocabulary`.

Example output:

```txt
beam
ml
```

For this sample and config:

```sh
--columns=text --min_frequency=2 --vocab_size=3
```

the expected output is:

```txt
beam
vocab
ml
```

### Additional test datasets

Test data for happy path and null/empty/missing columns is generated inline in
`mltransform_generate_vocab_test.py`.

### Performance testing pattern

- Small local files: functional correctness and output-stability tests.
- Large GCS files (or moderate file + `--input_expand_factor`): throughput/cost
  benchmarking on Dataflow.

