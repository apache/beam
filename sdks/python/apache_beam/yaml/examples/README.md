# Examples Catalog

<!-- TOC -->
* [Examples Catalog](#examples-catalog)
  * [Wordcount](#wordcount)
  * [Transforms](#transforms)
    * [Element-wise](#element-wise)
    * [Aggregation](#aggregation)
<!-- TOC -->

This module contains a series of Beam YAML code samples that can be run using 
the command:
```
python -m apache_beam.yaml.main --pipeline_spec_file=/path/to/example.yaml
```

## Wordcount
A good starting place is the [Wordcount](wordcount_minimal.yaml) example under 
the root example directory.
This example reads in a text file, splits the text on each word, groups by each 
word, and counts the occurrence of each word. This is a classic example used in
the other SDK's and shows off many of the functionalities of Beam YAML.

## Transforms

Examples in this directory show off the various built-in transforms of the Beam 
YAML framework.

### Element-wise
These examples leverage the built-in mapping transforms including `MapToFields`,
`Filter` and `Explode`. More information can be found about mapping transforms
[here](../docs/yaml_mapping.md).

### Aggregation
These examples leverage the built-in `Combine` transform for performing simple 
aggregations including sum, mean, count, etc.

These examples are experimental and require that 
`yaml_experimental_features: Combine` be specified under the `options` tag, or
by passing `--yaml_experimental_features=Combine` to the command to run the 
pipeline. i.e.
```
python -m apache_beam.yaml.main \
  --pipeline_spec_file=/path/to/example.yaml \
  --yaml_experimental_features=Combine
```
More information can be found about aggregation transforms
[here](../docs/yaml_combine.md).