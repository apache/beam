---
type: languages
title: "Beam SQL extensions: SET and RESET Statement"
aliases: /documentation/dsls/sql/set/
---
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

# Beam SQL extensions: SET and RESET Pipeline Options

Beam SQL's `SET` and `RESET` statements allow the user to [configure Pipeline
Options](/documentation/programming-guide/#configuring-pipeline-options)
via the SQL shell. These are the same Pipeline Options passed to other Beam
applications on the command line in the `--<option>=<value>` format.

## Syntax

```
SET option = value
```

The SET command sets a Pipeline Option.

*   `option`: The case sensitive name of the Pipeline Option, specified as an
    [Identifier](/documentation/dsls/sql/calcite/lexical/#identifiers).
*   `value`: The case sensitive value of the Pipeline Option, specified as an
    [Identifier](/documentation/dsls/sql/calcite/lexical/#identifiers).
    For flag options that have no value on the command line, use `true`.

```
RESET option
```

The RESET command resets a Pipeline Option to its default value.

*   `option`: The case sensitive name of the Pipeline Option, specified as an
    [Identifier](/documentation/dsls/sql/calcite/lexical#identifiers).

## Common Options

*   ```SET project = `my_gcp_project` ```: Sets the default GCP project
    to`my_gcp_project`.
*   `SET runner = DataflowRunner`: Sets the pipeline to run on Dataflow.
