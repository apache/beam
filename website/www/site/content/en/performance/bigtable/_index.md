---
title: "Bigtable Performance"
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

# Bigtable Performance

The following graphs show various metrics when reading from and writing to
Bigtable. See the [glossary](/performance/glossary) for definitions.

## Read

### What is the estimated cost to read from Bigtable?

{{< performance_looks io="bigtable" read_or_write="read" section="test_name" >}}

### How has various metrics changed when reading from Bigtable for different Beam SDK versions?

{{< performance_looks io="bigtable" read_or_write="read" section="version" >}}

### How has various metrics changed over time when reading from Bigtable?

{{< performance_looks io="bigtable" read_or_write="read" section="date" >}}

## Write

### What is the estimated cost to write to Bigtable?

{{< performance_looks io="bigtable" read_or_write="write" section="test_name" >}}

### How has various metrics changed when writing to Bigtable for different Beam SDK versions?

{{< performance_looks io="bigtable" read_or_write="write" section="version" >}}

### How has various metrics changed over time when writing to Bigtable?

{{< performance_looks io="bigtable" read_or_write="write" section="date" >}}
