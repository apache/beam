---
type: languages
title: "Beam SQL extensions: Joins"
aliases: /documentation/dsls/sql/joins/
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

# Beam SQL extensions: Joins

Supported `JOIN` types in Beam SQL:
* `INNER`, `LEFT OUTER`, `RIGHT OUTER`
* Only equijoins (where join condition is an equality check) are supported

Unsupported `JOIN` types in Beam SQL:
* `CROSS JOIN` is not supported (full cartesian product with no `ON` clause)
* `FULL OUTER JOIN` is not supported (combination of `LEFT OUTER` and `RIGHT OUTER` joins)

The scenarios of join can be categorized into 3 cases:

1. Bounded input `JOIN` bounded input
2. Unbounded input `JOIN` unbounded input
3. Unbounded input `JOIN` bounded input

## Bounded JOIN Bounded {#join-bounded-bounded}

Standard join implementation is used. All elements from one input are matched
with all elements from another input. Due to the fact that both inputs are
bounded, no windowing or triggering is involved.

## Unbounded JOIN Unbounded {#join-unbounded-unbounded}

Standard join implementation is used. All elements from one input are matched
with all elements from another input.

**Windowing and Triggering**

The following properties must be satisfied when joining unbounded inputs:

 - Inputs must have compatible windows, otherwise `IllegalArgumentException`
   will be thrown.
 - Triggers on each input should only fire once per window. Currently this
   means that the only supported trigger in this case is `DefaultTrigger` with
   zero allowed lateness. Using any other trigger will result in
   `UnsupportedOperationException` thrown.

This means that inputs are joined per-window. That is, when the trigger fires
(only once), then join is performed on all elements in the current window in
both inputs. This allows to reason about what kind of output is going to be
produced.

**Note:** similarly to `GroupByKeys` `JOIN` will update triggers using
`Trigger.continuationTrigger()`. Other aspects of the inputs' windowing
strategies remain unchanged.

## Unbounded JOIN Bounded {#join-unbounded-bounded}

For this type of `JOIN` bounded input is treated as a side-input by the
implementation. This means that window/trigger is inherented from upstreams.

