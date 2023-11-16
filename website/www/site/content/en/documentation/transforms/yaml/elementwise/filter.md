---
title: "Filter"
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

# Filter

{{< localstorage language language-yaml >}}

[//]: # ({{< button-pydoc path="apache_beam.transforms.core" class="Filter" >}})

Given a predicate, filter out all elements that don't satisfy that predicate.
May also be used to filter based on an inequality with a given value based
on the comparison ordering of the element.

## Examples

`Filter` accepts a function that keeps elements that return `True`, and filters out the remaining elements.

### Example 1: Filtering with an expression

We provide an expression which returns `True` if the element's `col1` is positive, and `False` otherwise.

```
- type: Filter
  config:
    language: python
    keep: 
      expression: col1 > 0
```

This can be shorthanded by putting the expression inline with `keep`.

```
- type: Filter
  config:
    language: python
    keep: col1 > 0
```

## Related transforms

[//]: # (* [FlatMap]&#40;/documentation/transforms/python/elementwise/flatmap&#41; behaves the same as `Map`, but for)

[//]: # (  each input it might produce zero or more outputs.)

[//]: # (* [ParDo]&#40;/documentation/transforms/python/elementwise/pardo&#41; is the most general elementwise mapping)

[//]: # (  operation, and includes other abilities such as multiple output collections and side-inputs.)

[//]: # ({{< button-pydoc path="apache_beam.transforms.core" class="Filter" >}})
