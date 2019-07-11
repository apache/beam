---
layout: section
title: "Side input patterns"
section_menu: section-menu/documentation.html
permalink: /documentation/patterns/side-input-patterns/
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

# Side input patterns

The samples on this page show you common Beam side input patterns. A side input is an additional input that your `DoFn` can access each time it processes an element in the input `PCollection`. For more information, see the [programming guide section on side inputs]({{ site.baseurl }}/documentation/programming-guide/#side-inputs).

## Slowly updating global window side inputs

You can retrieve side inputs from global windows to use them in a pipeline job with non-global windows, like a `FixedWindow`.

To slowly update global window side inputs in pipelines with non-global windows:

1. Write a `DoFn` that periodically pulls data from a bounded source into a global window.
    
    a. Use the `GenerateSequence` source transform to periodically emit a value.

    b. Instantiate a data-driven trigger that activates on each element and pulls data from a bounded source.
    
    c. Fire the trigger to pass the data into the global window.

1. Create the side input for downstream transforms. The side input should fit into memory.

The global window side input triggers on processing time, so the main pipeline nondeterministically matches the side input to elements in event time.

For instance, the following code sample uses a `Map` to create a `DoFn`. The `Map` becomes a `View.asSingleton` side input that’s rebuilt on each counter tick. The side input updates every 5 seconds in order to demonstrate the workflow. In a real-world scenario, the side input would typically update every few hours or once per day.

```java
{% github_sample /apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java tag:SideInputPatternSlowUpdateGlobalWindowSnip1
%}
```