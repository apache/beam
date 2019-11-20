---
layout: section
title: "Managing Python Pipeline Dependencies"
section_menu: section-menu/sdks.html
permalink: /documentation/sdks/python-pipeline-dependencies/
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

TODO update sdk overview page
TODO update sidebar

# Pickling in Beam

Consider this example pipeline:

TODO: possibly make this an inline snippet that gets tested.
```py
class MyAssignKeysFn(beam.DoFn):
    ...

def remove_key(element):
  return element[1]

def run()
    p = ...
    result = (p
              | beam.Filter(lambda e: e.is_foo())
              | beam.ParDo(MyAssignKeysFn)
              | beam.GroupByKey()
              | beam.FlatMap(remove_key))
```

To prepare this pipeline to run on a remote worker, the lambda, MyAssignKeysFn class,
and remove_key function are serialized (or pickled, using the dill library (TODO link)) such that the
internal state (if any) and code is available at the remote worker. This is
a different process from setting up pipeline dependencies (TODO link).

The 3 examples of user-defined-functions (UDFs) are all implemented as a `DoFn` arguments
(TODO verify) in Beam Pipelines. A ParDo can 

## Pickling and package dependencies
The above pipeline, as written, can run successfully on a remote worker without 
any additional package setup or staging.

## Pickling scope
When a parameter t TODO


## Beam pipeline elements that don't get pickled
These classes do not get pickled and need to be installed as
dependencies. (see TODO link to python-pipeline-dependencies.md) 
- Sources
- Sinks

Nested classes do not pickle well (TODO or at all?). The solution for these is
to define them at the module level. TODO what does Beam's nested class pickling support do?

## Save main session

TODO what it does, and what it doesn't

General advice is to avoid using save-main-session if you can.

TODO Alternatives to save main session
- TODO: import in `DoFn.process` method (TODO: example elsewhere in our docs)
- TODO: separate pipeline code into separate module.   

## Common pickling errors

TODO example error messages?
TODO suggested solutions

TODO recursion error

TODO module not found

TODO foo is not foo (typing module types, others?)

TODO link to SO questions?