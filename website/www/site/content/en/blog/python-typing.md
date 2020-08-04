---
layout: post
title:  "Python SDK Typing Changes"
date:   2020-05-28 00:00:01 -0800
categories:
  - blog 
  - python 
  - typing
authors:
  - chadrik
  - udim
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

Beam Python has recently increased its support and integration of Python 3 type
annotations for improved code clarity and type correctness checks.
Read on to find out what's new.

<!--more-->

Python supports type annotations on functions (PEP 484). Static type checkers,
such as mypy, are used to verify adherence to these types.
For example:
```
def f(v: int) -> int:
  return v[0]
```
Running mypy on the above code will give the error:
`Value of type "int" is not indexable`.

We've recently made changes to Beam in 2 areas:

Adding type annotations throughout Beam.  Type annotations make a large and 
sophisticated codebase like Beam easier to comprehend and navigate in your 
favorite IDE.

Second, we've added support for Python 3 type annotations. This allows SDK
users to specify a DoFn's type hints in one place. 
We've also expanded Beam's support of `typing` module types.

For more background see: 
[Ensuring Python Type Safety](https://beam.apache.org/documentation/sdks/python-type-safety/).

# Beam Is Typed

In tandem with the new type annotation support within DoFns, we've invested a
great deal of time adding type annotations to the Beam python code itself.
With this in place, we have begun using mypy, a static type 
checker, as part of Beam's code review process, which ensures higher quality 
contributions and fewer bugs.
The added context and insight that type annotations add throughout Beam is 
useful for all Beam developers, contributors and end users alike, but
it is especially beneficial for developers who are new to the project.
If you use an IDE that understands type annotations, it will provide richer
type completions and warnings than before.
You'll also be able to use your IDE to inspect the types of Beam functions and 
transforms to better understand how they work, which will ease your own 
development.
Finally, once Beam is fully annotated, end users will be able to benefit from
the use of static type analysis on their own pipelines and custom transforms.

# New Ways to Annotate

## Python 3 Syntax Annotations

Coming in Beam 2.21 (BEAM-8280), you will be able to use Python annotation
syntax to specify input and output types.

For example, this new form:
```
class MyDoFn(beam.DoFn):
  def process(self, element: int) -> typing.Text:
    yield str(element)
```
is equivalent to this:
```
@apache_beam.typehints.with_input_types(int)
@apache_beam.typehints.with_output_types(typing.Text)
class MyDoFn(beam.DoFn):
  def process(self, element):
    yield str(element)
```

One of the advantages of the new form is that you may already be using it
in tandem with a static type checker such as mypy, thus getting additional
runtime type checking for free.

This feature will be enabled by default, and there will be 2 mechanisms in
place to disable it:
1. Calling `apache_beam.typehints.disable_type_annotations()` before pipeline
construction will disable the new feature completely.
1. Decorating a function with `@apache_beam.typehints.no_annotations` will
tell Beam to ignore annotations for it. 
 
Uses of Beam's `with_input_type`, `with_output_type` methods and decorators will 
still work and take precedence over annotations.

### Sidebar

You might ask: couldn't we use mypy to type check Beam pipelines?
There are several reasons why this is not the case.
- Pipelines are constructed at runtime and may depend on information that is
only known at that time, such as a config file or database table schema.
- PCollections don't have the necessary type information, so mypy sees them as
effectively containing any element type.
This may change in in the future.
- Transforms using lambdas (ex: `beam.Map(lambda x: (1, x)`) cannot be
annotated properly using PEP 484.
However, Beam does a best-effort attempt to analyze the output type
from the bytecode.

## Typing Module Support

Python's [typing](https://docs.python.org/3/library/typing.html) module defines
types used in type annotations. This is what we call "native" types.
While Beam has its own typing types, it also supports native types.
While both Beam and native types are supported, for new code we encourage using
native typing types. Native types have  as these are supported by additional tools.

While working on Python 3 annotations syntax support, we've also discovered and
fixed issues with native type support. There may still be bugs and unsupported
native types. Please 
[let us know](https://beam.apache.org/community/contact-us/) if you encounter
issues. 
