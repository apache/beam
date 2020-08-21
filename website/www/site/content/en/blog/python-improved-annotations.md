---
layout: post
title:  "Improved Annotation Support for the Python SDK"
date:   2020-08-21 00:00:01 -0800
categories:
  - blog 
  - python 
  - typing
authors:
  - saavan
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

The importance of static type checking in a dynamically 
typed language like Python is not up for debate. Type hints 
allow developers to leverage a strong typing system to:
 - write better code, 
 - self-document ambiguous programming logic, and 
 - inform intelligent code completion in IDEs like PyCharm.

This is why we're excited to announce upcoming improvements to 
the `typehints` module of Beam's Python SDK, including support 
for typed PCollections and Python 3 style annotations on PTransforms.

# Improved Annotations
Today, you have the option to declare type hints on PTransforms using either
class decorators or inline functions.

For instance, a PTransform with decorated type hints might look like this:
```
@beam.typehints.with_input_types(int)
@beam.typehints.with_output_types(str)
class IntToStr(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Map(lambda num: str(num))

strings = numbers | beam.ParDo(IntToStr())
```

Using inline functions instead, the same transform would look like this:
```
class IntToStr(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Map(lambda num: str(num))

strings = numbers | beam.ParDo(IntToStr()).with_input_types(int).with_output_types(str)
```

Both methods have problems. Class decorators are syntax-heavy, 
requiring two additional lines of code, whereas inline functions provide type hints 
that aren't reusable across other instances of the same transform. Additionally, both 
methods are incompatible with static type checkers like MyPy.

With Python 3 annotations however, we can subvert these problems to provide a 
clean and reusable type hint experience. Our previous transform now looks like this:
```
class IntToStr(beam.PTransform):
    def expand(self, pcoll: PCollection[int]) -> PCollection[str]:
        return pcoll | beam.Map(lambda num: str(num))

strings = numbers | beam.ParDo(IntToStr())
```

These type hints will actively hook into the internal Beam typing system to
play a role in pipeline type checking, and runtime type checking. 

So how does this work?

## Typed PCollections
You guessed it! The PCollection class inherits from `typing.Generic`, allowing it to be 
parameterized with either zero types (denoted `PCollection`) or one type (denoted `PCollection[T]`). 
- A PCollection with zero types is implicitly converted to `PCollection[any]`.
- A PCollection with one type can have any nested type (e.g. `Union[int, str]`).

Internally, Beam's typing system makes these annotations compatible with other 
type hints by removing the outer PCollection container.

## PBegin, PDone, None
Finally, besides PCollection, a valid annotation on the `expand(...)` method of a PTransform is
`PBegin`, `PDone`, and `None`. These are generally used for I/O operations.

For instance, when saving data, your transform's output type should be `None`.
```
class SaveResults(beam.PTransform):
    def expand(self, pcoll: PCollection[str]) -> None:
        return pcoll | beam.io.WriteToBigQuery(...)
```

# Next Steps
What are you waiting for.. start using annotations on your transforms!

For more background on type hints in Python, see:
[Ensuring Python Type Safety](https://beam.apache.org/documentation/sdks/python-type-safety/). 

Finally, please 
[let us know](https://beam.apache.org/community/contact-us/) 
if you encounter any issues. 
