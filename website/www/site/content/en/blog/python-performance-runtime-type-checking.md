---
layout: post
title:  "Performance-Driven Runtime Type Checking for the Python SDK"
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

In this blog post, we're announcing the upcoming release of a new, opt-in
runtime type checking system for Beam's Python SDK that's optimized for performance
in both development and production environments.

But let's take a step back - why do we even care about runtime type checking
in the first place? Let's look at an example.

```
class MultiplyNumberByTwo(beam.DoFn):
    def process(self, element: int):
        return element * 2

p = Pipeline()
p | beam.Create(['1', '2'] | beam.ParDo(MultiplyNumberByTwo())
```

In this code, we passed a list of strings to a DoFn that's clearly intended for use with
integers. Luckily, this code will throw an error during pipeline construction because
the inferred output type of `beam.Create(['1', '2'])` is `str` which is incompatible with
the declared input type of `MultiplyNumberByTwo.process` which is `int`.

However, what if we turned pipeline type checking off using the `no_pipeline_type_check`
flag? Or more realistically, what if the input PCollection to `MultiplyNumberByTwo` arrived
from a database, meaning that the output data type can only be known at runtime?

In either case, no error would be thrown during pipeline construction.
And even at runtime, this code works. Each string would be multiplied by 2,
yielding a result of `['11', '22']`, but that's certainly not the outcome we want.

So how do you debug this breed of "hidden" errors? More broadly speaking, how do you debug
any typing or serialization error in Beam?

The answer is to use runtime type checking.

# Runtime Type Checking (RTC)
This feature works by checking that actual input and output values satisfy the declared
type constraints during pipeline execution. If you ran the code from before with
`runtime_type_check` on, you would receive the following error message:

```
Type hint violation for 'ParDo(MultiplyByTwo)': requires <class 'int'> but got <class 'str'> for element
```

This is an actionable error message - it tells you that either your code has a bug
or that your declared type hints are incorrect. Sounds simple enough, so what's the catch?

_It is soooo slowwwwww._ See for yourself.


| Element Size | Normal Pipeline | Runtime Type Checking Pipeline
| ------------ | --------------- | ------------------------------
| 1            | 5.3 sec         | 5.6 sec
| 2,001        | 9.4 sec         | 57.2 sec
| 10,001       | 24.5 sec        | 259.8 sec
| 18,001       | 38.7 sec        | 450.5 sec

In this micro-benchmark, the pipeline with runtime type checking was over 10x slower,
with the gap only increasing as our input PCollection increased in size.

So, is there any production-friendly alternative?

# Performance Runtime Type Check
There is! We developed a new flag called `performance_runtime_type_check` that
minimizes its footprint on the pipeline's time complexity using a combination of
- efficient Cython code,
- smart sampling techniques, and
- optimized mega type-hints.

So what do the new numbers look like?

| Element Size | Normal    | RTC        | Performance RTC
| -----------  | --------- | ---------- | ---------------
| 1            | 5.3 sec   | 5.6 sec    | 5.4 sec
| 2,001        | 9.4 sec   | 57.2 sec   | 11.2 sec
| 10,001       | 24.5 sec  | 259.8 sec  | 25.5 sec
| 18,001       | 38.7 sec  | 450.5 sec  | 39.4 sec

On average, the new Performance RTC is 4.4% slower than a normal pipeline whereas the old RTC
is over 900% slower! Additionally, as the size of the input PCollection increases, the fixed cost
of setting up the Performance RTC system is spread across each element, decreasing the relative
impact on the overall pipeline. With 18,001 elements, the difference is less than 1 second.

## How does it work?
There are three key factors responsible for this upgrade in performance.

1. Instead of type checking all values, we only type check a subset of values, known as
a sample in statistics. Initially, we sample a substantial number of elements, but as our
confidence that the element type won't change over time increases, we reduce our
sampling rate (up to a fixed minimum).

2. Whereas the old RTC system used heavy wrappers to perform the type check, the new RTC system
moves the type check to a Cython-optimized, non-decorated portion of the codebase. For reference,
Cython is a programming language that gives C-like performance to Python code.

3. Finally, we use a single mega type hint to type-check only the output values of transforms
instead of type-checking both the input and output values separately. This mega typehint is composed of
the original transform's output type constraints along with all consumer transforms' input type
constraints. Using this mega type hint allows us to reduce overhead while simultaneously allowing
us to throw _more actionable errors_. For instance, consider the following error (which was
generated from the old RTC system):
```
Runtime type violation detected within ParDo(DownstreamDoFn): Type-hint for argument: 'element' violated. Expected an instance of <class ‘str’>, instead found 9, an instance of <class ‘int’>.
```

This error tells us that the `DownstreamDoFn` received an `int` when it was expecting a `str`, but doesn't tell us
who created that `int` in the first place. Who is the offending upstream transform that's responsible for
this `int`? Presumably, _that_ transform's output type hints were too expansive (e.g. `Any`) or otherwise non-existent because
no error was thrown during the runtime type check of its output.

The problem here boils down to a lack of context. If we knew who our consumers were when type
checking our output, we could simultaneously type check our output value against our output type
constraints and every consumers' input type constraints to know whether there is _any_ possibility
for a mismatch. This is exactly what the mega type hint does, and it allows us to throw errors
at the point of declaration rather than the point of exception, saving you valuable time
while providing higher quality error messages.

So what would the same error look like using Performance RTC? It's the exact same string but with one additional line:
```
[while running 'ParDo(UpstreamDoFn)']
```

And that's much more actionable for an investigation :)

# Next Steps
Go play with the new `performance_runtime_type_check` feature!

It's in an experimental state so please
[let us know](https://beam.apache.org/community/contact-us/)
if you encounter any issues.
