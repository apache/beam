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

In this blog post, we are announcing a new, opt-in performance-driven runtime type check
for an upcoming release of Beam's Python SDK.

But let's take a step back - why do we even care about runtime type-checking? Let's look at an example.

```
class MultiplyNumberByTwo(beam.DoFn):
    def process(self, element: int):
        return element * 2

p = Pipeline()
p | beam.Create(['1', '2'] | beam.ParDo(MultiplyNumberByTwo())
```

In this code, we passed a list of strings to a DoFn that's clearly intended for processing
integers. Luckily, this code will throw an error during pipeline construction because
the inferred output type of `beam.Create(['1', '2'])` is `str` which is incompatible with
the declared input type hint of `MultiplyNumberByTwo.process` which is `int`.

However, what if we turned this pipeline type check off using the `no_pipeline_type_check` 
flag? Or more realistically, what if the input data to MultiplyNumberByTwo is coming 
from a database, preventing inference of the output data type?

In either case, no error would be thrown during pipeline construction. 
Even at runtime, this code works. Each string would be multiplied by 2, 
yielding a result of `['11', '22']`, but that's certainly not the outcome we want.

So how do you debug this breed of "hidden" errors? More broadly speaking, how do you
debug any error in Beam with a complex or confusing error message?

The answer is runtime type-checking.

# Runtime Type Checking
This feature works by checking that actual input and output values satisfy the declared
type constraints during pipeline execution. If you ran the code from before with RTC on, 
you would receive the following error message:

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

So, is there any production-friendly alternative for runtime type-checking?

# Performance Runtime Type Check
There is! We developed a new flag called `performance_runtime_type_check` that achieves
blazingly fast speeds using a combination of
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

On average, the new performance runtime type check is 4.4% slower than a 
normal pipeline whereas the old runtime type check is over 900% slower!

## How does it work?
There are three key factors responsible for this upgrade in performance.

First, sampling.

Second, Cython.

Finally, we use a single mega type hint to type-check only the output values of transforms
instead of type-checking the input and output values separately. The set of constraints that
form this mega typehint are the producer transform's output type constraints along with 
all producer transforms' input type constraints. Using this mega type hint allows us to reduce
overhead while simultaneously allowing us to throw _more actionable errors_.

# Next Steps
Play around with the new `performance_runtime_type_check` feature!

It's in an experimental state so please 
[let us know](https://beam.apache.org/community/contact-us/) 
if you encounter any issues. 
