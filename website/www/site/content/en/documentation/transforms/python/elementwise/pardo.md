---
title: "ParDo"
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

# ParDo

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.core" class="ParDo" >}}

A transform for generic parallel processing.
A `ParDo` transform considers each element in the input `PCollection`,
performs some processing function (your user code) on that element,
and emits zero or more elements to an output `PCollection`.

See more information in the
[Beam Programming Guide](/documentation/programming-guide/#pardo).

## Examples

In the following examples, we explore how to create custom `DoFn`s and access
the timestamp and windowing information.

### Example 1: ParDo with a simple DoFn

The following example defines a simple `DoFn` class called `SplitWords`
which stores the `delimiter` as an object field.
The `process` method is called once per element,
and it can yield zero or more output elements.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/pardo.py" pardo_dofn >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/pardo_test.py" plants >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/pardo.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/pardo" >}}

### Example 2: ParDo with timestamp and window information

In this example, we add new parameters to the `process` method to bind parameter values at runtime.

* [`beam.DoFn.TimestampParam`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn.TimestampParam)
  binds the timestamp information as an
  [`apache_beam.utils.timestamp.Timestamp`](https://beam.apache.org/releases/pydoc/current/apache_beam.utils.timestamp.html#apache_beam.utils.timestamp.Timestamp)
  object.
* [`beam.DoFn.WindowParam`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn.WindowParam)
  binds the window information as the appropriate
  [`apache_beam.transforms.window.*Window`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.window.html)
  object.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/pardo.py" pardo_dofn_params >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/pardo_test.py" dofn_params >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/pardo.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/pardo" >}}

### Example 3: ParDo with DoFn methods

A [`DoFn`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn)
can be customized with a number of methods that can help create more complex behaviors.
You can customize what a worker does when it starts and shuts down with `setup` and `teardown`.
You can also customize what to do when a
[*bundle of elements*](https://beam.apache.org/documentation/runtime/model/#bundling-and-persistence)
starts and finishes with `start_bundle` and `finish_bundle`.

* [`DoFn.setup()`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn.setup):
  Called *once per `DoFn` instance* when the `DoFn` instance is initialized.
  `setup` need not to be cached, so it could be called more than once per worker.
  This is a good place to connect to database instances, open network connections or other resources.

* [`DoFn.start_bundle()`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn.start_bundle):
  Called *once per bundle of elements* before calling `process` on the first element of the bundle.
  This is a good place to start keeping track of the bundle elements.

* [**`DoFn.process(element, *args, **kwargs)`**](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn.process):
  Called *once per element*, can *yield zero or more elements*.
  Additional `*args` or `**kwargs` can be passed through
  [`beam.ParDo()`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.ParDo).
  **[required]**

* [`DoFn.finish_bundle()`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn.finish_bundle):
  Called *once per bundle of elements* after calling `process` after the last element of the bundle,
  can *yield zero or more elements*. This is a good place to do batch calls on a bundle of elements,
  such as running a database query.

  For example, you can initialize a batch in `start_bundle`,
  add elements to the batch in `process` instead of yielding them,
  then running a batch query on those elements on `finish_bundle`, and yielding all the results.

  Note that yielded elements from `finish_bundle` must be of the type
  [`apache_beam.utils.windowed_value.WindowedValue`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/utils/windowed_value.py).
  You need to provide a timestamp as a unix timestamp, which you can get from the last processed element.
  You also need to provide a window, which you can get from the last processed element like in the example below.

* [`DoFn.teardown()`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn.teardown):
  Called *once (as a best effort) per `DoFn` instance* when the `DoFn` instance is shutting down.
  This is a good place to close database instances, close network connections or other resources.

  Note that `teardown` is called as a *best effort* and is *not guaranteed*.
  For example, if the worker crashes, `teardown` might not be called.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/pardo.py" pardo_dofn_methods >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/pardo_test.py" results >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/elementwise/pardo.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/pardo" >}}

> *Known issues:*
>
> * [[BEAM-7885]](https://issues.apache.org/jira/browse/BEAM-7885)
>   `DoFn.setup()` doesn't run for streaming jobs running in the `DirectRunner`.
> * [[BEAM-7340]](https://issues.apache.org/jira/browse/BEAM-7340)
>   `DoFn.teardown()` metrics are lost.

## Related transforms

* [Map](/documentation/transforms/python/elementwise/map) behaves the same, but produces exactly one output for each input.
* [FlatMap](/documentation/transforms/python/elementwise/flatmap) behaves the same as `Map`,
  but for each input it may produce zero or more outputs.
* [Filter](/documentation/transforms/python/elementwise/filter) is useful if the function is just
  deciding whether to output an element or not.

{{< button-pydoc path="apache_beam.transforms.core" class="Pardo" >}}
