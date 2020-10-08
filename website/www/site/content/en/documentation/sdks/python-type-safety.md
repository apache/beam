---
type: languages
title: "Ensuring Python Type Safety"
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

#  Ensuring Python Type Safety

Python is a dynamically-typed language with no static type checking. Because of the way Python's type checking works, as well as the deferred nature of runner execution, developer productivity can easily become bottle-necked by time spent investigating type-related errors.

The Apache Beam SDK for Python uses **type hints** during pipeline construction and runtime to try to emulate the correctness guarantees achieved by true static typing. Additionally, using type hints lays some groundwork that allows the backend service to perform efficient type deduction and registration of `Coder` objects.

Python version 3.5 introduces a module called **typing** to provide hints for type validators in the language.
The Beam SDK for Python implements a subset of [PEP 484](https://www.python.org/dev/peps/pep-0484/) and aims to follow it as closely as possible in its own typehints module.

These flags control Beam type safety:
- `--no_pipeline_type_check`

  Disables type checking during pipeline construction.
  Default is to perform these checks.
- `--runtime_type_check`

  Enables runtime type checking of every element.
  This may affect pipeline performance, so the default is to skip these checks.
- `--type_check_additional`

  Enables additional type checks. These are no enabled by default to preserve
  backwards compatibility. This flag accepts a comma-separate list of options:
  - `all`: Enable all additional checks.
  - `ptransform_fn`: Enable type hint decorators when used with the
    `@ptransform_fn` decorator.

## Benefits of Type Hints

When you use type hints, Beam raises exceptions during pipeline construction time, rather than runtime.
For example, Beam generates an exception if it detects that your pipeline applies mismatched `PTransforms` (where the expected outputs of one transform do not match the expected inputs of the following transform).
These exceptions are raised at pipeline construction time, regardless of where your pipeline will execute.
Introducing type hints for the `PTransforms` you define allows you to catch potential bugs up front in the local runner, rather than after minutes of execution into a deep, complex pipeline.

Consider the following example, in which `numbers` is a `PCollection` of `str` values:

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" type_hints_missing_define_numbers >}}
{{< /highlight >}}

The code then applies a `Filter` transform to the `numbers` collection with a callable that retrieves the even numbers.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" type_hints_missing_apply >}}
{{< /highlight >}}

When you call `p.run()`, this code generates an error when trying to execute this transform because `Filter` expects a `PCollection` of integers, but is given a `PCollection` of strings instead.
With type hints, this error could have been caught at pipeline construction time, before the pipeline even started running.

The Beam SDK for Python includes some automatic type hinting: for example, some `PTransforms`, such as `Create` and simple `ParDo` transforms, attempt to deduce their output type given their input.
However, Beam cannot deduce types in all cases.
Therefore, the recommendation is that you declare type hints to aid you in performing your own type checks.

## Declaring Type Hints

You can declare type hints on callables, `DoFns`, or entire `PTransforms`. There are three ways to declare type hints: inline during pipeline construction, as properties of the `DoFn` or `PTransform` using decorators, or as Python 3 type annotations on certain functions.

You can always declare type hints inline, but if you need them for code that is going to be reused, declare them as annotations or decorators.
For example, if your `DoFn` requires an `int` input, it makes more sense to declare the type hint for the input as an annotation of the arguments to `process` (or a property of the `DoFn`) rather than inline.

Using Annotations has the added benefit of allowing use of a static type checker (such as mypy) to additionally type check your code.
If you already use a type checker, using annotations instead of decorators reduces code duplication.
However, annotations do not cover all the use cases that decorators and inline declarations do.
For instance, they do not work for lambda functions.

### Declaring Type Hints Using Type Annotations

_New in version 2.21.0._

To specify type hints as annotations on certain functions, use them as usual and omit any decorator hints or inline hints.

Annotations are currently supported on:

 - `process()` methods on `DoFn` subclasses.
 - `expand()` methods on `PTransform` subclasses.
 - Functions passed to: `ParDo`, `Map`, `FlatMap`, `Filter`.

The following code declares an `int` input and a `str` output type hint on the `to_id` transform, using annotations on `my_fn`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test_py3.py" type_hints_map_annotations >}}
{{< /highlight >}}

The following code demonstrates how to use annotations on `PTransform` subclasses.
A valid annotation is a `PCollection` that wraps an internal (nested) type, `PBegin`, `PDone`, or `None`.
The following code declares typehints on a custom PTransform, that takes a `PCollection[int]` input
and outputs a `PCollection[str]`, using annotations.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test_py3.py" type_hints_ptransforms >}}
{{< /highlight >}}

The following code declares `int` input and output type hints on `filter_evens`, using annotations on `FilterEvensDoFn.process`.
Since `process` returns a generator, the output type for a DoFn producing a `PCollection[int]` is annotated as `Iterable[int]` (`Generator[int, None, None]` would also work here).
Beam will remove the outer iterable of the return type on the `DoFn.process` method and functions passed to `FlatMap` to deduce the element type of resulting PCollection .
It is an error to have a non-iterable return type annotation for these functions.
Other supported iterable types include: `Iterator`, `Generator`, `Tuple`, `List`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test_py3.py" type_hints_do_fn_annotations >}}
{{< /highlight >}}

The following code declares `int` input and output type hints on `double_evens`, using annotations on `FilterEvensDoubleDoFn.process`.
Since `process` returns a `list` or `None`, the output type is annotated as `Optional[List[int]]`.
Beam will also remove the outer `Optional` and (as above) the outer iterable of the return type, only on the `DoFn.process` method and functions passed to `FlatMap`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test_py3.py" type_hints_do_fn_annotations_optional >}}
{{< /highlight >}}

### Declaring Type Hints Inline

To specify type hints inline, use the methods `with_input_types` and `with_output_types`. The following example code declares an input type hint inline:

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" type_hints_takes >}}
{{< /highlight >}}

When you apply the Filter transform to the numbers collection in the example above, you'll be able to catch the error during pipeline construction.

### Declaring Type Hints Using Decorators

To specify type hints as properties of a `DoFn` or `PTransform`, use the decorators `@with_input_types()` and `@with_output_types()`.

The following code declares an `int` type hint on `FilterEvensDoFn`, using the decorator `@with_input_types()`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" type_hints_do_fn >}}
{{< /highlight >}}

Decorators receive an arbitrary number of positional and/or keyword arguments, typically interpreted in the context of the function they're wrapping. Generally the first argument is a type hint for the main input, and additional arguments are type hints for side inputs.

#### Disabling Annotations Use

Since this style of type hint declaration is enabled by default, here are some ways to disable it.

1. Using the `@beam.typehints.no_annotations` decorator on the specific function you want Beam to ignore annotations for.
1. Declaring type hints using the decorator or inline methods above.
These will take precedence over annotations.
1. Calling `beam.typehints.disable_type_annotations()` before pipeline creation.
This will prevent Beam from looking at annotations on all functions.

### Defining Generic Types

You can use type hint annotations to define generic types.
The following code specifies an input type hint that asserts the generic type `T`, and an output type hint that asserts the type `Tuple[int, T]`.
If the input to `MyTransform` is of type `str`, Beam will infer the output type to be `Tuple[int, str]`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" type_hints_transform >}}
{{< /highlight >}}

## Kinds of Type Hints

You can use type hints with any class, including Python primitive types, container classes, and user-defined classes. All classes, such as `int`, `float`, and user-defined classes, can be used to define type hints, called **simple type hints**. Container types such as lists, tuples, and iterables, can also be used to define type hints and are called **parameterized type hints**. Finally, there are some special types that don't correspond to any concrete Python classes, such as `Any`, `Optional`, and `Union`, that are also permitted as type hints.

Beam defines its own internal type hint types, which are still available for use for backward compatibility.
It also supports Python's typing module types, which are internally converted to Beam internal types.
> For new code, it is recommended to use [**typing**](https://docs.python.org/3/library/typing.html) module types.

### Simple Type Hints

Type hints can be of any class, from `int` and `str`, to user-defined classes. If you have a class as a type hint, you may want to define a coder for it.

### Parameterized Type Hints

Parameterized type hints are useful for hinting the types of container-like Python objects, such as `list`. These type hints further refine the elements in those container objects.

The parameters for parameterized type hints can be simple types, parameterized types, or type variables. Element types that are type variables, such as `T`, impose relationships between the inputs and outputs of an operation (for example, `List[T]` -> `T`). Type hints can be nested, allowing you to define type hints for complex types. For example, `List[Tuple[int, int, str]]`.

In order to avoid conflicting with the namespace of the built-in container types, the first letter is capitalized.

The following parameterized type hints are permitted:

* `Tuple[T, U]`
* `Tuple[T, ...]`
* `List[T]`
* `KV[T, U]`
* `Dict[T, U]`
* `Set[T]`
* `FrozenSet[T]`
* `Iterable[T]`
* `Iterator[T]`
* `Generator[T]`
* `PCollection[T]`

**Note:** The `Tuple[T, U]` type hint is a tuple with a fixed number of heterogeneously typed elements, while the `Tuple[T, ...]` type hint is a tuple with a variable of homogeneously typed elements.

### Special Type Hints

The following are special type hints that don't correspond to a class, but rather to special types introduced in [PEP 484](https://www.python.org/dev/peps/pep-0484/).

* `Any`
* `Union[T, U, V]`
* `Optional[T]`


## Runtime Type Checking

In addition to using type hints for type checking at pipeline construction, you can enable runtime type checking to check that actual elements satisfy the declared type constraints during pipeline execution.

For example, the following pipeline emits elements of the wrong type. Depending on the runner implementation, its execution may or may not fail at runtime.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" type_hints_runtime_off >}}
{{< /highlight >}}

However, if you enable runtime type checking, the code is guaranteed to fail at runtime. To enable runtime type checking, set the pipeline option `runtime_type_check` to `True`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" type_hints_runtime_on >}}
{{< /highlight >}}

Note that because runtime type checks are done for each `PCollection` element, enabling this feature may incur a significant performance penalty. It is therefore recommended that runtime type checks are disabled for production pipelines. See the following section for a quicker, production-friendly alternative.

### Faster Runtime Type Checking
You can enable faster, sampling-based runtime type checking by setting the pipeline option `performance_runtime_type_check` to `True`.

The is a Python 3 only feature that works by runtime type checking a small subset of values, called a sample, using optimized Cython code.

Currently, this feature does not support runtime type checking for side inputs or combining operations.
These are planned to be supported in a future release of Beam.

## Use of Type Hints in Coders

When your pipeline reads, writes, or otherwise materializes its data, the elements in your `PCollection` need to be encoded and decoded to and from byte strings. Byte strings are used for intermediate storage, for comparing keys in `GroupByKey` operations, and for reading from sources and writing to sinks.

The Beam SDK for Python uses Python's native support for serializing objects of unknown type, a process called **pickling**. However, using the `PickleCoder` comes with several drawbacks: it is less efficient in time and space, and the encoding used is not deterministic, which hinders distributed partitioning, grouping, and state lookup.

To avoid these drawbacks, you can define `Coder` classes for encoding and decoding types in a more efficient way. You can specify a `Coder` to describe how the elements of a given `PCollection` should be encoded and decoded.

In order to be correct and efficient, a `Coder` needs type information and for `PCollections` to be associated with a specific type. Type hints are what make this type information available. The Beam SDK for Python provides built-in coders for the standard Python types such as `int`, `float`, `str`, `bytes`, and `unicode`.

### Deterministic Coders

If you don't define a `Coder`, the default is a coder that falls back to pickling for unknown types. In some cases, you must specify a deterministic `Coder` or else you will get a runtime error.

For example, suppose you have a `PCollection` of key-value pairs whose keys are `Player` objects. If you apply a `GroupByKey` transform to such a collection, its key objects might be serialized differently on different machines when a nondeterministic coder, such as the default pickle coder, is used. Since `GroupByKey` uses this serialized representation to compare keys, this may result in incorrect behavior. To ensure that the elements are always encoded and decoded in the same way, you need to define a deterministic `Coder` for the `Player` class.

The following code shows the example `Player` class and how to define a `Coder` for it. When you use type hints, Beam infers which `Coders` to use, using `beam.coders.registry`. The following code registers `PlayerCoder` as a coder for the `Player` class. In the example, the input type declared for `CombinePerKey` is `Tuple[Player, int]`. In this case, Beam infers that the `Coder` objects to use are `TupleCoder`, `PlayerCoder`, and `IntCoder`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets_test.py" type_hints_deterministic_key >}}
{{< /highlight >}}
