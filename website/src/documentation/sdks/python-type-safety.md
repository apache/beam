---
layout: section
title: "Ensuring Python Type Safety"
section_menu: section-menu/sdks.html
permalink: /documentation/sdks/python-type-safety/
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

Python version 3.5 introduces a module called **typing** to provide hints for type validators in the language. The Beam SDK for Python, based on Python version 2.7, implements a subset of [PEP 484](https://www.python.org/dev/peps/pep-0484/) and aims to follow it as closely as possible in its own typehints module.

## Benefits of Type Hints

The Beam SDK for Python includes some automatic type checking: for example, some `PTransform`s, such as `Create` and simple `ParDo` transforms, attempt to deduce their output type given their input. However, the Beam cannot infer types in all cases. Therefore, the recommendation is that you declare type hints to aid you in performing your own type checks if necessary.

When you use type hints, the runner raises exceptions during pipeline construction time, rather than runtime. For example, the runner generates an exception if it detects that your pipeline applies mismatched `PTransforms` (where the expected outputs of one transform do not match the expected inputs of the following transform). These exceptions are raised at pipeline construction time, regardless of where your pipeline will execute. Introducing type hints for the `PTransform`s you define allows you to catch potential bugs up front in the local runner, rather than after minutes of execution into a deep, complex pipeline.

Consider the following example, in which `numbers` is a `PCollection` of `str` values:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets_test.py tag:type_hints_missing_define_numbers %}```

The code then applies a `Filter` transform to the `numbers` collection with a callable that retrieves the even numbers.

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets_test.py tag:type_hints_missing_apply %}```

When you call `p.run()`, this code generates an error because `Filter` expects a `PCollection` of integers, but is given a `PCollection` of strings instead.

## Declaring Type Hints

You can declare type hints on callables, `DoFn`s, or entire `PTransforms`. There are two ways to declare type hints: inline during pipeline construction, or as properties of the `DoFn` or `PTransform`, using decorators.

You can always declare type hints inline, but if you need them for code that is going to be reused, declare them as decorators. For example, if your `DoFn` requires an `int` input, it makes more sense to declare the type hint for the input as a property of the `DoFn` rather than inline.

### Declaring Type Hints Inline

To specify type hints inline, use the methods `with_input_types` and `with_output_types`. The following example code declares an input type hint inline:

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets_test.py tag:type_hints_takes %}```

When you apply the Filter transform to the numbers collection in the example above, you'll be able to catch the error during pipeline construction.

### Declaring Type Hints Using Decorators

To specify type hints as properties of a `DoFn` or `PTransform`, use the decorators `@with_input_types()` and `@with_output_types()`.

The following code declares an `int` type hint on `FilterEvensDoFn`, using the decorator `@with_input_types()`.

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets_test.py tag:type_hints_do_fn %}```

Decorators receive an arbitrary number of positional and/or keyword arguments, typically interpreted in the context of the function they're wrapping. Generally the first argument is a type hint for the main input, and additional arguments are type hints for side inputs.

### Defining Generic Types

You can use type hint annotations to define generic types. The following code specifies an input type hint that asserts the generic type `T`, and an output type hint that asserts the type `Tuple[int, T]`.

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets_test.py tag:type_hints_transform %}```

## Kinds of Type Hints

You can use type hints with any class, including Python primitive types, container classes, and user-defined classes. All classes, such as `int`, `float`, and user-defined classes, can be used to define type hints, called **simple type hints**. Container types such as lists, tuples, and iterables, can also be used to define type hints and are called **parameterized type hints**. Finally, there are some special types that don't correspond to any concrete Python classes, such as `Any`, `Optional`, and `Union`, that are also permitted as type hints.

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
* `Iterable[T]`
* `Iterator[T]`
* `Generator[T]`

**Note:** The `Tuple[T, U]` type hint is a tuple with a fixed number of heterogeneously typed elements, while the `Tuple[T, ...]` type hint is a tuple with a variable of homogeneously typed elements.

### Special Type Hints

The following are special type hints that don't correspond to a class, but rather to special types introduced in [PEP 484](https://www.python.org/dev/peps/pep-0484/).

* `Any`
* `Union[T, U, V]`
* `Optional[T]`


## Runtime Type Checking

In addition to using type hints for type checking at pipeline construction, you can enable runtime type checking to check that actual elements satisfy the declared type constraints during pipeline execution.

For example, the following pipeline emits elements of the wrong type. Depending on the runner implementation, its execution may or may not fail at runtime.

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets_test.py tag:type_hints_runtime_off %}```

However, if you enable runtime type checking, the code is guaranteed to fail at runtime. To enable runtime type checking, set the pipeline option `runtime_type_check` to `True`.

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets_test.py tag:type_hints_runtime_on %}```

Note that because runtime type checks are done for each `PCollection` element, enabling this feature may incur a significant performance penalty. It is therefore recommended that runtime type checks are disabled for production pipelines.

## Use of Type Hints in Coders

When your pipeline reads, writes, or otherwise materializes its data, the elements in your `PCollection` need to be encoded and decoded to and from byte strings. Byte strings are used for intermediate storage, for comparing keys in `GroupByKey` operations, and for reading from sources and writing to sinks.

The Beam SDK for Python uses Python's native support for serializing objects, a process called **pickling**, to serialize user functions. However, using the `PickleCoder` comes with several drawbacks: it is less efficient in time and space, and the encoding used is not deterministic, which hinders distributed partitioning, grouping, and state lookup.

To avoid these drawbacks, you can define `Coder` classes for encoding and decoding types in a more efficient way. You can specify a `Coder` to describe how the elements of a given `PCollection` should be encoded and decoded.

In order to be correct and efficient, a `Coder` needs type information and for `PCollection`s to be associated with a specific type. Type hints are what make this type information available. The Beam SDK for Python provides built-in coders for the standard Python types `int`, `float`, `str`, `bytes`, and `unicode`.

### Deterministic Coders

If you don't define a `Coder`, the default is `PickleCoder`, which is nondeterministic. In some cases, you must specify a deterministic `Coder` or else you will get a runtime error.

For example, suppose you have a `PCollection` of key-value pairs whose keys are `Player` objects. If you apply a `GroupByKey` transform to such a collection, its key objects might be serialized differently on different machines when a nondeterministic coder, such as the default pickle coder, is used. Since `GroupByKey` uses this serialized representation to compare keys, this may result in incorrect behavior. To ensure that the elements are always encoded and decoded in the same way, you need to define a deterministic `Coder` for the `Player` class.

The following code shows the example `Player` class and how to define a `Coder` for it. When you use type hints, Beam infers which `Coder`s to use, using `beam.coders.registry`. The following code registers `PlayerCoder` as a coder for the `Player` class. In the example, the input type declared for `CombinePerKey` is `Tuple[Player, int]`. In this case, Beam infers that the `Coder` objects to use are `TupleCoder`, `PlayerCoder`, and `IntCoder`.

```
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets_test.py tag:type_hints_deterministic_key %}```
