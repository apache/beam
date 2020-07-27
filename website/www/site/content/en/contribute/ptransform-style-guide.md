---
title: "PTransform Style Guide"
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

# PTransform Style Guide

_A style guide for writers of new reusable PTransforms._

{{< toc >}}

## Language-neutral considerations

### Consistency
Be consistent with prior art:

* Please read the [contribution guide](/contribute/).
* If there is already a similar transform in some SDK, make the API of your transform similar, so that users' experience with one of them will transfer to the other. This applies to transforms in the same-language SDK and different-language SDKs.
*Exception:* pre-existing transforms that clearly violate the current style guide for the sole reason that they were developed before this guide was ratified. In this case, the style guide takes priority over consistency with the existing transform.
* When there is no existing similar transform, stay within what is idiomatic within your language of choice (e.g. Java or Python).

### Exposing a PTransform vs. something else

So you want to develop a library that people will use in their Beam pipelines - a connector to a third-party system, a machine learning algorithm, etc. How should you expose it?

Do:

* Expose every major data-parallel task accomplished by your library as a composite `PTransform`. This allows the structure of the transform to evolve transparently to the code that uses it: e.g. something that started as a `ParDo` can become a more complex transform over time.
* Expose large, non-trivial, reusable sequential bits of the transform's code, which others might want to reuse in ways you haven't anticipated, as a regular function or class library. The transform should simply wire this logic together. As a side benefit, you can unit-test those functions and classes independently.
*Example:* when developing a transform that parses files in a custom data format, expose the format parser as a library; likewise for a transform that implements a complex machine learning algorithm, etc.
* In some cases, this may include Beam-specific classes, such as `CombineFn`, or nontrivial `DoFn`s (those that are more than just a single `@ProcessElement` method).
As a rule of thumb: expose these if you anticipate that the full packaged `PTransform` may be insufficient for a user's needs and the user may want to reuse the lower-level primitive.

Do not:

* Do not expose the exact way the transform is internally structured. E.g.: the public API surface of your library *usually* (with exception of the last bullet above) should not expose `DoFn`, concrete `Source` or `Sink` classes, etc., in order to avoid presenting users with a confusing choice between applying the `PTransform` or using the `DoFn`/`Source`/`Sink`.

### Naming

Do:

* Respect language-specific naming conventions, e.g. name classes in `PascalCase` in Java and Python, functions in `camelCase` in Java but `snake_case` in Python, etc.
* Name factory functions so that either the function name is a verb, or referring to the transform reads like a verb: e.g. `MongoDbIO.read()`, `Flatten.iterables()`.
* In typed languages, name `PTransform` classes also like verbs (e.g.: `MongoDbIO.Read`, `Flatten.Iterables`).
* Name families of transforms for interacting with a storage system using the word "IO": `MongoDbIO`, `JdbcIO`.

Do not:

* Do not use words `transform`, `source`, `sink`, `reader`, `writer`, `bound`, `unbound` in `PTransform` class names (note: `bounded` and `unbounded` are fine when referring to whether a `PCollection` is bounded or unbounded): these words are redundant, confusing, obsolete, or name an existing different concept in the SDK.

### Configuration

#### What goes into configuration vs. input collection

* **Into input `PCollection`:** anything of which there may be a very large number of instances (if there can be >1000 of it, it should be in a `PCollection`), or which is potentially not known at pipeline construction time.
E.g.: records to be processed or written to a third-party system; filenames to be read.
Exception: sometimes Beam APIs require things to be known at pipeline construction time - e.g. the `Bounded`/`UnboundedSource` API. If you absolutely have to use such an API, its input can of course go only into transform configuration.
* **Into transform configuration:** what is constant throughout the transform (including `ValueProvider`s) and does not depend on the contents of the transform's input `PCollection`s.
E.g.: a database query or connection string; credentials; a user-specified callback; a tuning parameter.
One advantage of putting a parameter into transform configuration is, it can be validated at pipeline construction time.

#### What parameters to expose

Do:

* **Expose** parameters that are necessary to compute the output.

Do not:

* **Do not expose** tuning knobs, such as batch sizes, connection pool sizes, unless it's impossible to automatically supply or compute a good-enough value (i.e., unless you can imagine a reasonable person reporting a bug about the absence of this knob).
* When developing a connector to a library that has many parameters, **do not mirror each parameter** of the underlying library - if necessary, reuse the underlying library's configuration class and let user supply a whole instance. Example: `JdbcIO`.
*Exception 1:* if some parameters of the underlying library interact with Beam semantics non-trivially, then expose them. E.g. when developing a connector to a pub/sub system that has a "delivery guarantee" parameter for publishers, expose the parameter but prohibit values incompatible with the Beam model (at-most-once and exactly-once).
*Exception 2:* if the underlying library's configuration class is cumbersome to use - e.g. does not declare a stable API, exposes problematic transitive dependencies, or does not obey [semantic versioning](https://semver.org/) - in this case, it is better to wrap it and expose a cleaner and more stable API to users of the transform.

### Error handling

#### Transform configuration errors

Detect errors early. Errors can be detected at the following stages:

* (in a compiled language) compilation of the source code of a user's pipeline
* constructing or setting up the transform
* applying the transform in a pipeline
* running the pipeline

For example:

* In a typed language, take advantage of compile-time error checking by making the API of the transform strongly-typed:
    * **Strongly-typed configuration:** e.g. in Java, a parameter that is a URL should use the `URL` class, rather than the `String` class.
    * **Strongly-typed input and output:** e.g. a transform that writes to Mongo DB should take a `PCollection<Document>` rather than `PCollection<String>` (assuming it is possible to provide a `Coder` for `Document`).
* Detect invalid values of individual parameters in setter methods.
* Detect invalid combinations of parameters in the transform's validate method.

#### Runtime errors and data consistency

Favor data consistency above everything else. Do not mask data loss or corruption. If data loss can't be prevented, fail.

Do:

* In a `DoFn`, retry transient failures if the operation is likely to succeed on retry. Perform such retries at the narrowest scope possible in order to minimize the amount of retried work (i.e. ideally at the level of the RPC library itself, or at the level of directly sending the failing RPC to a third-party system). Otherwise, let the runner retry work at the appropriate level of granularity for you (different runners may have different retry behavior, but most of them do *some* retrying).
* If the transform has side effects, strive to make them idempotent (i.e. safe to apply multiple times). Due to retries, the side effects may be executed multiple times, possibly in parallel.
* If the transform can have unprocessable (permanently failing) records and you want the pipeline to proceed despite that:
    * If bad records are safe to ignore, count the bad records in a metric. Make sure the transform's documentation mentions this aggregator. Beware that there is no programmatic access to reading the aggregator value from inside the pipeline during execution.
    * If bad records may need manual inspection by the user, emit them into an output that contains only those records.
    * Alternatively take a (default zero) threshold above which element failures become bundle failures (structure the transform to count the total number of elements and of failed elements, compare them and fail if failures are above the threshold).
* If the user requests a higher data consistency guarantee than you're able to provide, fail. E.g.: if a user requests QoS 2 (exactly-once delivery) from an MQTT connector, the connector should fail since Beam runners may retry writing to the connector and hence exactly-once delivery can't be done.

Do not:

* If you can't handle a failure, don't even catch it.
*Exception: *It may be valuable to catch the error, log a message, and rethrow it, if you're able to provide valuable context that the original error doesn't have.
* Never, ever, ever do this:
`catch(...)  { log an error; return null or false or otherwise ignore; }`
**Rule of thumb: if a bundle didn't fail, its output must be correct and complete.**
For a user, a transform that logged an error but succeeded is silent data loss.

### Performance

Many runners optimize chains of `ParDo`s in ways that improve performance if the `ParDo`s emit a small to moderate number of elements per input element, or have relatively cheap per-element processing (e.g. Dataflow's "fusion"), but limit parallelization if these assumptions are violated. In that case you may need a "fusion break" (`Reshuffle.of()`) to improve the parallelizability of processing the output `PCollection` of the `ParDo`.

* If the transform includes a `ParDo` that outputs a potentially large number of elements per input element, apply a fusion break after this `ParDo` to make sure downstream transforms can process its output in parallel.
* If the transform includes a `ParDo` that takes a very long time to process an element, insert a fusion break before this `ParDo` to make sure all or most elements can be processed in parallel regardless of how its input `PCollection` was produced.

### Documentation

Document how to configure the transform (give code examples), and what guarantees it expects about its input or provides about its output, accounting for the Beam model. E.g.:

* Are the input and output collections of this transform bounded or unbounded, or can it work with either?
* If the transform writes data to a third-party system, does it guarantee that data will be written at least once? at most once? exactly once? (how does it achieve exactly-once in case the runner executes a bundle multiple times due to retries or speculative execution a.k.a. backups?)
* If the transform reads data from a third-party system, what's the maximum potential degree of parallelism of the read? E.g., if the transform reads data sequentially (e.g. executes a single SQL query), documentation should mention that.
* If the transform is querying an external system during processing (e.g. joining a `PCollection` with information from an external key-value store), what are the guarantees on freshness of queried data: e.g. is it all loaded at the beginning of the transform, or queried per-element (in that case, what if data for a single element changes while the transform runs)?
* If there's a non-trivial relationship between arrival of items in the input `PCollection` and emitting output into the output `PCollection`, what is this relationship? (e.g. if the transform internally does windowing, triggering, grouping, or uses the state or timers API)

### Logging

Anticipate abnormal situations that a user of the transform may run into. Log information that they would have found sufficient for debugging, but limit the volume of logging. Here is some advice that applies to all programs, but is especially important when data volume is massive and execution is distributed.

Do:

* When handling an error from a third-party system, log the full error with any error details the third-party system provides about it, and include any additional context the transform knows. This enables the user to take action based on the information provided in the message. When handling an exception and rethrowing your own exception, wrap the original exception in it (some languages offer more advanced facilities, e.g. Java's "suppressed exceptions"). Never silently drop available information about an error.
* When performing a rare (not per-element) and slow operation (e.g. expanding a large file-pattern, or initiating an import/export job), log when the operation begins and ends. If the operation has an identifier, log the identifier, so the user can look up the operation for later debugging.
* When computing something low-volume that is critically important for correctness or performance of further processing, log the input and output, so a user in the process of debugging can sanity-check them or reproduce an abnormal result manually.
E.g. when expanding a filepattern into files, log what the filepattern was and how many parts it was split into; when executing a query, log the query and log how many results it produced.

Do not:

* Do not log at `INFO` per element or per bundle. `DEBUG`/`TRACE` may be okay because these levels are disabled by default.
* Avoid logging data payloads that may contain sensitive information, or sanitize them before logging (e.g. user data, credentials, etc).

### Testing

Data processing is tricky, full of corner cases, and difficult to debug, because pipelines take a long time to run, it's hard to check if the output is correct, you can't attach a debugger, and you often can't log as much as you wish to, due to high volume of data. Because of that, testing is particularly important.

#### Testing the transform's run-time behavior

* Unit-test the overall semantics of the transform using `TestPipeline` and `PAssert`. Start with testing against the direct runner. Assertions on `PCollection` contents should be strict: e.g. when a read from a database is expected to read the numbers 1 through 10, assert not just that there are 10 elements in the output `PCollection`, or that each element is in the range [1, 10] - but assert that each number 1 through 10 appears exactly once.
* Identify non-trivial sequential logic in the transform that is prone to corner cases which are difficult to reliably simulate using a `TestPipeline`, extract this logic into unit-testable functions, and unit-test them. Common corner cases are:
    * `DoFn`s processing empty bundles
    * `DoFn`s processing extremely large bundles (contents doesn't fit in memory, including "hot keys" with a very large number of values)
    * Third-party APIs failing
    * Third-party APIs providing wildly inaccurate information
    * Leaks of `Closeable`/`AutoCloseable` resources in failure cases
    * Common corner cases when developing sources: complicated arithmetic in `BoundedSource.split` (e.g. splitting key or offset ranges), iteration over empty data sources or composite data sources that have some empty components.
* Mock out the interactions with third-party systems, or better, use ["fake"](https://martinfowler.com/articles/mocksArentStubs.html) implementations when available. Make sure that the mocked-out interactions are representative of all interesting cases of the actual behavior of these systems.
* To unit test `DoFn`s, `CombineFn`s, and `BoundedSource`s, consider using `DoFnTester`, `CombineFnTester`, and `SourceTestUtils` respectively which can exercise the code in non-trivial ways to flesh out potential bugs.
* For transforms that work over unbounded collections, test their behavior in the presence of late or out-of-order data using `TestStream`.
* Tests must pass 100% of the time, including in hostile, CPU- or network-constrained environments (continuous integration servers). Never put timing-dependent code (e.g. sleeps) into tests. Experience shows that no reasonable amount of sleeping is enough - code can be suspended for more than several seconds.
* For detailed instructions on test code organization, see the [Beam Testing Guide](https://cwiki.apache.org/confluence/display/BEAM/Contribution+Testing+Guide).

#### Testing transform construction and validation

The code for constructing and validating a transform is usually trivial and mostly boilerplate. However, minor mistakes or typos in it can have serious consequences (e.g. ignoring a property that the user has set), so it needs to be tested as well. Yet, an excessive amount of trivial tests can be hard to maintain and give a false impression that the transform is well-tested.

Do:
* Test non-trivial validation code, where missing/incorrect/uninformative validation may lead to serious problems: data loss, counter-intuitive behavior, value of a property being silently ignored, or other hard-to-debug errors. Create 1 test per non-trivial class of validation error. Some examples of validation that should be tested:
    * If properties `withFoo()` and `withBar()` cannot both be specified at the same time, test that a transform specifying both of them is rejected, rather than one of the properties being silently ignored at runtime.
    * If the transform is known to behave incorrectly or counter-intuitively for a particular configuration, test that this configuration is rejected, rather than producing wrong results at runtime. For example, a transform might work properly only for bounded collections, or only for globally-windowed collections. Or, suppose a streaming system supports several levels of "quality of service", one of which is "exactly once delivery". However, a transform that writes to this system might be unable to provide exactly-once due to retries in case of failures. In that case, test that the transform disallows specifying exactly-once QoS, rather than failing to provide the expected end-to-end semantics at runtime.
* Test that each `withFoo()` method (including each overload) has effect (is not ignored), using `TestPipeline` and `PAssert` to create tests where the expected test results depend on the value of `withFoo()`.

Do not:
* Do not test successful validation (e.g. "validation does not fail when the transform is configured correctly")
* Do not test trivial validation errors (e.g. "validation fails when a property is unset/null/empty/negative/...")

### Compatibility

Do:

* Generally, follow the rules of [semantic versioning](https://semver.org/).
* If the API of the transform is not yet stable, annotate it as `@Experimental` (Java) or `@experimental` ([Python](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.utils.annotations.html)).
* If the API deprecated, annotate it as `@Deprecated` (Java) or `@deprecated` ([Python](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.utils.annotations.html)).
* Pay attention to the stability and versioning of third-party classes exposed by the transform's API: if they are unstable or improperly versioned (do not obey [semantic versioning](https://semver.org/)), it is better to wrap them in your own classes.

Do not:

* Do not silently change the behavior of the transform, in a way where code will keep compiling but will do something different than the previously documented behavior (e.g. produce different output or expect different input, of course unless the previous output was incorrect).
Strive to make such incompatible behavior changes cause a compile error (e.g. it's better to introduce a new transform for a new behavior and deprecate and then delete the old one (in a new major version), than to silently change the behavior of an existing transform), or at least a runtime error.
* If the behavior of the transform stays the same and you're merely changing implementation or API - do not change API of the transform in a way that will make a user's code stop compiling.

## Java specific considerations

Good examples for most of the practices below are `JdbcIO` and `MongoDbIO`.

### API

#### Choosing types of input and output PCollection's

Whenever possible, use types specific to the nature of the transform. People can wrap it with conversion `DoFn`s from their own types if necessary. E.g. a Datastore connector should use the Datastore `Entity` type, a MongoDb connector should use Mongo `Document` type, not a String representation of the JSON.

Sometimes that's not possible (e.g. JDBC does not provide a Beam-compatible (encodable with a Coder) "JDBC record" datatype) - then let the user provide a function for converting between the transform-specific type and a Beam-compatible type (e.g. see `JdbcIO` and `MongoDbGridFSIO`).

When the transform should logically return a composite type for which no Java class exists yet, create a new POJO class with well-named fields. Do not use generic tuple classes or `KV` (unless the fields are legitimately a key and a value).

#### Transforms with multiple output collections

If the transform needs to return multiple collections, it should be a `PTransform<..., PCollectionTuple>` and expose methods `getBlahTag()` for each collection.

E.g. if you want to return a `PCollection<Foo>` and a `PCollection<Bar>`, expose `TupleTag<Foo> getFooTag()` and `TupleTag<Bar> getBarTag()`.

For example:

{{< highlight java >}}
public class MyTransform extends PTransform<..., PCollectionTuple> {
  private final TupleTag<Moo> mooTag = new TupleTag<Moo>() {};
  private final TupleTag<Blah> blahTag = new TupleTag<Blah>() {};
  ...
  PCollectionTuple expand(... input) {
    ...
    PCollection<Moo> moo = ...;
    PCollection<Blah> blah = ...;
    return PCollectionTuple.of(mooTag, moo)
                           .and(blahTag, blah);
  }

  public TupleTag<Moo> getMooTag() {
    return mooTag;
  }

  public TupleTag<Blah> getBlahTag() {
    return blahTag;
  }
  ...
}
{{< /highlight >}}

#### Fluent builders for configuration

Make the transform class immutable, with methods to produce modified immutable objects. Use [AutoValue](https://github.com/google/auto/tree/master/value). Autovalue can provide a Builder helper class. Use `@Nullable` to mark parameters of class type that don't have a default value or whose default value is null, except for primitive types (e.g. int).

{{< highlight java >}}
@AutoValue
public abstract static class MyTransform extends PTransform<...> {
  int getMoo();
  @Nullable abstract String getBlah();

  abstract Builder toBuilder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setMoo(int moo);
    abstract Builder setBlah(String blah);

    abstract MyTransform build();
  }
  ...
}
{{< /highlight >}}

##### Factory methods

Provide a single argumentless static factory method, either in the enclosing class (see "Packaging a family of transforms") or in the transform class itself.

{{< highlight java >}}
public class Thumbs {
  public static Twiddle twiddle() {
    return new AutoValue_Thumbs_Twiddle.Builder().build();
  }

  public abstract static class Twiddle extends PTransform<...> { ... }
}

// or:
public abstract static class TwiddleThumbs extends PTransform<...> {
  public static TwiddleThumbs create() {
    return new AutoValue_Thumbs_Twiddle.Builder().build();
  }
  ...
}
{{< /highlight >}}


Exception: when transform has a single overwhelmingly most important parameter, then call the factory method `of` and put the parameter into an argument of the factory method: `ParDo.of(DoFn).withAllowedLateness()`.

##### Fluent builder methods for setting parameters

Call them `withBlah()`. All builder methods must return exactly the same type; if it's a parameterized (generic) type, with the same values of type parameters.

Treat `withBlah()` methods as an unordered set of keyword arguments - result must not depend on the order in which you call `withFoo()` and `withBar()` (e.g., `withBar()` must not read the current value of foo).

Document implications of each `withBlah` method: when to use this method at all, what values are allowed, what is the default, what are the implications of changing the value.

{{< highlight java >}}
/**
 * Returns a new {@link TwiddleThumbs} transform with moo set
 * to the given value.
 *
 * <p>Valid values are 0 (inclusive) to 100 (exclusive). The default is 42.
 *
 * <p>Higher values generally improve throughput, but increase chance
 * of spontaneous combustion.
 */
public Twiddle withMoo(int moo) {
  checkArgument(moo >= 0 && moo < 100,
      "Thumbs.Twiddle.withMoo() called with an invalid moo of %s. "
      + "Valid values are 0 (inclusive) to 100 (exclusive)",
      moo);
  return toBuilder().setMoo(moo).build();
}
{{< /highlight >}}

##### Default values for parameters

Specify them in the factory method (factory method returns an object with default values).

{{< highlight java >}}
public class Thumbs {
  public static Twiddle twiddle() {
    return new AutoValue_Thumbs_Twiddle.Builder().setMoo(42).build();
  }
  ...
}
{{< /highlight >}}

##### Packaging multiple parameters into a reusable object

If several parameters of the transform are very tightly logically coupled, sometimes it makes sense to encapsulate them into a container object. Use the same guidelines for this container object (make it immutable, use AutoValue with builders, document `withBlah()` methods, etc.). For an example, see [JdbcIO.DataSourceConfiguration](https://github.com/apache/beam/blob/master/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcIO.java).

#### Transforms with type parameters

All type parameters should be specified explicitly on factory method. Builder methods (`withBlah()`) should not change the types.

{{< highlight java >}}
public class Thumbs {
  public static Twiddle<T> twiddle() {
    return new AutoValue_Thumbs_Twiddle.Builder<T>().build();
  }

  @AutoValue
  public abstract static class Twiddle<T>
       extends PTransform<PCollection<Foo>, PCollection<Bar<T>>> {
    …
    @Nullable abstract Bar<T> getBar();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      …
      abstract Builder<T> setBar(Bar<T> bar);

      abstract Twiddle<T> build();
    }
    …
  }
}

// User code:
Thumbs.Twiddle<String> twiddle = Thumbs.<String>twiddle();
// Or:
PCollection<Bar<String>> bars = foos.apply(Thumbs.<String>twiddle() … );
{{< /highlight >}}

Exception: when the transform has a single most important parameter and this parameter depends on type T, then prefer to put it right into the factory method: e.g. `Combine.globally(SerializableFunction<Iterable<V>,V>`). This improves Java's type inference and allows the user not to specify type parameters explicitly.

When the transform has more than one type parameter, or if the meaning of the parameter is non-obvious, name the type parameters like `SomethingT`, e.g.: a PTransform implementing a classifier algorithm and assigning each input element with a label might be typed as `Classify<InputT, LabelT>`.

#### Injecting user-specified behavior

If the transform has an aspect of behavior to be customized by a user's code, make a decision as follows:

Do:

* If possible, just use PTransform composition as an extensibility device - i.e. if the same effect can be achieved by the user applying the transform in their pipeline and composing it with another `PTransform`, then the transform itself should not be extensible. E.g., a transform that writes JSON objects to a third-party system should take a `PCollection<JsonObject>` (assuming it is possible to provide a `Coder` for `JsonObject`), rather than taking a generic `PCollection<T>` and a `ProcessFunction<T, JsonObject>` (anti-example that should be fixed: `TextIO`).
* If extensibility by user code is necessary inside the transform, pass the user code as a `ProcessFunction` or define your own serializable function-like type (ideally single-method, for interoperability with Java 8 lambdas). Because Java erases the types of lambdas, you should be sure to have adequate type information even if a raw-type `ProcessFunction` is provided by the user. See `MapElements` and `FlatMapElements` for examples of how to use `ProcessFunction` and `InferableFunction` in tandem to provide good support for both lambdas and concrete subclasses with type information.

Do not:

* Do not use inheritance for extensibility: users should not subclass the `PTransform` class.

#### Packaging a family of transforms

When developing a family of highly related transforms (e.g. interacting with the same system in different ways, or providing different implementations of the same high-level task), use a top-level class as a namespace, with multiple factory methods returning transforms corresponding to each individual use case.

The container class must have a private constructor, so it can't be instantiated directly.

Document common stuff at `FooIO` level, and each factory method individually.

{{< highlight java >}}
/** Transforms for clustering data. */
public class Cluster {
  // Force use of static factory methods.
  private Cluster() {}

  /** Returns a new {@link UsingKMeans} transform. */
  public static UsingKMeans usingKMeans() { ... }
  public static Hierarchically hierarchically() { ... }

  /** Clusters data using the K-Means algorithm. */
  public static class UsingKMeans extends PTransform<...> { ... }
  public static class Hierarchically extends PTransform<...> { ... }
}

public class FooIO {
  // Force use of static factory methods.
  private FooIO() {}

  public static Read read() { ... }
  ...

  public static class Read extends PTransform<...> { ... }
  public static class Write extends PTransform<...> { ... }
  public static class Delete extends PTransform<...> { ... }
  public static class Mutate extends PTransform<...> { ... }
}
{{< /highlight >}}

When supporting multiple versions with incompatible APIs, use the version as a namespace-like class too, and put implementations of different API versions in different files.

{{< highlight java >}}
// FooIO.java
public class FooIO {
  // Force use of static factory methods.
  private FooIO() {}

  public static FooV1 v1() { return new FooV1(); }
  public static FooV2 v2() { return new FooV2(); }
}

// FooV1.java
public class FooV1 {
  // Force use of static factory methods outside the package.
  FooV1() {}
  public static Read read() { ... }
  public static class Read extends PTransform<...> { ... }
}

// FooV2.java
public static class FooV2 {
  // Force use of static factory methods outside the package.
  FooV2() {}
  public static Read read() { ... }

  public static class Read extends PTransform<...> { ... }
}
{{< /highlight >}}

### Behavior

#### Immutability

* Transform classes must be immutable: all variables must be private final and themselves immutable (e.g. if it's a list, it must be an `ImmutableList`).
* Elements of all `PCollection`s must be immutable.

#### Serialization

`DoFn`, `PTransform`, `CombineFn` and other instances will be serialized. Keep the amount of serialized data to a minimum: Mark fields that you don't want serialized as `transient`. Make classes `static` whenever possible (so that the instance doesn't capture and serialize the enclosing class instance). Note: In some cases this means that you cannot use anonymous classes.

#### Validation

* Validate individual parameters in `.withBlah()` methods using `checkArgument()`. Error messages should mention the name of the parameter, the actual value, and the range of valid values.
* Validate parameter combinations and missing required parameters in the `PTransform`'s `.expand()` method.
* Validate parameters that the `PTransform` takes from `PipelineOptions` in the `PTransform`'s `.validate(PipelineOptions)` method.
  These validations will be executed when the pipeline is already fully constructed/expanded and is about to be run with a particular `PipelineOptions`.
  Most `PTransform`s do not use `PipelineOptions` and thus don't need a `validate()` method - instead, they should perform their validation via the two other methods above.

{{< highlight java >}}
@AutoValue
public abstract class TwiddleThumbs
    extends PTransform<PCollection<Foo>, PCollection<Bar>> {
  abstract int getMoo();
  abstract String getBoo();

  ...
  // Validating individual parameters
  public TwiddleThumbs withMoo(int moo) {
    checkArgument(
        moo >= 0 && moo < 100,
        "Moo must be between 0 (inclusive) and 100 (exclusive), but was: %s",
        moo);
    return toBuilder().setMoo(moo).build();
  }

  public TwiddleThumbs withBoo(String boo) {
    checkArgument(boo != null, "Boo can not be null");
    checkArgument(!boo.isEmpty(), "Boo can not be empty");
    return toBuilder().setBoo(boo).build();
  }

  @Override
  public void validate(PipelineOptions options) {
    int woo = options.as(TwiddleThumbsOptions.class).getWoo();
    checkArgument(
       woo > getMoo(),
      "Woo (%s) must be smaller than moo (%s)",
      woo, getMoo());
  }

  @Override
  public PCollection<Bar> expand(PCollection<Foo> input) {
    // Validating that a required parameter is present
    checkArgument(getBoo() != null, "Must specify boo");

    // Validating a combination of parameters
    checkArgument(
        getMoo() == 0 || getBoo() == null,
        "Must specify at most one of moo or boo, but was: moo = %s, boo = %s",
        getMoo(), getBoo());

    ...
  }
}
{{< /highlight >}}

#### Coders

`Coder`s are a way for a Beam runner to materialize intermediate data or transmit it between workers when necessary. `Coder` should not be used as a general-purpose API for parsing or writing binary formats because the particular binary encoding of a `Coder` is intended to be its private implementation detail.

##### Providing default coders for types

Provide default `Coder`s for all new data types. Use `@DefaultCoder` annotations or `CoderProviderRegistrar` classes annotated with `@AutoService`: see usages of these classes in the SDK for examples. If performance is not important, you can use `SerializableCoder` or `AvroCoder`. Otherwise, develop an efficient custom coder (subclass `AtomicCoder` for concrete types, `StructuredCoder` for generic types).

##### Setting coders on output collections

All `PCollection`s created by your `PTransform` (both output and intermediate collections) must have a `Coder` set on them: a user should never need to call `.setCoder()` to "fix up" a coder on a `PCollection` produced by your `PTransform` (in fact, Beam intends to eventually deprecate `setCoder`). In some cases, coder inference will be sufficient to achieve this; in other cases, your transform will need to explicitly call `setCoder` on its collections.

If the collection is of a concrete type, that type usually has a corresponding coder. Use a specific most efficient coder (e.g. `StringUtf8Coder.of()` for strings, `ByteArrayCoder.of()` for byte arrays, etc.), rather than a general-purpose coder like `SerializableCoder`.

If the type of the collection involves generic type variables, the situation is more complex:
* If it coincides with the transform's input type or is a simple wrapper over it, you can reuse the coder of the input `PCollection`, available via `input.getCoder()`.
* Attempt to infer the coder via `input.getPipeline().getCoderRegistry().getCoder(TypeDescriptor)`. Use utilities in `TypeDescriptors` to obtain the `TypeDescriptor` for the generic type. For an example of this approach, see the implementation of `AvroIO.parseGenericRecords()`. However, coder inference for generic types is best-effort and in some cases it may fail due to Java type erasure.
* Always make it possible for the user to explicitly specify a `Coder` for the relevant type variable(s) as a configuration parameter of your `PTransform`. (e.g. `AvroIO.<T>parseGenericRecords().withCoder(Coder<T>)`). Fall back to inference if the coder was not explicitly specified.
