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
### Splittable DoFn

A `Splittable DoFn` is a type of transform in Apache Beam that allows for parallel processing of a large input collection by dividing it into smaller, independent chunks.

When using a regular `DoFn`, the input collection is processed by a single worker, which can lead to performance bottlenecks and slow processing times for very large inputs. With a `Splittable DoFn`, the input collection is divided into smaller, independent chunks that can be processed by multiple workers in parallel, allowing for faster and more efficient processing.

`Splittable DoFn`s are designed to work with sources that can be efficiently divided into chunks, such as files or databases. When using a `Splittable DoFn`, the input source is first divided into a set of splits, each of which can be processed independently by a worker. The number of splits is determined dynamically based on the size of the input collection and the available resources.

Overall, `Splittable DoFn`s are a powerful tool for processing large datasets in parallel, and are an important component of many Apache Beam data processing pipelines.


{{if (eq .Sdk "go")}}
To implement `Splittable DoFn` in the Go SDK, we need to create a `struct` with methods. There are basic methods that should be implemented.

* `CreateInitialRestriction` function is used to create an initial restriction for a Splittable DoFn. function takes a `DoFn` instance, an input element, and a `Restriction` instance, and returns an initial restriction for the input element.
* `SplitRestriction` is a method that is used in `Splittable DoFn` in Apache Beam to split a restriction into smaller sub-restrictions for parallel processing.
* `RestrictionSize` is a method used in `Splittable DoFn` in Apache Beam to specify the size of a restriction.
* `CreateTracker` is a method, creates a `LockRTrackers` tracker that wraps the `offsetRange.NewTracker` for each restriction.
* `ProcessElement` is a user-defined function that is executed for each element in a `PCollection`. It is typically used in a `ParDo` transform, which applies a user-defined function to each element in the input `PCollection`, producing zero or more output elements for each input element.
```
type readFn struct {
}

func (fn *readFn) CreateInitialRestriction(_ string, size int64) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: 0,
		End:   size,
	}
}

func (fn *readFn) SplitRestriction(_ string, _ int64, rest offsetrange.Restriction) []offsetrange.Restriction {
    ...
	return splits
}

func (fn *readFn) RestrictionSize(_ string, _ int64, rest offsetrange.Restriction) float64 {
	return rest.Size()
}

func (fn *readFn) CreateTracker(rest offsetrange.Restriction) *sdf.LockRTracker {
	return sdf.NewLockRTracker(offsetrange.NewTracker(rest))
}

func (fn *readFn) ProcessElement(ctx context.Context, rt *sdf.LockRTracker, filename string, _ int64, emit func(string)) error {
	log.Infof(ctx, "Reading from %v", filename)
	...
}
```
{{end}}
{{if (eq .Sdk "java")}}
To implement `Splittable DoFn` in the Java SDK, we need to create a class with annotated methods which extend `DoFn`. There are basic annotated that should be implemented.

* `@ProcessElement` is a method-level annotation in Apache Beam that is used to indicate that a method should be executed for each element of an input `PCollection` in a `ParDo` transform.
* `@GetInitialRestriction` is a method-level annotation in Apache Beam that is used to indicate that a method should be executed to generate the initial set of restrictions for a `Splittable DoFn`.
* `@GetRestrictionCoder` -is a method-level annotation in Apache Beam that is used in `Splittable DoFn` to indicate a method that returns a `Coder` for the restriction type of a `DoFn`. A `Splittable DoFn` processes an input collection by dividing it into smaller, independent chunks called restrictions that can be processed in parallel. Each restriction is encoded and decoded by a coder, which is responsible for serializing and deserializing the restriction between workers.
* `@SplitRestriction` is a method-level annotation in Apache Beam that is used in `Splittable DoFn` to indicate a method that splits a restriction into multiple sub-restrictions for parallel processing.
```
static class SplitLinesFn extends DoFn<String, KV<Long, String>> {

        @ProcessElement
        public void process(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
            ...
        }

        @GetInitialRestriction
        public OffsetRange getInitialRestriction(@Element String e) {
            ...
        }

        @GetRestrictionCoder
        public Coder<OffsetRange> getRestrictionCoder() {
            return OffsetRange.Coder.of();
        }

        @SplitRestriction
        public void splitRestriction(@Element String input, @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> receiver) throws Exception {
            ...
        }
}
```
{{end}}
{{if (eq .Sdk "python")}}
To implement `Splittable DoFn` in the Python SDK, we need to create a class with methods which extend `DoFn`. In the `process` method, you need to add the `RestrictionParam` parameter which accepts `RestrictionProvider`. There are basic methods that should be implemented.

* `create_tracker` - is a method used in Apache Beam's `Splittable DoFn` API to create a tracker object for tracking the progress of processing elements within a bundle.
* `initial_restriction` function is used to create an initial restriction for a Splittable DoFn. function takes a `DoFn` instance, an input element, and a `OffsetRestrictionTracker` instance, and returns an initial restriction for the input element.
* `restriction_size` is a method used in `Splittable DoFn` in Apache Beam to specify the size of a restriction.
* `split` - is a method that is used in `Splittable DoFn` in Apache Beam to split a restriction into smaller sub-restrictions for parallel processing.
* `process` is a user-defined function that is executed for each element in a `PCollection`. It is typically used in a `ParDo` transform, which applies a user-defined function to each element in the input `PCollection`, producing zero or more output elements for each input element.
```
class SplitLinesFn(beam.RestrictionProvider):
    def create_tracker(self, restriction):
        ...

    def initial_restriction(self, element):
        ...

    def restriction_size(self, element, restriction):
        ...

    def split(self, element, restriction):
        ...

class MyFileSource(beam.DoFn):
    def process(self, element, restriction_tracker=DoFn.RestrictionParam(SplitLinesFn())):
        input = element.split()
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
            yield cur, input[cur]
            cur += 1
```
{{end}}

### Playground exercise

You can implement `Splittable DoFn` for processing for big data. For example, databases. Write a program that reads the database using apache beam using `Splittable DoFn`.
