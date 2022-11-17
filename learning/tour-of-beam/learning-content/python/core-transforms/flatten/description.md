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
# Flatten

`Flatten` is a Beam transform for `PCollection` objects that store the same data type. `Flatten` merges multiple `PCollection` objects into a single logical `PCollection`.

The following example shows how to apply a `Flatten` transform to merge multiple `PCollection` objects.

```
# Flatten takes a tuple of PCollection objects.
# Returns a single PCollection that contains all of the elements in the PCollection objects in that tuple.

merged = (
    (pcoll1, pcoll2, pcoll3)
    # A list of tuples can be "piped" directly into a Flatten transform.
    | beam.Flatten())

```

Data encoding in merged collections
By default, the coder for the output `PCollection` is the same as the coder for the first `PCollection` in the input `PCollectionList`. However, the input `PCollection` objects can each use different coders, as long as they all contain the same data type in your chosen language.

Merging windowed collections
When using `Flatten` to merge `PCollection` objects that have a windowing strategy applied, all of the `PCollection` objects you want to merge must use a compatible windowing strategy and window sizing. For example, all the collections you’re merging must all use (hypothetically) identical 5-minute fixed windows or 4-minute sliding windows starting every 30 seconds.

If your pipeline attempts to use `Flatten` to merge `PCollection` objects with incompatible windows, Beam generates an `IllegalStateException` error when your pipeline is constructed.

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

Before you start, add a dependency:
```
"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
```

You can also combine data from a file:
```
input1 := textio.Read(s, "gs://apache-beam-samples/counts-00000-of-00003")
input2 := textio.Read(s, "gs://apache-beam-samples/counts-00001-of-00003")

output := applyTransform(s, input1, input2)
```

Overview [file1](https://storage.googleapis.com/apache-beam-samples/counts-00000-of-00003)

Overview [file2](https://storage.googleapis.com/apache-beam-samples/counts-00001-of-00003)

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.
