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
# Partition

`Partition` is a Beam transform for `PCollection` objects that store the same data type. `Partition` splits a single `PCollection` into a fixed number of smaller collections.

`Partition` divides the elements of a `PCollection` according to a partitioning function that you provide. The partitioning function contains the logic that determines how to split up the elements of the input `PCollection` into each resulting partition `PCollection`. The number of partitions must be determined at graph construction time. You can, for example, pass the number of partitions as a command-line option at runtime (which will then be used to build your pipeline graph), but you cannot determine the number of partitions in mid-pipeline (based on data calculated after your pipeline graph is constructed, for instance).

The following example divides a `PCollection` into percentile `groups.Partition`.
{{if (eq .Sdk "go")}}
```
func decileFn(student Student) int {
	return int(float64(student.Percentile) / float64(10))
}

// Partition returns a slice of PCollections
studentsByPercentile := beam.Partition(s, 10, decileFn, students)

// Each partition can be extracted by indexing into the slice.
fortiethPercentile := studentsByPercentile[4]
```
{{end}}

{{if (eq .Sdk "java")}}
```
// Provide an int value with the desired number of result partitions, and a PartitionFn that represents the
// partitioning function. In this example, we define the PartitionFn in-line. Returns a PCollectionList
// containing each of the resulting partitions as individual PCollection objects.
PCollection<Student> input = ...;
// Split students up into 10 partitions, by percentile:
PCollectionList<Student> studentsByPercentile =
    input.apply(Partition.of(10, new PartitionFn<Student>() {
        public int partitionFor(Student student, int numPartitions) {
            return student.getPercentile()  // 0..99
                 * numPartitions / 100;
        }}));

// You can extract each partition from the PCollectionList using the get method, as follows:
PCollection<Student> fortiethPercentile = studentsByPercentile.get(4);
```
{{end}}

{{if (eq .Sdk "python")}}
```
# Provide an int value with the desired number of result partitions, and a partitioning function (partition_fn in this example).
# Returns a tuple of PCollection objects containing each of the resulting partitions as individual PCollection objects.
input = ...

def partition_fn(student, num_partitions):
  return int(get_percentile(student) * num_partitions / 100)

by_decile = input | beam.Partition(partition_fn, 10)

# You can extract each partition from the tuple of PCollection objects as follows:

fortieth_percentile = by_decile[4]
```
{{end}}

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

The `applyTransforms` returns a slice of the `PCollection`, you can access it by index. In this case, we have two `PCollections`, one consists of numbers that are less than 100, the second is more than 100.

You can also divide other types into parts, for example: "strings" and others.

{{if (eq .Sdk "go")}}
Before you start, add a dependency:
```
"strings"
```

It is necessary to divide sentences into words. To do this, we use `ParDo`:
```
func extractWords(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string, emit func(string)){
    words := strings.Split(line, " ")
		for _, k := range words {
			word := string(k)
			if word != " " {
				emit(word)
			}
		}
	}, input)
}
```

Change the type of `integers` to `strings`:
```
func applyTransform(s beam.Scope, input beam.PCollection) []beam.PCollection {
	return beam.Partition(s, 2, func(element string) int {
		if element==strings.Title(element) {
			return 0
		}
		return 1
	}, input)
}
```

It is necessary to enter `strings` of `integers` together. Make a `PCollection` of them that contains words. Divide into portions:

```
input := beam.Create(s, "Apache Beam is an open source unified programming model","To define and execute data processing pipelines","Go SDK")

words := extractWords(s,input)

output := applyTransform(s, words)

debug.Printf(s, "Upper: %v", output[0])
debug.Printf(s, "Lower: %v", output[1])
```
{{end}}

{{if (eq .Sdk "java")}}
```
static PCollectionList<String> applyTransform(PCollection<String> input) {
        return input
                .apply(Partition.of(2,
                        (PartitionFn<String>) (word, wordPartitions) -> {
                            if (word.toUpperCase().equals(word)) {
                                return 0;
                            } else {
                                return 1;
                            }
                        }));
}
```
{{end}}

{{if (eq .Sdk "python")}}
```
def partition_fn(word, word_partitions):
    if word.isupper():
        return 0
    else:
        return 1

```
{{end}}