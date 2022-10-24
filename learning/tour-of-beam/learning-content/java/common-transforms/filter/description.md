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

### Using Filter

PCollection datasets can be filtered using the Filter transform. You can create a filter by supplying a predicate and, when applied, filtering out all the elements of PCollection that don’t satisfy the predicate.

```
PCollection<String> allStrings = pipeline
        .apply(Create.of(List.of("Hello","world","Hi")));

PCollection<String> filteredStrings = allStrings
        .apply(Filter.by(new SerializableFunction<String, Boolean>() {
            @Override
            public Boolean apply(String input) {
                return input.length() > 3;
            }
        }));
```

Output

```
Hello
world
```

### Built-in filters

The Java SDK has several filter methods built-in, like Filter.greaterThan and Filter.lessThen.  With Filter.greaterThan, the input PCollection can be filtered so that only the elements whose values are greater than the specified amount remain. Similarly, you can use Filter.lessThen to filter out elements of the input PCollection whose values are greater than the specified amount.

Other built-in filters are:

* Filter.greaterThanEq
* Filter.greaterThan
* Filter.lessThan
* Filter.lessThanEq
* Filter.equal


## Example 2: Filtering with a built-in methods

```
// List of integers
PCollection<Integer> numbers = pipeline.apply(Create.of(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));

// PCollection will contain [3, 4, 5, 6, 7, 8, 9, 10] at this point
PCollection<Integer> greaterThanEqNumbers = pipeline.apply(Filter.greaterThanEq(3));


// PCollection will contain [5, 6, 7, 8, 9, 10] at this point
PCollection<Integer> greaterThanNumbers = pipeline.apply(Filter.greaterThan(4));


// PCollection will contain [1, 2, 3, 4, 5, 6, 7, 8, 9] at this point
PCollection<Integer> lessThanNumbers = pipeline.apply(Filter.lessThan(10));


// PCollection will contain [1, 2, 3, 4 5, 6, 7] at this point
PCollection<Integer> lessThanEqNumbers = pipeline.apply(Filter.lessThanEq(7));


// PCollection will contain [9] at this point
PCollection<Integer> equalNumbers = pipeline.apply(Filter.equal(9));
```

You can find the full code of this example in the playground window, which you can run and experiment with.

`Filter` returns a number if it is divisible by 2 without remainder. You can use other method for filtering.

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.