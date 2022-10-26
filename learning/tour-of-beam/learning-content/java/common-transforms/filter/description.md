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

### Playground exercise 

You can find the complete code of the above example using 'Filter' in the playground window, which you can run and experiment with.

Filter transform can be used with both text and numerical collection. For example, let's try filtering the input collection that contains words so that only words that start with the letter 'a' are returned.

You can also chain several filter transforms to form more complex filtering based on several simple filters or implement more complex filtering logic within a single filter transform. For example, try both approaches to filter the same list of words such that only ones that start with a letter 'a' (regardless of the case) and containing more than three symbols are returned.

**Hint**

You can use the following code snippet to create an input PCollection:

Don't forget to add import:

```
import java.util.Arrays;
```

Create data for PCollection:

```
String str = "To be, or not to be: that is the question:Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune,Or to take arms against a sea of troubles,And by opposing end them. To die: to sleep";

PCollection<String> input = pipeline.apply(Create.of(Arrays.asList(str.split(" "))));
```

And filtering:

```
static PCollection<String> applyTransform(PCollection<String> input) {
        return input.apply(Filter.by(word -> word.toUpperCase().startsWith("A")));
}
```