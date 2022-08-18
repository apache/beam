### Using Filter

You can filter the dataset by criteria. It can also be used for equality based. Filter accepts a function that keeps elements that return True, and filters out the remaining elements.

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

The Java SDK has several filter methods built-in like ```Filter.greaterThan``` and ```Filter.lessThenEq```. Using this filter, input ```PCollection``` can be filtered such that only elements whose value is greater than specified remain.

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

