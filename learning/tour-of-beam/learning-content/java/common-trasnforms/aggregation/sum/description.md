# Sum

You can use Sum transforms to compute the sum of the elements in a collection or the sum of the values associated with each key in a collection of key-value pairs.

You can find the global sum value from the ```PCollection``` by using ```Sum.doublesGlobally()```

```
PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Double> sum = numbers.apply(Sum.doublesGlobally());
```

Output

```
55
```

You can use ```Sum.integersPerKey()```to calculate the sum Integer associated with each unique key (which is of type String).

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 4),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Integer>> sumPerKey = input.apply(Sum.integersPerKey());
```

Output

```
KV{🍆, 1}
KV{🍅, 12}
KV{🥕, 5}
```

### Description for example

Given a list of integers ```PCollection```. The ```applyTransform()``` function return sum of numbers from ```PCollection```.