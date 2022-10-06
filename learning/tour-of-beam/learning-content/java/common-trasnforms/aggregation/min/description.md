# Min

Min transforms find the minimum values globally or for each key in the input collection.

You can find the global minimum value from the ```PCollection``` by using ```Min.doublesGlobally()```

```
PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Double> min = input.apply(Min.doublesGlobally());
```

Output

```
1
```

You can use ```Min.integersPerKey()``` to calculate the minimum Integer associated with each unique key (which is of type String).

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 4),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Integer>> minPerKey = input.apply(Min.integersPerKey());
```

Output

```
KV{🍆, 1}
KV{🥕, 2}
KV{🍅, 3}
```

### Description for example

Given a list of integers ```PCollection```. The ```applyTransform()``` function return minimum number from ```PCollection```.