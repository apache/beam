# Min

Provides a variety of different transforms for computing the minimum values in a collection, either globally or for each key.

You can find the global minimum value from the ```PCollection``` by using ```Min.doublesGlobally()```

```
PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Double> min = input.apply(Min.doublesGlobally());
```

Output

```
1
```

To calculate the minimum Integer associated with each unique key (which is of type String), you can use ```Min.integersPerKey()```

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
