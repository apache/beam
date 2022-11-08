# Flatten

`Flatten` is a Beam transform for `PCollection` objects that store the same data type. `Flatten` merges multiple PCollection objects into a single logical `PCollection`.

The following example shows how to apply a `Flatten` transform to merge multiple `PCollection` objects.

```
// Flatten accepts any number of PCollections of the same element type.
// Returns a single PCollection that contains all of the elements in input PCollections.

merged := beam.Flatten(s, pcol1, pcol2, pcol3)
```

### Data encoding in merged collections

By default, the coder for the output `PCollection` is the same as the coder for the first `PCollection` in the input `PCollectionList`. However, the input `PCollection` objects can each use different coders, as long as they all contain the same data type in your chosen language.

### Merging windowed collections

When using `Flatten` to merge `PCollection` objects that have a windowing strategy applied, all of the `PCollection` objects you want to merge must use a compatible windowing strategy and window sizing. For example, all the collections you’re merging must all use (hypothetically) identical 5-minute fixed windows or 4-minute sliding windows starting every 30 seconds.

If your pipeline attempts to use `Flatten` to merge `PCollection` objects with incompatible windows, Beam generates an IllegalStateException error when your pipeline is constructed.

### Description for example

There are 2 string `PCollection`s at the input, one with words starting with "a" and the other with "b". Since the data type is the same using flatten you can get a combined `PCollection`.