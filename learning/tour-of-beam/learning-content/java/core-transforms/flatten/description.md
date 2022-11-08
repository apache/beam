# Flatten

`Flatten` is a Beam transform for `PCollection` objects that store the same data type. `Flatten` merges multiple `PCollection` objects into a single logical `PCollection`.  You can use it after you process different collections, eventually you will need to combine them into one `PCollection`.
The following example shows how to apply a `Flatten` transform to merge multiple `PCollection` objects.

```
// Flatten takes a PCollectionList of PCollection objects of a given type.
// Returns a single PCollection that contains all of the elements in the PCollection objects in that list.
PCollection<String> pc1 = ...;
PCollection<String> pc2 = ...;
PCollection<String> pc3 = ...;
PCollectionList<String> collections = PCollectionList.of(pc1).and(pc2).and(pc3);

PCollection<String> merged = collections.apply(Flatten.<String>pCollections());

```

### Data encoding in merged collections

By default, the coder for the output `PCollection` is the same as the coder for the first `PCollection` in the input `PCollectionList`. However, the input `PCollection` objects can each use different coders, as long as they all contain the same data type in your chosen language.

### Description for example

There are 2 string `PCollection`s at the input, one with words starting with "a" and the other with "b". Since the data type is the same using flatten you can get a combined `PCollection`