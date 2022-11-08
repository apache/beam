# FlatMap elements 

It works like `Map elements` , but inside the logic you can do complex operations like dividing the list into separate elements and processing

```
static PCollection<String> applyTransform(PCollection<String> input) {
        return input.apply(
                FlatMapElements.into(TypeDescriptors.strings())
                        .via(sentence -> Arrays.asList(sentence.split(" ")))
        );
    }
```

### Description for example 

At the input, the elements of the `PCollection` are represented as strings. The `applyTransform()` function uses `FlatMapElements` and returns a list of words that make up the sentence.